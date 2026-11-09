"""
Starts the consumer's three jobs in ONE Spark session and reports on them every 30 seconds.

    docker compose up -d consumer        start (it runs until stopped)
    docker compose logs -f consumer      watch the progress lines

Each progress line shows, per job:
    in/s, done/s    events arriving vs events processed per second (done < in for long = falling behind)
    lag             events in Kafka not processed yet (Spark keeps its offsets in its CHECKPOINT,
                    not in Kafka, so Kafka's own consumer-group tools cannot show this number)
    state           rows the job remembers between micro-batches (dedup keys, open windows)
"""
from __future__ import annotations

import logging
import signal
import time

import pyspark
from delta.tables import DeltaTable

import cloudlake
from consumer import orders, settings, status, totals
from consumer.metrics import MetricsRecorder
from lusitania.log import setup_logging
from lusitania.spark import get_spark

log = logging.getLogger("consumer")

# The Kafka connector for Spark: its version must match Spark's exactly
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"
REPORT_EVERY_S = 30


def progress_line(query) -> str:
    p = query.lastProgress
    if not p:
        return f"{query.name:<7} starting"
    source = p["sources"][0] if p["sources"] else {}
    lag = (source.get("metrics") or {}).get("maxOffsetsBehindLatest", "-")
    state = sum(op.get("numRowsTotal", 0) for op in p.get("stateOperators", []))
    return (f"{query.name:<7} batch {p['batchId']:>5}  in/s {p.get('inputRowsPerSecond', 0):>7.1f}  "
            f"done/s {p.get('processedRowsPerSecond', 0):>7.1f}  lag {lag:>6}  state {state:>6}")


def schema_changed(error) -> bool:
    text = str(error)
    return "DELTA_SCHEMA_CHANGED" in text or "schema change" in text.lower()


def main() -> None:
    setup_logging()
    packages = [KAFKA_PACKAGE] + ([cloudlake.hadoop_azure_package()] if settings.LAKE else [])
    spark = get_spark("consumer", cores="2", extra_packages=packages)
    if settings.LAKE:
        # log in to the cloud lake as the service principal (OAuth), before anything touches it
        hadoop = spark.sparkContext._jsc.hadoopConfiguration()
        for key, value in cloudlake.hadoop_conf(settings.LAKE, *cloudlake.credentials()).items():
            hadoop.set(key, value)
    # STATE STORE on RocksDB (disk) instead of Java memory: dedup keys and open windows can grow
    spark.conf.set("spark.sql.streaming.stateStore.providerClass",
                   "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    spark.conf.set("spark.sql.shuffle.partitions", "4")   # fixed in the checkpoint at first start
    settings.LOCAL_STREAM_DIR.mkdir(parents=True, exist_ok=True)   # metrics stay local in both modes
    log.info("tables in %s", settings.STREAM_DIR)
    # every micro-batch of every job → one line in data/stream/_metrics (for perf_report.py)
    spark.streams.addListener(MetricsRecorder())
    log.info("settings: maxOffsetsPerTrigger=%s, trigger=%s",
             settings.MAX_OFFSETS_PER_TRIGGER, settings.TRIGGER)

    queries = [orders.start(spark), status.start(spark)]
    log.info("jobs 1 (orders) and 3 (status) started, reading %s", settings.KAFKA_BOOTSTRAP)

    stopping = {"now": False}
    for sig in (signal.SIGINT, signal.SIGTERM):            # docker compose stop sends SIGTERM
        signal.signal(sig, lambda *_: stopping.update(now=True))

    totals_started = False
    last_report = 0.0
    while not stopping["now"]:
        # job 2 reads job 1's table, so it can only start once that table exists
        if not totals_started and DeltaTable.isDeltaTable(spark, str(settings.LIVE_TABLE)):
            queries.append(totals.start(spark))
            totals_started = True
            log.info("job 2 (totals) started, reading %s", settings.LIVE_TABLE)
        for i, query in enumerate(queries):
            error = query.exception()
            if error and query.name == "totals" and schema_changed(error):
                # job 1 added a column to the live table (schema v2). A Delta STREAM stops when its
                # source table's schema changes; restarting it is the documented fix: it continues
                # from its checkpoint, now with the new schema.
                log.warning("totals: the live table's schema changed, restarting job 2")
                queries[i] = totals.start(spark)
            elif error:                                    # any other failure: stop everything, loudly
                raise RuntimeError(f"job '{query.name}' failed: {error}")
        if time.time() - last_report >= REPORT_EVERY_S:
            for query in queries:
                log.info(progress_line(query))
            last_report = time.time()
        time.sleep(2)

    log.info("stopping: each job finishes its current micro-batch, then saves its checkpoint")
    for query in queries:
        query.stop()
    spark.stop()


if __name__ == "__main__":
    main()
