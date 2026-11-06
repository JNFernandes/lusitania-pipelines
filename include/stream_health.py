"""
Health checks of the STREAMING consumer, run by Airflow (DAG 5) every 15 minutes.

Airflow does not run the consumer: it INSPECTS it, from two sources that never lie:

    processed   the consumer's CHECKPOINT: offsets/N of the latest committed micro-batch says how
                far the job got in each partition, e.g. {"web_orders": {"0": 140, "1": 159, "2": 146}}
    available   KAFKA: the latest offset ("high watermark") of each partition

    lag = available − processed      (per partition, then summed)

Checks (exit code 1 = the Airflow task fails and alerts):

    python stream_health.py lag      too many events waiting           → the consumer cannot keep up
    python stream_health.py alive    events waiting, nothing written   → the consumer is stuck or down
    python stream_health.py dlq      many broken events recently       → a producer is sending bad data
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
STREAM_DIR = Path(os.getenv("STREAM_DIR", "/opt/lakehouse/data/stream"))

JOBS = {"orders": "web_orders", "status": "order_status"}   # consumer job → the topic it reads
LIVE_TABLE = "online_orders_live"
DLQ_TOPIC = "web_orders_dlq"

MAX_LAG = 1_000               # events waiting before we call it "falling behind"
STALE_MINUTES = 5             # events waiting AND no write for this long = stuck
DLQ_WINDOW_MINUTES = 15       # look at the broken events of the last 15 minutes...
DLQ_MAX_COUNT = 10            # ...alert only if there are more than 10 of them
DLQ_MAX_SHARE = 0.05          # ...AND they are more than 5% of the orders in that time


class CheckFailed(Exception):
    pass


# --- the consumer's side: its checkpoint and its Delta table ---------------------------------
def processed_offsets(job: str, stream_dir: Path = STREAM_DIR) -> dict[int, int]:
    """Offsets of the latest COMMITTED micro-batch: what the job has fully processed."""
    checkpoint = stream_dir / "_checkpoints" / job
    commits = [int(p.name) for p in (checkpoint / "commits").glob("[0-9]*")]
    if not commits:
        raise CheckFailed(f"consumer job '{job}' has no committed micro-batch yet (never ran?)")
    lines = (checkpoint / "offsets" / str(max(commits))).read_text().splitlines()
    offsets = json.loads(lines[2])                   # line 1 "v1", line 2 metadata, line 3 the source
    return {int(p): o for p, o in next(iter(offsets.values())).items()}


def last_write_age_minutes(table: str = LIVE_TABLE, stream_dir: Path = STREAM_DIR, now: float | None = None) -> float:
    """Minutes since the consumer last committed to a Delta table (from its newest _delta_log entry)."""
    log = stream_dir / table / "_delta_log"
    versions = sorted(log.glob("[0-9]*.json"))
    if not versions:
        raise CheckFailed(f"table {table} has no commits yet")
    for line in versions[-1].read_text().splitlines():
        entry = json.loads(line)
        if "commitInfo" in entry:
            return ((now or time.time()) * 1000 - entry["commitInfo"]["timestamp"]) / 60_000
    raise CheckFailed(f"no commitInfo in {versions[-1].name}")


# --- Kafka's side -----------------------------------------------------------------------------
def _consumer():
    from confluent_kafka import Consumer
    # group.id is required by the client, but this consumer only ASKS for offsets: it reads and
    # commits nothing, so it never changes anyone's position
    return Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "airflow-stream-health",
                     "enable.auto.commit": False,
                     "log_level": 2})          # only critical client logs: no retry spam when Kafka is down


def end_offsets(topic: str) -> dict[int, int]:
    from confluent_kafka import TopicPartition
    c = _consumer()
    try:
        partitions = c.list_topics(topic, timeout=10).topics[topic].partitions
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in partitions}
    finally:
        c.close()


def events_since(topic: str, minutes: float) -> int:
    """How many events arrived in a topic in the last N minutes (offset now − offset at that time)."""
    from confluent_kafka import TopicPartition
    c = _consumer()
    try:
        since_ms = int((time.time() - minutes * 60) * 1000)
        partitions = c.list_topics(topic, timeout=10).topics[topic].partitions
        at_time = c.offsets_for_times([TopicPartition(topic, p, since_ms) for p in partitions], timeout=10)
        total = 0
        for tp in at_time:
            high = c.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=10)[1]
            total += high - tp.offset if tp.offset >= 0 else 0      # -1 = nothing since then
        return total
    finally:
        c.close()


# --- the checks -------------------------------------------------------------------------------
def lag_per_job(get_end_offsets=end_offsets, get_processed=processed_offsets) -> dict[str, dict[int, int]]:
    result = {}
    for job, topic in JOBS.items():
        done = get_processed(job)          # first the checkpoint: "never ran" is the clearer message
        end = get_end_offsets(topic)
        result[job] = {p: end[p] - done.get(p, 0) for p in sorted(end)}
    return result


def check_lag(lags: dict[str, dict[int, int]]) -> str:
    lines = [f"{job:<7} lag per partition {parts}  total {sum(parts.values()):,}" for job, parts in lags.items()]
    report = "\n".join(lines)
    behind = {job: sum(p.values()) for job, p in lags.items() if sum(p.values()) > MAX_LAG}
    if behind:
        raise CheckFailed(f"consumer falling behind (more than {MAX_LAG:,} events waiting): {behind}\n{report}")
    return report


def check_alive(total_lag: int, minutes_since_write: float) -> str:
    report = f"events waiting: {total_lag:,} · last write to {LIVE_TABLE}: {minutes_since_write:.1f} min ago"
    if total_lag > 0 and minutes_since_write > STALE_MINUTES:
        raise CheckFailed(f"consumer looks STUCK or DOWN: events are waiting but nothing was written "
                          f"for over {STALE_MINUTES} minutes. {report}")
    if total_lag == 0 and minutes_since_write > STALE_MINUTES:
        report += " (no new orders: the shop is quiet, not a consumer problem)"
    return report


def check_dlq(broken: int, orders: int) -> str:
    share = broken / orders if orders else (1.0 if broken else 0.0)
    report = f"last {DLQ_WINDOW_MINUTES} min: {broken} broken events, {orders} orders ({share:.1%})"
    if broken > DLQ_MAX_COUNT and share > DLQ_MAX_SHARE:
        raise CheckFailed(f"too many broken events: a producer may be sending bad data. {report}")
    return report


def main(check: str) -> None:
    if check == "lag":
        print(check_lag(lag_per_job()))
    elif check == "alive":
        print(check_alive(sum(sum(p.values()) for p in lag_per_job().values()), last_write_age_minutes()))
    elif check == "dlq":
        print(check_dlq(events_since(DLQ_TOPIC, DLQ_WINDOW_MINUTES), events_since("web_orders", DLQ_WINDOW_MINUTES)))
    else:
        raise SystemExit("usage: python stream_health.py lag | alive | dlq")
    print("OK")


if __name__ == "__main__":
    from confluent_kafka import KafkaException
    try:
        main(sys.argv[1] if len(sys.argv) > 1 else "")
    except KafkaException as problem:
        print(f"CHECK FAILED: Kafka is not reachable at {KAFKA_BOOTSTRAP}: {problem}")
        sys.exit(1)
    except CheckFailed as problem:
        print(f"CHECK FAILED: {problem}")
        sys.exit(1)
