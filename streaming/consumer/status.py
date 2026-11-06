"""
JOB 3 · Kafka order_status → Delta order_status_current   (one row per order: its LATEST status)

The Delta table does what compaction does in the topic: keep the latest value per key.
Each micro-batch:

  1. keeps only the LAST event per order within the batch (an order can change twice in 10 s)
  2. MERGEs it into the table:
        tombstone (value = NULL)                   → DELETE the order's row
        newer status (a higher Kafka offset)       → UPDATE
        unknown order                              → INSERT

"Newer" is decided by the Kafka OFFSET, not by the clock: all events of one order are in one
partition (same key), where offsets only grow. Comparing offsets also makes a replayed batch
harmless: its events are not newer than what the table already holds (idempotent).
"""
from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQuery

from consumer import settings
from consumer.orders import read_kafka

log = logging.getLogger(__name__)

STATUS_SCHEMA = T.StructType([
    T.StructField("orderNumber", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("updatedAt", T.StringType()),
])


def latest_per_order(batch: DataFrame) -> DataFrame:
    """The last event of each order in this micro-batch (static: row_number is allowed here)."""
    events = batch.select(
        F.col("key").cast("string").alias("order_number"),
        F.col("value").cast("string").alias("raw"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    newest_first = Window.partitionBy("order_number").orderBy(F.col("kafka_offset").desc())
    latest = events.withColumn("_rn", F.row_number().over(newest_first)).filter("_rn = 1").drop("_rn")
    parsed = F.from_json("raw", STATUS_SCHEMA)
    return latest.select(
        "order_number",
        F.col("raw").isNull().alias("is_tombstone"),
        F.lower(parsed["status"]).alias("status"),
        F.to_timestamp(parsed["updatedAt"]).alias("updated_at"),
        "kafka_partition", "kafka_offset",
        F.current_timestamp().alias("consumer_updated_at"),
    )


def upsert(latest: DataFrame, table_path: str) -> None:
    spark = latest.sparkSession
    columns = [c for c in latest.columns if c != "is_tombstone"]
    if not DeltaTable.isDeltaTable(spark, table_path):
        latest.filter("NOT is_tombstone").select(*columns).write.format("delta").save(table_path)
        return
    newer = "s.kafka_offset > t.kafka_offset"
    (DeltaTable.forPath(spark, table_path).alias("t")
        .merge(latest.alias("s"), "t.order_number = s.order_number")
        .whenMatchedDelete(condition=f"s.is_tombstone AND {newer}")
        .whenMatchedUpdate(condition=f"NOT s.is_tombstone AND {newer}",
                           set={c: f"s.{c}" for c in columns})
        .whenNotMatchedInsert(condition="NOT s.is_tombstone",
                              values={c: f"s.{c}" for c in columns})
        .execute())


def process_batch(batch: DataFrame, batch_id: int, table_path: str) -> None:
    # Read the micro-batch from Kafka ONCE (same reason as in orders.process_batch): without it,
    # the emptiness check and the MERGE each read the events from Kafka again.
    batch = batch.localCheckpoint(eager=True)
    try:
        if batch.isEmpty():
            return
        latest = latest_per_order(batch).persist()
        tombstones = latest.filter("is_tombstone").count()
        upsert(latest, table_path)
        log.info("status batch %s: %s orders updated, %s tombstone(s)",
                 batch_id, latest.count() - tombstones, tombstones)
        latest.unpersist()
    finally:
        batch.unpersist()


def start(spark: SparkSession, source: DataFrame | None = None, table_path: str | None = None,
          checkpoint: str | None = None, trigger: dict | None = None) -> StreamingQuery:
    source = source if source is not None else read_kafka(spark, settings.STATUS_TOPIC)
    path = table_path or str(settings.STATUS_TABLE)
    return (source.writeStream
            .queryName("status")
            .foreachBatch(lambda batch, batch_id: process_batch(batch, batch_id, path))
            .option("checkpointLocation", checkpoint or settings.checkpoint("status"))
            .trigger(**(trigger or {"processingTime": settings.TRIGGER}))
            .start())
