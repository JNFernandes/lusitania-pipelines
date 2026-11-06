"""
JOB 1 · Kafka web_orders → Delta online_orders_live   (+ broken events → Kafka web_orders_dlq)

    Kafka ─► prepare() ─► dropDuplicatesWithinWatermark ─► foreachBatch(process_batch)
                │                 (streaming, stateful)            │
                parse the JSON                                     ├─ check each order: error reason or None
                with Project 2's                                   ├─ good → Project 2's explode_lines + to_eur
                online_sales.parse()                               │         → append to online_orders_live
                                                                   └─ bad  → web_orders_dlq, reason in headers

foreachBatch hands each micro-batch to a normal Python function as a normal (static)
DataFrame. Inside it, anything batch Spark can do is allowed: joins with the silver tables,
several outputs from one input, row_number... That is why the DLQ lives here.
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from consumer import settings
from lusitania.silver import online_sales

log = logging.getLogger(__name__)


# --- 1. Source ----------------------------------------------------------------------------
def read_kafka(spark: SparkSession, topic: str) -> DataFrame:
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")            # first start ever: read everything kept
            .option("maxOffsetsPerTrigger", settings.MAX_OFFSETS_PER_TRIGGER)   # backpressure
            .option("includeHeaders", "true")
            .load())                                          # after that, the checkpoint decides


def prepare(kafka_rows: DataFrame) -> DataFrame:
    """Kafka's columns → a raw order with its Kafka position, parsed with Project 2's parse(),
    and duplicates dropped. Works on a stream (no aggregation, only bounded state)."""
    raw = kafka_rows.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),                 # when Kafka received it: never NULL
        F.concat_ws(":", F.lit("kafka"), "partition", "offset").alias("_batch_id"),
    )
    orders = online_sales.parse(raw)
    # Duplicate = the same order number again. Broken events have no order number, so they get
    # a key of their own: otherwise every unparseable event would look like "the same" order.
    dedup_key = F.coalesce(F.col("orderNumber"),
                           F.concat_ws(":", F.lit("unparsed"), "kafka_partition", "kafka_offset"))
    return (orders.withColumn("dedup_key", dedup_key)
                  # watermark on kafka_ts, not orderDate: broken events may have no orderDate
                  .withWatermark("kafka_ts", settings.DEDUP_WATERMARK)
                  .dropDuplicatesWithinWatermark(["dedup_key"]))


# --- 2. The checks ------------------------------------------------------------------------
def add_error(orders: DataFrame, known_skus: DataFrame) -> DataFrame:
    """Adds `error`: why an order cannot be processed, or NULL when it is fine. (Static batch.)"""
    item_skus = (orders.select("dedup_key", F.explode_outer("items").alias("item"))
                       .select("dedup_key", F.upper(F.trim("item.sku")).alias("sku"))
                       .filter("sku IS NOT NULL"))
    unknown = (item_skus.join(known_skus, "sku", "left_anti")
                        .groupBy("dedup_key").agg(F.first("sku").alias("_unknown_sku")))
    version = F.coalesce(F.col("schemaVersion"), F.lit(1))
    error = (F.when(F.col("orderNumber").isNull(), F.lit("unparseable JSON"))
              .when(~version.isin(*settings.KNOWN_SCHEMA_VERSIONS),
                    F.concat(F.lit("unknown schema version "), version.cast("string")))
              .when(F.col("order_ts").isNull(), F.lit("missing or unreadable orderDate"))
              .when(F.col("items").isNull() | (F.size("items") == 0), F.lit("no items"))
              .when(F.col("_unknown_sku").isNotNull(), F.concat(F.lit("unknown SKU "), F.col("_unknown_sku"))))
    return orders.join(unknown, "dedup_key", "left").withColumn("error", error).drop("_unknown_sku")


def dlq_records(bad: DataFrame) -> DataFrame:
    """Kafka records for the DLQ: the ORIGINAL event unchanged, the reason and origin in HEADERS."""
    def header(name, column):
        return F.struct(F.lit(name).alias("key"), F.col(column).cast("string").cast("binary").alias("value"))
    return bad.select(
        F.col("kafka_key").alias("key"),
        F.col("raw_json").alias("value"),
        F.array(header("error-reason", "error"),
                header("original-topic", "kafka_topic"),
                header("original-partition", "kafka_partition"),
                header("original-offset", "kafka_offset")).alias("headers"),
    )


def rates_as_of(rates: DataFrame, orders: DataFrame) -> DataFrame:
    """The exchange rate for each (order date, currency) in the micro-batch, in silver's shape.

    The silver table has a rate for every day up to the last time the BATCH pipeline ran (DAG 4).
    An order from today, before today's run, would find no rate. So each order takes the rate of
    its own date or, if that day is not loaded yet, the most recent one before it: the same rate
    the batch pipeline itself gives a day with no new publication (it fills gaps forward). Only a
    rate older than MAX_RATE_AGE_DAYS is refused (the order goes to the DLQ: "no exchange rate")."""
    needed = orders.select(F.to_date("order_ts").alias("order_date"),
                           F.upper(F.trim("currency")).alias("currency")).distinct()
    known = rates.select(F.col("rate_date").alias("_rate_date"), F.col("currency").alias("_currency"), "rate")
    usable = ((F.col("_currency") == F.col("currency"))
              & (F.col("_rate_date") <= F.col("order_date"))
              & (F.col("_rate_date") >= F.date_sub("order_date", settings.MAX_RATE_AGE_DAYS)))
    return (needed.join(known, usable)
                  .groupBy("order_date", "currency")
                  .agg(F.max_by("rate", "_rate_date").alias("rate"))       # the most recent usable rate
                  .select(F.col("order_date").alias("rate_date"), "currency", "rate"))


def with_coupon(lines: DataFrame, orders: DataFrame) -> DataFrame:
    """SCHEMA v2: the optional couponCode → a coupon_code column (NULL for v1 orders).
    Project 2's silver lines do not carry it, so it is joined back from the parsed orders."""
    coupons = orders.select(F.upper(F.trim("orderNumber")).alias("order_number"),
                            F.upper(F.trim("couponCode")).alias("coupon_code"))
    return lines.join(coupons, "order_number", "left")


# --- 3. Where results go ---------------------------------------------------------------------
class Sinks:
    """The two outputs. A test replaces this class to capture what would be written."""

    def write_lines(self, lines: DataFrame, batch_id: int) -> None:
        (lines.write.format("delta").mode("append")
              # SCHEMA EVOLUTION: a column the table does not have yet (coupon_code) is ADDED to
              # the table instead of failing the write. Old rows read it as NULL.
              .option("mergeSchema", "true")
              # EXACTLY-ONCE: Delta remembers (txnAppId, txnVersion). If Spark replays micro-batch N
              # after a crash, Delta sees N was already committed and skips it: no duplicate rows.
              .option("txnAppId", "consumer-orders").option("txnVersion", batch_id)
              .save(str(settings.LIVE_TABLE)))

    def write_dlq(self, bad: DataFrame) -> None:
        # AT-LEAST-ONCE: a replayed micro-batch can write the same DLQ event twice. Whoever reads
        # the DLQ removes repeats by original-partition + original-offset.
        (dlq_records(bad).write.format("kafka")
            .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP)
            .option("topic", settings.DLQ_TOPIC).save())


# --- 4. One micro-batch -------------------------------------------------------------------
def process_batch(batch: DataFrame, batch_id: int, sinks: Sinks,
                  products_path: str, rates_path: str) -> dict:
    spark = batch.sparkSession
    # Compute the micro-batch ONCE. This function runs several actions on it (counts, a join,
    # two writes); without this, each action would recompute the streaming plan, including the
    # dedup state, about 10 times per batch: same result, 10x the work, and inflated metrics.
    # localCheckpoint(eager=True) stores the rows and cuts the link to the streaming plan.
    batch = batch.localCheckpoint(eager=True)
    try:
        # STREAM–STATIC JOIN: the silver tables are read again in every micro-batch, so new
        # products or today's exchange rate are picked up without restarting the consumer
        known_skus = spark.read.format("delta").load(products_path).select("sku")
        all_rates = spark.read.format("delta").load(rates_path)

        checked = add_error(batch, known_skus).persist()
        good_orders = checked.filter("error IS NULL")
        rates = rates_as_of(all_rates, good_orders)
        lines = with_coupon(online_sales.to_eur(online_sales.explode_lines(good_orders), rates,
                                                source_system="kafka"), good_orders)

        # an order whose currency has no rate for its date cannot be priced in EUR → DLQ too
        no_rate = lines.filter("fx_rate IS NULL").select("order_number").distinct()
        lines = lines.join(no_rate, "order_number", "left_anti")
        unpriced = (good_orders.withColumn("order_number", F.upper(F.trim("orderNumber")))   # same key as lines
                               .join(no_rate, "order_number").drop("order_number")
                               .withColumn("error", F.lit("no exchange rate for the order date")))
        bad = checked.filter("error IS NOT NULL").unionByName(unpriced)

        counts = {"events": batch.count(), "lines": lines.count(), "dlq": bad.count()}
        if counts["lines"]:
            sinks.write_lines(lines, batch_id)
        if counts["dlq"]:
            sinks.write_dlq(bad)
        checked.unpersist()
        if counts["events"]:
            log.info("orders batch %s: %s events → %s lines, %s to the DLQ",
                     batch_id, counts["events"], counts["lines"], counts["dlq"])
        return counts
    finally:
        batch.unpersist()              # free the stored rows of this micro-batch


def start(spark: SparkSession, source: DataFrame | None = None, sinks: Sinks | None = None,
          checkpoint: str | None = None, trigger: dict | None = None) -> StreamingQuery:
    """Starts job 1. The arguments exist so a test can use another source, sinks or trigger."""
    source = source if source is not None else read_kafka(spark, settings.ORDERS_TOPIC)
    sinks = sinks or Sinks()
    return (prepare(source).writeStream
            .queryName("orders")
            .foreachBatch(lambda batch, batch_id: process_batch(
                batch, batch_id, sinks, str(settings.SILVER_PRODUCTS), str(settings.SILVER_RATES)))
            .option("checkpointLocation", checkpoint or settings.checkpoint("orders"))
            .trigger(**(trigger or {"processingTime": settings.TRIGGER}))
            .start())
