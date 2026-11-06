"""
JOB 2 · Delta online_orders_live → Delta sales_per_5_minutes

Reads job 1's TABLE as a stream (each new Delta version = new rows), groups the lines into
5-minute windows of ORDER time (event time), and writes a window only when it is FINAL:

    watermark = latest order_ts seen − 10 minutes
    a window is final (written, never changed again) once the watermark passes its end
    an order older than the watermark is LATE: left out of the totals, still in the live table

Output mode "append": rows are only ever added, so the plain Delta sink is enough.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from consumer import settings


def build(live_lines: DataFrame) -> DataFrame:
    totals = (live_lines
              .withWatermark("order_ts", settings.LATE_WATERMARK)
              .groupBy(F.window("order_ts", settings.WINDOW))
              .agg(
                  # an exact COUNT(DISTINCT) is not allowed on a stream (it would have to remember
                  # every order number forever); approx_count_distinct uses a small fixed sketch
                  F.approx_count_distinct("order_number").alias("orders"),
                  F.count("*").alias("order_lines"),
                  F.sum("quantity").alias("items"),
                  F.sum("net_amount_eur").cast("decimal(14,2)").alias("revenue_eur"),
              ))
    return totals.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "orders", "order_lines", "items", "revenue_eur",
        F.current_timestamp().alias("written_at"),
    )


def start(spark: SparkSession, live_path: str | None = None, out_path: str | None = None,
          checkpoint: str | None = None, trigger: dict | None = None) -> StreamingQuery:
    live = spark.readStream.format("delta").load(live_path or str(settings.LIVE_TABLE))
    return (build(live).writeStream
            .queryName("totals")
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint or settings.checkpoint("totals"))
            .trigger(**(trigger or {"processingTime": settings.TRIGGER}))
            .start(out_path or str(settings.TOTALS_TABLE)))
