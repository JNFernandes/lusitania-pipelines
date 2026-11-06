"""
The consumer's three jobs, end to end, without Kafka.

Kafka is replaced by a FOLDER of JSON files with the same columns a Kafka source gives
(key, value, topic, partition, offset, timestamp): Spark reads a folder as a stream too.
Everything after the source is the real code: parsing, dedup, checks, DLQ records, Delta
writes, windows, watermark, MERGE with tombstones.
"""
import json
from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from consumer import orders, settings, status, totals

KAFKA_SCHEMA = "key string, value string, topic string, partition int, offset long, timestamp timestamp"
T0 = datetime(2026, 11, 5, 10, 0, 0)


def order_json(number, minute=0, sku="SKU-00010", currency="EUR", price=10.0, version=None, drop=None,
               coupon=None):
    doc = {"orderNumber": number, "orderDate": (T0 + timedelta(minutes=minute)).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "status": "paid", "currency": currency, "createdAt": {"$date": "2026-11-05T10:00:00.000Z"},
           "customer": {"code": "C00001", "email": "a@b.pt", "country": "PT"},
           "shipping": {"city": "Porto", "country": "PT", "cost": 0.0}, "payment": {"method": "card"},
           "items": [{"line": 1, "sku": sku, "name": "Mouse", "qty": 2, "unitPrice": price}]}
    if version:
        doc["schemaVersion"] = version
    if coupon:
        doc["couponCode"] = coupon
    if drop:
        del doc[drop]
    return json.dumps(doc)


def kafka_rows(folder, name, events):
    """Write (key, value) events as the files of a fake Kafka topic, one offset each."""
    folder.mkdir(parents=True, exist_ok=True)
    with open(folder / f"{name}.json", "w") as f:
        for offset, (key, value) in enumerate(events):
            f.write(json.dumps({"key": key, "value": value, "topic": "web_orders", "partition": 0,
                                "offset": offset, "timestamp": (T0 + timedelta(seconds=offset)).isoformat()}) + "\n")


@pytest.fixture
def lakehouse(spark, tmp_path, monkeypatch):
    """Reference tables (silver products and rates) + all consumer paths inside tmp_path."""
    products, rates = tmp_path / "silver_products", tmp_path / "silver_rates"
    spark.createDataFrame([("SKU-00010",), ("SKU-00042",)], "sku string").write.format("delta").save(str(products))
    spark.createDataFrame([(T0.date(), "GBP", 0.85)], "rate_date date, currency string, rate double") \
         .selectExpr("rate_date", "currency", "cast(rate AS decimal(12,6)) AS rate") \
         .write.format("delta").save(str(rates))
    monkeypatch.setattr(settings, "SILVER_PRODUCTS", products)
    monkeypatch.setattr(settings, "SILVER_RATES", rates)
    monkeypatch.setattr(settings, "LIVE_TABLE", tmp_path / "online_orders_live")
    return tmp_path


class CapturingSinks(orders.Sinks):
    """The real Delta write for good lines; DLQ records captured instead of sent to Kafka."""
    def __init__(self):
        self.dlq = []

    def write_dlq(self, bad):
        self.dlq += orders.dlq_records(bad).collect()


def run_orders(spark, lakehouse, events, file_name="events"):
    incoming = lakehouse / "topic_web_orders"
    kafka_rows(incoming, file_name, events)
    sinks = CapturingSinks()
    source = spark.readStream.schema(KAFKA_SCHEMA).json(str(incoming))
    orders.start(spark, source=source, sinks=sinks, checkpoint=str(lakehouse / "ckpt_orders"),
                 trigger={"availableNow": True}).awaitTermination()
    return spark.read.format("delta").load(str(settings.LIVE_TABLE)), sinks


def test_orders_good_duplicate_and_broken_events(spark, lakehouse):
    live, sinks = run_orders(spark, lakehouse, [
        ("WEB1", order_json("WEB1")),
        ("WEB1", order_json("WEB1")),                                 # duplicate
        ("WEB2", order_json("WEB2", sku="sku-00042", currency="GBP", price=17.0)),
        ("WEB3", '{"orderNumber": "WEB3", "items": [ BROKEN'),        # unparseable
        ("WEB4", '{"orderNumber": "WEB4", "items": [ ALSO BROKEN'),   # NOT a "duplicate" of WEB3
        ("WEB5", order_json("WEB5", drop="orderDate")),
        ("WEB6", order_json("WEB6", sku="SKU-99999")),
        ("WEB7", order_json("WEB7", version=99)),
        ("WEB8", order_json("WEB8", version=2)),                      # version 2 is known: fine
    ])
    rows = {r.order_number: r for r in live.collect()}
    assert sorted(rows) == ["WEB1", "WEB2", "WEB8"]                   # one line each, duplicate dropped
    assert rows["WEB2"].sku == "SKU-00042"                            # Project 2's cleaning
    assert str(rows["WEB2"].unit_price_eur) == "20.00"                # £17 / 0.85 (stream–static join)
    assert rows["WEB1"].source_system == "kafka"
    assert rows["WEB1"].bronze_batch_id == "kafka:0:0"                # traceable to its Kafka position

    reasons = {r.key: dict((h.key, bytes(h.value).decode()) for h in r.headers)["error-reason"]
               for r in sinks.dlq}
    assert reasons == {"WEB3": "unparseable JSON", "WEB4": "unparseable JSON",
                       "WEB5": "missing or unreadable orderDate", "WEB6": "unknown SKU SKU-99999",
                       "WEB7": "unknown schema version 99"}
    web6 = next(r for r in sinks.dlq if r.key == "WEB6")
    assert json.loads(web6.value)["orderNumber"] == "WEB6"           # the ORIGINAL event, unchanged
    assert {h.key for h in web6.headers} == {"error-reason", "original-topic", "original-partition",
                                             "original-offset"}


def test_rate_not_loaded_yet_uses_the_latest_one_and_too_old_goes_to_the_dlq(spark, lakehouse):
    # silver has the GBP rate of T0's day only (as if DAG 4 had not run since then)
    live, sinks = run_orders(spark, lakehouse, [
        ("WEB1", order_json("WEB1", currency="GBP", price=17.0)),                 # same day: its own rate
        ("WEB2", order_json("WEB2", minute=24 * 60, currency="GBP", price=17.0)),   # next day: not loaded yet
        ("WEB3", order_json("WEB3", minute=10 * 24 * 60, currency="GBP", price=17.0)),  # 10 days later: too old
    ])
    rows = {r.order_number: r for r in live.collect()}
    assert sorted(rows) == ["WEB1", "WEB2"]
    assert str(rows["WEB2"].unit_price_eur) == "20.00"                # priced with the day before's rate
    reasons = {r.key: dict((h.key, bytes(h.value).decode()) for h in r.headers)["error-reason"]
               for r in sinks.dlq}
    assert reasons == {"WEB3": "no exchange rate for the order date"}


def test_schema_v2_adds_coupon_code_to_an_existing_live_table(spark, lakehouse):
    # the live table as it was before schema v2: no coupon_code column
    spark.createDataFrame([("WEB0", 1)], "order_number string, line_number int") \
         .write.format("delta").save(str(settings.LIVE_TABLE))
    live, sinks = run_orders(spark, lakehouse, [
        ("WEB1", order_json("WEB1")),                                          # v1, still arriving
        ("WEB2", order_json("WEB2", version=2, coupon=" blackfriday ")),      # v2, with a coupon
        ("WEB3", order_json("WEB3", version=2)),                               # v2, coupon is optional
    ])
    coupons = {r.order_number: r.coupon_code for r in live.collect()}
    assert coupons == {"WEB0": None, "WEB1": None, "WEB2": "BLACKFRIDAY", "WEB3": None}
    assert sinks.dlq == []                                                    # no version refused


def test_restart_continues_from_the_checkpoint(spark, lakehouse):
    run_orders(spark, lakehouse, [("WEB1", order_json("WEB1"))], file_name="first")
    live, _ = run_orders(spark, lakehouse, [("WEB1", order_json("WEB1")), ("WEB2", order_json("WEB2"))],
                         file_name="second")
    # a restarted query on the same folder: the checkpoint knows "first" was done, so only
    # "second" is read, and WEB1 in it is still a duplicate (the dedup state survived the restart)
    assert sorted(r.order_number for r in live.collect()) == ["WEB1", "WEB2"]


def test_a_replayed_micro_batch_is_written_once(spark, lakehouse):
    lines = spark.createDataFrame([("WEB1", 1)], "order_number string, line_number int")
    sinks = orders.Sinks()
    sinks.write_lines(lines, batch_id=7)
    sinks.write_lines(lines, batch_id=7)                              # Spark replays batch 7 after a crash
    assert spark.read.format("delta").load(str(settings.LIVE_TABLE)).count() == 1


def test_totals_windows_wait_for_the_watermark_and_drop_late_orders(spark, tmp_path):
    live, out, ckpt = str(tmp_path / "live"), str(tmp_path / "totals"), str(tmp_path / "ckpt_totals")

    def add_lines(*minutes_and_amounts):
        rows = [(f"WEB{m}{a}", 1, T0 + timedelta(minutes=m), 1, float(a)) for m, a in minutes_and_amounts]
        spark.createDataFrame(rows, "order_number string, line_number int, order_ts timestamp, quantity int, "
                                    "net_amount_eur double") \
             .withColumn("net_amount_eur", F.col("net_amount_eur").cast("decimal(12,2)")) \
             .write.format("delta").mode("append").save(live)

    def run():
        totals.start(spark, live_path=live, out_path=out, checkpoint=ckpt,
                     trigger={"availableNow": True}).awaitTermination()
        try:
            return {r.window_start.strftime("%H:%M"): (r.orders, float(r.revenue_eur))
                    for r in spark.read.format("delta").load(out).collect()}
        except Exception:                                             # nothing written yet
            return {}

    add_lines((0, 10), (3, 20))                                       # 10:00 and 10:03
    assert run() == {}                                                # 10:00–10:05 not final yet
    add_lines((21, 5))                                                # 10:21 → watermark 10:11
    assert run() == {"10:00": (2, 30.0)}                              # now 10:00–10:05 is final
    add_lines((2, 999))                                               # a 10:02 order arriving now
    assert run() == {"10:00": (2, 30.0)}                              # LATE: left out of the total


def test_status_keeps_the_latest_and_tombstones_delete(spark, tmp_path):
    table = str(tmp_path / "status")

    def batch(*events):   # (offset, key, status or None for a tombstone)
        rows = [(key, None if s is None else json.dumps({"orderNumber": key, "status": s,
                 "updatedAt": "2026-11-05T10:00:00+00:00"}), 0, offset) for offset, key, s in events]
        return spark.createDataFrame(rows, "key string, value string, partition int, offset long")

    def current():
        return {r.order_number: r.status for r in spark.read.format("delta").load(table).collect()}

    status.process_batch(batch((0, "WEB1", "paid"), (1, "WEB2", "paid"), (2, "WEB1", "shipped")), 0, table)
    assert current() == {"WEB1": "shipped", "WEB2": "paid"}          # latest per order within the batch
    status.process_batch(batch((3, "WEB1", "delivered"), (4, "WEB2", None)), 1, table)
    assert current() == {"WEB1": "delivered"}                        # WEB2's tombstone deleted it
    status.process_batch(batch((2, "WEB1", "shipped")), 0, table)    # an OLD batch replayed
    assert current() == {"WEB1": "delivered"}                        # older offset: ignored (idempotent)
