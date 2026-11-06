"""
The WEB SHOP, simulated: sends live web orders to Kafka, one event per order.

    web_orders     one event per order, in the SAME JSON format as the MongoDB documents
                   (so the streaming job reuses Project 2's silver cleaning). Key = orderNumber.
    order_status   the order's status changes: paid → shipped → delivered. Compacted topic:
                   Kafka keeps only the latest status per order. A tombstone deletes one.

On purpose, like the MongoDB generator, it sends problems the consumer must handle:
    --late-pct   orders whose orderDate is minutes in the past (some beyond the 10-minute watermark)
    --dup-pct    the same order sent again a moment later, with a newer status
    --bad-pct    broken events for the dead letter queue (bad JSON, no date, unknown SKU, unknown version)

Run it from the Project 2 venv (it reads customers and SKUs from SQL Server with lusitania.config):

    python streaming\\producer.py                       2 orders per second, until Ctrl+C
    python streaming\\producer.py --rate 10 --duration 60
    python streaming\\producer.py --burst 5000           as fast as possible, then stop (lag test)
    python streaming\\producer.py --schema-version 2     adds the optional field couponCode
    python streaming\\producer.py --tombstone WEB2000123 delete one order from order_status
    python streaming\\producer.py --dry-run --duration 5 print the events, no Kafka needed
    python streaming\\producer.py --burst 20000 --label spike-500   named run, for perf_report.py
"""
from __future__ import annotations

import argparse
import json
import random
import signal
import time
from datetime import UTC, datetime, timedelta

BOOTSTRAP = "localhost:9092"            # Kafka's EXTERNAL door, for programs on the laptop
ORDERS_TOPIC = "web_orders"
STATUS_TOPIC = "order_status"
SOURCE = "lusitania-webshop"            # header: which system produced the event

COUNTRY_CURRENCY = {"PT": "EUR", "ES": "EUR", "FR": "EUR", "DE": "EUR", "GB": "GBP", "US": "USD"}
APPROX_RATE = {"EUR": 1.0, "GBP": 0.85, "USD": 1.08}
FOREIGN_CITIES = {"ES": ["Madrid", "Vigo"], "FR": ["Paris", "Lyon"], "DE": ["Berlin"],
                  "GB": ["London", "Manchester"], "US": ["New York", "Boston"]}
NEXT_STATUS = {"paid": "shipped", "shipped": "delivered"}
STATUS_EVERY_S = 5                      # a status changes at most every 5 seconds per order


def status_event(order_number: str, status: str, now: datetime) -> str:
    return json.dumps({"orderNumber": order_number, "status": status, "updatedAt": now.isoformat()})


def headers(event_type: str, schema_version: int | None = None) -> list[tuple[str, str]]:
    """HEADERS = metadata ABOUT the event, outside its data (the value): like the outside of an
    envelope. Consumers can route or check an event without parsing its JSON."""
    h = [("event-type", event_type), ("source", SOURCE)]
    if schema_version is not None:
        h.append(("schema-version", str(schema_version)))
    return h


# ---------------------------------------------------------------------------------------
# Reference data: real customers and SKUs, so events join to the rest of the lakehouse
# ---------------------------------------------------------------------------------------
def load_reference_data():
    import pyodbc

    from lusitania import config
    with pyodbc.connect(config.sql_connection_string()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT CustomerCode, Email, City FROM sales.Customer")
        customers = [tuple(r) for r in cur.fetchall()]
        cur.execute("SELECT SKU, ProductName, ListPrice FROM product.Product WHERE IsActive = 1")
        products = [(r.SKU, r.ProductName, float(r.ListPrice)) for r in cur.fetchall()]
    return customers, products


def sample_reference_data():
    """For --dry-run without SQL Server: a few fake customers and products."""
    customers = [(f"C{n:05d}", f"c{n}@mail.pt", "Lisboa") for n in range(1, 21)]
    products = [(f"SKU-{n:05d}", f"Product {n}", 10.0 * n) for n in range(1, 21)]
    return customers, products


# ---------------------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------------------
def bson_date(dt: datetime) -> dict:
    return {"$date": dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"}


def format_order_date(rng: random.Random, dt: datetime):
    """The three formats of the MongoDB source, in the same proportions."""
    roll = rng.random()
    if roll < 0.80:
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if roll < 0.95:
        return dt.strftime("%d/%m/%Y %H:%M")
    return bson_date(dt)


class OrderFactory:
    def __init__(self, rng, customers, products, schema_version: int, late_pct: float):
        self.rng, self.customers, self.products = rng, customers, products
        self.schema_version, self.late_pct = schema_version, late_pct
        # live order numbers: WEB + the run's start second + a 6-digit counter, e.g. WEB1793983675000001.
        # Unique across runs (two runs never start in the same second) and never clashing with the
        # MongoDB history (WEB0000001 to WEB0008000). The old "clock × 5" numbers clashed when one
        # run sent more than 5 orders per second: a burst reused the numbers of an earlier run.
        self.run_id = int(time.time())
        self.next_number = 0

    def order_number(self) -> str:
        return f"WEB{self.run_id}{self.next_number:06d}"

    def new_order(self, now: datetime) -> dict:
        rng = self.rng
        self.next_number += 1
        order_dt = now
        self.last_was_late = rng.random() < self.late_pct
        if self.last_was_late:                           # LATE: placed minutes ago, arriving now
            order_dt = now - timedelta(minutes=rng.choice([2, 5, 8, 15, 25]))  # 15 and 25: beyond the watermark

        country = rng.choices(list(COUNTRY_CURRENCY), weights=[80, 6, 4, 3, 5, 2])[0]
        currency = COUNTRY_CURRENCY[country]
        rate = APPROX_RATE[currency]
        items = [{"line": i, "sku": sku.lower() if rng.random() < 0.05 else sku, "name": name,
                  "qty": rng.choices([1, 2, 3], weights=[75, 20, 5])[0], "unitPrice": round(price * rate, 2)}
                 for i, (sku, name, price) in enumerate(rng.sample(self.products, rng.randint(1, 3)), start=1)]

        doc = {
            "orderNumber": self.order_number(),
            "orderDate": format_order_date(rng, order_dt),
            "status": "paid",
            "currency": currency,
            "shipping": {"city": rng.choice(FOREIGN_CITIES.get(country, ["Lisboa"])), "country": country,
                         "cost": 0.0 if rng.random() < 0.4 else round(4.99 * rate, 2)},
            "items": items,
            "createdAt": bson_date(now),
        }
        if rng.random() < 0.08:
            doc["guestEmail"] = f"guest{self.next_number}@example.net"
        else:
            code, email, city = rng.choice(self.customers)
            doc["customer"] = {"code": code, "email": email, "country": country}
            if country == "PT":
                doc["shipping"]["city"] = city
        if rng.random() > 0.03:
            doc["payment"] = {"method": rng.choice(["card", "card", "mbway", "paypal"])}
        if self.schema_version >= 2:                     # SCHEMA EVOLUTION: one new, OPTIONAL field
            doc["schemaVersion"] = 2
            doc["couponCode"] = rng.choice([None, None, "BLACKFRIDAY", "WELCOME10"])
        return doc

    def broken_event(self, now: datetime) -> tuple[str, str]:
        """(key, value) that the consumer must send to the dead letter queue, never crash on."""
        self.next_number += 1
        number = self.order_number()
        kind = self.rng.choice(["bad_json", "no_date", "unknown_sku", "unknown_version"])
        if kind == "bad_json":
            return number, '{"orderNumber": "' + number + '", "items": [ BROKEN'
        doc = self.new_order(now)
        doc["orderNumber"] = number
        if kind == "no_date":
            del doc["orderDate"]
        elif kind == "unknown_sku":
            doc["items"][0]["sku"] = "SKU-99999"
        else:
            doc["schemaVersion"] = 99
        return number, json.dumps(doc)


# ---------------------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------------------
class Sender:
    """Wraps the Kafka producer (or prints, with --dry-run) and counts what happened."""

    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.stats = {"orders": 0, "late": 0, "duplicates": 0, "broken": 0, "status": 0, "failed": 0}
        if not dry_run:
            from confluent_kafka import Producer
            self.producer = Producer({
                "bootstrap.servers": BOOTSTRAP,
                # IDEMPOTENT producer: if a send is retried after a lost acknowledgement, the broker
                # drops the repeat (producer id + sequence number). The Python client does NOT turn
                # this on by default, unlike the Java client.
                "enable.idempotence": True,
                "acks": "all",                   # confirmed only when all in-sync replicas have it
                "linger.ms": 20,                 # wait up to 20 ms to send events together (batching)
                "compression.type": "lz4",       # less data on the network
            })

    def _delivered(self, err, msg):
        if err is not None:
            self.stats["failed"] += 1
            print(f"  DELIVERY FAILED for key {msg.key()}: {err}")

    def send(self, topic: str, key: str, value: str | None, event_headers: list | None = None):
        if self.dry_run:
            shown = "null (TOMBSTONE)" if value is None else value[:160] + ("…" if len(value) > 160 else "")
            print(f"[{topic}] key={key} headers={dict(event_headers or [])} value={shown}")
            return
        while True:
            try:
                self.producer.produce(topic, key=key, value=value, headers=event_headers,
                                      on_delivery=self._delivered)
                break
            except BufferError:                  # local queue full (bursts): wait for Kafka to catch up
                self.producer.poll(0.5)
        self.producer.poll(0)                    # run delivery callbacks

    def close(self):
        if not self.dry_run:
            remaining = self.producer.flush(30)  # wait until every event is confirmed
            if remaining:
                print(f"WARNING: {remaining} event(s) not confirmed by Kafka")


# ---------------------------------------------------------------------------------------
def run(args):
    rng = random.Random(args.seed)
    customers, products = sample_reference_data() if args.dry_run else load_reference_data()
    factory = OrderFactory(rng, customers, products, args.schema_version, args.late_pct)
    sender = Sender(args.dry_run)

    if args.tombstone:
        sender.send(STATUS_TOPIC, args.tombstone, None, headers("order_deleted"))
        sender.close()
        print(f"Tombstone sent for {args.tombstone}: after compaction it disappears from {STATUS_TOPIC}")
        return

    stop = {"now": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(now=True))   # Ctrl+C = stop cleanly (flush first)

    open_orders: list[dict] = []           # orders that will get later status events
    pending_duplicates: list[dict] = []
    total = args.burst or None
    interval = 0 if args.burst else 1 / args.rate
    started = last_report = time.time()
    print(f"Sending to {'(dry run)' if args.dry_run else BOOTSTRAP}: "
          f"{'burst of ' + str(args.burst) if args.burst else str(args.rate) + ' orders/s'}, "
          f"late {args.late_pct:.0%}, duplicates {args.dup_pct:.0%}, broken {args.bad_pct:.0%}, "
          f"schema v{args.schema_version}. Ctrl+C to stop.")

    while not stop["now"]:
        now = datetime.now(UTC)
        if rng.random() < args.bad_pct:
            key, value = factory.broken_event(now)
            sender.send(ORDERS_TOPIC, key, value, headers("order_created", args.schema_version))
            sender.stats["broken"] += 1
        else:
            order = factory.new_order(now)
            sender.send(ORDERS_TOPIC, order["orderNumber"], json.dumps(order),  # order has no "_" keys yet
                        headers("order_created", args.schema_version))
            sender.send(STATUS_TOPIC, order["orderNumber"], status_event(order["orderNumber"], "paid", now),
                        headers("order_status_changed"))
            sender.stats["orders"] += 1
            sender.stats["status"] += 1
            sender.stats["late"] += factory.last_was_late
            order["_status_at"] = time.time()
            open_orders.append(order)
            if rng.random() < args.dup_pct:
                pending_duplicates.append(order)

        # a DUPLICATE: an order already sent, sent again with a newer status and createdAt
        if pending_duplicates and rng.random() < 0.5:
            dup = {k: v for k, v in pending_duplicates.pop(0).items() if not k.startswith("_")}
            dup["createdAt"] = bson_date(now + timedelta(seconds=1))   # same order, stored again later
            sender.send(ORDERS_TOPIC, dup["orderNumber"], json.dumps(dup),
                        headers("order_created", args.schema_version))
            sender.stats["duplicates"] += 1

        # STATUS CHANGES for older orders: paid → shipped → delivered (the compacted topic)
        if open_orders and rng.random() < 0.3:
            order = open_orders[rng.randrange(len(open_orders))]
            nxt = NEXT_STATUS.get(order["status"])
            if not nxt:
                open_orders.remove(order)                    # delivered: no more changes
            elif time.time() - order["_status_at"] >= STATUS_EVERY_S:
                order["status"], order["_status_at"] = nxt, time.time()
                sender.send(STATUS_TOPIC, order["orderNumber"], status_event(order["orderNumber"], nxt, now),
                            headers("order_status_changed"))
                sender.stats["status"] += 1

        sent = sender.stats["orders"] + sender.stats["broken"]
        if total and sent >= total:
            break
        if args.duration and time.time() - started >= args.duration:
            break
        if time.time() - last_report >= 5 and not args.dry_run:
            print(f"  {time.strftime('%H:%M:%S')}  " + "  ".join(f"{k}={v:,}" for k, v in sender.stats.items()))
            last_report = time.time()
        if interval:
            time.sleep(interval)

    sender.close()
    ended = time.time()
    print("Done: " + "  ".join(f"{k}={v:,}" for k, v in sender.stats.items()) +
          f"  ({ended - started:.0f}s)")
    if not args.dry_run:
        record_run(args, started, ended, sender.stats)


def record_run(args, started: float, ended: float, stats: dict) -> None:
    """One line per run in data/stream/_metrics/producer_runs.jsonl: perf_report.py uses it to
    find each experiment's time window and to mark it on the graphs."""
    from lusitania import config
    folder = config.DATA_DIR / "stream" / "_metrics"
    folder.mkdir(parents=True, exist_ok=True)
    iso = lambda s: datetime.fromtimestamp(s, UTC).isoformat(timespec="milliseconds")  # noqa: E731
    record = {"label": args.label or f"run-{datetime.now():%Y%m%d-%H%M%S}",
              "start": iso(started), "end": iso(ended),
              "mode": f"burst {args.burst}" if args.burst else f"rate {args.rate}/s",
              "schema_version": args.schema_version, **stats}
    with open(folder / "producer_runs.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
    print(f"Recorded as '{record['label']}' (python streaming\\perf_report.py --label {record['label']})")


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Simulated web shop: live orders → Kafka")
    p.add_argument("--rate", type=float, default=2.0, help="orders per second (default 2)")
    p.add_argument("--burst", type=int, default=0, help="send N events as fast as possible, then stop")
    p.add_argument("--duration", type=float, default=0, help="stop after N seconds (default: until Ctrl+C)")
    p.add_argument("--late-pct", type=float, default=0.10, help="share of late orders (default 0.10)")
    p.add_argument("--dup-pct", type=float, default=0.02, help="share of duplicated orders (default 0.02)")
    p.add_argument("--bad-pct", type=float, default=0.01, help="share of broken events (default 0.01)")
    p.add_argument("--schema-version", type=int, default=1, choices=[1, 2])
    p.add_argument("--tombstone", metavar="ORDER_NUMBER", help="send a tombstone for this order and stop")
    p.add_argument("--seed", type=int, default=None, help="random seed (same seed = same events)")
    p.add_argument("--dry-run", action="store_true", help="print the events instead of sending them")
    p.add_argument("--label", help="name of this run in the performance report, e.g. spike-500")
    return p.parse_args(argv)


if __name__ == "__main__":
    run(parse_args())
