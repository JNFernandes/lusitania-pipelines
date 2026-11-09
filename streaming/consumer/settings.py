"""Every name, path and tuning knob of the consumer, in one place."""
import os

import eventhubs
from lusitania import config

# --- Kafka: local (the INTERNAL door, the consumer runs in a container) or Event Hubs ------
EVENTHUBS = eventhubs.connection_string()          # empty = local Kafka
KAFKA_BOOTSTRAP = eventhubs.bootstrap(EVENTHUBS) if EVENTHUBS else os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")


def kafka_options() -> dict:
    """Connection options for every Spark Kafka source and sink."""
    return eventhubs.spark_options(EVENTHUBS) if EVENTHUBS else {"kafka.bootstrap.servers": KAFKA_BOOTSTRAP}


ORDERS_TOPIC = "web_orders"
STATUS_TOPIC = "order_status"
DLQ_TOPIC = "web_orders_dlq"

# --- Delta tables: in Project 2's data/ folder, next to bronze and silver ----------------
STREAM_DIR = config.DATA_DIR / eventhubs.stream_folder()   # stream/ or stream_eventhubs/
LIVE_TABLE = STREAM_DIR / "online_orders_live"
TOTALS_TABLE = STREAM_DIR / "sales_per_5_minutes"
STATUS_TABLE = STREAM_DIR / "order_status_current"
CHECKPOINTS = STREAM_DIR / "_checkpoints"            # one sub-folder per job: its offsets and state
METRICS_DIR = STREAM_DIR / "_metrics"                # performance records (consumer + producer)

# reference data from the BATCH silver layer (stream–static joins)
SILVER_PRODUCTS = config.SILVER_DIR / "products"
SILVER_RATES = config.SILVER_DIR / "exchange_rates"

# --- Tuning -------------------------------------------------------------------------------
# The two knobs of the performance experiments can be changed without editing code:
#   $env:MAX_OFFSETS_PER_TRIGGER=2000; docker compose up -d consumer
TRIGGER = os.getenv("TRIGGER_INTERVAL", "10 seconds")                  # a micro-batch every 10 s
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "500"))  # BACKPRESSURE: per batch
DEDUP_WATERMARK = "10 minutes"       # remember order numbers for 10 min to drop duplicates
LATE_WATERMARK = "10 minutes"        # wait 10 min for late orders before a 5-minute total is final
WINDOW = "5 minutes"
KNOWN_SCHEMA_VERSIONS = (1, 2)       # schemaVersion missing = 1
MAX_RATE_AGE_DAYS = 7                # today's rate not loaded yet? use the latest one, if at most 7 days old


def checkpoint(job: str) -> str:
    return str(CHECKPOINTS / job)
