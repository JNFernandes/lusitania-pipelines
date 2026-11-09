"""
KAFKA IN THE CLOUD: Azure Event Hubs, switched on by ONE environment variable.

    EVENTHUBS_CONNECTION_STRING not set  →  local Kafka (Docker), exactly as before
    EVENTHUBS_CONNECTION_STRING set      →  Event Hubs, through its Kafka endpoint

Event Hubs speaks the Kafka protocol, so the producer and the consumer keep their Kafka clients.
Only the connection changes:

    bootstrap      NAMESPACE.servicebus.windows.net:9093   (taken from the connection string)
    security       SASL_SSL: TLS is mandatory
    login          SASL PLAIN, username "$ConnectionString", password = the connection string

The connection string is a SECRET. It lives in Key Vault (Terraform puts it there) and is read into
the PowerShell session only when needed — never written to a file:

    $env:EVENTHUBS_CONNECTION_STRING = az keyvault secret show --vault-name kv-lusitania-dev-jf01 `
        --name eventhubs-connection-string --query value -o tsv

Differences that matter on the Standard tier: no compression (Premium/Dedicated only), so the
producer turns it off; idempotent producers are supported.
"""
from __future__ import annotations

import os

ENV_VAR = "EVENTHUBS_CONNECTION_STRING"
PORT = 9093


def connection_string() -> str:
    return os.getenv(ENV_VAR, "").strip()


def enabled() -> bool:
    return bool(connection_string())


def bootstrap(conn: str) -> str:
    """'Endpoint=sb://evhns-x.servicebus.windows.net/;SharedAccessKeyName=…' → 'evhns-x.servicebus.windows.net:9093'"""
    parts = dict(p.split("=", 1) for p in conn.split(";") if "=" in p)
    endpoint = parts.get("Endpoint", "")
    if not endpoint.startswith("sb://"):
        raise ValueError(f"{ENV_VAR} has no 'Endpoint=sb://…' part: is it an Event Hubs connection string?")
    return f"{endpoint.removeprefix('sb://').strip('/')}:{PORT}"


def client_config(conn: str) -> dict:
    """Settings for confluent-kafka (the producer, the health checks)."""
    return {
        "bootstrap.servers": bootstrap(conn),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "$ConnectionString",           # literally this text: Event Hubs' convention
        "sasl.password": conn,
    }


def spark_options(conn: str) -> dict:
    """The same, as options of Spark's Kafka source and sink (each Kafka setting gets 'kafka.')."""
    jaas = ('org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="$ConnectionString" password="{conn}";')
    return {
        "kafka.bootstrap.servers": bootstrap(conn),
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas,
        "kafka.request.timeout.ms": "60000",              # the cloud is further away than Docker
        "kafka.session.timeout.ms": "30000",
    }


def stream_folder() -> str:
    """Live tables and checkpoints of a cloud run go to their own folder: a checkpoint holds the
    OFFSETS of one particular Kafka, and Event Hubs' offsets are not the local Kafka's."""
    return "stream_eventhubs" if enabled() else "stream"
