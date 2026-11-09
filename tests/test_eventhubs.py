"""The switch between local Kafka and Event Hubs (streaming/eventhubs.py)."""
import pytest

import eventhubs

CONN = ("Endpoint=sb://evhns-lusitania-dev-jf01.servicebus.windows.net/;"
        "SharedAccessKeyName=lusitania-app;SharedAccessKey=abc123=")


def test_bootstrap_comes_from_the_connection_string():
    assert eventhubs.bootstrap(CONN) == "evhns-lusitania-dev-jf01.servicebus.windows.net:9093"


def test_not_an_event_hubs_string_is_refused():
    with pytest.raises(ValueError, match="Endpoint"):
        eventhubs.bootstrap("localhost:9092")


def test_client_and_spark_settings():
    client = eventhubs.client_config(CONN)
    assert (client["security.protocol"], client["sasl.mechanism"], client["sasl.username"]) == \
        ("SASL_SSL", "PLAIN", "$ConnectionString")
    assert client["sasl.password"] == CONN
    spark = eventhubs.spark_options(CONN)
    assert spark["kafka.bootstrap.servers"].endswith(":9093")
    assert f'password="{CONN}";' in spark["kafka.sasl.jaas.config"]


def test_one_variable_switches_everything(monkeypatch):
    monkeypatch.delenv(eventhubs.ENV_VAR, raising=False)
    assert not eventhubs.enabled() and eventhubs.stream_folder() == "stream"
    monkeypatch.setenv(eventhubs.ENV_VAR, CONN)
    assert eventhubs.enabled() and eventhubs.stream_folder() == "stream_eventhubs"
