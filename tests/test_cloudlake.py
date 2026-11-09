"""The switch between the laptop folder and the cloud lake (streaming/cloudlake.py)."""
import importlib

import pytest

import cloudlake


def test_root_is_an_abfss_address():
    assert cloudlake.root("stlusitaniadevjf01", "stream_eventhubs") == \
        "abfss://stream@stlusitaniadevjf01.dfs.core.windows.net/stream_eventhubs"


def test_hadoop_azure_matches_the_hadoop_inside_pyspark():
    assert cloudlake.hadoop_azure_package() == "org.apache.hadoop:hadoop-azure:3.3.4"


def test_oauth_settings_are_per_account():
    conf = cloudlake.hadoop_conf("acct", "tenant-1", "client-1", "s3cret")
    host = "acct.dfs.core.windows.net"
    assert conf[f"fs.azure.account.auth.type.{host}"] == "OAuth"
    assert conf[f"fs.azure.account.oauth2.client.id.{host}"] == "client-1"
    assert conf[f"fs.azure.account.oauth2.client.endpoint.{host}"] == \
        "https://login.microsoftonline.com/tenant-1/oauth2/token"


def test_missing_credentials_are_explained(monkeypatch):
    for v in ("LAKE_TENANT_ID", "LAKE_CLIENT_ID", "LAKE_CLIENT_SECRET"):
        monkeypatch.delenv(v, raising=False)
    with pytest.raises(RuntimeError, match="cloud_env.ps1"):
        cloudlake.credentials()


def test_settings_point_every_table_to_the_lake(monkeypatch):
    from consumer import settings
    monkeypatch.setenv("LAKE_ACCOUNT", "stlusitaniadevjf01")
    monkeypatch.delenv("EVENTHUBS_CONNECTION_STRING", raising=False)
    try:
        cloud = importlib.reload(settings)
        assert cloud.LIVE_TABLE == "abfss://stream@stlusitaniadevjf01.dfs.core.windows.net/stream/online_orders_live"
        assert cloud.checkpoint("orders").endswith("/stream/_checkpoints/orders")
        assert not str(cloud.METRICS_DIR).startswith("abfss")         # metrics stay on the laptop
        assert cloud.SILVER_RATES == "abfss://silver@stlusitaniadevjf01.dfs.core.windows.net/exchange_rates"
    finally:
        monkeypatch.delenv("LAKE_ACCOUNT")
        importlib.reload(settings)
    assert not settings.LIVE_TABLE.startswith("abfss")
