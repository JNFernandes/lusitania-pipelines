"""
THE CLOUD LAKE: the consumer writes its Delta tables to Azure (ADLS Gen2) instead of the laptop.

    LAKE_ACCOUNT not set  →  tables in Project 2's data/ folder, as before
    LAKE_ACCOUNT set      →  tables in abfss://stream@<account>.dfs.core.windows.net/<folder>/

WHO writes: Spark runs in a Docker container, which doesn't have your `az login`. So it logs in as
its own identity, the SERVICE PRINCIPAL sp-lusitania-consumer (Project 4, step 8), whose only role
is "Storage Blob Data Contributor" on the lake. Its three values come from Key Vault into the
PowerShell session (streaming/cloud_env.ps1), never from a file:

    LAKE_TENANT_ID, LAKE_CLIENT_ID     which directory, which identity (not secret)
    LAKE_CLIENT_SECRET                 its password (secret)

They are NOT named AZURE_CLIENT_ID/AZURE_CLIENT_SECRET on purpose: with those names, every Azure
tool in the same PowerShell window (DefaultAzureCredential) would quietly log in as the service
principal instead of as you.

How Spark talks to ADLS: the hadoop-azure connector ("ABFS" driver), with an OAuth login
(client id + secret → a token from Entra ID → the token is sent with every request).
"""
from __future__ import annotations

import glob
import os
import re

ACCOUNT_VAR = "LAKE_ACCOUNT"
CONTAINER = "stream"


def account() -> str:
    return os.getenv(ACCOUNT_VAR, "").strip()


def enabled() -> bool:
    return bool(account())


def root(account_name: str, folder: str) -> str:
    """abfss://CONTAINER@ACCOUNT.dfs.core.windows.net/FOLDER — ADLS Gen2's address format."""
    return f"abfss://{CONTAINER}@{account_name}.dfs.core.windows.net/{folder}"


def silver(account_name: str, table: str) -> str:
    """Project 2's silver tables, uploaded to the lake: the consumer's reference data in the cloud."""
    return f"abfss://silver@{account_name}.dfs.core.windows.net/{table}"


def hadoop_azure_package() -> str:
    """The ABFS connector; its version must match the Hadoop that ships inside pyspark."""
    import pyspark
    jars = glob.glob(os.path.join(os.path.dirname(pyspark.__file__), "jars", "hadoop-client-api-*.jar"))
    version = re.search(r"hadoop-client-api-([\d.]+)\.jar", jars[0]).group(1) if jars else "3.3.4"
    return f"org.apache.hadoop:hadoop-azure:{version}"


def hadoop_conf(account_name: str, tenant: str, client_id: str, secret: str) -> dict:
    """OAuth (client credentials) for ONE storage account: ABFS settings are per account."""
    host = f"{account_name}.dfs.core.windows.net"
    return {
        f"fs.azure.account.auth.type.{host}": "OAuth",
        f"fs.azure.account.oauth.provider.type.{host}":
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        f"fs.azure.account.oauth2.client.id.{host}": client_id,
        f"fs.azure.account.oauth2.client.secret.{host}": secret,
        f"fs.azure.account.oauth2.client.endpoint.{host}":
            f"https://login.microsoftonline.com/{tenant}/oauth2/token",
    }


def credentials() -> tuple[str, str, str]:
    values = tuple(os.getenv(v, "").strip() for v in ("LAKE_TENANT_ID", "LAKE_CLIENT_ID", "LAKE_CLIENT_SECRET"))
    if not all(values):
        raise RuntimeError("LAKE_ACCOUNT is set but LAKE_TENANT_ID / LAKE_CLIENT_ID / LAKE_CLIENT_SECRET "
                           "are not: load them with  . .\\streaming\\cloud_env.ps1")
    return values
