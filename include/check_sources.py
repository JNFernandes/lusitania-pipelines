"""
Checks, from INSIDE an Airflow container, that every source and tool of the pipeline works.
Run by the DAG 03_check_connections, one check per task:

    python check_sources.py sqlserver | mongodb | api | spark | dbt

It uses the lusitania package from Project 2 (mounted at /opt/lakehouse), so it tests the
real configuration the pipeline will use. Exit code 0 = OK, anything else = the task fails.
"""
import subprocess
import sys


def sqlserver():
    import pyodbc

    from lusitania import config
    with pyodbc.connect(config.sql_connection_string(), timeout=10) as conn:
        customers = conn.cursor().execute("SELECT COUNT(*) FROM sales.Customer").fetchone()[0]
    print(f"SQL Server OK: {customers:,} customers in {config.OLTP_DATABASE} "
          f"on {config.SQL_SERVER}, login {config.SQL_USER or '(Windows)'}")


def mongodb():
    from pymongo import MongoClient

    from lusitania import config
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=10_000)
    try:
        orders = client[config.MONGO_DATABASE]["orders"].count_documents({})
    finally:
        client.close()
    print(f"MongoDB OK: {orders:,} documents in {config.MONGO_DATABASE}.orders at {config.MONGO_URI}")


def api():
    import requests

    from lusitania import config
    response = requests.get(f"{config.FX_API_URL}/latest", params={"from": config.FX_BASE}, timeout=30)
    response.raise_for_status()
    print(f"Exchange-rate API OK: rates for {response.json()['date']} from {config.FX_API_URL}")


def spark():
    from lusitania.spark import get_spark
    session = get_spark("check_sources", cores="1")
    path = "/tmp/check_delta"
    session.range(5).write.format("delta").mode("overwrite").save(path)
    rows = session.read.format("delta").load(path).count()
    print(f"Spark {session.version} + Delta OK: wrote and read back {rows} rows (Java at {_java_home()})")
    session.stop()


def dbt():
    from lusitania import config
    # dbt debug = "can dbt read the project and open its database?" (the local DuckDB file)
    result = subprocess.run(["/opt/lusitania-venv/bin/dbt", "debug", "--target", "local"],
                            cwd=config.DBT_DIR, capture_output=True, text=True)
    print(result.stdout[-2500:])
    if result.returncode != 0:
        print(result.stderr[-2500:])
        raise SystemExit(f"dbt debug failed (exit code {result.returncode})")
    print(f"dbt OK: project and DuckDB connection in {config.DBT_DIR}")


def _java_home():
    import os
    return os.environ.get("JAVA_HOME", "?")


CHECKS = {"sqlserver": sqlserver, "mongodb": mongodb, "api": api, "spark": spark, "dbt": dbt}

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in CHECKS:
        raise SystemExit(f"usage: python check_sources.py {' | '.join(CHECKS)}")
    CHECKS[sys.argv[1]]()
