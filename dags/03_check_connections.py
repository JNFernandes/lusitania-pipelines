"""
DAG 3 · Check connections: before building the real pipeline, prove that Airflow can reach
every source and run every tool it will need.

    sqlserver   mongodb   api   spark   dbt        (5 independent checks, in parallel)

Each task is a BashOperator: it runs a command in a shell, here a Python script executed by
the PIPELINE's environment (/opt/lusitania-venv), not by Airflow's own Python.
A command that ends with a non-zero exit code makes the task fail (red).

Run it by hand (the ▶ button): it has no schedule.
"""
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

PIPELINE_PYTHON = "/opt/lusitania-venv/bin/python"
CHECK_SCRIPT = "/opt/airflow/include/check_sources.py"
# Make Project 2's package importable: its source is mounted at /opt/lakehouse (docker-compose.yaml)
PIPELINE_ENV = {"PYTHONPATH": "/opt/lakehouse/src"}


@dag(
    dag_id="03_check_connections",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["setup"],
)
def check_connections():
    for check in ["sqlserver", "mongodb", "api", "spark", "dbt"]:
        BashOperator(
            task_id=f"check_{check}",
            bash_command=f"{PIPELINE_PYTHON} {CHECK_SCRIPT} {check}",
            env=PIPELINE_ENV,
            append_env=True,          # keep the container's variables (SQL_USER, MONGO_URI...) too
        )


check_connections()
