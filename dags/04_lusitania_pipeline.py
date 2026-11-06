"""
### DAG 4 · The Lusitânia pipeline: bronze → silver → gold, every day at 06:00 (Lisbon)

    check_sources ─► load_bronze ─► build_silver ─► dbt_snapshot ─► dbt_run ─► dbt_test ─► report_gold

Every step calls code that already exists in Project 2, mounted at /opt/lakehouse:
the `lusitania` CLI for bronze and silver, and dbt for gold. This DAG adds no data logic:
it decides ORDER, TIMING and what happens on FAILURE.

| Step | Fails because of | Retries |
|---|---|---|
| check_sources, load_bronze | a source not answering (network, a service restarting) | 2, five minutes apart |
| build_silver, dbt_* | bad data or a bug: a data check or a dbt test fails | none (retrying gives the same result) |

`max_active_runs=1`: never two runs at the same time, because both would write the same Delta
tables and the same DuckDB file (DuckDB allows one writer).
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

PYTHON = "/opt/lusitania-venv/bin/python"          # the pipeline's environment (Dockerfile)
DBT = "/opt/lusitania-venv/bin/dbt"
DBT_DIR = "/opt/lakehouse/dbt"                     # Project 2's dbt project
INCLUDE = "/opt/airflow/include"
ENV = {"PYTHONPATH": "/opt/lakehouse/src"}         # makes `import lusitania` work


def alert_on_failure(context):
    """Runs when a task fails for good (after its retries). Here it writes a clear line to the
    task log; in a company the same function would post to Slack, Teams or e-mail."""
    ti = context["ti"]
    print(f"ALERT: pipeline task '{ti.task_id}' FAILED in run '{ti.run_id}' "
          f"after {ti.try_number} attempt(s). Open its log in the Airflow UI.")


@dag(
    dag_id="04_lusitania_pipeline",
    schedule="0 6 * * *",                          # every day at 06:00, Lisbon time
    start_date=datetime(2026, 11, 1),
    catchup=False,                                 # no runs for days already past
    max_active_runs=1,                             # one run at a time (see above)
    dagrun_timeout=timedelta(hours=1),             # a run stuck for over an hour is failed
    default_args={
        "retries": 0,                              # default: no retries (data steps)
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": alert_on_failure,
    },
    doc_md=__doc__,                                # this docstring is shown on the DAG's page in the UI
    tags=["lusitania", "production"],
)
def lusitania_pipeline():
    network_retries = {"retries": 2, "retry_delay": timedelta(minutes=5)}

    check_sources = BashOperator(
        task_id="check_sources",
        bash_command=(f"{PYTHON} {INCLUDE}/check_sources.py sqlserver && "
                      f"{PYTHON} {INCLUDE}/check_sources.py mongodb && "
                      f"{PYTHON} {INCLUDE}/check_sources.py api"),
        env=ENV, append_env=True, **network_retries,
    )

    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command=f"{PYTHON} -m lusitania bronze",          # = `lusitania bronze` on the laptop
        env=ENV, append_env=True, **network_retries,
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=f"{PYTHON} -m lusitania silver",          # stops on a failed data check (exit code 1)
        env=ENV, append_env=True,
    )

    # Gold = dbt build split in three, so the Grid view shows WHICH part failed
    dbt_snapshot = BashOperator(task_id="dbt_snapshot", bash_command=f"{DBT} snapshot --target local",
                                cwd=DBT_DIR, env=ENV, append_env=True)
    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"{DBT} run --target local",
                           cwd=DBT_DIR, env=ENV, append_env=True)
    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"{DBT} test --target local",
                            cwd=DBT_DIR, env=ENV, append_env=True)

    report_gold = BashOperator(
        task_id="report_gold",
        bash_command=f"{PYTHON} {INCLUDE}/report_gold.py",
        env=ENV, append_env=True,
    )

    check_sources >> load_bronze >> build_silver >> dbt_snapshot >> dbt_run >> dbt_test >> report_gold


lusitania_pipeline()
