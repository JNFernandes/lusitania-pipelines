"""
DAG 2 · Retries and failures: what happens when things go wrong.

    flaky_api ──► load_after_api
    broken_task ──► never_runs

  flaky_api       fails on its 1st and 2nd try, succeeds on the 3rd → shows RETRIES (yellow: up_for_retry)
  load_after_api  waits until flaky_api finally succeeds
  broken_task     fails every time, uses up its retries → FAILED (red)
  never_runs      depends on broken_task, so Airflow never starts it → UPSTREAM_FAILED (orange)

Run it by hand (the ▶ button): it has no schedule.
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, get_current_context, task

DEFAULTS = {
    "retries": 2,                          # try again up to 2 more times...
    "retry_delay": timedelta(seconds=15),  # ...waiting 15 s between tries (real pipelines: minutes)
}


@dag(
    dag_id="02_retries_and_failures",
    schedule=None,                         # no schedule: only runs when you trigger it
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args=DEFAULTS,                 # applied to every task in this DAG
    tags=["learning"],
)
def retries_and_failures():

    @task
    def flaky_api():
        attempt = get_current_context()["ti"].try_number   # 1, 2, 3, ...
        print(f"Calling the exchange-rate API, attempt {attempt}")
        if attempt < 3:
            raise ConnectionError("API did not answer (simulated)")
        print("API answered")

    @task
    def load_after_api():
        print("Loading the rates, now that the API answered")

    @task
    def broken_task():
        raise ValueError("A real bug: retrying will not fix this (simulated)")

    @task
    def never_runs():
        print("You will never see this line")

    flaky_api() >> load_after_api()
    broken_task() >> never_runs()


retries_and_failures()
