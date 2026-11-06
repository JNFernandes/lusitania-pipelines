"""
### DAG 5 · Stream health: Airflow WATCHING the streaming consumer, every 15 minutes

    check_lag   check_consumer_alive   check_dlq        (independent: they run in parallel)

Airflow does not run the stream (a streaming job never finishes, and Airflow runs tasks that
finish). It checks the stream from outside: the consumer's checkpoint, its Delta table and
Kafka's offsets. A failed check turns red and runs the alert.

| Check | Fails when |
|---|---|
| check_lag | more than 1,000 events in Kafka not processed yet: the consumer cannot keep up |
| check_consumer_alive | events are waiting but nothing was written for 5 minutes: stuck or down |
| check_dlq | in 15 minutes, more than 10 broken events and over 5% of the orders: bad data upstream |
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

PYTHON = "/opt/lusitania-venv/bin/python"
SCRIPT = "/opt/airflow/include/stream_health.py"


def alert_on_failure(context):
    """In a company: Slack, Teams or e-mail. Here: a clear line in the task log."""
    ti = context["ti"]
    print(f"ALERT: stream check '{ti.task_id}' FAILED in run '{ti.run_id}'. Open its log in the Airflow UI.")


@dag(
    dag_id="05_stream_health",
    schedule="*/15 * * * *",                # every 15 minutes
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={
        "retries": 0,                       # a check that fails is the RESULT, not a glitch to retry
        "execution_timeout": timedelta(minutes=5),
        "on_failure_callback": alert_on_failure,
    },
    doc_md=__doc__,
    tags=["lusitania", "streaming", "monitoring"],
)
def stream_health():
    for check, task_id in [("lag", "check_lag"), ("alive", "check_consumer_alive"), ("dlq", "check_dlq")]:
        BashOperator(task_id=task_id, bash_command=f"{PYTHON} {SCRIPT} {check}",
                     env={"KAFKA_BOOTSTRAP": "kafka:19092"}, append_env=True)


stream_health()
