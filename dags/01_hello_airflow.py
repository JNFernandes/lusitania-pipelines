"""
DAG 1 · Hello Airflow: the basic parts of a DAG, with nothing that can break.

    start ──┬──► count_customers ──┐
            └──► count_products  ──┴──► report

What to look for in the web UI:
  - Graph view: the boxes and arrows drawn from the code below
  - the two "count" tasks run AT THE SAME TIME (parallel), report waits for both
  - the value returned by a task travels to the next one (XCom)
  - each task's log (click a task square → Logs)
"""
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="01_hello_airflow",
    schedule="0 6 * * *",              # cron: every day at 06:00 (Lisbon, see docker-compose.yaml)
    start_date=datetime(2026, 11, 1),
    catchup=False,                     # don't create runs for the days already past
    tags=["learning"],
)
def hello_airflow():

    @task
    def start():
        print("Pipeline started")

    @task
    def count_customers() -> int:
        n = 3001                       # pretend this came from SQL Server
        print(f"Counted {n} customers")
        return n                       # a returned value is saved as an XCom

    @task
    def count_products() -> int:
        n = 241
        print(f"Counted {n} products")
        return n

    @task
    def report(customers: int, products: int):
        # Airflow passes the XCom values of the two tasks above as arguments
        print(f"Report: {customers} customers and {products} products")

    # The arrows. Calling the tasks inside the DAG function defines the boxes;
    # >> defines the order. Passing a result into report() also creates an arrow.
    started = start()
    customers = count_customers()
    products = count_products()
    started >> [customers, products]
    report(customers, products)


hello_airflow()
