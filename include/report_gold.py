"""
The last step of the pipeline: a short summary of the gold layer, printed to the task log.

Reads the DuckDB file built by dbt in READ-ONLY mode (it never changes anything), so each
run's log ends with the numbers a person would check first.
"""
import duckdb

from lusitania import config

TABLES = ["dim_date", "dim_customer", "dim_product", "dim_category", "dim_department", "dim_store", "fact_sales"]


def main():
    con = duckdb.connect(str(config.GOLD_DATABASE), read_only=True)
    try:
        cur = con.cursor()
        print("Gold layer after this run:")
        for table in TABLES:
            rows = cur.execute(f"SELECT COUNT(*) FROM gold.{table}").fetchone()[0]
            print(f"  gold.{table:<16} {rows:>10,} rows")

        channel_totals = cur.execute("""
            SELECT channel, COUNT(*) AS lines, ROUND(SUM(net_amount_eur), 2) AS revenue_eur
            FROM gold.fact_sales
            WHERE is_revenue
            GROUP BY channel
            ORDER BY channel
        """).fetchall()
        print("\nRevenue by channel (cancelled and returned orders excluded):")
        for channel, lines, revenue in channel_totals:
            print(f"  {channel:<8} {lines:>8,} lines  €{revenue:>14,.2f}")

        versions = cur.execute(
            "SELECT COUNT(*) FROM gold.dim_customer WHERE version_number > 1").fetchone()[0]
        print(f"\nSCD2: {versions} customer version(s) beyond the first")
    finally:
        con.close()


if __name__ == "__main__":
    main()
