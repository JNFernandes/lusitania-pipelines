"""
A quick look at the consumer's three live tables (and the numbers to reconcile with Kafka).

Run it with the Project 2 venv, while the consumer is running or after:

    python streaming\\look_stream.py
    python streaming\\look_stream.py --order WEB29923399      one order: its lines and its status

It only READS the Delta tables (with DuckDB), so it is safe at any time.
"""
import argparse

import duckdb

from lusitania import config

STREAM = config.DATA_DIR / "stream"


def scan(table: str) -> str:
    return f"delta_scan('{(STREAM / table).as_posix()}')"


def show(cur, title: str, table: str, sql: str) -> None:
    print(f"\n=== {title} ===")
    if not (STREAM / table / "_delta_log").exists():
        print(f"  (not created yet: {table})")
        return
    try:
        result = cur.execute(sql.format(t=scan(table)))
    except duckdb.BinderException as problem:          # e.g. coupon_code before the first v2 write
        print(f"  (not available yet: {str(problem).splitlines()[0]})")
        return
    columns = [d[0] for d in result.description]
    rows = result.fetchall()
    widths = [max(len(str(c)), *(len(str(r[i])) for r in rows)) if rows else len(str(c))
              for i, c in enumerate(columns)]
    print("  " + "  ".join(str(c).ljust(w) for c, w in zip(columns, widths, strict=True)))
    for r in rows:
        print("  " + "  ".join(str(v).ljust(w) for v, w in zip(r, widths, strict=True)))


def show_order(cur, number: str) -> None:
    """One order across the live tables: are its lines there, what is its current status?"""
    safe = number.replace("'", "")
    show(cur, f"Order {safe} · online_orders_live", "online_orders_live", f"""
        SELECT order_number, line_number, order_ts, sku, net_amount_eur, bronze_batch_id AS kafka_position
        FROM {{t}} WHERE order_number = '{safe}' ORDER BY line_number""")
    show(cur, f"Order {safe} · order_status_current (no row = deleted or never seen)",
         "order_status_current", f"""
        SELECT order_number, status, updated_at, kafka_partition, kafka_offset, consumer_updated_at
        FROM {{t}} WHERE order_number = '{safe}'""")


def main() -> None:
    p = argparse.ArgumentParser(description="A quick look at the consumer's live tables")
    p.add_argument("--order", help="show one order instead of the overview")
    args = p.parse_args()
    con = duckdb.connect()
    cur = con.cursor()
    cur.execute("INSTALL delta")
    cur.execute("LOAD delta")
    if args.order:
        show_order(cur, args.order)
        con.close()
        return

    show(cur, "Job 1 · online_orders_live: totals (reconcile with Kafka)", "online_orders_live", """
        SELECT COUNT(DISTINCT order_number) AS orders, COUNT(*) AS lines,
               SUM(net_amount_eur) AS revenue_eur, MIN(order_ts) AS first_order, MAX(order_ts) AS last_order
        FROM {t}""")
    show(cur, "Job 1 · online_orders_live: duplicates check", "online_orders_live", """
        SELECT COUNT(*) AS lines,
               -- the SAME Kafka event written twice (e.g. a replayed micro-batch): must be 0
               COUNT(*) - COUNT(DISTINCT (bronze_batch_id, line_number)) AS same_event_twice,
               -- DIFFERENT events with the same order number (a producer numbering clash)
               COUNT(DISTINCT (bronze_batch_id, line_number))
                 - COUNT(DISTINCT (order_number, line_number)) AS order_number_reused
        FROM {t}""")
    show(cur, "Job 1 · online_orders_live: the 5 newest lines", "online_orders_live", """
        SELECT order_number, line_number, order_ts, sku, currency, net_amount_eur, bronze_batch_id AS kafka_position
        FROM {t} ORDER BY order_ts DESC LIMIT 5""")
    show(cur, "Job 1 · online_orders_live: schema versions (coupon_code exists from schema v2)",
         "online_orders_live", """
        SELECT coalesce(coupon_code, '(none)') AS coupon_code, COUNT(DISTINCT order_number) AS orders,
               MIN(order_ts) AS first_order, MAX(order_ts) AS last_order
        FROM {t} WHERE order_ts > now() - INTERVAL 1 HOUR
        GROUP BY ALL ORDER BY orders DESC""")
    show(cur, "Job 2 · sales_per_5_minutes: the newest final windows", "sales_per_5_minutes", """
        SELECT window_start, window_end, orders, order_lines, revenue_eur, written_at
        FROM {t} ORDER BY window_start DESC LIMIT 6""")
    show(cur, "Job 3 · order_status_current: orders per status", "order_status_current", """
        SELECT status, COUNT(*) AS orders FROM {t} GROUP BY status ORDER BY orders DESC""")
    con.close()


if __name__ == "__main__":
    main()
