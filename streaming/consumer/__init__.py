"""
The CONSUMER: reads the live events out of Kafka and keeps three Delta tables up to date.

    orders.py   job 1  Kafka web_orders   → online_orders_live   (+ broken events → Kafka web_orders_dlq)
    totals.py   job 2  online_orders_live → sales_per_5_minutes  (reads the Delta table AS A STREAM)
    status.py   job 3  Kafka order_status → order_status_current (latest status; tombstone = delete)
    main.py     starts the three jobs and logs their progress (lag, rates, state) every 30 s
"""
