# Performance: crash

Generated 2026-11-06 16:53 by `streaming/perf_report.py`.

| metric | crash |
|---|---|
| producer | burst 5000 |
| maxOffsetsPerTrigger | 500 |
| events sent | 5,099 |
| send time (s) | 1 |
| send rate (ev/s) | 9,305 |
| peak lag | 4,103 |
| peak at (s) | 72 |
| back to real time (s) | 322 |
| catch-up throughput (ev/s) | 15.9 |
| batch duration median (s) | 27.5 |
| batch duration p95 (s) | 31.0 |
| time above DAG 5 alert (s) | 197 |
| consumer restarts | 1 |

![crash](crash.png)

How to read it:
- **peak lag**: the most events waiting in Kafka at one time (the queue at its longest).
- **back to real time**: seconds from the first event sent until the lag was 0 again.
- **catch-up throughput**: events the consumer processed per second while it had a backlog: its real capacity.
- **time above DAG 5 alert**: how long `check_lag` would have been red.
- **batch duration** above the trigger line means each micro-batch takes longer than the trigger interval: the consumer runs batch after batch without pausing.
