"""
The PERFORMANCE REPORT of the streaming pipeline: graphs + a summary table per experiment.

It reads what the experiments left behind, in Project 2's data/stream/_metrics/:

    producer_runs.jsonl       one line per producer run (--label names it): when it started and ended
    consumer_batches.jsonl    one line per micro-batch of each consumer job (written by the consumer)
    consumer_events.jsonl     consumer jobs started / stopped (restarts and crashes)

and writes, to docs/performance/ of this project:

    <name>.png                lag, throughput and batch duration over time (one line per experiment)
    <name>.md                 the summary table + the graph, ready for the README

Run it from the Project 2 venv (pip install -r streaming\\requirements.txt adds matplotlib):

    python streaming\\perf_report.py --list                                  the recorded runs
    python streaming\\perf_report.py --label spike-500                       one experiment
    python streaming\\perf_report.py --label spike-500 spike-2000 --name tuning   compared on one graph
"""
from __future__ import annotations

import argparse
import json
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

LAG_ALERT = 1_000                         # DAG 5's check_lag threshold (include/stream_health.py)
OUT_DIR = Path(__file__).resolve().parents[1] / "docs" / "performance"
MAX_WINDOW = timedelta(hours=2)           # never look further than 2 h after a run started
AFTER_RECOVERY = timedelta(seconds=60)    # keep 1 minute of "back to normal" on the graph


def metrics_dir() -> Path:
    from lusitania import config
    return config.DATA_DIR / "stream" / "_metrics"


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@dataclass
class Experiment:
    label: str
    run: dict
    batches: list[dict]          # the ORDERS job's micro-batches in the window, oldest first
    status_batches: list[dict]
    events: list[dict]           # consumer started/stopped in the window

    @property
    def start(self) -> datetime:
        return ts(self.run["start"])

    @property
    def end(self) -> datetime:
        return ts(self.run["end"])

    def seconds(self, when: str) -> float:
        return (ts(when) - self.start).total_seconds()


def find_experiment(label: str, runs: list[dict], batches: list[dict], events: list[dict]) -> Experiment:
    matching = [r for r in runs if r["label"] == label]
    if not matching:
        raise SystemExit(f"no producer run labelled '{label}' (see --list)")
    run = matching[-1]                                    # the latest run with that label
    start, end = ts(run["start"]), ts(run["end"])
    window = [b for b in batches if start - timedelta(seconds=30) <= ts(b["ts"]) <= start + MAX_WINDOW]
    orders = [b for b in window if b["job"] == "orders"]
    # cut the window 1 minute after the consumer is back to real time (lag 0 after the run ended)
    recovered = next((ts(b["ts"]) for b in orders if ts(b["ts"]) >= end and b["lag"] == 0), None)
    limit = (recovered + AFTER_RECOVERY) if recovered else start + MAX_WINDOW
    keep = [b for b in window if ts(b["ts"]) <= limit]
    return Experiment(label, run,
                      [b for b in keep if b["job"] == "orders"],
                      [b for b in keep if b["job"] == "status"],
                      [e for e in events if start - timedelta(seconds=30) <= ts(e["ts"]) <= limit])


def summary(x: Experiment) -> dict:
    b = x.batches
    sent = x.run.get("orders", 0) + x.run.get("broken", 0) + x.run.get("duplicates", 0)
    lags = [r["lag"] for r in b if r["lag"] is not None]
    peak = max(b, key=lambda r: r["lag"] or 0) if b else None
    recovered = next((r for r in b if ts(r["ts"]) >= x.end and r["lag"] == 0), None)
    # throughput while CATCHING UP: from the first batch to recovery, how many events per second
    busy = [r for r in b if not recovered or ts(r["ts"]) <= ts(recovered["ts"])]
    busy_s = (ts(busy[-1]["ts"]) - ts(busy[0]["ts"])).total_seconds() if len(busy) > 1 else 0
    # seconds above DAG 5's threshold: sum of the gaps between consecutive batches with lag > 1,000
    above = sum((ts(n["ts"]) - ts(c["ts"])).total_seconds()
                for c, n in zip(b, b[1:], strict=False) if (c["lag"] or 0) > LAG_ALERT)
    durations = sorted(r["duration_ms"] / 1000 for r in b if r["duration_ms"])
    send_s = (x.end - x.start).total_seconds()
    return {
        "experiment": x.label,
        "producer": x.run["mode"],
        "maxOffsetsPerTrigger": b[0]["max_offsets"] if b else "-",
        "events sent": sent,
        "send time (s)": round(send_s),
        "send rate (ev/s)": round(sent / send_s) if send_s else "-",
        "peak lag": max(lags) if lags else "-",
        "peak at (s)": round(x.seconds(peak["ts"])) if peak else "-",
        "back to real time (s)": round(x.seconds(recovered["ts"])) if recovered else "not yet",
        "catch-up throughput (ev/s)": round(sum(r["rows"] for r in busy[1:]) / busy_s, 1) if busy_s else "-",
        "batch duration median (s)": round(statistics.median(durations), 1) if durations else "-",
        "batch duration p95 (s)": round(durations[int(0.95 * (len(durations) - 1))], 1) if durations else "-",
        "time above DAG 5 alert (s)": round(above),
        "consumer restarts": sum(1 for e in x.events if e["event"] == "started" and e["job"] == "orders"),
    }


def draw(experiments: list[Experiment], path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")                                  # no window: write the file only
    import matplotlib.pyplot as plt

    fig, (lag_ax, rate_ax, dur_ax) = plt.subplots(3, 1, figsize=(11, 10), sharex=True,
                                                  gridspec_kw={"height_ratios": [3, 2, 2]})
    single = len(experiments) == 1
    for i, x in enumerate(experiments):
        color = f"C{i}"
        secs = [x.seconds(r["ts"]) for r in x.batches]
        lag_ax.plot(secs, [r["lag"] for r in x.batches], color=color, lw=2, label=f"{x.label}: orders")
        if single and x.status_batches:
            lag_ax.plot([x.seconds(r["ts"]) for r in x.status_batches], [r["lag"] for r in x.status_batches],
                        color=color, lw=1, ls="--", label=f"{x.label}: status")
        rate_ax.plot(secs, [r["done_per_s"] for r in x.batches], color=color, lw=1.5,
                     label=f"{x.label}: processed per second")
        if single:
            rate_ax.plot(secs, [r["in_per_s"] for r in x.batches], color="grey", lw=1, ls=":",
                         label="arriving per second")
        dur_ax.plot(secs, [r["duration_ms"] / 1000 for r in x.batches], color=color, lw=1.5, marker=".",
                    label=x.label)
        for ax in (lag_ax, rate_ax, dur_ax):               # the producer sending = shaded
            ax.axvspan(0, (x.end - x.start).total_seconds(), color=color, alpha=0.06)
        for e in x.events:                                 # restarts (crash experiment)
            if e["job"] == "orders":
                lag_ax.axvline(x.seconds(e["ts"]), color="red" if e["event"] == "stopped" else "green",
                               ls="-.", lw=1)

    lag_ax.axhline(LAG_ALERT, color="red", lw=1, ls="--", label=f"DAG 5 alert ({LAG_ALERT:,})")
    lag_ax.set_ylabel("lag (events waiting)")
    lag_ax.set_title("Consumer under load: how far behind Kafka, how fast it catches up")
    rate_ax.set_ylabel("events per second")
    dur_ax.set_ylabel("micro-batch duration (s)")
    trigger = experiments[0].batches[0]["trigger"] if experiments[0].batches else "10 seconds"
    dur_ax.axhline(float(trigger.split()[0]), color="grey", lw=1, ls="--", label=f"trigger ({trigger})")
    dur_ax.set_xlabel("seconds since the producer started (shaded: producer sending)")
    for ax in (lag_ax, rate_ax, dur_ax):
        ax.grid(alpha=0.3)
        ax.legend(loc="upper right", fontsize=8)
        ax.set_ylim(bottom=0)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=130)
    plt.close(fig)


def markdown(name: str, rows: list[dict], image: str) -> str:
    cols = list(rows[0])
    table = ["| " + " | ".join(["metric"] + [str(r["experiment"]) for r in rows]) + " |",
             "|" + "---|" * (len(rows) + 1)]
    table += ["| " + " | ".join([c] + [f"{r[c]:,}" if isinstance(r[c], int) else str(r[c]) for r in rows]) + " |"
              for c in cols[1:]]
    return "\n".join([
        f"# Performance: {name}", "",
        f"Generated {datetime.now():%Y-%m-%d %H:%M} by `streaming/perf_report.py`.", "",
        *table, "",
        f"![{name}]({image})", "",
        "How to read it:",
        "- **peak lag**: the most events waiting in Kafka at one time (the queue at its longest).",
        "- **back to real time**: seconds from the first event sent until the lag was 0 again.",
        "- **catch-up throughput**: events the consumer processed per second while it had a backlog: "
        "its real capacity.",
        "- **time above DAG 5 alert**: how long `check_lag` would have been red.",
        "- **batch duration** above the trigger line means each micro-batch takes longer than the "
        "trigger interval: the consumer runs batch after batch without pausing.", "",
    ])


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Performance report of the streaming consumer")
    p.add_argument("--label", nargs="+", help="experiment label(s), from producer.py --label")
    p.add_argument("--name", help="file name of the report (default: the labels joined)")
    p.add_argument("--list", action="store_true", help="list the recorded producer runs")
    p.add_argument("--metrics", type=Path, help="metrics folder (default: Project 2's data/stream/_metrics)")
    p.add_argument("--out", type=Path, default=OUT_DIR)
    args = p.parse_args(argv)

    folder = args.metrics or metrics_dir()
    runs = read_jsonl(folder / "producer_runs.jsonl")
    if args.list or not args.label:
        for r in runs:
            print(f"{r['label']:<28} {r['start'][:19]}  {r['mode']:<14} orders={r.get('orders', 0):,}")
        return
    batches = read_jsonl(folder / "consumer_batches.jsonl")
    events = read_jsonl(folder / "consumer_events.jsonl")
    experiments = [find_experiment(label, runs, batches, events) for label in args.label]
    name = args.name or "_vs_".join(args.label)
    rows = [summary(x) for x in experiments]
    draw(experiments, args.out / f"{name}.png")
    (args.out / f"{name}.md").write_text(markdown(name, rows, f"{name}.png"), encoding="utf-8")
    for r in rows:
        print("\n".join(f"  {k:<28} {v}" for k, v in r.items()), "\n")
    print(f"Report: {args.out / (name + '.md')}")


if __name__ == "__main__":
    main()
