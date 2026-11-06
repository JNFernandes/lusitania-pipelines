"""The performance records (consumer listener + producer runs) and the report built from them."""
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "streaming"))
import perf_report  # noqa: E402

from consumer import metrics  # noqa: E402

T0 = datetime(2026, 11, 6, 14, 0, 0, tzinfo=UTC)


def fake_spike(folder: Path, label="spike-500", per_batch=500, every_s=20, total=5000, hour=0):
    """A burst of `total` events sent in 30 s, drained `per_batch` events per micro-batch."""
    def iso(seconds: float) -> str:
        return (T0 + timedelta(hours=hour, seconds=seconds)).isoformat()
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "producer_runs.jsonl").write_text(json.dumps(
        {"label": label, "start": iso(0), "end": iso(30), "mode": f"burst {total}",
         "orders": total - 100, "broken": 50, "duplicates": 50, "status": total}) + "\n")
    lines, done, t = [], 0, 5
    while True:
        rows = min(per_batch, total - done)
        done += rows
        lines.append({"ts": iso(t), "job": "orders", "batch": len(lines), "rows": rows,
                      "in_per_s": 100.0, "done_per_s": rows / 18, "duration_ms": 18_000,
                      "lag": total - done, "lag_max_partition": (total - done) // 3, "state_rows": 0,
                      "max_offsets": per_batch, "trigger": "10 seconds"})
        if done == total:
            break
        t += every_s
    lines.append(dict(lines[-1], ts=iso(t + 20), rows=0, lag=0, batch=len(lines)))   # quiet afterwards
    with open(folder / "consumer_batches.jsonl", "w") as f:
        f.writelines(json.dumps(r) + "\n" for r in lines)


def test_summary_of_a_spike(tmp_path):
    fake_spike(tmp_path / "m")
    x = perf_report.find_experiment("spike-500", perf_report.read_jsonl(tmp_path / "m/producer_runs.jsonl"),
                                    perf_report.read_jsonl(tmp_path / "m/consumer_batches.jsonl"), [])
    s = perf_report.summary(x)
    assert s["events sent"] == 5000
    assert s["peak lag"] == 4500                       # after the first batch of 500
    assert s["back to real time (s)"] == 185           # 10 batches, 20 s apart, starting at 5 s
    assert s["catch-up throughput (ev/s)"] == 25.0     # 4,500 events in 180 s
    assert s["time above DAG 5 alert (s)"] == 140      # lag > 1,000 for 7 gaps of 20 s
    assert s["maxOffsetsPerTrigger"] == 500


def test_report_compares_two_runs_and_writes_png_and_markdown(tmp_path):
    fake_spike(tmp_path / "a", label="spike-500")
    fake_spike(tmp_path / "b", label="spike-2000", per_batch=2000, every_s=30, hour=1)   # an hour later
    for name in ("producer_runs.jsonl", "consumer_batches.jsonl"):
        (tmp_path / name).write_text((tmp_path / "a" / name).read_text() + (tmp_path / "b" / name).read_text())
    perf_report.main(["--metrics", str(tmp_path), "--out", str(tmp_path / "out"),
                      "--label", "spike-500", "spike-2000", "--name", "tuning"])
    md = (tmp_path / "out/tuning.md").read_text()
    assert "| back to real time (s) | 185 | 65 |" in md      # 2,000 per batch: 3 batches
    assert (tmp_path / "out/tuning.png").stat().st_size > 10_000


def test_listener_record_from_a_kafka_progress():
    source = SimpleNamespace(metrics={"maxOffsetsBehindLatest": "700"},
                             endOffset=json.dumps({"web_orders": {"0": 100, "1": 120, "2": 90}}),
                             latestOffset=json.dumps({"web_orders": {"0": 800, "1": 400, "2": 290}}))
    progress = SimpleNamespace(timestamp="2026-11-06T14:00:05.000Z", name="orders", batchId=3, numInputRows=500,
                               inputRowsPerSecond=120.0, processedRowsPerSecond=27.5,
                               durationMs={"triggerExecution": 18_200}, sources=[source],
                               stateOperators=[SimpleNamespace(numRowsTotal=40)])
    r = metrics.batch_record(progress)
    assert r["lag"] == 700 + 280 + 200                  # all partitions, not only the worst one
    assert r["lag_max_partition"] == 700
    assert (r["rows"], r["duration_ms"], r["state_rows"]) == (500, 18_200, 40)


def test_listener_writes_a_line_per_micro_batch(spark, tmp_path):
    recorder = metrics.MetricsRecorder(tmp_path / "metrics")
    spark.streams.addListener(recorder)
    try:
        (tmp_path / "in").mkdir()
        (tmp_path / "in/a.json").write_text('{"n": 1}\n{"n": 2}\n')
        (spark.readStream.schema("n int").json(str(tmp_path / "in"))
              .writeStream.queryName("orders").format("noop")
              .option("checkpointLocation", str(tmp_path / "ckpt"))
              .trigger(availableNow=True).start().awaitTermination())
        import time
        for _ in range(20):                             # the listener runs a moment after the batch
            if (tmp_path / "metrics/consumer_batches.jsonl").exists():
                break
            time.sleep(0.5)
    finally:
        spark.streams.removeListener(recorder)
    batch = json.loads((tmp_path / "metrics/consumer_batches.jsonl").read_text().splitlines()[0])
    assert (batch["job"], batch["rows"], batch["lag"]) == ("orders", 2, None)   # a file source has no lag
    lines = (tmp_path / "metrics/consumer_events.jsonl").read_text().splitlines()
    events = [json.loads(line)["event"] for line in lines]
    assert "started" in events
