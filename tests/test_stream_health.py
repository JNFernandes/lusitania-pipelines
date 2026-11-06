"""The stream health checks (DAG 5), with a fake checkpoint and fake Kafka numbers."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
import stream_health as sh  # noqa: E402


def make_checkpoint(stream_dir: Path, job: str, batches: dict[int, dict]):
    """Spark's checkpoint layout: offsets/N (3 lines) for each batch, commits/N for finished ones."""
    for batch, offsets in batches.items():
        (stream_dir / "_checkpoints" / job / "offsets").mkdir(parents=True, exist_ok=True)
        (stream_dir / "_checkpoints" / job / "commits").mkdir(parents=True, exist_ok=True)
        (stream_dir / "_checkpoints" / job / "offsets" / str(batch)).write_text(
            "v1\n" + json.dumps({"batchWatermarkMs": 0}) + "\n" + json.dumps({"web_orders": offsets}))
    return stream_dir


def test_processed_offsets_come_from_the_latest_committed_batch(tmp_path):
    make_checkpoint(tmp_path, "orders", {11: {"0": 100, "1": 120, "2": 90}, 12: {"0": 140, "1": 159, "2": 146}})
    (tmp_path / "_checkpoints/orders/commits/11").write_text("v1")
    # batch 12 has offsets but no commit yet: it is still running, so it does not count
    assert sh.processed_offsets("orders", tmp_path) == {0: 100, 1: 120, 2: 90}
    (tmp_path / "_checkpoints/orders/commits/12").write_text("v1")
    assert sh.processed_offsets("orders", tmp_path) == {0: 140, 1: 159, 2: 146}


def test_a_consumer_that_never_ran_fails(tmp_path):
    with pytest.raises(sh.CheckFailed, match="never ran"):
        sh.processed_offsets("orders", tmp_path)


def test_lag_is_available_minus_processed_per_partition():
    lags = sh.lag_per_job(get_end_offsets=lambda topic: {0: 150, 1: 159, 2: 146},
                          get_processed=lambda job: {0: 140, 1: 159, 2: 140})
    assert lags["orders"] == {0: 10, 1: 0, 2: 6}
    assert "total 16" in sh.check_lag(lags)


def test_lag_above_the_limit_fails():
    with pytest.raises(sh.CheckFailed, match="falling behind"):
        sh.check_lag({"orders": {0: 900, 1: 400, 2: 0}, "status": {0: 0}})


def test_alive_quiet_shop_vs_stuck_consumer():
    assert "shop is quiet" in sh.check_alive(total_lag=0, minutes_since_write=40)   # nothing to do: fine
    assert "OK" not in sh.check_alive(total_lag=30, minutes_since_write=1)          # working: fine
    with pytest.raises(sh.CheckFailed, match="STUCK or DOWN"):
        sh.check_alive(total_lag=30, minutes_since_write=12)                         # work waiting, no writes


def test_last_write_age_reads_the_newest_delta_commit(tmp_path):
    log = tmp_path / sh.LIVE_TABLE / "_delta_log"
    log.mkdir(parents=True)
    (log / "00000000000000000000.json").write_text(json.dumps({"commitInfo": {"timestamp": 1_000_000}}))
    (log / "00000000000000000001.json").write_text(
        json.dumps({"txn": {}}) + "\n" + json.dumps({"commitInfo": {"timestamp": 1_600_000}}))
    assert sh.last_write_age_minutes(stream_dir=tmp_path, now=1_600 + 300) == pytest.approx(5.0)


def test_dlq_needs_both_many_and_a_high_share():
    assert "2 broken" in sh.check_dlq(broken=2, orders=1000)        # normal background noise
    assert sh.check_dlq(broken=20, orders=1000)                      # 20 is many, but only 2%: fine
    with pytest.raises(sh.CheckFailed, match="bad data"):
        sh.check_dlq(broken=60, orders=1000)                         # 6% and 60: a producer problem
