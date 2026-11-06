"""
Records how the consumer performs, one JSON line per micro-batch, for the performance report.

Spark calls a LISTENER after every micro-batch of every job with its "progress": how many
events it read, how fast, how long the batch took, how far behind Kafka it is. We append each
one to a file in Project 2's data folder, next to the live tables:

    data/stream/_metrics/consumer_batches.jsonl     one line per micro-batch (all three jobs)
    data/stream/_metrics/consumer_events.jsonl      job started / stopped (restarts, crashes)

streaming/perf_report.py reads these files (and the producer's runs) and draws the graphs.
The file only grows by a few hundred bytes per batch: about 10 MB per day of non-stop running.
"""
from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql.streaming import StreamingQueryListener

from consumer import settings


def _lag(source) -> int | None:
    """Kafka sources report maxOffsetsBehindLatest: the biggest lag of any partition."""
    value = (source.metrics or {}).get("maxOffsetsBehindLatest") if source else None
    return int(value) if value not in (None, "") else None


def _total_lag(source) -> int | None:
    """The lag of ALL partitions together: latest offset in Kafka − end offset of this batch."""
    try:
        latest, end = json.loads(source.latestOffset), json.loads(source.endOffset)
        return sum(latest[t][p] - end[t][p] for t in latest for p in latest[t])
    except (TypeError, ValueError, KeyError, AttributeError):
        return None                                     # not a Kafka source (job 2 reads Delta)


def batch_record(progress) -> dict:
    source = progress.sources[0] if progress.sources else None
    return {
        "ts": progress.timestamp,                       # when the micro-batch started (UTC)
        "job": progress.name,
        "batch": progress.batchId,
        "rows": progress.numInputRows,
        "in_per_s": round(progress.inputRowsPerSecond or 0, 1),      # arrival rate since last batch
        "done_per_s": round(progress.processedRowsPerSecond or 0, 1),  # how fast this batch went
        "duration_ms": (progress.durationMs or {}).get("triggerExecution"),
        "lag": _total_lag(source),
        "lag_max_partition": _lag(source),
        "state_rows": sum(op.numRowsTotal for op in progress.stateOperators),
        # the settings in force, so the report can compare runs with different tuning
        "max_offsets": settings.MAX_OFFSETS_PER_TRIGGER,
        "trigger": settings.TRIGGER,
    }


class MetricsRecorder(StreamingQueryListener):
    def __init__(self, folder: Path | None = None):
        self.folder = Path(folder or settings.METRICS_DIR)
        self.folder.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()                  # the three jobs report from different threads
        self._names: dict[str, str] = {}               # query id → job name (a "stopped" event has only the id)

    def _append(self, file: str, record: dict) -> None:
        with self._lock, open(self.folder / file, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def _event(self, event_type: str, name: str | None, **extra) -> None:
        self._append("consumer_events.jsonl",
                     {"ts": datetime.now(UTC).isoformat(timespec="milliseconds"), "event": event_type,
                      "job": name, **extra})

    def onQueryStarted(self, event):
        self._names[str(event.id)] = event.name
        self._event("started", event.name)

    def onQueryProgress(self, event):
        self._append("consumer_batches.jsonl", batch_record(event.progress))

    def onQueryIdle(self, event):                      # no new data: nothing to record
        pass

    def onQueryTerminated(self, event):
        self._event("stopped", self._names.get(str(event.id)), error=event.exception)
