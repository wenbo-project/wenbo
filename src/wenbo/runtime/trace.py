"""Normalized scheduling traces shared by runtime and simulation engines."""

from __future__ import annotations

import json
import math
import os
import weakref
from collections import Counter
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import IO
from typing import Any
from uuid import uuid4


TRACE_STREAM_SCHEMA_VERSION = "wenbo.scheduling-trace.v1"


def _percentile(sorted_values: list[float], quantile: float) -> float | None:
    """Return a linearly interpolated percentile for an already sorted list."""
    if not sorted_values:
        return None
    position = (len(sorted_values) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[lower]
    fraction = position - lower
    return (
        sorted_values[lower] * (1.0 - fraction)
        + sorted_values[upper] * fraction
    )


class TraceMode(str, Enum):
    """Amount of trace data retained by a collector."""

    SUMMARY = "summary"
    FULL = "full"
    STREAM = "stream"

    @classmethod
    def parse(cls, value: "TraceMode | str") -> "TraceMode":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as exc:
            supported = ", ".join(mode.value for mode in cls)
            raise ValueError(
                f"unsupported trace mode {value!r}; expected one of: {supported}"
            ) from exc


class TraceEventType(str, Enum):
    DAG_ARRIVED = "dag.arrived"
    DAG_COMPLETED = "dag.completed"
    DAG_FAILED = "dag.failed"
    DAG_CANCELLED = "dag.cancelled"
    NODE_READY = "node.ready"
    NODE_STARTED = "node.started"
    NODE_COMPLETED = "node.completed"
    NODE_FAILED = "node.failed"
    NODE_CANCELLED = "node.cancelled"
    NODE_SKIPPED = "node.skipped"
    RESOURCE_ACQUIRED = "resource.acquired"
    RESOURCE_RELEASED = "resource.released"


@dataclass(frozen=True)
class TraceEvent:
    sequence: int
    timestamp_s: float
    clock: str
    event_type: str
    runner: str
    dag_id: str
    node_id: str | None = None
    resource_type: str | None = None
    policy_id: str | None = None
    scheduling_round: int | None = None
    worker_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in asdict(self).items()
            if value is not None and value != {}
        }


def _cleanup_incomplete_stream(stream: IO[str], temporary_path: Path) -> None:
    try:
        if not stream.closed:
            stream.close()
    finally:
        temporary_path.unlink(missing_ok=True)


class _JsonlTraceWriter:
    """Write one trace atomically without retaining its events in memory."""

    def __init__(self, path: str | Path, *, runner: str, clock: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._temporary_path = self.path.with_name(
            f".{self.path.name}.{uuid4().hex}.tmp"
        )
        self._stream = self._temporary_path.open(
            "x",
            encoding="utf-8",
            newline="\n",
        )
        self._finished = False
        self._finalizer = weakref.finalize(
            self,
            _cleanup_incomplete_stream,
            self._stream,
            self._temporary_path,
        )
        try:
            self._write({
                "record_type": "trace.header",
                "schema_version": TRACE_STREAM_SCHEMA_VERSION,
                "runner": runner,
                "clock": clock,
            })
        except Exception:
            self.abort()
            raise

    def write_event(self, event: dict[str, Any]) -> None:
        if self._finished:
            raise RuntimeError("trace stream is already closed")
        try:
            self._write({"record_type": "trace.event", **event})
        except Exception:
            self.abort()
            raise

    def finish(self, summary: dict[str, Any]) -> None:
        if self._finished:
            return
        try:
            self._write({
                "record_type": "trace.footer",
                "schema_version": TRACE_STREAM_SCHEMA_VERSION,
                "completed": True,
                "event_count": summary["event_count"],
                "last_sequence": summary["event_count"] - 1,
                "summary": summary,
            })
            self._stream.flush()
            os.fsync(self._stream.fileno())
            self._stream.close()
            os.replace(self._temporary_path, self.path)
        except Exception:
            self.abort()
            raise
        self._finished = True
        self._finalizer.detach()

    def abort(self) -> None:
        if self._finished:
            return
        self._finished = True
        self._finalizer()

    def _write(self, record: dict[str, Any]) -> None:
        self._stream.write(
            json.dumps(
                record,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        self._stream.write("\n")


class SchedulingTrace:
    """Collect counts and optionally retain or stream individual events."""

    def __init__(
        self,
        *,
        runner: str,
        clock: str,
        mode: TraceMode | str = TraceMode.SUMMARY,
        stream_path: str | Path | None = None,
    ) -> None:
        self.runner = str(runner)
        self.clock = str(clock)
        self.mode = TraceMode.parse(mode)
        if self.mode is TraceMode.STREAM and stream_path is None:
            raise ValueError("stream trace mode requires stream_path")
        if self.mode is not TraceMode.STREAM and stream_path is not None:
            raise ValueError("stream_path is only supported in stream trace mode")
        self._counts: Counter[str] = Counter()
        self._events: list[TraceEvent] = []
        self._event_count = 0
        self._first_timestamp_s: float | None = None
        self._last_timestamp_s: float | None = None
        self._dag_arrival_times: dict[str, float] = {}
        self._dag_completion_latencies: list[float] = []
        # Keep absolute completion times even in summary mode.  Benchmark
        # callers can therefore evaluate a fixed observation window without
        # retaining the full event stream.
        self._dag_completion_timestamps: list[float] = []
        self._closed = False
        self._stream_writer = (
            _JsonlTraceWriter(
                stream_path,
                runner=self.runner,
                clock=self.clock,
            )
            if stream_path is not None
            else None
        )

    def record(
        self,
        event_type: TraceEventType | str,
        timestamp_s: float,
        dag_id: str,
        *,
        node_id: str | None = None,
        resource_type: str | None = None,
        policy_id: str | None = None,
        scheduling_round: int | None = None,
        worker_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        if self._closed:
            raise RuntimeError("trace is already closed")
        event_name = (
            event_type.value
            if isinstance(event_type, TraceEventType)
            else str(event_type)
        )
        timestamp = float(timestamp_s)
        if not math.isfinite(timestamp):
            raise ValueError("trace timestamp must be finite")
        if (
            self._last_timestamp_s is not None
            and timestamp < self._last_timestamp_s
        ):
            raise ValueError("trace timestamps must be monotonically non-decreasing")
        sequence = self._event_count
        self._event_count += 1
        self._counts[event_name] += 1
        if self._first_timestamp_s is None:
            self._first_timestamp_s = timestamp
        self._last_timestamp_s = timestamp
        dag_key = str(dag_id)
        if event_name == TraceEventType.DAG_ARRIVED.value:
            self._dag_arrival_times[dag_key] = timestamp
        elif event_name == TraceEventType.DAG_COMPLETED.value:
            arrival = self._dag_arrival_times.pop(dag_key, None)
            if arrival is not None:
                latency = timestamp - arrival
                if latency < 0:
                    raise ValueError("DAG completion precedes its arrival")
                self._dag_completion_latencies.append(latency)
            self._dag_completion_timestamps.append(timestamp)
        if self.mode is TraceMode.SUMMARY:
            return
        event = TraceEvent(
            sequence=sequence,
            timestamp_s=timestamp,
            clock=self.clock,
            event_type=event_name,
            runner=self.runner,
            dag_id=str(dag_id),
            node_id=str(node_id) if node_id is not None else None,
            resource_type=resource_type,
            policy_id=policy_id,
            scheduling_round=scheduling_round,
            worker_id=worker_id,
            details=dict(details or {}),
        )
        if self.mode is TraceMode.FULL:
            self._events.append(event)
            return
        try:
            assert self._stream_writer is not None
            self._stream_writer.write_event(event.to_dict())
        except Exception:
            self._closed = True
            raise

    def close(self) -> None:
        if self._closed:
            return
        if self._stream_writer is not None:
            summary = self._summary(stream_completed=True)
            try:
                self._stream_writer.finish(summary)
            except Exception:
                self._closed = True
                raise
        self._closed = True

    def abort(self) -> None:
        if self._closed:
            return
        if self._stream_writer is not None:
            self._stream_writer.abort()
        self._closed = True

    @property
    def events(self) -> list[dict[str, Any]]:
        return [event.to_dict() for event in self._events]

    @property
    def summary(self) -> dict[str, Any]:
        return self._summary(
            stream_completed=(
                self.mode is TraceMode.STREAM and self._closed
            )
        )

    @property
    def dag_completion_times(self) -> list[float]:
        """Absolute virtual completion times in deterministic event order."""
        return list(self._dag_completion_timestamps)

    def _summary(self, *, stream_completed: bool) -> dict[str, Any]:
        dag_latencies = sorted(self._dag_completion_latencies)
        summary = {
            "mode": self.mode.value,
            "clock": self.clock,
            "event_count": self._event_count,
            "retained_event_count": len(self._events),
            "first_timestamp_s": self._first_timestamp_s,
            "last_timestamp_s": self._last_timestamp_s,
            "event_counts": dict(sorted(self._counts.items())),
            "dag_completion_latency_count": len(dag_latencies),
            "dag_completion_latency_mean_s": (
                sum(dag_latencies) / len(dag_latencies)
                if dag_latencies
                else None
            ),
            "dag_completion_latency_p50_s": _percentile(dag_latencies, 0.50),
            "dag_completion_latency_p95_s": _percentile(dag_latencies, 0.95),
            "dag_completion_latency_p99_s": _percentile(dag_latencies, 0.99),
        }
        if self._stream_writer is not None:
            summary.update({
                "stream_path": str(self._stream_writer.path),
                "stream_schema_version": TRACE_STREAM_SCHEMA_VERSION,
                "stream_completed": stream_completed,
            })
        return summary


__all__ = [
    "SchedulingTrace",
    "TRACE_STREAM_SCHEMA_VERSION",
    "TraceEvent",
    "TraceEventType",
    "TraceMode",
]
