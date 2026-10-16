"""Timeline and scheduling-decision collection."""

from __future__ import annotations

import time
from typing import Any

from agentx.runtime.event import Event, EventType
from agentx.runtime.trace import SchedulingTrace, TraceEventType, TraceMode


_EVENT_TYPE_MAP = {
    EventType.DAG_COMPLETED: TraceEventType.DAG_COMPLETED,
    EventType.DAG_FAILED: TraceEventType.DAG_FAILED,
    EventType.DAG_CANCELLED: TraceEventType.DAG_CANCELLED,
    EventType.NODE_READY: TraceEventType.NODE_READY,
    EventType.NODE_STARTED: TraceEventType.NODE_STARTED,
    EventType.NODE_COMPLETED: TraceEventType.NODE_COMPLETED,
    EventType.NODE_FAILED: TraceEventType.NODE_FAILED,
    EventType.NODE_CANCELLED: TraceEventType.NODE_CANCELLED,
    EventType.NODE_SKIPPED: TraceEventType.NODE_SKIPPED,
}


class TimelineCollector:
    def __init__(self, policy_id: str | None = None) -> None:
        self._timeline: list[dict[str, Any]] = []
        self._decisions: list[dict[str, Any]] = []
        self._policy_id = policy_id
        self._trace = SchedulingTrace(
            runner="runtime",
            clock="monotonic",
            mode=TraceMode.FULL,
        )

    def record(
        self,
        dag_id: str,
        node_id: str,
        event: str,
        *,
        worker_id: str | None = None,
    ) -> None:
        entry = {
            "dag_id": dag_id,
            "node_id": node_id,
            "event": event,
            "time": time.monotonic(),
        }
        if worker_id is not None:
            entry["worker_id"] = worker_id
        self._timeline.append(entry)

    def record_dag_arrived(self, dag_id: str, scheduling_round: int = 0) -> None:
        self._trace.record(
            TraceEventType.DAG_ARRIVED,
            time.monotonic(),
            dag_id,
            policy_id=self._policy_id,
            scheduling_round=scheduling_round,
        )

    def record_event(self, event: Event, scheduling_round: int = 0) -> None:
        event_type = _EVENT_TYPE_MAP.get(event.event_type)
        if event_type is None or event.dag_id is None:
            return
        self._trace.record(
            event_type,
            time.monotonic(),
            event.dag_id,
            node_id=event.node_id,
            resource_type=(
                str(event.data["resource_type"])
                if "resource_type" in event.data
                else None
            ),
            policy_id=self._policy_id,
            scheduling_round=scheduling_round,
            worker_id=event.worker_id,
        )

    def record_resource(
        self,
        event_type: TraceEventType,
        dag_id: str,
        node_id: str,
        resource_type: str,
        *,
        scheduling_round: int,
        worker_id: str | None = None,
        policy_id: str | None = None,
    ) -> None:
        if event_type not in {
            TraceEventType.RESOURCE_ACQUIRED,
            TraceEventType.RESOURCE_RELEASED,
        }:
            raise ValueError("record_resource requires a resource trace event type")
        self._trace.record(
            event_type,
            time.monotonic(),
            dag_id,
            node_id=node_id,
            resource_type=resource_type,
            policy_id=policy_id or self._policy_id,
            scheduling_round=scheduling_round,
            worker_id=worker_id,
        )

    def record_trace(
        self,
        event_type: TraceEventType,
        dag_id: str,
        *,
        node_id: str | None = None,
        resource_type: str | None = None,
        scheduling_round: int = 0,
        worker_id: str | None = None,
        policy_id: str | None = None,
    ) -> None:
        self._trace.record(
            event_type,
            time.monotonic(),
            dag_id,
            node_id=node_id,
            resource_type=resource_type,
            policy_id=policy_id or self._policy_id,
            scheduling_round=scheduling_round,
            worker_id=worker_id,
        )

    def record_decisions(
        self,
        decisions: list[Any],
        worker_assignments: dict[tuple[str, str], str] | None = None,
    ) -> None:
        recorded_at = time.monotonic()
        worker_assignments = worker_assignments or {}
        for rank, decision in enumerate(decisions):
            key = (decision.candidate.dag_id, decision.candidate.node_id)
            self._decisions.append({
                "time": recorded_at,
                "rank": rank,
                "dag_id": decision.candidate.dag_id,
                "node_id": decision.candidate.node_id,
                "policy_name": decision.policy_name,
                "policy_version": decision.policy_version,
                "policy_id": decision.policy_id,
                "score": decision.score,
                "reason": decision.reason,
                **({"worker_id": worker_assignments[key]} if key in worker_assignments else {}),
            })

    @property
    def timeline(self) -> list[dict[str, Any]]:
        return list(self._timeline)

    @property
    def decisions(self) -> list[dict[str, Any]]:
        return list(self._decisions)

    @property
    def trace(self) -> list[dict[str, Any]]:
        return self._trace.events

    @property
    def trace_summary(self) -> dict[str, Any]:
        return self._trace.summary


__all__ = ["TimelineCollector"]
