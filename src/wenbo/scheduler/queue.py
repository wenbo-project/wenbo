"""Ready pool for schedulable node candidates."""

from __future__ import annotations

import time
from collections import deque
from typing import Any

from wenbo.scheduler.types import SchedulingCandidate


class ReadyQueue:
    """FIFO ready pool deduplicated by (dag_id, node_id), with lazy deletion."""

    def __init__(self) -> None:
        self._queue: deque[tuple[str, str, Any]] = deque()
        self._items: dict[tuple[str, str], tuple[str, str, Any]] = {}
        self._keys_by_dag: dict[str, set[tuple[str, str]]] = {}
        self._tombstones = 0

    def enqueue(self, dag: Any, node: Any) -> None:
        # Deduplicate by DAG and node ID so repeated events do not enqueue a node twice.
        """Append a node once; repeated readiness events preserve its queue position."""
        key = (dag.dag_id, node.node_id)
        if key in self._items:
            return
        item = (dag.dag_id, node.node_id, node)
        self._items[key] = item
        self._keys_by_dag.setdefault(dag.dag_id, set()).add(key)
        self._queue.append(item)

    def remove(self, dag_id: str, node_id: str) -> bool:
        """Remove a live entry and return whether it was present."""
        key = (dag_id, node_id)
        if key not in self._items:
            return False
        # The deque is compacted lazily. This keeps removal O(1) while
        # preserving FIFO order for the remaining live candidates.
        self._items.pop(key, None)
        dag_keys = self._keys_by_dag.get(dag_id)
        if dag_keys is not None:
            dag_keys.discard(key)
            if not dag_keys:
                self._keys_by_dag.pop(dag_id, None)
        self._tombstones += 1
        self._maybe_compact()
        return True

    def remove_dag(self, dag_id: str) -> int:
        """Remove all live entries for a graph and return their count."""
        keys = list(self._keys_by_dag.get(dag_id, ()))
        for _, node_id in keys:
            self.remove(dag_id, node_id)
        return len(keys)

    def clear(self) -> None:
        self._queue.clear()
        self._items.clear()
        self._keys_by_dag.clear()
        self._tombstones = 0

    def _maybe_compact(self) -> None:
        # Rebuilding occasionally makes lazy deletion amortized O(1) while
        # bounding memory and scan cost during long simulations.
        if self._tombstones < 64 or self._tombstones <= len(self._items):
            return
        self._queue = deque(
            item for item in self._queue if self._items.get((item[0], item[1])) is item
        )
        self._tombstones = 0

    def list_candidates(self, dags_by_id: Any) -> list[SchedulingCandidate]:
        """Build eligible candidates without consuming queue entries.

        dags_by_id supplies a get(dag_id) lookup, normally DAGRegistry.
        Only READY nodes in RUNNING graphs whose monotonic retry deadline
        has passed are included. Candidate order follows live queue order.
        """
        candidates: list[SchedulingCandidate] = []
        ready_order = 0
        now = time.monotonic()
        for item in self._queue:
            dag_id, node_id, node = item
            key = (dag_id, node_id)
            if self._items.get(key) is not item:
                continue
            dag = dags_by_id.get(dag_id)
            state = getattr(node, "state", None)
            state_value = getattr(state, "value", state)
            dag_state = getattr(getattr(dag, "state", None), "value", None)
            if dag is None or dag_state != "running" or state_value != "ready":
                continue
            # Exclude nodes whose retry backoff has not yet expired from this scheduling round.
            if float(getattr(node, "retry_not_before", 0.0) or 0.0) > now:
                continue
            candidates.append(
                SchedulingCandidate(
                    dag_id=dag_id,
                    agent_id=dag.agent_id,
                    dag_priority=dag.priority,
                    node_id=node_id,
                    node=node,
                    node_priority=node.priority,
                    estimated_tokens=node.estimated_tokens,
                    ready_order=ready_order,
                )
            )
            ready_order += 1
        return candidates

    def get_by_dag(self, dag_id: str) -> list[tuple[str, str, Any]]:
        return [
            item
            for item in self._queue
            if item[0] == dag_id and self._items.get((item[0], item[1])) is item
        ]

    def peek(self, count: int = 1) -> list[tuple[str, str, Any]]:
        result: list[tuple[str, str, Any]] = []
        for item in self._queue:
            if self._items.get((item[0], item[1])) is not item:
                continue
            result.append(item)
            if len(result) >= count:
                break
        return result

    @property
    def size(self) -> int:
        return len(self._items)

    def __len__(self) -> int:
        return self.size

    def __bool__(self) -> bool:
        return self.size > 0


__all__ = ["ReadyQueue"]
