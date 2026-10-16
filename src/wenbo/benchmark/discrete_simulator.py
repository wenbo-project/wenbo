"""Discrete-event benchmark simulator for synthetic DAG scheduling.

The simulator uses one deterministic node-duration model for all methods and
advances virtual time instead of measuring wall-clock time. This keeps the
comparison focused on scheduling rules.
"""

from __future__ import annotations

import csv
import heapq
import json
import math
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any, Collection, Iterable, Iterator, Sequence

from wenbo.runtime.trace import SchedulingTrace, TraceEventType, TraceMode


POLICY_VERSIONS = {
    # Bind the abundant resource on admission; acquire the other resource on
    # first use and retain every acquired slot until the whole task completes.
    "parallel": ("task_lifetime_fifo", "1"),
    "wenbo": ("global_fifo", "1"),
}

DEFAULT_WENBO_ACTIVE_DAGS = 1024


class SimulationInvariantError(RuntimeError):
    """Raised when simulator state violates a scheduling invariant."""


@dataclass(frozen=True)
class DurationConfig:
    gpu_base_latency_ms: float = 2.0
    cpu_base_latency_ms: float = 1.0
    gpu_tokens_per_second: float = 20_000.0
    cpu_tokens_per_second: float = 8_000.0
    time_scale: float = 0.001
    min_latency_ms: float = 0.1


@dataclass
class SimNode:
    node_id: str
    hardware: str
    tokens: int
    duration_s: float
    predecessors: set[str] = field(default_factory=set)
    successors: set[str] = field(default_factory=set)


@dataclass
class SimDAG:
    dag_id: str
    nodes: dict[str, SimNode]
    node_order: dict[str, int]

    def clone(self) -> "SimDAG":
        nodes = {
            node_id: SimNode(
                node_id=node.node_id,
                hardware=node.hardware,
                tokens=node.tokens,
                duration_s=node.duration_s,
                predecessors=set(node.predecessors),
                successors=set(node.successors),
            )
            for node_id, node in self.nodes.items()
        }
        return SimDAG(
            dag_id=self.dag_id,
            nodes=nodes,
            node_order=dict(self.node_order),
        )


@dataclass
class DAGRuntime:
    dag: SimDAG
    pending: set[str]
    ready: set[str]
    remaining_predecessors: dict[str, int]
    ready_heaps: dict[str, list[tuple[int, str]]] = field(default_factory=dict)
    running: set[str] = field(default_factory=set)
    completed: set[str] = field(default_factory=set)
    completed_tokens: int = 0
    gpu_busy: int = 0
    cpu_busy: int = 0
    check_invariants: bool = False

    @classmethod
    def from_dag(
        cls,
        dag: SimDAG,
        *,
        check_invariants: bool = False,
    ) -> "DAGRuntime":
        remaining = {
            node_id: len(node.predecessors)
            for node_id, node in dag.nodes.items()
        }
        ready = {
            node_id
            for node_id, count in remaining.items()
            if count == 0
        }
        runtime = cls(
            dag=dag,
            pending=set(dag.nodes) - ready,
            ready=ready,
            remaining_predecessors=remaining,
            ready_heaps={
                hardware: [
                    (dag.node_order[node_id], node_id)
                    for node_id in ready
                    if dag.nodes[node_id].hardware == hardware
                ]
                for hardware in ("gpu", "cpu")
            },
            check_invariants=check_invariants,
        )
        for ready_heap in runtime.ready_heaps.values():
            heapq.heapify(ready_heap)
        if check_invariants:
            runtime.assert_invariants()
        return runtime

    def is_completed(self) -> bool:
        return len(self.completed) == len(self.dag.nodes)

    def ready_nodes(self) -> list[SimNode]:
        return [
            self.dag.nodes[node_id]
            for node_id in sorted(self.ready, key=self._stable_node_key)
        ]

    def next_ready_node(
        self,
        *,
        gpu_available: bool,
        cpu_available: bool,
    ) -> SimNode | None:
        gpu_head = self._peek_ready("gpu") if gpu_available else None
        cpu_head = self._peek_ready("cpu") if cpu_available else None
        if gpu_head is None and cpu_head is None:
            return None
        if cpu_head is None or (gpu_head is not None and gpu_head <= cpu_head):
            return self.dag.nodes[gpu_head[1]]
        return self.dag.nodes[cpu_head[1]]

    def start(self, node_id: str) -> SimNode:
        if node_id not in self.ready:
            raise SimulationInvariantError(
                f"DAG {self.dag.dag_id} node {node_id} started outside READY state"
            )
        node = self.dag.nodes[node_id]
        if not node.predecessors <= self.completed:
            raise SimulationInvariantError(
                f"DAG {self.dag.dag_id} node {node_id} started before its predecessors"
            )
        ready_heap = self.ready_heaps[node.hardware]
        if self._peek_ready(node.hardware) == self._stable_node_key(node_id):
            heapq.heappop(ready_heap)
        self.ready.remove(node_id)
        self.running.add(node_id)
        if self.check_invariants:
            self.assert_invariants()
        return node

    def complete(self, node_id: str) -> list[SimNode]:
        if node_id not in self.running:
            raise SimulationInvariantError(
                f"DAG {self.dag.dag_id} node {node_id} completed outside RUNNING state"
            )
        self.running.remove(node_id)
        self.completed.add(node_id)
        newly_ready: list[SimNode] = []
        node = self.dag.nodes[node_id]
        self.completed_tokens += node.tokens
        for successor_id in sorted(node.successors, key=self._stable_node_key):
            remaining = self.remaining_predecessors[successor_id]
            if remaining <= 0:
                raise SimulationInvariantError(
                    f"DAG {self.dag.dag_id} node {successor_id} dependency "
                    "counter was decremented more than once"
                )
            remaining -= 1
            self.remaining_predecessors[successor_id] = remaining
            if remaining == 0:
                if successor_id not in self.pending:
                    raise SimulationInvariantError(
                        f"DAG {self.dag.dag_id} node {successor_id} became READY "
                        "from an invalid state"
                    )
                self.pending.remove(successor_id)
                self.ready.add(successor_id)
                heapq.heappush(
                    self.ready_heaps[self.dag.nodes[successor_id].hardware],
                    self._stable_node_key(successor_id),
                )
                newly_ready.append(self.dag.nodes[successor_id])
        if self.check_invariants:
            self.assert_invariants()
        return newly_ready

    def assert_invariants(self) -> None:
        state_sets = (self.pending, self.ready, self.running, self.completed)
        all_nodes = set().union(*state_sets)
        if all_nodes != set(self.dag.nodes):
            raise SimulationInvariantError(
                f"DAG {self.dag.dag_id} runtime state does not cover every node"
            )
        for index, left in enumerate(state_sets):
            for right in state_sets[index + 1:]:
                if left & right:
                    raise SimulationInvariantError(
                        f"DAG {self.dag.dag_id} node exists in multiple runtime states"
                    )
        advanced = self.ready | self.running | self.completed
        for node_id, node in self.dag.nodes.items():
            expected = sum(
                predecessor_id not in self.completed
                for predecessor_id in node.predecessors
            )
            if self.remaining_predecessors.get(node_id) != expected:
                raise SimulationInvariantError(
                    f"DAG {self.dag.dag_id} node {node_id} has an inconsistent "
                    "remaining predecessor count"
                )
            if node_id in advanced and expected != 0:
                raise SimulationInvariantError(
                    f"DAG {self.dag.dag_id} node {node_id} advanced before dependencies"
                )
        if self.gpu_busy < 0 or self.cpu_busy < 0:
            raise SimulationInvariantError(
                f"DAG {self.dag.dag_id} has a negative private resource count"
            )
        expected_tokens = sum(
            self.dag.nodes[node_id].tokens
            for node_id in self.completed
        )
        if self.completed_tokens != expected_tokens:
            raise SimulationInvariantError(
                f"DAG {self.dag.dag_id} has an inconsistent completed token count"
            )

    def _stable_node_key(self, node_id: str) -> tuple[int, str]:
        return (self.dag.node_order[node_id], node_id)

    def _peek_ready(self, hardware: str) -> tuple[int, str] | None:
        ready_heap = self.ready_heaps[hardware]
        while ready_heap and ready_heap[0][1] not in self.ready:
            heapq.heappop(ready_heap)
        return ready_heap[0] if ready_heap else None


@dataclass
class DiscreteResult:
    runner: str
    resource_count: int
    dag_count: int
    wall_clock_s: float
    completed_dags: int
    completed_nodes: int
    completed_tokens: int
    gpu_busy_s: float
    cpu_busy_s: float
    sum_node_exec_s: float
    gpu_count: int | None = None
    cpu_count: int | None = None
    policy_name: str = ""
    policy_version: str = "1"
    peak_active_dags: int = 0
    trace_summary: dict[str, Any] = field(default_factory=dict)
    trace_events: list[dict[str, Any]] = field(default_factory=list, repr=False)
    dag_completion_times: list[float] = field(default_factory=list, repr=False)

    @property
    def policy_id(self) -> str:
        return f"{self.policy_name}@{self.policy_version}"

    def to_row(self) -> dict[str, Any]:
        safe_wall = max(self.wall_clock_s, 0.000001)
        gpu_count = self.gpu_count if self.gpu_count is not None else self.resource_count
        cpu_count = self.cpu_count if self.cpu_count is not None else self.resource_count
        gpu_capacity_s = safe_wall * max(gpu_count, 1)
        cpu_capacity_s = safe_wall * max(cpu_count, 1)
        resource_capacity_s = gpu_capacity_s + cpu_capacity_s
        return {
            "resource_count": self.resource_count,
            "gpu_count": gpu_count,
            "cpu_count": cpu_count,
            "dag_count": self.dag_count,
            "runner": self.runner,
            "policy_name": self.policy_name,
            "policy_version": self.policy_version,
            "policy_id": self.policy_id,
            "wall_clock_s": round(self.wall_clock_s, 6),
            "completed_dags": self.completed_dags,
            "completed_nodes": self.completed_nodes,
            "completed_tokens": self.completed_tokens,
            "peak_active_dags": self.peak_active_dags,
            "dags_per_second": round(self.completed_dags / safe_wall, 6),
            "nodes_per_second": round(self.completed_nodes / safe_wall, 6),
            "tokens_per_second": round(self.completed_tokens / safe_wall, 6),
            "gpu_busy_s": round(self.gpu_busy_s, 6),
            "cpu_busy_s": round(self.cpu_busy_s, 6),
            "sum_node_exec_s": round(self.sum_node_exec_s, 6),
            "parallelism": round(self.sum_node_exec_s / safe_wall, 6),
            "resource_utilization_pct": round(
                self.sum_node_exec_s / resource_capacity_s * 100.0,
                3,
            ),
            "gpu_utilization_pct": round(self.gpu_busy_s / gpu_capacity_s * 100.0, 3),
            "cpu_utilization_pct": round(self.cpu_busy_s / cpu_capacity_s * 100.0, 3),
            "trace_event_count": int(self.trace_summary.get("event_count", 0)),
            "dag_jct_mean_s": self.trace_summary.get(
                "dag_completion_latency_mean_s"
            ),
            "dag_jct_p50_s": self.trace_summary.get(
                "dag_completion_latency_p50_s"
            ),
            "dag_jct_p95_s": self.trace_summary.get(
                "dag_completion_latency_p95_s"
            ),
            "dag_jct_p99_s": self.trace_summary.get(
                "dag_completion_latency_p99_s"
            ),
        }

    def to_payload(self) -> dict[str, Any]:
        payload = self.to_row()
        payload["trace"] = {"summary": dict(self.trace_summary)}
        if self.trace_events:
            payload["trace"]["events"] = list(self.trace_events)
        return payload


@dataclass
class _SimulationTotals:
    dag_count: int = 0
    completed_dags: int = 0
    completed_nodes: int = 0
    completed_tokens: int = 0
    peak_active_dags: int = 0

    def admit(self, active_dags: int) -> None:
        self.dag_count += 1
        self.peak_active_dags = max(self.peak_active_dags, active_dags)

    def complete(self, runtime: DAGRuntime) -> None:
        if not runtime.is_completed():
            raise SimulationInvariantError(
                f"DAG {runtime.dag.dag_id} was released before completion"
            )
        if runtime.pending or runtime.ready or runtime.running:
            raise SimulationInvariantError(
                f"DAG {runtime.dag.dag_id} retained non-terminal node states"
            )
        if runtime.gpu_busy != 0 or runtime.cpu_busy != 0:
            raise SimulationInvariantError(
                f"DAG {runtime.dag.dag_id} retained private resources"
            )
        self.completed_dags += 1
        self.completed_nodes += len(runtime.completed)
        self.completed_tokens += runtime.completed_tokens


class _IncrementalJSONReader:
    """Decode consecutive JSON values while retaining at most one value."""

    def __init__(self, stream: IO[str], *, chunk_size: int) -> None:
        if chunk_size <= 0:
            raise ValueError("JSON chunk size must be a positive integer")
        self._stream = stream
        self._chunk_size = chunk_size
        self._buffer = ""
        self._position = 0
        self._eof = False
        self._decoder = json.JSONDecoder()

    def peek(self) -> str | None:
        self._skip_whitespace()
        if not self._ensure_available():
            return None
        return self._buffer[self._position]

    def expect(self, expected: str) -> None:
        actual = self.peek()
        if actual != expected:
            raise ValueError(
                f"invalid DAG JSON: expected {expected!r}, found {actual!r}"
            )
        self._position += 1

    def value(self) -> Any:
        self._skip_whitespace()
        while True:
            if not self._ensure_available():
                raise ValueError("invalid DAG JSON: expected a value before end of file")
            try:
                value, end = self._decoder.raw_decode(
                    self._buffer,
                    self._position,
                )
            except json.JSONDecodeError as exc:
                if self._eof:
                    raise ValueError(f"invalid DAG JSON: {exc.msg}") from exc
                self._read_more()
                continue
            self._position = end
            return value

    def ensure_finished(self) -> None:
        if self.peek() is not None:
            raise ValueError("invalid DAG JSON: trailing content after top-level value")

    def _skip_whitespace(self) -> None:
        while self._ensure_available():
            if not self._buffer[self._position].isspace():
                return
            self._position += 1

    def _ensure_available(self) -> bool:
        while self._position >= len(self._buffer) and not self._eof:
            self._read_more()
        return self._position < len(self._buffer)

    def _read_more(self) -> None:
        if self._position:
            self._buffer = self._buffer[self._position :]
            self._position = 0
        chunk = self._stream.read(self._chunk_size)
        if chunk == "":
            self._eof = True
        else:
            self._buffer += chunk


def _iter_json_array(reader: _IncrementalJSONReader) -> Iterator[Any]:
    reader.expect("[")
    if reader.peek() == "]":
        reader.expect("]")
        return
    while True:
        yield reader.value()
        delimiter = reader.peek()
        if delimiter == ",":
            reader.expect(",")
            continue
        if delimiter == "]":
            reader.expect("]")
            return
        raise ValueError(
            f"invalid DAG JSON array: expected ',' or ']', found {delimiter!r}"
        )


def _iter_json_records(
    path: Path,
    *,
    chunk_size: int,
) -> Iterator[dict[str, Any]]:
    if path.suffix.lower() == ".jsonl":
        with path.open("r", encoding="utf-8-sig") as stream:
            for line_number, raw_line in enumerate(stream, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    value = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"invalid DAG JSONL at line {line_number}: {exc.msg}"
                    ) from exc
                if not isinstance(value, dict):
                    raise ValueError(
                        f"DAG JSONL line {line_number} must contain an object"
                    )
                yield value
        return

    with path.open("r", encoding="utf-8-sig") as stream:
        reader = _IncrementalJSONReader(stream, chunk_size=chunk_size)
        first = reader.peek()
        if first == "[":
            values = _iter_json_array(reader)
        elif first == "{":
            reader.expect("{")
            found_dags = False
            while reader.peek() != "}":
                key = reader.value()
                if not isinstance(key, str):
                    raise ValueError("DAG input object keys must be strings")
                reader.expect(":")
                if key == "dags":
                    if found_dags:
                        raise ValueError("DAG input object contains duplicate 'dags' keys")
                    found_dags = True
                    for value in _iter_json_array(reader):
                        if not isinstance(value, dict):
                            raise ValueError("each DAG definition must be an object")
                        yield value
                else:
                    reader.value()
                delimiter = reader.peek()
                if delimiter == ",":
                    reader.expect(",")
                    continue
                if delimiter != "}":
                    raise ValueError(
                        "invalid DAG input object: expected ',' or '}'"
                    )
            reader.expect("}")
            if not found_dags:
                raise ValueError("DAG input object must contain a 'dags' array")
            reader.ensure_finished()
            return
        else:
            raise ValueError("DAG input must be an array or an object containing 'dags'")

        for value in values:
            if not isinstance(value, dict):
                raise ValueError("each DAG definition must be an object")
            yield value
        reader.ensure_finished()


def iter_sim_dags(
    file_path: str | Path,
    limit: int | None = None,
    duration_config: DurationConfig | None = None,
    resource_estimate_file: str | Path | None = None,
    *,
    chunk_size: int = 1024 * 1024,
    validate_unique_ids: bool = True,
) -> Iterator[SimDAG]:
    """Yield validated DAGs without retaining the complete input document.

    Resource estimates are merged in input order. Set ``validate_unique_ids``
    to false for strictly bounded memory; per-DAG structural validation remains
    enabled, but duplicate IDs separated by more than the active window are not
    detected.
    """
    duration_config = duration_config or DurationConfig()
    _validate_duration_config(duration_config)
    if limit is not None and (
        not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0
    ):
        raise ValueError("DAG limit must be a positive integer")
    if (
        not isinstance(chunk_size, int)
        or isinstance(chunk_size, bool)
        or chunk_size <= 0
    ):
        raise ValueError("JSON chunk size must be a positive integer")
    if not isinstance(validate_unique_ids, bool):
        raise ValueError("validate_unique_ids must be a boolean")

    raw_dags = _iter_json_records(Path(file_path), chunk_size=chunk_size)
    raw_estimates = (
        _iter_json_records(Path(resource_estimate_file), chunk_size=chunk_size)
        if resource_estimate_file
        else None
    )

    def generate() -> Iterator[SimDAG]:
        seen_ids: set[str] | None = set() if validate_unique_ids else None
        yielded = 0
        estimates = iter(raw_estimates) if raw_estimates is not None else None
        raw_dag_source = iter(raw_dags)
        while limit is None or yielded < limit:
            try:
                raw_dag = next(raw_dag_source)
            except StopIteration:
                break
            estimate_nodes: dict[str, dict[str, Any]] = {}
            if estimates is not None:
                try:
                    raw_estimate = next(estimates)
                except StopIteration as exc:
                    raise ValueError(
                        "resource estimate input ended before the DAG input"
                    ) from exc
                dag_id = str(raw_dag.get("dag_id", ""))
                estimate_dag_id = str(raw_estimate.get("dag_id", ""))
                if dag_id != estimate_dag_id:
                    raise ValueError(
                        "streaming resource estimates must use the same DAG order; "
                        f"expected {dag_id!r}, found {estimate_dag_id!r}"
                    )
                estimate_nodes = _estimate_nodes(raw_estimate)
            dag = _build_sim_dag(raw_dag, estimate_nodes, duration_config)
            if seen_ids is not None:
                if dag.dag_id in seen_ids:
                    raise ValueError(f"duplicate DAG id: {dag.dag_id}")
                seen_ids.add(dag.dag_id)
            yielded += 1
            yield dag
            del dag
        if yielded == 0:
            raise ValueError("at least one DAG is required")

    return generate()


def load_sim_dags(
    file_path: str | Path,
    limit: int | None = None,
    duration_config: DurationConfig | None = None,
    resource_estimate_file: str | Path | None = None,
) -> list[SimDAG]:
    """Load all DAGs, preserving ID-based resource-estimate compatibility."""
    duration_config = duration_config or DurationConfig()
    _validate_duration_config(duration_config)
    if limit is not None and (
        not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0
    ):
        raise ValueError("DAG limit must be a positive integer")
    estimates_by_dag = _load_resource_estimates(resource_estimate_file)
    dags: list[SimDAG] = []
    dag_ids: set[str] = set()
    for raw_dag in _iter_json_records(Path(file_path), chunk_size=1024 * 1024):
        if limit is not None and len(dags) >= limit:
            break
        dag_id = str(raw_dag.get("dag_id", ""))
        dag = _build_sim_dag(
            raw_dag,
            estimates_by_dag.get(dag_id, {}),
            duration_config,
        )
        if dag.dag_id in dag_ids:
            raise ValueError(f"duplicate DAG id: {dag.dag_id}")
        dag_ids.add(dag.dag_id)
        dags.append(dag)
    if not dags:
        raise ValueError("at least one DAG is required")
    return dags


def _build_sim_dag(
    raw_dag: dict[str, Any],
    estimate_nodes: dict[str, dict[str, Any]],
    duration_config: DurationConfig,
) -> SimDAG:
    """Build one DAG while enforcing every invariant implied by raw input."""
    dag_id = str(raw_dag.get("dag_id", ""))
    if not dag_id:
        raise ValueError("DAG id must not be empty")
    nodes = {}
    node_order = {}
    raw_nodes = raw_dag.get("nodes", [])
    if not isinstance(raw_nodes, list):
        raise ValueError(f"DAG {dag_id} nodes must be an array")
    if not raw_nodes:
        raise ValueError(f"DAG {dag_id} has no nodes")
    for index, node_def in enumerate(raw_nodes):
        if not isinstance(node_def, dict):
            raise ValueError(f"DAG {dag_id} contains a non-object node")
        node_id = str(node_def.get("node_id", ""))
        if not node_id:
            raise ValueError(f"DAG {dag_id} contains a node without an id")
        if node_id in nodes:
            raise ValueError(f"DAG {dag_id} contains duplicate node id: {node_id}")
        estimate_def = estimate_nodes.get(node_id, {})
        effective_node = {**node_def, **estimate_def}
        hardware = _hardware(effective_node)
        if hardware not in {"gpu", "cpu"}:
            raise ValueError(
                f"DAG {dag_id} node {node_id} uses unsupported hardware "
                f"{hardware!r}"
            )
        tokens = _tokens(effective_node)
        if tokens < 0:
            raise ValueError(f"DAG {dag_id} node {node_id} has negative tokens")
        duration_s = _duration_s(
            hardware,
            tokens,
            duration_config,
            _explicit_time_ms(effective_node),
        )
        if not math.isfinite(duration_s) or duration_s < 0:
            raise ValueError(f"DAG {dag_id} node {node_id} has invalid duration")
        nodes[node_id] = SimNode(
            node_id=node_id,
            hardware=hardware,
            tokens=tokens,
            duration_s=duration_s,
        )
        node_order[node_id] = index

    raw_edges = raw_dag.get("edges", [])
    if not isinstance(raw_edges, list):
        raise ValueError(f"DAG {dag_id} edges must be an array")
    edge_pairs: set[tuple[str, str]] = set()
    for edge in raw_edges:
        if not isinstance(edge, dict):
            raise ValueError(f"DAG {dag_id} contains a non-object edge")
        source = str(edge.get("source", ""))
        target = str(edge.get("target", ""))
        if source not in nodes or target not in nodes:
            raise ValueError(
                f"DAG {dag_id} edge {source!r} -> {target!r} references "
                "a missing node"
            )
        if source == target:
            raise ValueError(f"DAG {dag_id} contains a self-loop at node {source}")
        if (source, target) in edge_pairs:
            raise ValueError(
                f"DAG {dag_id} contains duplicate edge: {source} -> {target}"
            )
        edge_pairs.add((source, target))
        nodes[source].successors.add(target)
        nodes[target].predecessors.add(source)

    dag = SimDAG(dag_id, nodes, node_order)
    _validate_acyclic(dag)
    return dag


def validate_sim_dags(dags: Sequence[SimDAG]) -> None:
    """Validate deterministic simulator inputs before scheduling starts."""
    if not dags:
        raise ValueError("at least one DAG is required")
    dag_ids: set[str] = set()
    for dag in dags:
        if not dag.dag_id:
            raise ValueError("DAG id must not be empty")
        if dag.dag_id in dag_ids:
            raise ValueError(f"duplicate DAG id: {dag.dag_id}")
        dag_ids.add(dag.dag_id)
        if not dag.nodes:
            raise ValueError(f"DAG {dag.dag_id} has no nodes")
        if set(dag.node_order) != set(dag.nodes):
            raise ValueError(f"DAG {dag.dag_id} node_order does not match its nodes")
        node_ids = set(dag.nodes)
        order_values = list(dag.node_order.values())
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in order_values
        ):
            raise ValueError(
                f"DAG {dag.dag_id} node order values must be non-negative integers"
            )
        if len(set(order_values)) != len(order_values):
            raise ValueError(f"DAG {dag.dag_id} has duplicate node order values")

        for node_id, node in dag.nodes.items():
            if node.node_id != node_id:
                raise ValueError(
                    f"DAG {dag.dag_id} node key {node_id!r} does not match "
                    f"node id {node.node_id!r}"
                )
            if node.hardware not in {"gpu", "cpu"}:
                raise ValueError(
                    f"DAG {dag.dag_id} node {node_id} uses unsupported hardware "
                    f"{node.hardware!r}"
                )
            if not math.isfinite(node.duration_s) or node.duration_s < 0:
                raise ValueError(
                    f"DAG {dag.dag_id} node {node_id} has invalid duration"
                )
            if node.tokens < 0:
                raise ValueError(
                    f"DAG {dag.dag_id} node {node_id} has negative tokens"
                )
            missing_predecessors = node.predecessors - node_ids
            missing_successors = node.successors - node_ids
            if missing_predecessors or missing_successors:
                raise ValueError(
                    f"DAG {dag.dag_id} node {node_id} references missing nodes"
                )
            for predecessor_id in node.predecessors:
                if node_id not in dag.nodes[predecessor_id].successors:
                    raise ValueError(
                        f"DAG {dag.dag_id} edge {predecessor_id} -> {node_id} "
                        "is not mirrored by the predecessor"
                    )
            for successor_id in node.successors:
                if node_id not in dag.nodes[successor_id].predecessors:
                    raise ValueError(
                        f"DAG {dag.dag_id} edge {node_id} -> {successor_id} "
                        "is not mirrored by the successor"
                    )
        _validate_acyclic(dag)


def _validate_acyclic(dag: SimDAG) -> None:
    indegree = {
        node_id: len(node.predecessors)
        for node_id, node in dag.nodes.items()
    }
    ready = deque(node_id for node_id, degree in indegree.items() if degree == 0)
    visited = 0
    while ready:
        node_id = ready.popleft()
        visited += 1
        for successor_id in dag.nodes[node_id].successors:
            indegree[successor_id] -= 1
            if indegree[successor_id] == 0:
                ready.append(successor_id)
    if visited != len(dag.nodes):
        raise ValueError(f"DAG {dag.dag_id} contains a cycle")


def _validate_duration_config(config: DurationConfig) -> None:
    values = {
        "gpu_base_latency_ms": config.gpu_base_latency_ms,
        "cpu_base_latency_ms": config.cpu_base_latency_ms,
        "time_scale": config.time_scale,
        "min_latency_ms": config.min_latency_ms,
    }
    for name, value in values.items():
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{name} must be a finite non-negative number")
    rates = {
        "gpu_tokens_per_second": config.gpu_tokens_per_second,
        "cpu_tokens_per_second": config.cpu_tokens_per_second,
    }
    for name, value in rates.items():
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be a finite positive number")


def _load_resource_estimates(
    resource_estimate_file: str | Path | None,
) -> dict[str, dict[str, dict[str, Any]]]:
    if not resource_estimate_file:
        return {}
    with Path(resource_estimate_file).open("r", encoding="utf-8") as f:
        data = json.load(f)
    raw_dags = data.get("dags", []) if isinstance(data, dict) else data
    estimates: dict[str, dict[str, dict[str, Any]]] = {}
    for dag in raw_dags:
        dag_id = dag.get("dag_id")
        if not dag_id:
            continue
        estimates[str(dag_id)] = _estimate_nodes(dag)
    return estimates


def _estimate_nodes(raw_dag: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_nodes = raw_dag.get("nodes", [])
    if not isinstance(raw_nodes, list):
        raise ValueError("resource estimate DAG nodes must be an array")
    return {
        str(node["node_id"]): node
        for node in raw_nodes
        if isinstance(node, dict) and "node_id" in node
    }


def run_all_methods(
    dags: Sequence[SimDAG],
    resource_count: int,
    gpu_count: int | None = None,
    cpu_count: int | None = None,
    parallel_active_limit: int | None = None,
    wenbo_active_limit: int | None = DEFAULT_WENBO_ACTIVE_DAGS,
    trace_mode: TraceMode | str = TraceMode.SUMMARY,
    trace_output_dir: str | Path | None = None,
    check_invariants: bool = False,
) -> list[DiscreteResult]:
    validate_sim_dags(dags)
    gpu_count, cpu_count = _validate_resources(resource_count, gpu_count, cpu_count)
    trace_mode = TraceMode.parse(trace_mode)
    trace_paths = _trace_paths(trace_mode, trace_output_dir)
    return [
        run_parallel(
            dags,
            resource_count,
            gpu_count,
            cpu_count,
            parallel_active_limit,
            trace_mode=trace_mode,
            trace_output_path=trace_paths.get("parallel"),
            check_invariants=check_invariants,
            validate_input=False,
        ),
        run_wenbo(
            dags,
            resource_count,
            gpu_count,
            cpu_count,
            active_limit=wenbo_active_limit,
            trace_mode=trace_mode,
            trace_output_path=trace_paths.get("wenbo"),
            check_invariants=check_invariants,
            validate_input=False,
        ),
    ]


def run_all_methods_from_file(
    file_path: str | Path,
    resource_count: int,
    *,
    limit: int | None = None,
    duration_config: DurationConfig | None = None,
    resource_estimate_file: str | Path | None = None,
    gpu_count: int | None = None,
    cpu_count: int | None = None,
    parallel_active_limit: int | None = None,
    max_active_dags: int = DEFAULT_WENBO_ACTIVE_DAGS,
    trace_mode: TraceMode | str = TraceMode.SUMMARY,
    trace_output_dir: str | Path | None = None,
    check_invariants: bool = False,
    chunk_size: int = 1024 * 1024,
    validate_unique_ids: bool = False,
) -> list[DiscreteResult]:
    """Run every method by reopening a bounded-memory DAG stream per method."""
    gpu_count, cpu_count = _validate_resources(resource_count, gpu_count, cpu_count)
    if (
        not isinstance(max_active_dags, int)
        or isinstance(max_active_dags, bool)
        or max_active_dags <= 0
    ):
        raise ValueError("max_active_dags must be a positive integer")
    if not isinstance(validate_unique_ids, bool):
        raise ValueError("validate_unique_ids must be a boolean")
    trace_mode = TraceMode.parse(trace_mode)
    trace_paths = _trace_paths(trace_mode, trace_output_dir)

    def source() -> Iterator[SimDAG]:
        return iter_sim_dags(
            file_path,
            limit=limit,
            duration_config=duration_config,
            resource_estimate_file=resource_estimate_file,
            chunk_size=chunk_size,
            validate_unique_ids=validate_unique_ids,
        )

    return [
        run_parallel(
            source(),
            resource_count,
            gpu_count,
            cpu_count,
            parallel_active_limit,
            trace_mode=trace_mode,
            trace_output_path=trace_paths.get("parallel"),
            check_invariants=check_invariants,
            validate_input=False,
        ),
        run_wenbo(
            source(),
            resource_count,
            gpu_count,
            cpu_count,
            active_limit=max_active_dags,
            trace_mode=trace_mode,
            trace_output_path=trace_paths.get("wenbo"),
            check_invariants=check_invariants,
            validate_input=False,
        ),
    ]


def run_parallel(
    dags: Iterable[SimDAG],
    resource_count: int,
    gpu_count: int | None = None,
    cpu_count: int | None = None,
    active_limit: int | None = None,
    *,
    trace_mode: TraceMode | str = TraceMode.SUMMARY,
    trace_output_path: str | Path | None = None,
    check_invariants: bool = False,
    validate_input: bool = True,
) -> DiscreteResult:
    """Run task-level parallelism with resources retained for the task lifetime.

    Admit up to max(GPU, CPU) DAGs and bind one abundant-resource slot to each
    (GPU wins ties). The other resource is acquired on its first runnable node.
    Both slots, once acquired, remain owned until the entire DAG completes,
    including idle periods between nodes. Each DAG can execute at most one
    node of each resource type concurrently; independent CPU/GPU nodes may
    overlap. No slot is borrowed from another active DAG.
    """
    gpu_count, cpu_count = _validate_resources(resource_count, gpu_count, cpu_count)
    capacity = max(gpu_count, cpu_count)
    active_limit = capacity if active_limit is None else active_limit
    if (not isinstance(active_limit, int) or isinstance(active_limit, bool)
            or not 0 < active_limit <= capacity):
        raise ValueError("parallel active_limit must be positive and no greater than max(GPU, CPU)")
    primary = "gpu" if gpu_count >= cpu_count else "cpu"
    free = {"gpu": deque(range(gpu_count)), "cpu": deque(range(cpu_count))}
    owned: dict[int, dict[str, int]] = {}
    active: dict[int, DAGRuntime] = {}
    runtime_indexes: dict[int, int] = {}
    runtime_source = iter(_iter_runtimes(dags, check_invariants=check_invariants,
                                       validate_input=validate_input))
    trace = _new_trace("parallel", trace_mode, trace_output_path)
    totals = _SimulationTotals()
    running: list[tuple[float, int, DAGRuntime, SimNode]] = []
    now, seq, scheduling_round, next_dag_index = 0.0, 0, 0, 0
    gpu_busy, cpu_busy = 0.0, 0.0
    source_exhausted = False

    def acquire(index: int, hardware: str) -> None:
        slot = free[hardware].popleft()
        owned[index][hardware] = slot
        trace.record(TraceEventType.RESOURCE_ACQUIRED, now, active[index].dag.dag_id,
                     resource_type=hardware, worker_id=f"{hardware}-{slot}",
                     policy_id=_policy_id("parallel"), scheduling_round=scheduling_round,
                     details={"scope": "task_lifetime"})

    def assert_reservations() -> None:
        if not check_invariants:
            return
        _assert_private_resources(active.values(), gpu_count, cpu_count)
        for hardware, count in (("gpu", gpu_count), ("cpu", cpu_count)):
            slots = [leases[hardware] for leases in owned.values() if hardware in leases]
            available = list(free[hardware])
            if (len(set(slots)) != len(slots) or len(set(available)) != len(available)
                    or set(slots) & set(available)
                    or set(slots) | set(available) != set(range(count))):
                raise SimulationInvariantError(f"parallel {hardware} reservation ownership is inconsistent")
        for index, runtime in active.items():
            if primary not in owned[index]:
                raise SimulationInvariantError("parallel task lost its bound execution lane")
            for node_id in runtime.running:
                if runtime.dag.nodes[node_id].hardware not in owned[index]:
                    raise SimulationInvariantError("parallel node started without a task-owned resource")

    def admit() -> None:
        nonlocal next_dag_index, source_exhausted
        while not source_exhausted and len(active) < active_limit:
            try:
                runtime = next(runtime_source)
            except StopIteration:
                source_exhausted = True
                return
            index = next_dag_index
            next_dag_index += 1
            active[index] = runtime
            owned[index] = {}
            runtime_indexes[id(runtime)] = index
            totals.admit(len(active))
            _record_runtime_arrived(trace, runtime, now, scheduling_round, "parallel")
            acquire(index, primary)
        assert_reservations()

    def schedule(candidates: Iterable[int] | None = None) -> None:
        nonlocal seq, scheduling_round, gpu_busy, cpu_busy
        scheduling_round += 1
        # With task-lifetime leases, only completed nodes can enable more
        # work unless a whole DAG releases resources. Preserve admission order.
        schedulable = deque(active.values() if candidates is None else (active[index] for index in candidates))
        while schedulable:
            runtime = schedulable.popleft()
            index = runtime_indexes[id(runtime)]
            node = runtime.next_ready_node(
                gpu_available=(runtime.gpu_busy < 1 and ("gpu" in owned[index] or bool(free["gpu"]))),
                cpu_available=(runtime.cpu_busy < 1 and ("cpu" in owned[index] or bool(free["cpu"]))),
            )
            if node is None:
                continue
            if node.hardware not in owned[index]:
                acquire(index, node.hardware)
            _start_private(runtime, node)
            trace.record(TraceEventType.NODE_STARTED, now, runtime.dag.dag_id,
                         node_id=node.node_id, resource_type=node.hardware,
                         worker_id=f"{node.hardware}-{owned[index][node.hardware]}",
                         policy_id=_policy_id("parallel"), scheduling_round=scheduling_round)
            heapq.heappush(running, (now + node.duration_s, seq, runtime, node))
            seq += 1
            if node.hardware == "gpu":
                gpu_busy += node.duration_s
            else:
                cpu_busy += node.duration_s
            assert_reservations()
            schedulable.append(runtime)

    admit()
    schedule()
    while active or running or not source_exhausted:
        if not running:
            raise SimulationInvariantError("parallel stalled with unfinished tasks and no events")
        now = running[0][0]
        newly_ready: list[tuple[DAGRuntime, SimNode]] = []
        touched: dict[int, DAGRuntime] = {}
        for _, _, runtime, node in _pop_finished(running, now):
            index = runtime_indexes[id(runtime)]
            newly_ready.extend((runtime, successor) for successor in _finish_private(runtime, node))
            touched[index] = runtime
            trace.record(TraceEventType.NODE_COMPLETED, now, runtime.dag.dag_id,
                         node_id=node.node_id, resource_type=node.hardware,
                         worker_id=f"{node.hardware}-{owned[index][node.hardware]}",
                         policy_id=_policy_id("parallel"), scheduling_round=scheduling_round)
        # All simultaneous node completions precede task resource releases.
        resource_released = False
        for index in sorted(touched):
            runtime = touched[index]
            if not runtime.is_completed():
                continue
            resource_released = True
            _record_dag_completed(trace, now, scheduling_round, runtime, "parallel")
            totals.complete(runtime)
            for hardware, slot in owned.pop(index).items():
                trace.record(TraceEventType.RESOURCE_RELEASED, now, runtime.dag.dag_id,
                             resource_type=hardware, worker_id=f"{hardware}-{slot}",
                             policy_id=_policy_id("parallel"), scheduling_round=scheduling_round,
                             details={"scope": "task_lifetime"})
                free[hardware].append(slot)
            del active[index]
            del runtime_indexes[id(runtime)]
        assert_reservations()
        _record_newly_ready(trace, now, scheduling_round, newly_ready, "parallel")
        admit()
        if active:
            schedule(None if resource_released else (index for index in sorted(touched) if index in active))
    _assert_totals_finished(totals, "parallel")
    return _result("parallel", resource_count, totals, now, gpu_busy, cpu_busy,
                   gpu_count, cpu_count, trace)


def run_wenbo(
    dags: Iterable[SimDAG],
    resource_count: int,
    gpu_count: int | None = None,
    cpu_count: int | None = None,
    *,
    active_limit: int | None = DEFAULT_WENBO_ACTIVE_DAGS,
    trace_mode: TraceMode | str = TraceMode.SUMMARY,
    trace_output_path: str | Path | None = None,
    check_invariants: bool = False,
    validate_input: bool = True,
) -> DiscreteResult:
    """Run global FIFO scheduling with bounded incremental DAG admission.

    Omitting ``active_limit`` or passing ``None`` uses
    ``DEFAULT_WENBO_ACTIVE_DAGS``. Callers may provide a larger positive
    integer explicitly, but input streams are never admitted without a bound.
    """
    gpu_count, cpu_count = _validate_resources(resource_count, gpu_count, cpu_count)
    if active_limit is None:
        active_limit = DEFAULT_WENBO_ACTIVE_DAGS
    if (
        not isinstance(active_limit, int)
        or isinstance(active_limit, bool)
        or active_limit <= 0
    ):
        raise ValueError("wenbo active_limit must be a positive integer")
    runtime_source = iter(
        _iter_runtimes(
            dags,
            check_invariants=check_invariants,
            validate_input=validate_input,
        )
    )
    trace = _new_trace("wenbo", trace_mode, trace_output_path)
    totals = _SimulationTotals()
    runtime_indexes: dict[int, int] = {}
    active: dict[int, DAGRuntime] = {}
    running: list[tuple[float, int, DAGRuntime, SimNode]] = []
    ready_gpu: list[tuple[int, int, str, str, DAGRuntime, SimNode]] = []
    ready_cpu: list[tuple[int, int, str, str, DAGRuntime, SimNode]] = []
    now = 0.0
    seq = 0
    gpu_running = 0
    cpu_running = 0
    gpu_busy = 0.0
    cpu_busy = 0.0
    scheduling_round = 0
    next_dag_index = 0
    source_exhausted = False

    def admit() -> None:
        nonlocal next_dag_index, source_exhausted
        while (
            not source_exhausted
            and len(active) < active_limit
        ):
            try:
                runtime = next(runtime_source)
            except StopIteration:
                source_exhausted = True
                return
            dag_index = next_dag_index
            next_dag_index += 1
            active[dag_index] = runtime
            runtime_indexes[id(runtime)] = dag_index
            totals.admit(len(active))
            initial_ready = _record_runtime_arrived(
                trace,
                runtime,
                now,
                scheduling_round,
                "wenbo",
            )
            for node in initial_ready:
                _push_ready(ready_gpu, ready_cpu, dag_index, runtime, node)

    def schedule() -> None:
        nonlocal seq, gpu_running, cpu_running, gpu_busy, cpu_busy
        nonlocal scheduling_round
        scheduling_round += 1
        while True:
            candidate = _pop_next_available(
                ready_gpu,
                ready_cpu,
                gpu_running < gpu_count,
                cpu_running < cpu_count,
            )
            if candidate is None:
                return
            _, _, _, _, runtime, node = candidate
            runtime.start(node.node_id)
            if node.hardware == "gpu":
                gpu_running += 1
                gpu_busy += node.duration_s
            elif node.hardware == "cpu":
                cpu_running += 1
                cpu_busy += node.duration_s
            _assert_resource_counts(
                gpu_running,
                cpu_running,
                gpu_count,
                cpu_count,
                runner="wenbo",
            )
            _record_node_started(
                trace,
                now,
                scheduling_round,
                runtime,
                node,
            )
            heapq.heappush(running, (now + node.duration_s, seq, runtime, node))
            seq += 1

    admit()
    schedule()
    while active or running or not source_exhausted:
        if not running:
            raise SimulationInvariantError(
                "wenbo simulation stalled with unfinished DAGs and no events"
            )
        now = running[0][0]
        finished = _pop_finished(running, now)
        newly_ready: list[tuple[DAGRuntime, SimNode]] = []
        released: list[tuple[DAGRuntime, SimNode]] = []
        touched_runtimes: dict[int, DAGRuntime] = {}
        for _, _, runtime, node in finished:
            unlocked = runtime.complete(node.node_id)
            dag_index = runtime_indexes[id(runtime)]
            touched_runtimes[dag_index] = runtime
            trace.record(
                TraceEventType.NODE_COMPLETED,
                now,
                runtime.dag.dag_id,
                node_id=node.node_id,
                resource_type=node.hardware,
                policy_id=_policy_id("wenbo"),
                scheduling_round=scheduling_round,
            )
            if node.hardware == "gpu":
                gpu_running -= 1
            elif node.hardware == "cpu":
                cpu_running -= 1
            released.append((runtime, node))
            newly_ready.extend((runtime, item) for item in unlocked)
        for runtime, node in released:
            trace.record(
                TraceEventType.RESOURCE_RELEASED,
                now,
                runtime.dag.dag_id,
                node_id=node.node_id,
                resource_type=node.hardware,
                policy_id=_policy_id("wenbo"),
                scheduling_round=scheduling_round,
            )
        _assert_resource_counts(
            gpu_running,
            cpu_running,
            gpu_count,
            cpu_count,
            runner="wenbo",
        )
        _record_newly_ready(trace, now, scheduling_round, newly_ready, "wenbo")
        for runtime, successor in newly_ready:
            _push_ready(
                ready_gpu,
                ready_cpu,
                runtime_indexes[id(runtime)],
                runtime,
                successor,
            )
        for dag_index in sorted(touched_runtimes):
            runtime = touched_runtimes[dag_index]
            if runtime.is_completed():
                _record_dag_completed(trace, now, scheduling_round, runtime, "wenbo")
                totals.complete(runtime)
                del active[dag_index]
                del runtime_indexes[id(runtime)]
        admit()
        if active:
            schedule()

    _assert_resource_counts(
        gpu_running,
        cpu_running,
        gpu_count,
        cpu_count,
        runner="wenbo",
    )
    _assert_totals_finished(totals, "wenbo")
    return _result(
        "wenbo",
        resource_count,
        totals,
        now,
        gpu_busy,
        cpu_busy,
        gpu_count,
        cpu_count,
        trace,
    )


def _push_ready(
    ready_gpu: list[tuple[int, int, str, str, DAGRuntime, SimNode]],
    ready_cpu: list[tuple[int, int, str, str, DAGRuntime, SimNode]],
    dag_index: int,
    runtime: DAGRuntime,
    node: SimNode,
) -> None:
    target = ready_gpu if node.hardware == "gpu" else ready_cpu
    heapq.heappush(
        target,
        (
            dag_index,
            runtime.dag.node_order[node.node_id],
            runtime.dag.dag_id,
            node.node_id,
            runtime,
            node,
        ),
    )


def _pop_next_available(
    ready_gpu: list[tuple[int, int, str, str, DAGRuntime, SimNode]],
    ready_cpu: list[tuple[int, int, str, str, DAGRuntime, SimNode]],
    gpu_available: bool,
    cpu_available: bool,
) -> tuple[int, int, str, str, DAGRuntime, SimNode] | None:
    gpu_head = ready_gpu[0] if gpu_available and ready_gpu else None
    cpu_head = ready_cpu[0] if cpu_available and ready_cpu else None
    if gpu_head is None and cpu_head is None:
        return None
    if cpu_head is None or (gpu_head is not None and gpu_head[:4] <= cpu_head[:4]):
        return heapq.heappop(ready_gpu)
    return heapq.heappop(ready_cpu)


def write_discrete_csv(path: str | Path, results: list[DiscreteResult]) -> None:
    rows = [result.to_row() for result in results]
    _add_speedups(rows)
    fieldnames = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_discrete_json(
    path: str | Path,
    results: list[DiscreteResult],
    metadata: dict[str, Any] | None = None,
) -> None:
    rows = [result.to_payload() for result in results]
    _add_speedups(rows)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8") as f:
        json.dump(
            {
                "metadata": metadata or {},
                "results": rows,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )


def _duration_s(
    hardware: str,
    tokens: int,
    config: DurationConfig,
    explicit_time_ms: float = 0.0,
) -> float:
    if explicit_time_ms > 0:
        latency_ms = explicit_time_ms
    elif hardware == "gpu":
        token_ms = tokens / max(config.gpu_tokens_per_second, 1.0) * 1000.0
        latency_ms = config.gpu_base_latency_ms + token_ms
    else:
        token_ms = tokens / max(config.cpu_tokens_per_second, 1.0) * 1000.0
        latency_ms = config.cpu_base_latency_ms + token_ms
    return max(latency_ms * config.time_scale, config.min_latency_ms) / 1000.0


def _hardware(node_def: dict[str, Any]) -> str:
    explicit = str(node_def.get("hardware", "")).lower()
    if explicit:
        return explicit
    node_type = str(node_def.get("node_type", "")).lower()
    if node_type == "gpu":
        return "gpu"
    if node_type == "cpu":
        return "cpu"
    return "cpu"


def _tokens(node_def: dict[str, Any]) -> int:
    tokens = node_def.get("tokens")
    if isinstance(tokens, dict):
        return int(tokens.get("tokens", 0) or 0)
    return int(node_def.get("estimated_tokens", 0) or 0)


def _explicit_time_ms(node_def: dict[str, Any]) -> float:
    cpu = node_def.get("cpu")
    if isinstance(cpu, dict) and "time_ms" in cpu:
        return float(cpu.get("time_ms", 0.0) or 0.0)

    raw = node_def.get("resource_requirement")
    if isinstance(raw, dict) and "estimated_time_ms" in raw:
        return float(raw.get("estimated_time_ms", 0.0) or 0.0)

    estimated_cost = node_def.get("estimated_cost")
    if isinstance(estimated_cost, dict) and "latency_ms" in estimated_cost:
        return float(estimated_cost.get("latency_ms", 0.0) or 0.0)

    return 0.0


def _iter_runtimes(
    dags: Iterable[SimDAG],
    *,
    check_invariants: bool = False,
    validate_input: bool = True,
) -> Iterator[DAGRuntime]:
    seen_ids: set[str] = set()
    yielded = False
    for dag in dags:
        if validate_input:
            if dag.dag_id in seen_ids:
                raise ValueError(f"duplicate DAG id: {dag.dag_id}")
            validate_sim_dags([dag])
            seen_ids.add(dag.dag_id)
        yielded = True
        runtime = DAGRuntime.from_dag(
            dag,
            check_invariants=check_invariants,
        )
        yield runtime
        del runtime
    if not yielded:
        raise ValueError("at least one DAG is required")


def _start_private(runtime: DAGRuntime, node: SimNode) -> None:
    runtime.start(node.node_id)
    if node.hardware == "gpu":
        runtime.gpu_busy += 1
    elif node.hardware == "cpu":
        runtime.cpu_busy += 1
    if runtime.gpu_busy > 1 or runtime.cpu_busy > 1:
        raise SimulationInvariantError(
            f"DAG {runtime.dag.dag_id} exceeded its private resource pair"
        )


def _finish_private(runtime: DAGRuntime, node: SimNode) -> list[SimNode]:
    newly_ready = runtime.complete(node.node_id)
    if node.hardware == "gpu":
        runtime.gpu_busy -= 1
    elif node.hardware == "cpu":
        runtime.cpu_busy -= 1
    if runtime.check_invariants:
        runtime.assert_invariants()
    return newly_ready


def _pop_finished(
    running: list[tuple[float, int, DAGRuntime, SimNode]],
    now: float,
) -> list[tuple[float, int, DAGRuntime, SimNode]]:
    finished = []
    while running and running[0][0] == now:
        finished.append(heapq.heappop(running))
    return finished


def _result(
    name: str,
    resource_count: int,
    totals: _SimulationTotals,
    wall_clock_s: float,
    gpu_busy_s: float,
    cpu_busy_s: float,
    gpu_count: int | None = None,
    cpu_count: int | None = None,
    trace: SchedulingTrace | None = None,
) -> DiscreteResult:
    policy_name, policy_version = POLICY_VERSIONS[name]
    trace = trace or _new_trace(name, TraceMode.SUMMARY)
    trace.close()
    return DiscreteResult(
        runner=name,
        resource_count=resource_count,
        dag_count=totals.dag_count,
        wall_clock_s=wall_clock_s,
        completed_dags=totals.completed_dags,
        completed_nodes=totals.completed_nodes,
        completed_tokens=totals.completed_tokens,
        gpu_busy_s=gpu_busy_s,
        cpu_busy_s=cpu_busy_s,
        sum_node_exec_s=gpu_busy_s + cpu_busy_s,
        gpu_count=gpu_count,
        cpu_count=cpu_count,
        policy_name=policy_name,
        policy_version=policy_version,
        peak_active_dags=totals.peak_active_dags,
        trace_summary=trace.summary,
        trace_events=trace.events,
        dag_completion_times=trace.dag_completion_times,
    )


def _validate_resources(
    resource_count: int,
    gpu_count: int | None,
    cpu_count: int | None,
) -> tuple[int, int]:
    if not isinstance(resource_count, int) or isinstance(resource_count, bool):
        raise ValueError("resource_count must be a positive integer")
    if resource_count <= 0:
        raise ValueError("resource_count must be a positive integer")
    resolved_gpu = resource_count if gpu_count is None else gpu_count
    resolved_cpu = resource_count if cpu_count is None else cpu_count
    for name, value in (("gpu_count", resolved_gpu), ("cpu_count", resolved_cpu)):
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    return resolved_gpu, resolved_cpu


def _new_trace(
    runner: str,
    mode: TraceMode | str,
    stream_path: str | Path | None = None,
) -> SchedulingTrace:
    return SchedulingTrace(
        runner=runner,
        clock="virtual",
        mode=mode,
        stream_path=stream_path,
    )


def _trace_paths(
    mode: TraceMode,
    output_dir: str | Path | None,
) -> dict[str, Path]:
    if mode is not TraceMode.STREAM:
        if output_dir is not None:
            raise ValueError(
                "trace_output_dir is only supported in stream trace mode"
            )
        return {}
    if output_dir is None:
        raise ValueError("stream trace mode requires trace_output_dir")
    root = Path(output_dir)
    return {
        runner: root / f"{runner}.trace.jsonl"
        for runner in ("parallel", "wenbo")
    }


def _policy_id(runner: str) -> str:
    name, version = POLICY_VERSIONS[runner]
    return f"{name}@{version}"


def _record_runtime_arrived(
    trace: SchedulingTrace,
    runtime: DAGRuntime,
    now: float,
    scheduling_round: int,
    runner: str,
) -> list[SimNode]:
    ready_nodes = runtime.ready_nodes()
    trace.record(
        TraceEventType.DAG_ARRIVED,
        now,
        runtime.dag.dag_id,
        policy_id=_policy_id(runner),
        scheduling_round=scheduling_round,
    )
    for node in ready_nodes:
        trace.record(
            TraceEventType.NODE_READY,
            now,
            runtime.dag.dag_id,
            node_id=node.node_id,
            resource_type=node.hardware,
            policy_id=_policy_id(runner),
            scheduling_round=scheduling_round,
        )
    return ready_nodes


def _record_node_started(
    trace: SchedulingTrace,
    now: float,
    scheduling_round: int,
    runtime: DAGRuntime,
    node: SimNode,
) -> None:
    common = {
        "node_id": node.node_id,
        "resource_type": node.hardware,
        "policy_id": _policy_id(trace.runner),
        "scheduling_round": scheduling_round,
    }
    trace.record(
        TraceEventType.RESOURCE_ACQUIRED,
        now,
        runtime.dag.dag_id,
        **common,
    )
    trace.record(
        TraceEventType.NODE_STARTED,
        now,
        runtime.dag.dag_id,
        **common,
    )


def _record_newly_ready(
    trace: SchedulingTrace,
    now: float,
    scheduling_round: int,
    newly_ready: Sequence[tuple[DAGRuntime, SimNode]],
    runner: str,
) -> None:
    for runtime, node in newly_ready:
        trace.record(
            TraceEventType.NODE_READY,
            now,
            runtime.dag.dag_id,
            node_id=node.node_id,
            resource_type=node.hardware,
            policy_id=_policy_id(runner),
            scheduling_round=scheduling_round,
        )


def _record_dag_completed(
    trace: SchedulingTrace,
    now: float,
    scheduling_round: int,
    runtime: DAGRuntime,
    runner: str,
) -> None:
    trace.record(
        TraceEventType.DAG_COMPLETED,
        now,
        runtime.dag.dag_id,
        policy_id=_policy_id(runner),
        scheduling_round=scheduling_round,
    )


def _assert_resource_counts(
    gpu_running: int,
    cpu_running: int,
    gpu_count: int,
    cpu_count: int,
    *,
    runner: str,
) -> None:
    if not 0 <= gpu_running <= gpu_count:
        raise SimulationInvariantError(
            f"{runner} GPU usage {gpu_running} exceeds capacity {gpu_count}"
        )
    if not 0 <= cpu_running <= cpu_count:
        raise SimulationInvariantError(
            f"{runner} CPU usage {cpu_running} exceeds capacity {cpu_count}"
        )


def _assert_private_resources(
    active: Collection[DAGRuntime],
    gpu_count: int,
    cpu_count: int,
    *,
    runner: str = "parallel",
) -> None:
    for runtime in active:
        expected_gpu = sum(
            runtime.dag.nodes[node_id].hardware == "gpu"
            for node_id in runtime.running
        )
        expected_cpu = sum(
            runtime.dag.nodes[node_id].hardware == "cpu"
            for node_id in runtime.running
        )
        if runtime.gpu_busy != expected_gpu or runtime.cpu_busy != expected_cpu:
            raise SimulationInvariantError(
                f"DAG {runtime.dag.dag_id} private resources do not match running nodes"
            )
        if runtime.gpu_busy > 1 or runtime.cpu_busy > 1:
            raise SimulationInvariantError(
                f"DAG {runtime.dag.dag_id} exceeded its private resource pair"
            )
    _assert_resource_counts(
        sum(runtime.gpu_busy for runtime in active),
        sum(runtime.cpu_busy for runtime in active),
        gpu_count,
        cpu_count,
        runner=runner,
    )


def _assert_totals_finished(totals: _SimulationTotals, runner: str) -> None:
    if totals.dag_count <= 0:
        raise SimulationInvariantError(f"{runner} finished without admitting a DAG")
    if totals.completed_dags != totals.dag_count:
        raise SimulationInvariantError(
            f"{runner} completed {totals.completed_dags} of {totals.dag_count} DAGs"
        )


def _add_speedups(rows: list[dict[str, Any]]) -> None:
    by_group: dict[tuple[int, int, int, int], dict[str, dict[str, Any]]] = {}
    for row in rows:
        key = (
            int(row["resource_count"]),
            int(row["gpu_count"]),
            int(row["cpu_count"]),
            int(row["dag_count"]),
        )
        by_group.setdefault(key, {})[str(row["runner"])] = row

    for methods in by_group.values():
        wenbo = methods.get("wenbo")
        if not wenbo:
            continue
        for baseline_name in ("parallel",):
            baseline = methods.get(baseline_name)
            if not baseline:
                continue
            wenbo[f"speedup_nodes_vs_{baseline_name}"] = _ratio(
                wenbo["nodes_per_second"],
                baseline["nodes_per_second"],
            )
            wenbo[f"speedup_tokens_vs_{baseline_name}"] = _ratio(
                wenbo["tokens_per_second"],
                baseline["tokens_per_second"],
            )


def _ratio(numerator: float, denominator: float) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0
