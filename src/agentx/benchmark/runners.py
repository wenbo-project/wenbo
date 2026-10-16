"""Benchmark runners for throughput experiments."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from agentx.benchmark.metrics import summarize_throughput
from agentx.dag.model import DAGState, DynamicDAG, Node, NodeState
from agentx.dag.registry import DAGRegistry
from agentx.executor.base import ExecutorBackend, LLMRequest
from agentx.runtime.coordinator import build_node_request_metadata
from agentx.runtime.event import EventBus
from agentx.runtime.state import DAGStateTracker
from agentx.scheduler import FIFOPolicy, Scheduler, SchedulingPolicy


@dataclass
class BenchmarkResult:
    name: str
    metrics: dict[str, Any]
    timeline: list[dict[str, Any]]
    decisions: list[dict[str, Any]]
    diagnostics: dict[str, Any] = field(default_factory=dict)


class NaiveSequentialRunner:
    """Execute every DAG node one at a time in dependency order."""

    def __init__(self, executor: ExecutorBackend, node_timeout: float = 30.0) -> None:
        self._executor = executor
        self._node_timeout = node_timeout
        self._timeline: list[dict[str, Any]] = []

    async def run(self, dags: list[DynamicDAG]) -> BenchmarkResult:
        start = time.monotonic()
        for dag in dags:
            dag.state = DAGState.RUNNING
            while True:
                ready = self._ready_nodes(dag)
                if not ready:
                    break
                node = sorted(ready, key=lambda item: item.node_id)[0]
                await self._execute_node(dag, node)

            dag.state = DAGState.COMPLETED if dag.is_completed() else DAGState.FAILED

        elapsed = time.monotonic() - start
        return BenchmarkResult(
            name="sequential",
            metrics=summarize_throughput(dags, self._timeline, elapsed, max_concurrent=1),
            timeline=list(self._timeline),
            decisions=[],
        )

    @staticmethod
    def _ready_nodes(dag: DynamicDAG) -> list[Node]:
        ready = []
        for node in dag.nodes.values():
            if node.state not in (NodeState.PENDING, NodeState.READY):
                continue
            if all(dag.nodes[dep].state == NodeState.COMPLETED for dep in node.dependencies):
                ready.append(node)
        return ready

    async def _execute_node(self, dag: DynamicDAG, node: Node) -> None:
        node.state = NodeState.RUNNING
        self._record(dag.dag_id, node.node_id, "started")
        request = LLMRequest(
            model=node.model_config.model,
            prompt=node.prompt,
            sampling_params=node.model_config.sampling_params,
            request_id=f"{dag.dag_id}_{node.node_id}",
            metadata=build_node_request_metadata(dag.dag_id, node.node_id, node, dag),
        )
        request_id = await self._executor.submit(request)
        response = await self._executor.wait(request_id, timeout=self._node_timeout)
        if response.success:
            node.state = NodeState.COMPLETED
            node.result = {
                "output": response.output,
                "raw_output": response.output,
                "sanitize_ok": True,
            }
        else:
            node.state = NodeState.FAILED
            node.result = {"error": response.error or "unknown"}
        self._record(dag.dag_id, node.node_id, "finished")

    def _record(self, dag_id: str, node_id: str, event: str) -> None:
        self._timeline.append({
            "dag_id": dag_id,
            "node_id": node_id,
            "event": event,
            "time": time.monotonic(),
        })


class PlainFCFSResourceRunner:
    """Execute DAGs one by one with FCFS ready-node order and shared resource slots.

    This runner intentionally does not use AgentX's registry, event bus, scheduler,
    ready queue, or policy layer. It is a plain baseline with the same coarse
    resource limits used by AgentX-FCFS.
    """

    def __init__(
        self,
        executor: ExecutorBackend,
        max_concurrent: int = 8,
        node_timeout: float = 30.0,
        resource_limits: dict[str, int] | None = None,
    ) -> None:
        self._executor = executor
        self._max_concurrent = max_concurrent
        self._node_timeout = node_timeout
        self._resource_limits = {
            hardware: limit
            for hardware, limit in (resource_limits or {}).items()
            if limit > 0
        }
        self._running_by_hardware: dict[str, int] = defaultdict(int)
        self._timeline: list[dict[str, Any]] = []

    async def run(self, dags: list[DynamicDAG], timeout_s: float = 120.0) -> BenchmarkResult:
        start = time.monotonic()
        await asyncio.wait_for(self._run_all(dags), timeout=timeout_s)
        elapsed = time.monotonic() - start
        return BenchmarkResult(
            name="plain_fcfs",
            metrics=summarize_throughput(
                dags,
                self._timeline,
                elapsed,
                max_concurrent=self._max_concurrent,
                resource_limits=self._resource_limits,
            ),
            timeline=list(self._timeline),
            decisions=[],
        )

    async def _run_all(self, dags: list[DynamicDAG]) -> None:
        for dag in dags:
            await self._run_one_dag(dag)

    async def _run_one_dag(self, dag: DynamicDAG) -> None:
        dag.state = DAGState.RUNNING
        running: dict[asyncio.Task, Node] = {}

        while not dag.is_completed() and not dag.has_failed():
            self._fill_slots(dag, running)

            if not running:
                dag.state = DAGState.FAILED
                break

            done, _ = await asyncio.wait(running.keys(), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                node = running.pop(task)
                self._release_resource(node.hardware)
                task.result()

        if running:
            done = await asyncio.gather(*running.keys(), return_exceptions=True)
            for node in list(running.values()):
                self._release_resource(node.hardware)
            for result in done:
                if isinstance(result, Exception):
                    dag.state = DAGState.FAILED

        dag.state = DAGState.COMPLETED if dag.is_completed() else DAGState.FAILED

    def _fill_slots(self, dag: DynamicDAG, running: dict[asyncio.Task, Node]) -> None:
        while len(running) < self._max_concurrent:
            ready = [
                node
                for node in self._ready_nodes(dag)
                if self._resource_available(node.hardware)
            ]
            if not ready:
                return

            node = sorted(ready, key=lambda item: item.node_id)[0]
            node.state = NodeState.RUNNING
            self._reserve_resource(node.hardware)
            task = asyncio.create_task(self._execute_node(dag, node))
            running[task] = node

    @staticmethod
    def _ready_nodes(dag: DynamicDAG) -> list[Node]:
        ready = []
        for node in dag.nodes.values():
            if node.state not in (NodeState.PENDING, NodeState.READY):
                continue
            if all(dag.nodes[dep].state == NodeState.COMPLETED for dep in node.dependencies):
                ready.append(node)
        return ready

    async def _execute_node(self, dag: DynamicDAG, node: Node) -> None:
        self._record(dag.dag_id, node.node_id, "started")
        request = LLMRequest(
            model=node.model_config.model,
            prompt=node.prompt,
            sampling_params=node.model_config.sampling_params,
            request_id=f"{dag.dag_id}_{node.node_id}",
            metadata=build_node_request_metadata(dag.dag_id, node.node_id, node, dag),
        )
        request_id = await self._executor.submit(request)
        response = await self._executor.wait(request_id, timeout=self._node_timeout)
        if response.success:
            node.state = NodeState.COMPLETED
            node.result = {
                "output": response.output,
                "raw_output": response.output,
                "sanitize_ok": True,
            }
        else:
            node.state = NodeState.FAILED
            node.result = {"error": response.error or "unknown"}
        self._record(dag.dag_id, node.node_id, "finished")

    def _resource_available(self, hardware: str) -> bool:
        limit = self._resource_limits.get(hardware)
        if limit is None:
            return True
        return self._running_by_hardware[hardware] < limit

    def _reserve_resource(self, hardware: str) -> None:
        if hardware in self._resource_limits:
            self._running_by_hardware[hardware] += 1

    def _release_resource(self, hardware: str) -> None:
        if hardware in self._resource_limits:
            self._running_by_hardware[hardware] = max(0, self._running_by_hardware[hardware] - 1)

    def _record(self, dag_id: str, node_id: str, event: str) -> None:
        self._timeline.append({
            "dag_id": dag_id,
            "node_id": node_id,
            "event": event,
            "time": time.monotonic(),
        })


class TaskParallelResourcePairRunner:
    """Run ``parallel`` with task-lifetime resource reservations.

    Bind an abundant-resource slot on admission, acquire the other slot on
    first use, and release both only on task termination. The active window
    is bounded by max(GPU, CPU) and max_active_tasks. Each DAG may execute
    one node of each resource type concurrently. The class name is retained
    for compatibility with existing benchmark entry points.
    """

    def __init__(
        self,
        executor: ExecutorBackend,
        max_active_tasks: int = 1,
        node_timeout: float = 30.0,
        resource_limits: dict[str, int] | None = None,
    ) -> None:
        if (
            not isinstance(max_active_tasks, int)
            or isinstance(max_active_tasks, bool)
            or max_active_tasks <= 0
        ):
            raise ValueError("max_active_tasks must be a positive integer")
        self._executor = executor
        self._node_timeout = node_timeout
        limits = {
            hardware: limit
            for hardware, limit in (resource_limits or {}).items()
            if limit > 0
        }
        gpu_capacity = limits.get("gpu", max_active_tasks)
        cpu_capacity = limits.get("cpu", max_active_tasks)
        self._active_capacity = min(max_active_tasks, max(gpu_capacity, cpu_capacity))
        self._max_active_tasks = self._active_capacity
        self._gpu_capacity = gpu_capacity
        self._cpu_capacity = cpu_capacity
        self._dag_slots = asyncio.Semaphore(self._active_capacity)
        self._gpu_slots = asyncio.Semaphore(gpu_capacity)
        self._cpu_slots = asyncio.Semaphore(cpu_capacity)
        self._primary = "gpu" if gpu_capacity >= cpu_capacity else "cpu"
        self._task_resources: dict[str, set[str]] = {}
        self._resources_changed = asyncio.Event()
        self._resource_limits = {
            "gpu": gpu_capacity,
            "cpu": cpu_capacity,
        }
        self._timeline: list[dict[str, Any]] = []

    async def run(self, dags: list[DynamicDAG], timeout_s: float = 120.0) -> BenchmarkResult:
        start = time.monotonic()
        await asyncio.wait_for(
            asyncio.gather(*(self._run_one_task(dag) for dag in dags)),
            timeout=timeout_s,
        )
        elapsed = time.monotonic() - start
        return BenchmarkResult(
            name="parallel",
            metrics=summarize_throughput(
                dags,
                self._timeline,
                elapsed,
                max_concurrent=self._gpu_capacity + self._cpu_capacity,
                resource_limits=self._resource_limits,
            ),
            timeline=list(self._timeline),
            decisions=[],
            diagnostics={
                "policy_id": "task_lifetime_fifo@1",
                "resource_release": "task_termination",
                "max_active_tasks": self._max_active_tasks,
                "active_dag_window": self._active_capacity,
                "gpu_capacity": self._gpu_capacity,
                "cpu_capacity": self._cpu_capacity,
                "gpu_groups": self._active_capacity,
                "cpu_slots": self._cpu_capacity,
                "active_dags": self._active_capacity,
            },
        )

    async def _run_one_task(self, dag: DynamicDAG) -> None:
        await self._dag_slots.acquire()
        held: set[str] = set()
        try:
            slots = self._gpu_slots if self._primary == "gpu" else self._cpu_slots
            await slots.acquire()
            held.add(self._primary)
            self._task_resources[dag.dag_id] = held
            await self._run_one_dag_with_private_pair(dag)
        finally:
            for hardware in held:
                slots = self._gpu_slots if hardware == "gpu" else self._cpu_slots
                slots.release()
            self._task_resources.pop(dag.dag_id, None)
            self._resources_changed.set()
            self._dag_slots.release()

    async def _run_one_dag_with_private_pair(self, dag: DynamicDAG) -> None:
        dag.state = DAGState.RUNNING
        running: dict[asyncio.Task, Node] = {}
        running_gpu = 0
        running_cpu = 0

        try:
            while not dag.is_completed() and not dag.has_failed():
                await self._fill_private_slots(dag, running, running_gpu, running_cpu)
                running_gpu = sum(node.hardware == "gpu" for node in running.values())
                running_cpu = sum(node.hardware == "cpu" for node in running.values())

                if not running:
                    if self._ready_nodes(dag):
                        self._resources_changed.clear()
                        await self._resources_changed.wait()
                        continue
                    dag.state = DAGState.FAILED
                    break

                done, _ = await asyncio.wait(
                    running.keys(), return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    node = running.pop(task)
                    if node.hardware == "gpu":
                        running_gpu = max(0, running_gpu - 1)
                    elif node.hardware == "cpu":
                        running_cpu = max(0, running_cpu - 1)
                    try:
                        task.result()
                    except Exception:
                        dag.state = DAGState.FAILED
        except BaseException:
            # Cancel child executions before the task-owned leases are
            # released by _run_one_task, including timeout/cancellation paths.
            pending_tasks = list(running)
            for task in pending_tasks:
                task.cancel()
            running.clear()
            if pending_tasks:
                cleanup = asyncio.gather(*pending_tasks, return_exceptions=True)
                cleanup.add_done_callback(lambda _: None)
            raise
        finally:
            if running:
                results = await asyncio.gather(
                    *running.keys(), return_exceptions=True
                )
                if any(isinstance(result, BaseException) for result in results):
                    dag.state = DAGState.FAILED
                running.clear()

        dag.state = DAGState.COMPLETED if dag.is_completed() else DAGState.FAILED

    async def _run_one_dag_with_shared_resources(self, dag: DynamicDAG) -> None:
        """Compatibility alias; not selected by the ``parallel`` experiment."""
        await self._run_one_dag_with_private_pair(dag)

    async def _run_one_dag_with_shared_cpu(self, dag: DynamicDAG) -> None:
        """Compatibility alias for the historical helper name."""
        await self._run_one_dag_with_private_pair(dag)

    async def _fill_private_pair_slots(
        self,
        dag: DynamicDAG,
        running: dict[asyncio.Task, Node],
        running_gpu: int,
        running_cpu: int,
    ) -> None:
        while True:
            held = self._task_resources[dag.dag_id]
            node = None
            for candidate in sorted(self._ready_nodes(dag), key=lambda item: item.node_id):
                if candidate.hardware == "gpu" and running_gpu >= 1:
                    continue
                if candidate.hardware == "cpu" and running_cpu >= 1:
                    continue
                slots = self._gpu_slots if candidate.hardware == "gpu" else self._cpu_slots
                if candidate.hardware not in held and slots.locked():
                    continue
                node = candidate
                break
            if node is None:
                return

            node.state = NodeState.RUNNING
            if node.hardware == "gpu":
                if "gpu" not in held:
                    await self._gpu_slots.acquire()
                    held.add("gpu")
                running_gpu += 1
            elif node.hardware == "cpu":
                if "cpu" not in held:
                    await self._cpu_slots.acquire()
                    held.add("cpu")
                running_cpu += 1
            running[asyncio.create_task(self._execute_node(dag, node))] = node
            # Let other admitted DAGs reserve work before this DAG scans its
            # remaining ready nodes, preserving deterministic DAG-level
            # fairness.
            await asyncio.sleep(0)

    async def _fill_private_slots(
        self,
        dag: DynamicDAG,
        running: dict[asyncio.Task, Node],
        running_gpu: int,
        running_cpu: int | None = None,
    ) -> None:
        """Fill the one-GPU/one-CPU slots owned by this admitted DAG."""
        if running_cpu is None:
            running_cpu = sum(node.hardware == "cpu" for node in running.values())
        await self._fill_private_pair_slots(dag, running, running_gpu, running_cpu)

    async def _fill_shared_slots(
        self,
        dag: DynamicDAG,
        running: dict[asyncio.Task, Node],
        running_gpu: int,
        running_cpu: int,
    ) -> None:
        """Compatibility alias; shared-pool semantics are not experimental."""
        await self._fill_private_pair_slots(dag, running, running_gpu, running_cpu)

    @staticmethod
    def _ready_nodes(dag: DynamicDAG) -> list[Node]:
        ready = []
        for node in dag.nodes.values():
            if node.state not in (NodeState.PENDING, NodeState.READY):
                continue
            if all(dag.nodes[dep].state == NodeState.COMPLETED for dep in node.dependencies):
                ready.append(node)
        return ready

    async def _execute_node(self, dag: DynamicDAG, node: Node) -> None:
        self._record(dag.dag_id, node.node_id, "started")
        request = LLMRequest(
            model=node.model_config.model,
            prompt=node.prompt,
            sampling_params=node.model_config.sampling_params,
            request_id=f"{dag.dag_id}_{node.node_id}",
            metadata=build_node_request_metadata(dag.dag_id, node.node_id, node, dag),
        )
        request_id = await self._executor.submit(request)
        response = await self._executor.wait(request_id, timeout=self._node_timeout)
        if response.success:
            node.state = NodeState.COMPLETED
            node.result = {
                "output": response.output,
                "raw_output": response.output,
                "sanitize_ok": True,
            }
        else:
            node.state = NodeState.FAILED
            node.result = {"error": response.error or "unknown"}
        self._record(dag.dag_id, node.node_id, "finished")

    def _record(self, dag_id: str, node_id: str, event: str) -> None:
        self._timeline.append({
            "dag_id": dag_id,
            "node_id": node_id,
            "event": event,
            "time": time.monotonic(),
        })


class AgentXGlobalRunner:
    """Run DAGs through the existing global AgentX scheduler."""

    def __init__(
        self,
        executor: ExecutorBackend,
        policy: SchedulingPolicy | None = None,
        max_concurrent: int = 8,
        debounce_ms: float = 1.0,
        heartbeat_ms: float = 10.0,
        node_timeout: float = 30.0,
        resource_limits: dict[str, int] | None = None,
        max_active_dags: int | None = None,
    ) -> None:
        self._executor = executor
        self._policy = policy or FIFOPolicy()
        self._max_concurrent = max_concurrent
        self._debounce_ms = debounce_ms
        self._heartbeat_ms = heartbeat_ms
        self._node_timeout = node_timeout
        self._resource_limits = resource_limits or {}
        self._max_active_dags = max_active_dags

    async def run(self, dags: list[DynamicDAG], timeout_s: float = 120.0) -> BenchmarkResult:
        event_bus = EventBus()
        registry = DAGRegistry()
        state_tracker = DAGStateTracker(event_bus, registry)
        scheduler = Scheduler(
            event_bus=event_bus,
            registry=registry,
            state_tracker=state_tracker,
            executor=self._executor,
            policy=self._policy,
            max_concurrent=self._max_concurrent,
            debounce_ms=self._debounce_ms,
            heartbeat_ms=self._heartbeat_ms,
            node_timeout=self._node_timeout,
            permissive_output=True,
            resource_limits=self._resource_limits,
        )
        await scheduler.start()
        start = time.monotonic()
        try:
            pending = list(dags)
            max_active = self._max_active_dags or len(dags) or 1
            max_active = max(1, max_active)
            active: set[str] = set()
            submitted_count = 0

            async def submit_next() -> None:
                nonlocal submitted_count
                if not pending:
                    return
                dag = pending.pop(0)
                active.add(dag.dag_id)
                submitted_count += 1
                await scheduler.submit_dag(dag)

            for _ in range(min(max_active, len(pending))):
                await submit_next()

            deadline = time.monotonic() + timeout_s
            while time.monotonic() < deadline:
                finished = {
                    dag.dag_id
                    for dag in dags
                    if dag.dag_id in active and dag.state in (DAGState.COMPLETED, DAGState.FAILED)
                }
                if finished:
                    active.difference_update(finished)
                    while pending and len(active) < max_active:
                        await submit_next()
                if not active and not pending:
                    break
                await asyncio.sleep(0.01)
            else:
                raise TimeoutError(f"Not all DAGs finished within {timeout_s}s")

            elapsed = time.monotonic() - start
            return BenchmarkResult(
                name="agentx_global",
                metrics=summarize_throughput(
                    dags,
                    scheduler.timeline,
                    elapsed,
                    max_concurrent=self._max_concurrent,
                    resource_limits=self._resource_limits,
                ),
                timeline=scheduler.timeline,
                decisions=scheduler.decisions,
                diagnostics={
                    "scheduler": scheduler.stats,
                    "max_active_dags": max_active,
                    "submitted_dags": submitted_count,
                    "admission_window_enabled": self._max_active_dags is not None,
                },
            )
        finally:
            await scheduler.stop()

class DAGSequentialRunner:
    """Run DAGs one by one, while allowing parallelism inside each DAG."""

    def __init__(
        self,
        executor_factory,
        policy: SchedulingPolicy | None = None,
        max_concurrent: int = 8,
        debounce_ms: float = 1.0,
        heartbeat_ms: float = 10.0,
        node_timeout: float = 30.0,
        resource_limits: dict[str, int] | None = None,
    ) -> None:
        self._executor_factory = executor_factory
        self._policy = policy or FIFOPolicy()
        self._max_concurrent = max_concurrent
        self._debounce_ms = debounce_ms
        self._heartbeat_ms = heartbeat_ms
        self._node_timeout = node_timeout
        self._resource_limits = resource_limits or {}

    async def run(self, dags: list[DynamicDAG], timeout_s: float = 120.0) -> BenchmarkResult:
        timeline: list[dict[str, Any]] = []
        decisions: list[dict[str, Any]] = []
        start = time.monotonic()

        for dag in dags:
            runner = AgentXGlobalRunner(
                executor=self._executor_factory(),
                policy=self._policy,
                max_concurrent=self._max_concurrent,
                debounce_ms=self._debounce_ms,
                heartbeat_ms=self._heartbeat_ms,
                node_timeout=self._node_timeout,
                resource_limits=self._resource_limits,
            )
            result = await runner.run([dag], timeout_s=timeout_s)
            timeline.extend(result.timeline)
            decisions.extend(result.decisions)

        elapsed = time.monotonic() - start
        return BenchmarkResult(
            name="dag_sequential",
            metrics=summarize_throughput(
                dags,
                timeline,
                elapsed,
                max_concurrent=self._max_concurrent,
                resource_limits=self._resource_limits,
            ),
            timeline=timeline,
            decisions=decisions,
        )


class PerDAGLocalRunner:
    """Run each DAG with a local FIFO scheduler and shared global concurrency."""

    def __init__(
        self,
        executor: ExecutorBackend,
        max_concurrent: int = 8,
        node_timeout: float = 30.0,
        resource_limits: dict[str, int] | None = None,
    ) -> None:
        self._executor = executor
        self._max_concurrent = max_concurrent
        self._node_timeout = node_timeout
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._resource_limits = resource_limits or {}
        self._hardware_semaphores = {
            hardware: asyncio.Semaphore(limit)
            for hardware, limit in self._resource_limits.items()
            if limit > 0
        }
        self._timeline: list[dict[str, Any]] = []

    async def run(self, dags: list[DynamicDAG], timeout_s: float = 120.0) -> BenchmarkResult:
        start = time.monotonic()
        await asyncio.wait_for(
            asyncio.gather(*(self._run_one_dag(dag) for dag in dags)),
            timeout=timeout_s,
        )
        elapsed = time.monotonic() - start
        return BenchmarkResult(
            name="per_dag_local",
            metrics=summarize_throughput(
                dags,
                self._timeline,
                elapsed,
                max_concurrent=self._max_concurrent,
                resource_limits=self._resource_limits,
            ),
            timeline=list(self._timeline),
            decisions=[],
        )

    async def _run_one_dag(self, dag: DynamicDAG) -> None:
        dag.state = DAGState.RUNNING
        running: set[asyncio.Task] = set()

        while not dag.is_completed() and not dag.has_failed():
            for node in sorted(self._ready_nodes(dag), key=lambda item: item.node_id):
                node.state = NodeState.RUNNING
                running.add(asyncio.create_task(self._execute_node(dag, node)))

            if not running:
                dag.state = DAGState.FAILED
                break

            done, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()

        if running:
            await asyncio.gather(*running)

        dag.state = DAGState.COMPLETED if dag.is_completed() else DAGState.FAILED

    @staticmethod
    def _ready_nodes(dag: DynamicDAG) -> list[Node]:
        ready = []
        for node in dag.nodes.values():
            if node.state not in (NodeState.PENDING, NodeState.READY):
                continue
            if all(dag.nodes[dep].state == NodeState.COMPLETED for dep in node.dependencies):
                ready.append(node)
        return ready

    async def _execute_node(self, dag: DynamicDAG, node: Node) -> None:
        await self._semaphore.acquire()
        hardware_semaphore = self._hardware_semaphores.get(node.hardware)
        if hardware_semaphore is not None:
            await hardware_semaphore.acquire()
        try:
            self._record(dag.dag_id, node.node_id, "started")
            request = LLMRequest(
                model=node.model_config.model,
                prompt=node.prompt,
                sampling_params=node.model_config.sampling_params,
                request_id=f"{dag.dag_id}_{node.node_id}",
                metadata=build_node_request_metadata(dag.dag_id, node.node_id, node, dag),
            )
            request_id = await self._executor.submit(request)
            response = await self._executor.wait(request_id, timeout=self._node_timeout)
            if response.success:
                node.state = NodeState.COMPLETED
                node.result = {
                    "output": response.output,
                    "raw_output": response.output,
                    "sanitize_ok": True,
                }
            else:
                node.state = NodeState.FAILED
                node.result = {"error": response.error or "unknown"}
            self._record(dag.dag_id, node.node_id, "finished")
        finally:
            if hardware_semaphore is not None:
                hardware_semaphore.release()
            self._semaphore.release()

    def _record(self, dag_id: str, node_id: str, event: str) -> None:
        self._timeline.append({
            "dag_id": dag_id,
            "node_id": node_id,
            "event": event,
            "time": time.monotonic(),
        })
