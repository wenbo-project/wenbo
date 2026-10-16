"""Event-driven DAG scheduler."""

from __future__ import annotations

import asyncio
import logging
import time
from enum import Enum
from typing import Optional

from wenbo.dag.model import DynamicDAG, NodeState
from wenbo.director import Director
from wenbo.executor.base import ExecutorBackend
from wenbo.executor.worker import WorkerPool
from wenbo.runtime.coordinator import ExecutionCoordinator
from wenbo.runtime.event import Event, EventBus, EventType
from wenbo.runtime.prompt import PromptResolver
from wenbo.runtime.timeline import TimelineCollector
from wenbo.runtime.trace import TraceEventType
from wenbo.scheduler.candidate import CandidateBuilder
from wenbo.scheduler.policy import FIFOPolicy, SchedulingPolicy
from wenbo.scheduler.queue import ReadyQueue

logger = logging.getLogger(__name__)


class SchedState(str, Enum):
    IDLE = "idle"
    COLLECTING = "collecting"
    SCHEDULING = "scheduling"


class Scheduler:
    def __init__(
        self,
        event_bus: EventBus,
        registry,
        state_tracker,
        executor: ExecutorBackend,
        policy: Optional[SchedulingPolicy] = None,
        max_concurrent: int = 4,
        debounce_ms: float = 5.0,
        heartbeat_ms: float = 100.0,
        node_timeout: float = 30.0,
        permissive_output: bool = False,
        resource_limits: Optional[dict[str, int]] = None,
        worker_pool: Optional[WorkerPool] = None,
        director: Optional[Director] = None,
    ) -> None:
        self._event_bus = event_bus
        self._registry = registry
        self._state_tracker = state_tracker
        self._executor = executor
        self._director = director or Director()
        self._policy = policy or FIFOPolicy()
        self._ready_queue = ReadyQueue()
        self._prompt_resolver = PromptResolver(registry)
        self._timeline_collector = TimelineCollector(policy_id=self._policy.policy_id)
        self._candidate_builder = CandidateBuilder(registry, self._ready_queue)
        self._worker_pool = worker_pool or WorkerPool.from_backend(
            executor,
            max_concurrent=max_concurrent,
            resource_limits=resource_limits,
        )
        self._placement_constraints = any(
            hardware != "any"
            for hardware in self._worker_pool.capacity_by_hardware
        )
        self._execution_coordinator = ExecutionCoordinator(
            registry=registry,
            state_tracker=state_tracker,
            executor=executor,
            prompt_resolver=self._prompt_resolver,
            timeline_collector=self._timeline_collector,
            node_timeout=node_timeout,
            on_finished=self._on_node_execution_finished,
            permissive_output=permissive_output,
        )
        self._max_concurrent = max(int(max_concurrent), 1)
        self._debounce_s = debounce_ms / 1000.0
        self._heartbeat_s = heartbeat_ms / 1000.0

        self._state = SchedState.IDLE
        self._pending_events: list[Event] = []
        self._running_count = 0
        self._debounce_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._shutdown = False
        self._started = False
        self._cancelled_dags: set[str] = set()
        self._terminating_dags: set[str] = set()
        self._traced_terminal_dags: set[str] = set()
        self._stats = {
            "schedule_rounds": 0,
            "candidate_builds": 0,
            "candidates_seen": 0,
            "decisions_made": 0,
            "policy_select_s": 0.0,
            "schedule_loop_s": 0.0,
            "resource_blocked_rounds": 0,
        }

    async def start(self) -> None:
        if self._started:
            return
        self._shutdown = False
        self._event_bus.subscribe(EventType.NODE_READY, self._on_node_ready)
        self._event_bus.subscribe(EventType.NODE_COMPLETED, self._on_event)
        self._event_bus.subscribe(EventType.NODE_FAILED, self._on_event)
        self._event_bus.subscribe(EventType.DAG_SUBMITTED, self._on_event)
        self._event_bus.subscribe(EventType.DAG_STRUCTURE_CHANGED, self._on_event)
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._started = True
        logger.info("Scheduler started (max_concurrent=%d)", self._max_concurrent)

    async def stop(self) -> None:
        if not self._started and self._shutdown:
            return
        self._shutdown = True
        tasks = []
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            tasks.append(self._heartbeat_task)
        if self._debounce_task:
            self._debounce_task.cancel()
            tasks.append(self._debounce_task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        # A scheduler stop is a cancellation boundary for unfinished local
        # work.  Marking DAGs terminal prevents a restart from rescheduling
        # nodes whose backend requests were already cancelled.
        for dag in self._registry.get_active_dags():
            await self._state_tracker.on_dag_cancelled(dag, "scheduler stopped")
        await self._execution_coordinator.close(cancel=True)
        self._ready_queue.clear()
        self._event_bus.unsubscribe(EventType.NODE_READY, self._on_node_ready)
        self._event_bus.unsubscribe(EventType.NODE_COMPLETED, self._on_event)
        self._event_bus.unsubscribe(EventType.NODE_FAILED, self._on_event)
        self._event_bus.unsubscribe(EventType.DAG_SUBMITTED, self._on_event)
        self._event_bus.unsubscribe(EventType.DAG_STRUCTURE_CHANGED, self._on_event)
        self._heartbeat_task = None
        self._debounce_task = None
        self._started = False
        logger.info("Scheduler stopped")

    async def submit_dag(self, dag: DynamicDAG) -> None:
        admitted = self._director.admit(dag, self._registry)
        self._timeline_collector.record_dag_arrived(
            admitted.dag_id,
            scheduling_round=self._stats["schedule_rounds"],
        )
        await self._state_tracker.on_dag_submitted(admitted)
        await self._event_bus.publish(Event(
            event_type=EventType.DAG_SUBMITTED,
            dag_id=admitted.dag_id,
            run_id=admitted.run_id,
        ))

    async def cancel_dag(self, dag_id: str, reason: str = "cancelled by caller") -> bool:
        """Cancel queued and running work for one admitted DAG."""
        dag = self._registry.get(dag_id)
        if dag is None:
            return False
        if dag.state.value in {"completed", "failed", "cancelled"}:
            return False
        self._cancelled_dags.add(dag_id)
        self._terminating_dags.add(dag_id)
        self._ready_queue.remove_dag(dag_id)
        cancelled = await self._state_tracker.on_dag_cancelled(dag, reason)
        self._record_terminal_dag(dag)
        await self._execution_coordinator.cancel_dag(dag_id)
        return cancelled

    async def _on_node_ready(self, event: Event) -> None:
        if event.dag_id and event.node_id:
            if event.dag_id in self._cancelled_dags:
                return
            dag = self._registry.get(event.dag_id)
            if dag:
                node = dag.nodes.get(event.node_id)
                if node and node.state == NodeState.READY:
                    self._ready_queue.enqueue(dag, node)
                    logger.debug("Enqueued node %s from DAG %s", event.node_id, event.dag_id)
        await self._on_event(event)

    async def _on_event(self, event: Event) -> None:
        if not event.data.get("_wenbo_scheduler_wakeup"):
            self._timeline_collector.record_event(
                event,
                scheduling_round=self._stats["schedule_rounds"],
            )
        self._pending_events.append(event)
        # 将短时间内的多个事件合并为一轮调度，减少重复扫描。
        if self._state == SchedState.IDLE:
            self._state = SchedState.COLLECTING
            self._debounce_task = asyncio.create_task(self._debounce_wait())

    async def _debounce_wait(self) -> None:
        await asyncio.sleep(self._debounce_s)
        if not self._shutdown:
            self._state = SchedState.SCHEDULING
            await self._schedule()

    async def _schedule(self) -> None:
        round_start = time.perf_counter()
        self._stats["schedule_rounds"] += 1
        self._pending_events.clear()

        try:
            # 可启动节点数同时受全局并发上限和空闲 Worker 数限制。
            available = self._max_concurrent - self._running_count
            available = min(available, self._worker_pool.available_count)
            if available <= 0 or not self._ready_queue:
                self._state = SchedState.IDLE
                return

            all_candidates = self._build_candidates()
            unsupported = [
                candidate
                for candidate in all_candidates
                if not self._worker_pool.supports(candidate.node)
            ]
            for candidate in unsupported:
                self._ready_queue.remove(candidate.dag_id, candidate.node_id)
                dag = self._registry.get(candidate.dag_id)
                if dag is not None:
                    await self._state_tracker.on_node_unschedulable(
                        dag,
                        candidate.node_id,
                        (
                            f"No registered worker capability supports hardware="
                            f"'{candidate.node.hardware}' and its resource requirements"
                        ),
                    )
                    if getattr(dag.state, "value", None) == "failed":
                        self._ready_queue.remove_dag(candidate.dag_id)
                        self._record_terminal_dag(dag)

            candidates = [
                candidate
                for candidate in all_candidates
                if self._worker_pool.can_run(candidate.node)
                and getattr(self._registry.get(candidate.dag_id).state, "value", None)
                == "running"
            ]

            if not self._placement_constraints:
                decisions = self._select(candidates, available)
                started_decisions = []
                assignments = {}
                for decision in decisions:
                    lease = await self._start_candidate(decision)
                    if lease is None:
                        continue
                    started_decisions.append(decision)
                    assignments[(decision.candidate.dag_id, decision.candidate.node_id)] = lease.worker_id
                self._record_decisions(started_decisions, assignments)
                self._state = SchedState.IDLE
                return

            started_decisions = []
            assignments = {}
            # Rank once per scheduling round.  A placement attempt may fail
            # after an earlier decision consumes the last compatible worker,
            # so continue through the ranked list to fill other hardware
            # buckets without rebuilding every candidate for every slot.
            ranked = self._select(candidates, len(candidates))
            for decision in ranked:
                if available <= 0:
                    break
                lease = await self._start_candidate(decision)
                if lease is None:
                    continue
                started_decisions.append(decision)
                assignments[(decision.candidate.dag_id, decision.candidate.node_id)] = lease.worker_id
                available -= 1

            if not started_decisions and self._ready_queue:
                self._stats["resource_blocked_rounds"] += 1

            self._record_decisions(started_decisions, assignments)

            self._state = SchedState.IDLE
        finally:
            self._stats["schedule_loop_s"] += time.perf_counter() - round_start

    def _build_candidates(self):
        candidates = self._candidate_builder.build()
        self._stats["candidate_builds"] += 1
        self._stats["candidates_seen"] += len(candidates)
        return candidates

    def _select(self, candidates, available_slots: int):
        select_start = time.perf_counter()
        decisions = self._policy.select(candidates, available_slots)
        self._stats["policy_select_s"] += time.perf_counter() - select_start
        return decisions

    async def _start_candidate(self, decision):
        candidate = decision.candidate
        # 启动前先占用兼容资源；启动失败时会归还该租约。
        lease = self._worker_pool.acquire(
            candidate.node,
            dag_id=candidate.dag_id,
            node_id=candidate.node_id,
            attempt=candidate.node.attempt + 1,
        )
        if lease is None:
            return None
        resource_type = str(candidate.node.hardware or lease.hardware).lower()
        self._timeline_collector.record_resource(
            TraceEventType.RESOURCE_ACQUIRED,
            candidate.dag_id,
            candidate.node_id,
            resource_type,
            scheduling_round=self._stats["schedule_rounds"],
            worker_id=lease.worker_id,
            policy_id=decision.policy_id,
        )
        if not self._ready_queue.remove(candidate.dag_id, candidate.node_id):
            self._worker_pool.release(lease)
            self._timeline_collector.record_resource(
                TraceEventType.RESOURCE_RELEASED,
                candidate.dag_id,
                candidate.node_id,
                resource_type,
                scheduling_round=self._stats["schedule_rounds"],
                worker_id=lease.worker_id,
                policy_id=decision.policy_id,
            )
            return None
        self._running_count += 1
        try:
            started = await self._execution_coordinator.start(candidate, lease)
        except Exception:
            self._running_count = max(0, self._running_count - 1)
            self._worker_pool.release(lease)
            self._timeline_collector.record_resource(
                TraceEventType.RESOURCE_RELEASED,
                candidate.dag_id,
                candidate.node_id,
                resource_type,
                scheduling_round=self._stats["schedule_rounds"],
                worker_id=lease.worker_id,
                policy_id=decision.policy_id,
            )
            logger.exception(
                "Failed to start node %s on worker %s",
                candidate.node_id,
                lease.worker_id,
            )
            return None
        if not started:
            self._running_count = max(0, self._running_count - 1)
            self._worker_pool.release(lease)
            self._timeline_collector.record_resource(
                TraceEventType.RESOURCE_RELEASED,
                candidate.dag_id,
                candidate.node_id,
                resource_type,
                scheduling_round=self._stats["schedule_rounds"],
                worker_id=lease.worker_id,
                policy_id=decision.policy_id,
            )
            return None
        self._timeline_collector.record_trace(
            TraceEventType.NODE_STARTED,
            candidate.dag_id,
            node_id=candidate.node_id,
            resource_type=resource_type,
            scheduling_round=self._stats["schedule_rounds"],
            worker_id=lease.worker_id,
            policy_id=decision.policy_id,
        )
        return lease

    def _record_decisions(self, decisions, assignments=None) -> None:
        self._stats["decisions_made"] += len(decisions)
        self._timeline_collector.record_decisions(decisions, assignments)

    async def _on_node_execution_finished(
        self,
        dag_id: str,
        node_id: str,
        worker_id: Optional[str] = None,
        lease_id: Optional[str] = None,
    ) -> None:
        self._running_count = max(0, self._running_count - 1)
        dag = self._registry.get(dag_id)
        node = dag.nodes.get(node_id) if dag is not None else None
        resource_type = str(getattr(node, "hardware", "any") or "any")
        released = False
        if lease_id is not None:
            released = self._worker_pool.release(lease_id)
        elif worker_id is not None:
            released = self._worker_pool.release(worker_id)
        if released:
            self._timeline_collector.record_resource(
                TraceEventType.RESOURCE_RELEASED,
                dag_id,
                node_id,
                resource_type,
                scheduling_round=self._stats["schedule_rounds"],
                worker_id=worker_id,
                policy_id=self._policy.policy_id,
            )
        if self._shutdown:
            return
        event_type = EventType.DAG_STRUCTURE_CHANGED
        if dag is not None:
            dag_state = getattr(dag.state, "value", None)
            if dag_state in {
                "completed",
                "failed",
                "cancelled",
            }:
                self._ready_queue.remove_dag(dag_id)
                self._record_terminal_dag(dag)
            if dag_state == "failed" and dag_id not in self._terminating_dags:
                self._terminating_dags.add(dag_id)
                await self._execution_coordinator.cancel_dag(dag_id, wait=False)
            node = dag.nodes.get(node_id)
            if node is not None:
                if node.state == NodeState.COMPLETED:
                    event_type = EventType.NODE_COMPLETED
                elif node.state == NodeState.FAILED:
                    event_type = EventType.NODE_FAILED
        await self._on_event(Event(
            event_type=event_type,
            dag_id=dag_id,
            run_id=dag.run_id if dag is not None else None,
            node_id=node_id,
            data={"_wenbo_scheduler_wakeup": True},
        ))

    def _record_terminal_dag(self, dag: DynamicDAG) -> None:
        if dag.dag_id in self._traced_terminal_dags:
            return
        event_type = {
            "completed": TraceEventType.DAG_COMPLETED,
            "failed": TraceEventType.DAG_FAILED,
            "cancelled": TraceEventType.DAG_CANCELLED,
        }.get(getattr(dag.state, "value", None))
        if event_type is None:
            return
        self._traced_terminal_dags.add(dag.dag_id)
        self._timeline_collector.record_trace(
            event_type,
            dag.dag_id,
            scheduling_round=self._stats["schedule_rounds"],
            policy_id=self._policy.policy_id,
        )

    async def _heartbeat_loop(self) -> None:
        while not self._shutdown:
            await asyncio.sleep(self._heartbeat_s)
            if self._state == SchedState.IDLE and (self._ready_queue or self._pending_events):
                await self._on_event(Event(event_type=EventType.NODE_COMPLETED))

    @property
    def running_count(self) -> int:
        return self._running_count

    @property
    def queue_size(self) -> int:
        return self._ready_queue.size

    @property
    def running_by_hardware(self) -> dict[str, int]:
        return self._worker_pool.running_by_hardware

    @property
    def workers(self):
        """Return a snapshot of registered workers for observability."""
        return self._worker_pool.workers

    @property
    def worker_pool(self) -> WorkerPool:
        return self._worker_pool

    @property
    def timeline(self) -> list[dict]:
        return self._timeline_collector.timeline

    @property
    def decisions(self) -> list[dict]:
        return self._timeline_collector.decisions

    @property
    def trace(self) -> list[dict]:
        return self._timeline_collector.trace

    @property
    def trace_summary(self) -> dict:
        return self._timeline_collector.trace_summary

    @property
    def stats(self) -> dict:
        stats = dict(self._stats)
        stats["policy_select_s"] = round(stats["policy_select_s"], 6)
        stats["schedule_loop_s"] = round(stats["schedule_loop_s"], 6)
        rounds = max(int(stats["schedule_rounds"]), 1)
        stats["avg_candidates_per_round"] = round(stats["candidates_seen"] / rounds, 3)
        stats["avg_decisions_per_round"] = round(stats["decisions_made"] / rounds, 3)
        stats["avg_schedule_loop_ms"] = round(stats["schedule_loop_s"] / rounds * 1000.0, 3)
        stats["avg_policy_select_ms"] = round(stats["policy_select_s"] / rounds * 1000.0, 3)
        return stats


__all__ = ["Scheduler", "SchedState"]
