"""Execution coordination for scheduled nodes."""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from typing import Any

from wenbo.executor.base import ExecutorBackend, LLMRequest
from wenbo.executor.worker import WorkerLease
from wenbo.runtime.prompt import PromptResolver
from wenbo.runtime.timeline import TimelineCollector

logger = logging.getLogger(__name__)

FinishCallback = Callable[[str, str, str | None, str | None], Awaitable[None]]


def _strip_reasoning(text: str) -> str:
    cleaned = text.strip()
    cleaned = re.sub(r"<think>[\s\S]*?</think>", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"</?think>", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\*\*Answer:\*\*", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(final answer|answer)\s*:\s*", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"Thinking Process:[\s\S]*", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .\n\t:-")
    return cleaned


def _invalid_short_answer(text: str) -> bool:
    if not text:
        return True
    lowered = text.lower().strip()
    if lowered in {"answer", "final answer", "unknown", "n/a"}:
        return True
    if len(lowered) > 48 and " " in lowered:
        return True
    if re.fullmatch(r"\d{8,}", lowered):
        return True
    return False


def _fallback_output(output: str) -> str:
    without_think = re.sub(r"<think>[\s\S]*?</think>", "\n", output, flags=re.IGNORECASE)
    without_think = re.sub(r"</?think>", " ", without_think, flags=re.IGNORECASE)
    lines = [line.strip(" .\t:-") for line in without_think.splitlines() if line.strip()]
    if lines:
        tail = lines[-1]
        if tail and len(tail) <= 160:
            return tail

    cleaned = _strip_reasoning(output)
    if not cleaned:
        return ""

    sentence = re.split(r"[\n。?!?]", cleaned)[0].strip(" .\t:-")
    return sentence[:160]


def _sanitize_output(prompt: str, output: str) -> str:
    cleaned = _strip_reasoning(output)
    lowered_prompt = prompt.lower()

    if re.search(r"\byes or no\b", lowered_prompt):
        match = re.search(r"\b(yes|no)\b", cleaned, flags=re.IGNORECASE)
        return match.group(1).lower() if match else ""

    if re.search(r"\bnumber only\b|\byear only\b|\binteger\b", lowered_prompt):
        match = re.search(r"-?\d+", cleaned)
        return match.group(0) if match else ""

    first_line = cleaned.splitlines()[0].strip() if cleaned else ""
    first_sentence = re.split(r"[\n。?!?]", first_line)[0].strip()
    candidate = first_sentence or cleaned[:64].strip()
    return "" if _invalid_short_answer(candidate) else candidate


def build_node_request_metadata(
    dag_id: str,
    node_id: str,
    node: Any,
    dag: Any = None,
    *,
    worker_id: str | None = None,
    worker_hardware: str | None = None,
    attempt: int = 0,
    lease_id: str | None = None,
) -> dict:
    """Collect graph, resource, worker, attempt, and lease metadata for a request.

    Transfer sizes are in bytes; resource estimates keep their ResourceReq
    units. Incoming size comes from node metadata, or is derived from
    incoming graph edges when that metadata is absent.
    """
    resource = node.resource_requirement
    incoming_data_volume_bytes = node.metadata.get("incoming_data_volume_bytes")
    if incoming_data_volume_bytes is None and dag is not None:
        incoming_data_volume_bytes = sum(
            edge.data_volume_bytes for edge in dag.edges.values() if edge.target == node_id
        )
    incoming_data_volume_bytes = int(incoming_data_volume_bytes or 0)

    metadata = {
        "dag_id": dag_id,
        "run_id": getattr(dag, "run_id", None),
        "dag_version": getattr(dag, "version", 1),
        "node_id": node_id,
        "attempt": int(attempt),
        "lease_id": lease_id,
        "node_type": node.node_type.value,
        "hardware": node.hardware,
        "estimated_tokens": node.estimated_tokens,
        "estimated_cost": dict(node.estimated_cost),
        "resource_requirement": resource.__dict__ if resource is not None else {},
        "incoming_data_volume_bytes": incoming_data_volume_bytes,
        "node_metadata": dict(node.metadata),
    }
    if worker_id is not None:
        metadata["worker_id"] = worker_id
    if worker_hardware is not None:
        metadata["worker_hardware"] = worker_hardware
    return metadata


class ExecutionCoordinator:
    """Manage local async execution tasks and report results to the state tracker.

    node_timeout is the backend wait limit in seconds. on_finished is
    awaited to notify the scheduler after each execution task releases
    its local bookkeeping. A worker lease can override the default backend.
    """

    def __init__(
        self,
        registry: Any,
        state_tracker: Any,
        executor: ExecutorBackend,
        prompt_resolver: PromptResolver,
        timeline_collector: TimelineCollector,
        node_timeout: float,
        on_finished: FinishCallback,
        permissive_output: bool = False,
        output_mode: str = "raw",
    ) -> None:
        if output_mode not in {"raw", "short_answer"}:
            raise ValueError("output_mode must be raw or short_answer")
        self._output_mode = output_mode
        self._registry = registry
        self._state_tracker = state_tracker
        self._executor = executor
        self._prompt_resolver = prompt_resolver
        self._timeline_collector = timeline_collector
        self._node_timeout = node_timeout
        self._on_finished = on_finished
        self._permissive_output = permissive_output
        self._tasks: set[asyncio.Task] = set()
        self._finalizers: set[asyncio.Task] = set()
        self._cleanup_tasks: dict[asyncio.Task, asyncio.Task] = {}
        self._active_requests: dict[asyncio.Task, tuple[ExecutorBackend, str]] = {}
        self._task_metadata: dict[asyncio.Task, tuple[str, str]] = {}

    async def start(self, candidate: Any, worker: WorkerLease | None = None) -> bool:
        """Start a background execution task; return False if the graph or READY node is unavailable."""
        dag = self._registry.get(candidate.dag_id)
        if dag is None:
            return False

        worker_id = worker.worker_id if worker is not None else None
        attempt = worker.attempt if worker is not None else max(candidate.node.attempt + 1, 1)
        lease_id = worker.lease_id if worker is not None else None
        started = await self._state_tracker.on_node_started(
            dag,
            candidate.node_id,
            attempt=attempt,
            lease_id=lease_id,
            worker_id=worker.worker_id if worker is not None else None,
        )
        if started is False:
            return False
        self._timeline_collector.record(
            candidate.dag_id,
            candidate.node_id,
            "started",
            worker_id=worker_id,
        )
        task = asyncio.create_task(self._execute(candidate, worker))
        self._tasks.add(task)
        self._task_metadata[task] = (candidate.dag_id, candidate.node_id)

        def done(execution: asyncio.Task) -> None:
            finalizer = asyncio.create_task(self._finish(execution, candidate, worker))
            self._finalizers.add(finalizer)
            self._cleanup_tasks[execution] = finalizer
            finalizer.add_done_callback(self._finalizers.discard)

        task.add_done_callback(done)
        return True

    async def close(self, cancel: bool = True) -> None:
        """Await active tasks, requesting backend and task cancellation when cancel is True."""
        if not self._tasks:
            return
        tasks = list(self._tasks)
        if cancel:
            for executor, request_id in list(self._active_requests.values()):
                try:
                    await executor.cancel(request_id)
                except Exception:
                    logger.exception("Failed to cancel executor request %s", request_id)
            for task in tasks:
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        finalizers = [t for t in self._finalizers if t is not asyncio.current_task()]
        if finalizers:
            await asyncio.gather(*finalizers)

    async def cancel_dag(self, dag_id: str, *, wait: bool = True) -> None:
        """Cancel execution tasks belonging to one DAG and its backend calls."""
        current_task = asyncio.current_task()
        tasks = [
            task
            for task in list(self._tasks)
            if self._task_metadata.get(task, (None, None))[0] == dag_id and task is not current_task
        ]
        if not tasks:
            return
        for task in tasks:
            active = self._active_requests.get(task)
            if active is not None:
                executor, request_id = active
                try:
                    await executor.cancel(request_id)
                except Exception:
                    logger.exception("Failed to cancel executor request %s", request_id)
            if not task.done():
                task.cancel()
        if wait:
            await asyncio.gather(*tasks, return_exceptions=True)
            finalizers = [
                self._cleanup_tasks[t]
                for t in tasks
                if t in self._cleanup_tasks and self._cleanup_tasks[t] is not current_task
            ]
            if finalizers:
                await asyncio.gather(*finalizers)

    async def _execute(self, candidate: Any, worker: WorkerLease | None = None) -> None:
        dag_id = candidate.dag_id
        node_id = candidate.node_id
        node = candidate.node
        task = asyncio.current_task()
        request_id = ""
        attempt = worker.attempt if worker is not None else node.attempt
        lease_id = worker.lease_id if worker is not None else None
        try:
            # Resolve upstream outputs before submitting the request through the assigned backend.
            prompt = self._prompt_resolver.resolve(dag_id, node)
            executor = worker.backend if worker is not None else self._executor
            dag = self._registry.get(dag_id)
            run_id = getattr(dag, "run_id", "run")
            request = LLMRequest(
                model=node.model_config.model,
                prompt=prompt,
                sampling_params=node.model_config.sampling_params,
                request_id=f"{dag_id}_{run_id}_{node_id}_{attempt}",
                metadata=build_node_request_metadata(
                    dag_id,
                    node_id,
                    node,
                    dag,
                    worker_id=worker.worker_id if worker is not None else None,
                    worker_hardware=worker.hardware if worker is not None else None,
                    attempt=attempt,
                    lease_id=lease_id,
                ),
            )
            request_id = await executor.submit(request)
            node.request_id = request_id
            if task is not None:
                self._active_requests[task] = (executor, request_id)
            response = await executor.wait(request_id, timeout=self._node_timeout)

            dag = self._registry.get(dag_id)
            if dag is None:
                return

            if response.success:
                sanitized_output = (
                    _sanitize_output(prompt, response.output)
                    if self._output_mode == "short_answer"
                    else response.output
                )
                final_output = sanitized_output
                if self._permissive_output and not final_output:
                    final_output = _fallback_output(response.output)

                if self._output_mode == "raw" or final_output:
                    await self._state_tracker.on_node_completed(
                        dag,
                        node_id,
                        {
                            "output": final_output,
                            "raw_output": response.output,
                            "sanitized_output": sanitized_output,
                            "sanitize_ok": bool(sanitized_output),
                        },
                        attempt=attempt,
                        lease_id=lease_id,
                        worker_id=worker.worker_id if worker is not None else None,
                    )
                else:
                    await self._state_tracker.on_node_failed(
                        dag,
                        node_id,
                        "invalid sanitized output",
                        attempt=attempt,
                        lease_id=lease_id,
                        worker_id=worker.worker_id if worker is not None else None,
                        retryable=False,
                    )
                    node.result = {
                        "output": "",
                        "raw_output": response.output,
                        "sanitized_output": sanitized_output,
                        "sanitize_ok": False,
                    }
            else:
                await self._state_tracker.on_node_failed(
                    dag,
                    node_id,
                    response.error or "unknown",
                    attempt=attempt,
                    lease_id=lease_id,
                    worker_id=worker.worker_id if worker is not None else None,
                )
        except asyncio.CancelledError:
            logger.info("Cancelled execution of node %s in DAG %s", node_id, dag_id)
            dag = self._registry.get(dag_id)
            if dag is not None:
                await self._state_tracker.on_node_cancelled(
                    dag,
                    node_id,
                    "execution cancelled",
                    attempt=attempt,
                    lease_id=lease_id,
                    worker_id=worker.worker_id if worker is not None else None,
                )
            raise
        except Exception as e:
            logger.error("Error executing node %s: %s", node_id, e)
            dag = self._registry.get(dag_id)
            if dag:
                await self._state_tracker.on_node_failed(
                    dag,
                    node_id,
                    str(e),
                    attempt=attempt,
                    lease_id=lease_id,
                    worker_id=worker.worker_id if worker is not None else None,
                )
        finally:
            if task is not None:
                self._active_requests.pop(task, None)
            node.request_id = None

    async def _finish(self, task: asyncio.Task, candidate: Any, worker: WorkerLease | None) -> None:
        # A done callback runs even if cancellation prevented coroutine entry.
        dag_id, node_id = candidate.dag_id, candidate.node_id
        try:
            if task.cancelled():
                dag = self._registry.get(dag_id)
                if dag is not None:
                    await self._state_tracker.on_node_cancelled(
                        dag,
                        node_id,
                        "execution cancelled",
                        attempt=worker.attempt if worker is not None else candidate.node.attempt,
                        lease_id=worker.lease_id if worker is not None else None,
                    )
        finally:
            self._active_requests.pop(task, None)
            candidate.node.request_id = None
            try:
                self._timeline_collector.record(
                    dag_id,
                    node_id,
                    "finished",
                    worker_id=worker.worker_id if worker is not None else None,
                )
                # Notify the scheduler on success, failure, or cancellation to release resources and resume scheduling.
                await self._on_finished(
                    dag_id,
                    node_id,
                    worker.worker_id if worker is not None else None,
                    worker.lease_id if worker is not None else None,
                )
            finally:
                self._cleanup_tasks.pop(task, None)
                self._tasks.discard(task)
                self._task_metadata.pop(task, None)


__all__ = [
    "ExecutionCoordinator",
    "_fallback_output",
    "_sanitize_output",
    "build_node_request_metadata",
]
