"""In-process worker placement for the Wenbo scheduler.

The production architecture can place a node on a remote worker.  Wenbo keeps
the same boundary locally: a :class:`WorkerPool` owns worker capabilities and
leases, while an :class:`~wenbo.executor.base.ExecutorBackend` remains the
execution implementation.  This makes placement observable and replaceable
without forcing a distributed runtime on the current project.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any, Iterable, Optional
from uuid import uuid4

from wenbo.executor.base import ExecutorBackend


@dataclass(frozen=True)
class WorkerCapability:
    """Resources a worker advertises to the scheduler.

    A value of zero for a capacity means "unknown/unbounded".  That is useful
    for the existing generic mock and local backends, which do not expose
    device-level telemetry yet.
    """

    hardware: str = "any"
    gpu_memory_mb: float = 0.0
    cpu_cores: float = 0.0
    accepts_concrete_hardware: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    def supports(self, node: Any) -> bool:
        requested_hardware = str(getattr(node, "hardware", "any") or "any").lower()
        worker_hardware = self.hardware.lower()
        if requested_hardware not in {"", "any"}:
            if worker_hardware == "any" and not self.accepts_concrete_hardware:
                return False
            if worker_hardware not in {requested_hardware, "any"}:
                return False

        requirement = getattr(node, "resource_requirement", None)
        if requirement is None:
            return True

        # Generic workers deliberately do not claim a concrete capacity.  A
        # concrete worker, however, must reject requests it cannot satisfy.
        if worker_hardware == "cpu" and getattr(requirement, "gpu_count", 0) > 0:
            return False
        required_gpu_memory = max(
            float(getattr(requirement, "gpu_memory_mb", 0.0) or 0.0),
            float(getattr(requirement, "estimated_gpu_memory_mb", 0.0) or 0.0),
        )
        if self.gpu_memory_mb > 0 and required_gpu_memory > self.gpu_memory_mb:
            return False
        required_cpu = float(getattr(requirement, "cpu_cores", 0.0) or 0.0)
        if self.cpu_cores > 0 and required_cpu > self.cpu_cores:
            return False
        return True


@dataclass
class Worker:
    """A schedulable execution endpoint."""

    worker_id: str
    backend: ExecutorBackend
    capability: WorkerCapability = field(default_factory=WorkerCapability)
    healthy: bool = True
    active_tasks: int = 0

    @property
    def hardware(self) -> str:
        return self.capability.hardware

    def can_run(self, node: Any) -> bool:
        return self.healthy and self.active_tasks == 0 and self.capability.supports(node)


@dataclass(frozen=True)
class WorkerLease:
    """The immutable assignment passed to an execution task."""

    worker_id: str
    backend: ExecutorBackend
    hardware: str
    lease_id: str = field(default_factory=lambda: uuid4().hex)
    dag_id: str = ""
    node_id: str = ""
    attempt: int = 1
    expires_at: Optional[float] = None


class WorkerPool:
    """Small, synchronous worker registry used by the event-loop scheduler.

    The scheduler performs placement serially inside one scheduling round, so
    a lock would only add overhead.  Each worker has one slot today; multiple
    slots can be represented by registering multiple workers with the same
    backend when a backend supports concurrent requests.
    """

    def __init__(self, workers: Iterable[Worker]) -> None:
        self._workers = list(workers)
        if not self._workers:
            raise ValueError("WorkerPool requires at least one worker")
        ids = [worker.worker_id for worker in self._workers]
        if len(ids) != len(set(ids)):
            raise ValueError("Worker IDs must be unique")
        self._workers_by_id = {worker.worker_id: worker for worker in self._workers}
        self._worker_index = {
            worker.worker_id: index for index, worker in enumerate(self._workers)
        }
        self._workers_by_hardware: dict[str, list[Worker]] = {}
        for worker in self._workers:
            self._workers_by_hardware.setdefault(worker.hardware.lower(), []).append(worker)
        self._active_leases: dict[str, WorkerLease] = {}
        self._available_count = sum(
            1 for worker in self._workers if worker.healthy and worker.active_tasks == 0
        )
        self._capacity_by_hardware: dict[str, int] = {}
        for worker in self._workers:
            hardware = worker.hardware.lower()
            self._capacity_by_hardware[hardware] = (
                self._capacity_by_hardware.get(hardware, 0) + 1
            )
        self._cursor = 0

    @classmethod
    def from_backend(
        cls,
        backend: ExecutorBackend,
        *,
        max_concurrent: int = 1,
        resource_limits: Optional[dict[str, int]] = None,
    ) -> "WorkerPool":
        """Build a local pool while preserving the legacy Scheduler options."""
        limits = {
            str(hardware).lower(): int(limit)
            for hardware, limit in (resource_limits or {}).items()
            if int(limit) > 0
        }
        workers: list[Worker] = []
        if limits:
            for hardware, limit in limits.items():
                for index in range(limit):
                    workers.append(
                        Worker(
                            worker_id=f"{hardware}-{index}",
                            backend=backend,
                            capability=WorkerCapability(hardware=hardware),
                        )
                    )
            # A limit only constrains the resource named by the caller.  Keep
            # the previous behavior for the other common device type and for
            # explicitly hardware-agnostic nodes, subject to max_concurrent.
            generic_count = max(int(max_concurrent), 1)
            workers.extend(
                Worker(
                    worker_id=f"any-{index}",
                    backend=backend,
                    capability=WorkerCapability(
                        accepts_concrete_hardware=False,
                    ),
                )
                for index in range(generic_count)
            )
            for fallback_hardware in ("gpu", "cpu"):
                if fallback_hardware in limits:
                    continue
                for index in range(generic_count):
                    workers.append(
                        Worker(
                            worker_id=f"{fallback_hardware}-unlimited-{index}",
                            backend=backend,
                            capability=WorkerCapability(hardware=fallback_hardware),
                        )
                    )
        else:
            count = max(int(max_concurrent), 1)
            workers = [
                Worker(
                    worker_id=f"local-{index}",
                    backend=backend,
                    capability=WorkerCapability(),
                )
                for index in range(count)
            ]
        return cls(workers)

    def _candidate_workers(self, node: Any) -> list[Worker]:
        requested = str(getattr(node, "hardware", "any") or "any").lower()
        if requested in {"", "any"}:
            return self._workers
        candidates = list(self._workers_by_hardware.get(requested, ()))
        candidates.extend(
            worker
            for worker in self._workers_by_hardware.get("any", ())
            if worker.capability.accepts_concrete_hardware
        )
        return candidates

    def can_run(self, node: Any) -> bool:
        return any(worker.can_run(node) for worker in self._candidate_workers(node))

    # 区分硬件能力不支持与资源暂时忙碌，避免把等待资源的节点判为失败。
    def supports(self, node: Any) -> bool:
        """Return whether any registered capability can ever run the node."""
        return any(
            worker.capability.supports(node)
            for worker in self._candidate_workers(node)
        )

    def acquire(
        self,
        node: Any,
        *,
        dag_id: str = "",
        node_id: str = "",
        attempt: int = 1,
        lease_ttl_s: Optional[float] = None,
    ) -> Optional[WorkerLease]:
        """Reserve one compatible healthy worker, if one is available."""
        candidates = self._candidate_workers(node)
        count = len(candidates)
        if not count:
            return None
        for offset in range(count):
            worker = candidates[(self._cursor + offset) % count]
            if not worker.can_run(node):
                continue
            if worker.active_tasks == 0:
                self._available_count -= 1
            worker.active_tasks += 1
            self._cursor = (self._worker_index[worker.worker_id] + 1) % len(self._workers)
            lease = WorkerLease(
                worker_id=worker.worker_id,
                backend=worker.backend,
                hardware=worker.hardware,
                dag_id=dag_id,
                node_id=node_id,
                attempt=max(int(attempt), 1),
                expires_at=(
                    time.monotonic() + float(lease_ttl_s)
                    if lease_ttl_s is not None
                    else None
                ),
            )
            self._active_leases[lease.lease_id] = lease
            return lease
        return None

    def release(self, lease: WorkerLease | str) -> bool:
        if isinstance(lease, WorkerLease):
            active = self._active_leases.pop(lease.lease_id, None)
            if active is None:
                return False
            worker = self._workers_by_id.get(active.worker_id)
        else:
            active = self._active_leases.pop(lease, None)
            if active is not None:
                worker = self._workers_by_id.get(active.worker_id)
            else:
                worker = self._workers_by_id.get(lease)
            if worker is None:
                return False
                # Keep the legacy worker-id release API, but still retire the
                # corresponding lease.  Otherwise a string release would
                # leave a stale lease forever and make expiration incorrect.
                active_id = next(
                    (
                        lease_id
                        for lease_id, active_lease in self._active_leases.items()
                        if active_lease.worker_id == worker.worker_id
                    ),
                    None,
                )
                if active_id is not None:
                    self._active_leases.pop(active_id, None)
        if worker is None or worker.active_tasks <= 0:
            return False
        worker.active_tasks -= 1
        if worker.healthy and worker.active_tasks == 0:
            self._available_count += 1
        return True

    def reap_expired(self, now: Optional[float] = None) -> list[WorkerLease]:
        """Release leases whose monotonic deadline has elapsed.

        The scheduler can call this from its heartbeat.  Returning the
        expired leases lets a higher layer decide whether the corresponding
        node should be retried, failed, or marked unknown.
        """
        current = time.monotonic() if now is None else float(now)
        expired: list[WorkerLease] = []
        for lease_id, lease in list(self._active_leases.items()):
            if lease.expires_at is None or lease.expires_at > current:
                continue
            self._active_leases.pop(lease_id, None)
            worker = self._workers_by_id.get(lease.worker_id)
            if worker is None or worker.active_tasks <= 0:
                continue
            worker.active_tasks -= 1
            if worker.healthy and worker.active_tasks == 0:
                self._available_count += 1
            expired.append(lease)
        return expired

    def set_health(self, worker_id: str, healthy: bool) -> bool:
        worker = self._workers_by_id.get(worker_id)
        if worker is None:
            return False
        if worker.healthy == healthy:
            return True
        if worker.active_tasks == 0:
            self._available_count += 1 if healthy else -1
        worker.healthy = healthy
        return True

    @property
    def workers(self) -> list[Worker]:
        return list(self._workers)

    @property
    def available_count(self) -> int:
        return self._available_count

    @property
    def active_count(self) -> int:
        return sum(worker.active_tasks for worker in self._workers)

    @property
    def active_lease_count(self) -> int:
        return len(self._active_leases)

    @property
    def running_by_hardware(self) -> dict[str, int]:
        running: dict[str, int] = {}
        for worker in self._workers:
            if worker.active_tasks:
                running[worker.hardware] = running.get(worker.hardware, 0) + worker.active_tasks
        return running

    @property
    def capacity_by_hardware(self) -> dict[str, int]:
        return dict(self._capacity_by_hardware)


__all__ = ["Worker", "WorkerCapability", "WorkerLease", "WorkerPool"]
