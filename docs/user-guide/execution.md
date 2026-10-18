# Execution Backends and Workers

ExecutorBackend defines how requests are submitted and awaited. WorkerPool determines which execution endpoint can run a node now. Scheduler connects them through a `WorkerLease`.

## Built-in backend

| Backend | Execution model | Primary use |
| --- | --- | --- |
| `SyntheticBackend` | Estimate latency from structured resource estimates, then wait asynchronously | Throughput experiments through the runtime execution path |


SyntheticBackend prioritizes a positive `resource_requirement.estimated_time_ms`. Without an explicit duration, it estimates latency from hardware, node type, tokens, and incoming bytes. Waiting occurs in the host event loop, unlike the discrete-event virtual time used by the [separate benchmark](../experiments/real-datasets.md).

## Backend protocol

| Method | Purpose | Return value |
| --- | --- | --- |
| `await submit(request)` | Accept an `LLMRequest` | Backend request ID |
| `await query(request_id)` | Query backend status | `RequestStatus` |
| `await wait(request_id, timeout=30.0)` | Await an execution response; timeout is in seconds | `LLMResponse` |
| `await cancel(request_id)` | Request cancellation | Boolean indicating whether the backend accepted it |

`LLMRequest` contains a model name, prompt, sampling parameters, and structured metadata. Coordinator adds DAG/run/node IDs, attempt, lease ID, resource requirements, and worker identity. `LLMResponse` contains output, success, error, token counts, and latency in milliseconds.


Custom backends subclass `ExecutorBackend`, implement these four asynchronous methods, and manage request status, timeouts, and cancellation.

## Explicit worker pools


The following function configures two GPU slots and sixteen CPU slots for a caller-supplied backend:

```python
from wenbo.executor.worker import Worker, WorkerCapability, WorkerPool


def create_worker_pool(backend):
    return WorkerPool([
        Worker(
            worker_id=f"{hardware}-{index}",
            backend=backend,
            capability=WorkerCapability(hardware=hardware),
        )
        for hardware, count in (("gpu", 2), ("cpu", 16))
        for index in range(count)
    ])
```

Pass `worker_pool=pool` and an appropriate `max_concurrent` when constructing Scheduler. An explicit worker pool takes precedence over `resource_limits`; global concurrency remains bounded by `max_concurrent`. Slots are logical execution endpoints: creating workers does not allocate physical hardware.

Without an explicit pool, Scheduler creates local endpoints through `WorkerPool.from_backend()`. Its `resource_limits` constrain only explicitly listed hardware types with positive limits. Unlisted CPU/GPU types and `any` nodes still have fallback slots. Use an explicit WorkerPool when endpoint types must be controlled precisely.

## Capabilities, health, and leases

`WorkerCapability.supports(node)` checks capabilities such as hardware, declared CPU cores, and GPU memory, without considering occupancy or health. CPU workers reject nodes declaring GPU requirements. Zero capacity means unknown or unlimited, not physically zero capacity. The current matcher does not perform complete physical device resource reservation.

`Worker.can_run(node)` additionally requires a healthy, idle worker. Each worker currently has one slot. Register multiple endpoints for multiple slots, and ensure a shared backend supports concurrency. Update health with `pool.set_health(worker_id, healthy)`; direct worker mutation bypasses pool counters.

`acquire()` returns a lease or `None`. A lease contains a unique lease ID, worker, DAG/node identifiers, and attempt. Optional expiry uses seconds on the `time.monotonic()` clock. `release()` accepts a lease, lease ID, or worker ID; releasing by worker ID handles only one matching lease.

`reap_expired()` reclaims and returns expired leases. It updates allocation records only and does not automatically cancel backend work or retry nodes. In the normal scheduling path, Scheduler releases leases when execution ends. WorkerPool is intended for serial allocation within one event loop; it is not a thread-safe distributed resource manager.
