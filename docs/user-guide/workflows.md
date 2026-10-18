# System Usage

This page constructs a two-node `draft → review` DAG through the Python API. Install Wenbo as described in [Quick Start](../quickstart.md) before running the examples.

## Build a DAG

```python
from wenbo.dag.builder import DAGBuilder

builder = DAGBuilder()
dag = builder.create_dag("workflow", priority=1)
builder.add_node(dag, "draft", prompt="Write a short draft.")
builder.add_node(dag, "review", prompt="Review: {draft_output}", dependencies=["draft"])
builder.validate(dag)
```

For execution, provide a backend implementing `ExecutorBackend` and pass it to `Scheduler`. See the backend and worker configuration below. The construction example above submits no model requests; see [Quick Start](../quickstart.md) for running paper experiments.

## Nodes and dependencies

`DAGBuilder.add_node(..., dependencies=[...])` requires predecessors to exist and creates control dependency edges. Alternatively, create all nodes first, then connect them with `add_dependency()`. Nodes without dependencies start as READY; nodes with dependencies start as PENDING.

`builder.validate(dag)` checks for cycles and complete dependency references, raising `ValidationError` on failure. `DAGValidator.validate(dag)` returns an error string or `None`. At submission, Director also checks for nonempty graphs, supported versions, resource estimates, and retry parameters, and normalizes hardware labels.

Node IDs must be unique within a DAG; DAG IDs must be unique within the target Registry. Create a new graph instance and DAG ID for each independent run. Do not directly reuse node states from an executed graph.

Load JSON input with `SyntheticDAGLoader.load_from_file()`, which returns a list of `(DynamicDAG, SyntheticDAGSpec)` pairs. See [DAG JSON Fields](../dag-schema.md) for the format and defaults.

## Pass upstream results

The prompt placeholder `{draft_output}` is replaced with the direct predecessor `draft`'s `result["output"]`, stripped of surrounding whitespace. Only direct dependencies are substituted; placeholders remain unchanged when results are missing. `Edge.data_mapping` is metadata and is not currently interpreted by PromptResolver.

By default, `Scheduler(output_mode="raw")` preserves successful backend outputs. Read a completed node's output from `node.result["output"]`.

## States, retries, and cancellation

A typical node follows `pending → ready → running → completed`. A submitted graph moves from `created` to `running`, then to `completed` when all nodes are completed or skipped. Failure and cancellation correspond to `failed` and `cancelled`.

Set `max_retries` and `retry_backoff_ms` when constructing nodes. The former is the number of additional attempts allowed after the initial failure; the latter is a fixed delay before each retry, in milliseconds. A retrying node may already be READY but cannot become a candidate until its backoff expires. Only in short_answer mode are outputs rejected by sanitization marked non-retriable.

Cancel a workflow with `await scheduler.cancel_dag(dag_id, reason)`. Unknown or terminal graphs return `False`; otherwise, the workflow is removed from the queue and its local execution tasks are cancelled. `await scheduler.stop()` cancels all unfinished graphs and cleans up background tasks. To retain complete results, wait for terminal graph states before stopping Scheduler.

The runtime continuously mutates submitted graph objects. Direct changes to `state`, dependencies, retry counters, or lease fields bypass event and index updates. This guide covers graph construction before submission. Changing graph structure during execution requires the caller to coordinate the relevant state; editing dictionaries alone is insufficient.

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

Workers are logical execution endpoints, not physical device reservations. Backend integrations are responsible for performing the actual computation.

## Built-in policies

| Name | Python class | Ordering |
| --- | --- | --- |
| `fifo` | `FIFOPolicy` | Preserve current candidate queue order |
| `sjf` | `ShortestJobFirstPolicy` | Ascending positive `estimated_tokens`; missing or nonpositive estimates last; ties use ready order |
| `priority` | `PriorityPolicy` | Descending DAG priority, then node priority, then ready order |
| `fair_rr` | `FairRoundRobinPolicy` | Select one node per DAG in round-robin order, following each DAG's first appearance among candidates |
| `fair_sjf` | `FairShortestJobFirstPolicy` | Round-robin across DAGs, with SJF ordering within each DAG |

`fair_rr` and `fair_sjf` regroup candidates on each `select` call and retain no cursor across scheduling rounds. They therefore provide no strict fairness guarantee across rounds. SJF uses structured token estimates rather than inferring costs from prompt length.

Create a policy with the factory function:

```python
from wenbo.scheduler.policy import build_policy

policy = build_policy("fair_sjf")
assert policy.policy_id == "fair_sjf@1"
```


Pass `policy=policy` when constructing Scheduler. Unknown policy names raise `ValueError`.


## Inspect execution

Use `scheduler.timeline` for node start and completion milestones, `scheduler.decisions` for policy choices, and `scheduler.trace_summary` for execution statistics. Runtime durations use a monotonic host clock; paper experiments use the separate benchmark's virtual clock.
