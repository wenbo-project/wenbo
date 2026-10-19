# System Usage

The [Introduction](../introduction.rst) explains Wenbo's paper architecture. This page shows how to use the current Python runtime: construct a DAG, provide an execution backend, and submit the graph to the scheduler.

## From graph to execution

Callers provide the DAG. During submission, **Director** validates and admits it; **Scheduler** tracks dependency-ready nodes across submitted DAGs and selects available **Workers**; Workers use an `ExecutorBackend` to execute nodes and return results. Completed nodes can unlock their successors.

The paper describes reasoning-graph generation and ongoing refinement. The current `Director` API accepts a pre-built DAG; it does not generate one from a task description. Worker slots represent execution capacity in this runtime, while the backend performs the actual computation.

## Build a DAG

```python
from wenbo.dag.builder import DAGBuilder

builder = DAGBuilder()
dag = builder.create_dag("workflow", priority=1)
builder.add_node(dag, "draft", prompt="Write a short draft.")
builder.add_node(dag, "review", prompt="Review: {draft_output}", dependencies=["draft"])
builder.validate(dag)
```

`draft` can run immediately; `review` becomes ready after `draft` completes. `builder.validate(dag)` checks the dependency graph. Node IDs must be unique within a DAG.

The `{draft_output}` placeholder uses the direct predecessor's result. A completed node's output is available at `node.result["output"]`. See the [JSON format](../dag-schema.md) if your workflows come from files.

## Execute nodes

Provide an `ExecutorBackend` to run the actual computation. Its asynchronous interface has four methods: `submit`, `query`, `wait`, and `cancel`. A backend accepts `LLMRequest` objects and yields `LLMResponse` objects through `wait()`; see the [API reference](../reference/api.md) for module paths.

Workers are logical execution endpoints. For example, this function configures one GPU slot and one CPU slot for a caller-supplied backend:

```python
from wenbo.executor.worker import Worker, WorkerCapability, WorkerPool


def create_worker_pool(backend):
    return WorkerPool([
        Worker(
            worker_id=f"{hardware}-{index}",
            backend=backend,
            capability=WorkerCapability(hardware=hardware),
        )
        for hardware, count in (("gpu", 1), ("cpu", 1))
        for index in range(count)
    ])
```

Pass the resulting pool as `worker_pool` when constructing `Scheduler`, and set `max_concurrent` to bound simultaneous execution. Creating worker slots does not allocate physical hardware; the backend performs the actual work.

Start the scheduler with `await scheduler.start()` and submit a graph with `await scheduler.submit_dag(dag)`. Submission returns before execution finishes; wait for the graph to reach a terminal state before calling `await scheduler.stop()`, which cancels unfinished work. Nodes progress from pending to ready, running, and completed. Retry and cancellation are available through node retry settings and `scheduler.cancel_dag()`.

## Scheduling and results

The scheduler chooses ready nodes across active DAGs and matches them to available worker slots. Built-in policy names include `fifo`, `sjf`, `priority`, `fair_rr`, and `fair_sjf`; choose one with `build_policy(name)` and pass it as `policy` to `Scheduler`.

Inspect `scheduler.timeline` for node milestones, `scheduler.decisions` for policy choices, and `scheduler.trace_summary` for runtime statistics. These host runtime records are separate from the benchmark's virtual-time results.
