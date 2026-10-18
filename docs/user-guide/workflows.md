# Building and Running Workflows

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

For execution, provide a backend implementing `ExecutorBackend` and pass it to `Scheduler`. See [Execution Backends and Workers](execution.md) for the backend interface and worker configuration. The construction example above submits no model requests; see [Quick Start](../quickstart.md) for running paper experiments.

## Nodes and dependencies

`DAGBuilder.add_node(..., dependencies=[...])` requires predecessors to exist and creates control dependency edges. Alternatively, create all nodes first, then connect them with `add_dependency()`. Nodes without dependencies start as READY; nodes with dependencies start as PENDING.

`builder.validate(dag)` checks for cycles and complete dependency references, raising `ValidationError` on failure. `DAGValidator.validate(dag)` returns an error string or `None`. At submission, Director also checks for nonempty graphs, supported versions, resource estimates, and retry parameters, and normalizes hardware labels.

Node IDs must be unique within a DAG; DAG IDs must be unique within the target Registry. Create a new graph instance and DAG ID for each independent run. Do not directly reuse node states from an executed graph.

Load JSON input with `SyntheticDAGLoader.load_from_file()`, which returns a list of `(DynamicDAG, SyntheticDAGSpec)` pairs. See [DAG JSON Fields](../dag-schema.md) for the format and defaults.

## Pass upstream results

The prompt placeholder `{draft_output}` is replaced with the direct predecessor `draft`'s `result["output"]`, stripped of surrounding whitespace. Only direct dependencies are substituted; placeholders remain unchanged when results are missing. `Edge.data_mapping` is metadata and is not currently interpreted by PromptResolver.

By default, `Scheduler(output_mode="raw")` preserves successful backend responses unchanged, including JSON, multiline text, and empty strings. Successful node results retain `output`, `raw_output`, `sanitized_output`, and `sanitize_ok`. In raw mode, the first three contain the original output, and `sanitize_ok` indicates whether it is nonempty. To use legacy short-answer rules, explicitly set `output_mode="short_answer"`. That mode extracts a short answer and treats extraction failure as non-retriable; `permissive_output=True` enables fallback extraction.

## States, retries, and cancellation

A typical node follows `pending → ready → running → completed`. A submitted graph moves from `created` to `running`, then to `completed` when all nodes are completed or skipped. Failure and cancellation correspond to `failed` and `cancelled`.

Set `max_retries` and `retry_backoff_ms` when constructing nodes. The former is the number of additional attempts allowed after the initial failure; the latter is a fixed delay before each retry, in milliseconds. A retrying node may already be READY but cannot become a candidate until its backoff expires. Only in short_answer mode are outputs rejected by sanitization marked non-retriable.

Cancel a workflow with `await scheduler.cancel_dag(dag_id, reason)`. Unknown or terminal graphs return `False`; otherwise, the workflow is removed from the queue and its local execution tasks are cancelled. `await scheduler.stop()` cancels all unfinished graphs and cleans up background tasks. To retain complete results, wait for terminal graph states before stopping Scheduler.

The runtime continuously mutates submitted graph objects. Direct changes to `state`, dependencies, retry counters, or lease fields bypass event and index updates. This guide covers graph construction before submission. Changing graph structure during execution requires the caller to coordinate the relevant state; editing dictionaries alone is insufficient.
