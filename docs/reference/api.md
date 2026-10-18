# API Index

This page lists common Python interfaces and source entry points for the current version. Explicit module paths support precise imports; consult the corresponding source docstrings for defaults, units, return values, and exceptions.

See the [workflow guide](../user-guide/workflows.md) for usage examples and [System Architecture](../architecture.md) for component relationships. This index is maintained manually; documentation maintainers may add generated API documentation later.

## Graph models and construction

| Module | Symbols | Purpose |
| --- | --- | --- |
| `wenbo.dag.model` | `DynamicDAG`, `Node`, `Edge` | Mutable workflow graphs, nodes, and directed dependencies |
| `wenbo.dag.model` | `NodeType`, `NodeState`, `DAGState` | Semantic type and execution state enumerations |
| `wenbo.dag.model` | `ModelConfig`, `ResourceReq`, `QoS` | Model parameters, resource estimates, and service objective metadata |
| `wenbo.dag.builder` | `DAGBuilder` | Create graphs, add nodes and dependencies, and request validation |
| `wenbo.dag.validator` | `DAGValidator`, `ValidationError` | Graph structure validation and the exception used by Builder |
| `wenbo.dag.registry` | `DAGRegistry` | Index graph instances by graph ID, agent, and state |

Source: [model](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/dag/model.py), [builder](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/dag/builder.py), [validator](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/dag/validator.py), [registry](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/dag/registry.py).

`DAGBuilder.create_dag()` creates a graph; `add_node()` can also declare existing predecessors; `add_dependency()` connects existing nodes. `builder.validate(dag)` raises `ValidationError` on failure, while `DAGValidator.validate(dag)` returns an error string or `None`.

Registry retains the original object without copying the graph. Its `update_state()` maintains only the state index and does not modify `dag.state`. Normal runtime state transitions should go through StateTracker.

`ResourceReq` uses milliseconds for durations and MB for memory; data transfer volume is expressed separately in bytes. `QoS` is descriptive metadata and does not guarantee latency or throughput objectives during execution.

## Admission and scheduling

| Module | Symbols | Purpose |
| --- | --- | --- |
| `wenbo.director.director` | `Director`, `AdmissionLimits` | Normalize and validate graphs and enforce admission limits |
| `wenbo.scheduler.scheduler` | `Scheduler` | Asynchronous scheduling lifecycle, submission, cancellation, and observation |
| `wenbo.scheduler.policy` | `SchedulingPolicy`, `build_policy` | Policy extension protocol and built-in policy factory |
| `wenbo.scheduler.policy` | `FIFOPolicy`, `ShortestJobFirstPolicy`, `PriorityPolicy` | FIFO, token-estimate ordering, and priority ordering |
| `wenbo.scheduler.policy` | `FairRoundRobinPolicy`, `FairShortestJobFirstPolicy` | Round-robin selection across DAGs within a single selection call |
| `wenbo.scheduler.types` | `SchedulingCandidate`, `SchedulingDecision` | Policy inputs, selection order, and explanations |
| `wenbo.scheduler.queue` | `ReadyQueue` | Deduplicated ready-node queue |
| `wenbo.scheduler.candidate` | `CandidateBuilder` | Filter candidates from the Registry and ready queue |

Source: [admission](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/director/director.py), [scheduler](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/scheduler/scheduler.py), [policies](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/scheduler/policy.py), [record types](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/scheduler/types.py), [queue](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/scheduler/queue.py), [candidate construction](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/scheduler/candidate.py).

| Common method | Contract |
| --- | --- |
| `Director.compile(dag)` | Normalize and validate in place, returning the same graph; constraint violations raise `ValidationError`, and invalid input types raise `TypeError` |
| `Director.admit(dag, registry)` | Compile and register the graph; reject DAG IDs already in the Registry |
| `await Scheduler.start()` | Subscribe to events and start the background heartbeat; repeated calls do not start it again |
| `await Scheduler.submit_dag(dag)` | Admit through Director, initialize runtime state, and trigger ready events |
| `await Scheduler.cancel_dag(dag_id, reason)` | Cancel a nonterminal graph; return `False` for unknown or terminal graphs |
| `await Scheduler.stop()` | Cancel unfinished workflows and clean up background execution tasks |
| `SchedulingPolicy.select(candidates, available_slots)` | Return ordered decisions without directly modifying graphs or allocating workers |

See [Scheduling Policies](../user-guide/scheduling.md) and [Execution Backends and Workers](../user-guide/execution.md) for Scheduler concurrency, resource, and policy constraints.

## Execution backends and resources

| Module | Symbols | Purpose |
| --- | --- | --- |
| `wenbo.executor.base` | `ExecutorBackend`, `LLMRequest`, `LLMResponse`, `RequestStatus` | Asynchronous backend protocol and request/response structures |
| `wenbo.executor.synthetic_backend` | `SyntheticBackend`, `SyntheticBackendConfig` | Simulate runtime delays from structured costs |
| `wenbo.executor.worker` | `Worker`, `WorkerCapability` | Single-slot endpoints, health, and capability matching |
| `wenbo.executor.worker` | `WorkerPool`, `WorkerLease` | Allocate, release, and track leases |


Source: [backend protocol](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/executor/base.py), [workers](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/executor/worker.py).

Backends implement four asynchronous methods: `submit`, `query`, `wait`, and `cancel`. The timeout in `wait(timeout=...)` is in seconds; response `latency_ms` is in milliseconds. Coordinator processes and checks successful backend outputs according to the configured output mode.

`WorkerPool.acquire()` returns `WorkerLease` or `None`. Scheduler releases leases after normal execution. Reclaiming expired leases changes allocation records only; it does not automatically cancel backend tasks.

## Runtime and tracing

| Module | Symbols | Purpose |
| --- | --- | --- |
| `wenbo.runtime.event` | `Event`, `EventType`, `EventBus` | Event structures, types, and asynchronous in-process notifications |
| `wenbo.runtime.state` | `DAGStateTracker`, `RuntimeStateMachine` | State transitions, retries, cancellation, and successor unlocking |
| `wenbo.runtime.prompt` | `PromptResolver` | Substitute output placeholders for direct predecessors |
| `wenbo.runtime.coordinator` | `ExecutionCoordinator`, `build_node_request_metadata` | Submit and await execution and build request metadata |
| `wenbo.runtime.timeline` | `TimelineCollector` | Collect node milestones, policy decisions, and standardized traces |
| `wenbo.runtime.trace` | `SchedulingTrace`, `TraceEvent`, `TraceEventType`, `TraceMode` | Summaries, full in-memory records, and JSONL output |

Source: [events](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/runtime/event.py), [state](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/runtime/state.py), [prompts](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/runtime/prompt.py), [coordinator](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/runtime/coordinator.py), [timeline](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/runtime/timeline.py), [trace](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/runtime/trace.py).

Scheduler calls Coordinator and StateTracker; typical callers do not need to invoke state transition methods manually. Asynchronous components should share the same EventBus and Registry.

Events use Unix timestamps, while runtime records use a monotonic clock; these timestamps cannot be subtracted directly. See [Events, Timelines, and Tracing](../user-guide/observability.md) for trace modes, fields, and a JSONL example.

## JSON workloads

| Module | Symbols | Purpose |
| --- | --- | --- |
| `wenbo.workload.synthetic_loader` | `SyntheticDAGLoader`, `SyntheticDAGSpec`, `parse_synthetic_specs` | Convert ordinary JSON workloads into runtime graphs and input specifications |

Source: [runtime JSON loader](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/workload/synthetic_loader.py).

`SyntheticDAGLoader.load_from_file()` returns a list of `(DynamicDAG, SyntheticDAGSpec)` pairs; `limit` controls how many entries are loaded from the start of the input. See the [DAG field reference](../dag-schema.md) for ordinary JSON fields.

The separate benchmark maintains shard indexes, frozen resource files, discrete-event simulation, and paper metrics. They are not distributed as system package APIs; see [Experiments](../experiments/real-datasets.md).
