# System Architecture

The Wenbo system repository provides an asynchronous workflow runtime. The official Quick Start invokes the paper experiment entry point in the separate [ARGB benchmark repository](https://github.com/wenbo-project/ARGB), which maintains the discrete-event simulator and datasets.

## Asynchronous workflow runtime

```text
Python DAG / JSON workload
           |
           v
Director -----> DAGRegistry
           |
           v
DAGStateTracker ---- NODE_READY ----> EventBus
                                          |
                                          v
                                     Scheduler
                                          |
                              ReadyQueue / CandidateBuilder
                                          |
                                   SchedulingPolicy
                                          |
                                      WorkerPool
                                          |
                                ExecutionCoordinator
                                          |
                                   ExecutorBackend
                                          |
                            result -> DAGStateTracker
                                          |
                              successors become ready
```

| Component | Responsibility | Main input / output |
| --- | --- | --- |
| `DynamicDAG` / `DAGBuilder` | Represent nodes, dependencies, costs, and states | Graphs constructed by callers |
| `Director` | Normalize hardware labels, recompute incoming data volumes, validate, and admit graphs | Constructed graph → registered graph |
| `DAGRegistry` | Index graphs by DAG ID, agent, and state | Retains the same mutable graph instance |
| `DAGStateTracker` | Manage state transitions, dependency resolution, retries, and terminal states | Execution notifications → lifecycle events |
| `EventBus` | Publish asynchronous notifications within the process | Events → subscribed handlers |
| `Scheduler` | Coalesce events, build candidates, schedule nodes, and coordinate resources | READY nodes → execution tasks |
| `SchedulingPolicy` | Rank and select candidate nodes | Candidate list → ordered decisions |
| `WorkerPool` | Match healthy idle workers and issue or release leases | Node → `WorkerLease` |
| `ExecutionCoordinator` | Resolve prompts, submit and await backend requests, and report results | Decision + lease → state notifications |
| `ExecutorBackend` | Implement request submission, status queries, waiting, and cancellation | `LLMRequest` → `LLMResponse` |
| `TimelineCollector` | Record node execution, policy decisions, and standardized traces | Runtime events → serializable records |

Director accepts preconstructed DAGs; it does not generate graphs from natural language. Node types are semantic labels. The backend determines actual tool calls, retrieval, or inference; setting `node_type` does not connect the corresponding service automatically.

## Lifecycle of a run

1. The caller constructs a DAG and creates a shared EventBus, Registry, and StateTracker.
2. Start the Scheduler, then submit the DAG. Director normalizes and validates the input in place and rejects duplicate DAG IDs.
3. StateTracker marks the graph as RUNNING and publishes READY events for root nodes.
4. After the event collection window, Scheduler filters candidates. The policy determines their order, and WorkerPool selects available execution endpoints.
5. Coordinator submits requests, processes responses, and notifies StateTracker with the attempt and lease ID.
6. Upstream completion unlocks successors. Retriable failures become eligible for scheduling again after a fixed backoff.
7. Once all nodes are completed or skipped, the graph becomes COMPLETED. Unrecoverable failures and explicit cancellation lead to their respective terminal states.

The runtime is intended primarily for a single `asyncio` event loop. Workers are local endpoints with one execution slot each and may share a backend that supports concurrent requests. The current implementation provides no remote worker registration, cross-process queue, or persistent recovery protocol.

## Quick Start and experiment scope

`examples/quickstart.py` forwards paths, datasets, seeds, sample limits, and the output directory to the separate benchmark's `paper_real_datasets.py --profile quickstart` through an isolated subprocess. This profile fixes GPU=2 and CPU=16 and assigns every node a duration of one virtual time unit. The baseline holds a private CPU/GPU pair for each DAG's lifetime, admitting at most `min(GPU, CPU)` DAGs concurrently. Wenbo admits the full selected batch and releases resources after each node completes. The paper does not specify a shared active window.


The system distribution does not include the paper simulator, frozen datasets, or experiment metrics modules. See [Experiments](experiments/real-datasets.md) for installation, the four real datasets, and reproduction scope.

## Reading the source

Start with the [project layout](reference/project-layout.md), then follow the [Scheduler](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/scheduler/scheduler.py), [state tracker](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/runtime/state.py), and [execution coordinator](https://github.com/wenbo-project/wenbo/blob/main/src/wenbo/runtime/coordinator.py). The [API index](reference/api.md) lists the main importable interfaces.
