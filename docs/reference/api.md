# API Reference

The main interfaces below support graph construction, scheduling, and backend integration. See [System Usage](../user-guide/workflows.md) for examples; source docstrings describe detailed parameters and return values.

| Component | Module | Main interfaces |
| --- | --- | --- |
| Graph representation | `wenbo.dag.model` | `DynamicDAG`, `Node`, `Edge`, `NodeType`, `NodeState`, `DAGState` |
| Graph construction | `wenbo.dag.builder` | `DAGBuilder.create_dag()`, `add_node()`, `add_dependency()`, `validate()` |
| Admission | `wenbo.director.director` | `Director.compile()`, `Director.admit()` |
| Execution lifecycle | `wenbo.scheduler.scheduler` | `Scheduler.start()`, `submit_dag()`, `cancel_dag()`, `stop()` (asynchronous) |
| Scheduling policies | `wenbo.scheduler.policy` | `build_policy()`, `SchedulingPolicy.select()` |
| Backend integration | `wenbo.executor.base` | `ExecutorBackend`, `LLMRequest`, `LLMResponse`, `RequestStatus` |
| Worker configuration | `wenbo.executor.worker` | `Worker`, `WorkerCapability`, `WorkerPool` |
| Runtime delay simulation | `wenbo.executor.synthetic_backend` | `SyntheticBackend`, `SyntheticBackendConfig` |
| JSON input | `wenbo.workload.synthetic_loader` | `SyntheticDAGLoader.load_from_file()` |

Browse the [source package](https://github.com/wenbo-project/wenbo/tree/main/src/wenbo) for implementation details. The runtime JSON format is documented in the [DAG field reference](../dag-schema.md).

The separate [ARGB repository](https://github.com/wenbo-project/ARGB) owns the paper simulator, datasets, and evaluation APIs.
