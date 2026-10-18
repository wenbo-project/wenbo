# DAG JSON Field Reference

This page describes the runtime input format accepted by `wenbo.workload.synthetic_loader.SyntheticDAGLoader`. Loaders in the external benchmark repository handle shard indexes and separate resource files for frozen paper datasets; see [Real Dataset Experiments](experiments/real-datasets.md).

The tables distinguish loader requirements from usage recommendations. They are not a standalone JSON Schema validation configuration.

## Top-level structure

The loader accepts either an array of DAGs or an object containing a `dags` array:

```json
{"dags": []}
```

Other top-level structures raise `ValueError`. The loader can return an empty list, but Director rejects individual DAGs with no nodes. Inputs intended for execution should contain at least one nonempty graph.

## DAG objects

| Field | Required | Default / meaning |
| --- | --- | --- |
| `dag_id` | Yes | DAG identifier; must be unique within the target Registry |
| `dag_type` | No | `synthetic`; classification metadata, not restricted to a fixed set of strings |
| `topology` | No | `custom`; topology classification metadata |
| `nodes` | No | `[]`; node list; execution requires at least one node |
| `edges` | No | `[]`; directed dependency list |
| `estimated_cost` | No | `{}`; aggregate graph estimates; does not override node costs |
| `graph_stats` | No | Node, edge, root, and leaf counts are computed when missing or empty |
| `metadata` | No | `{}`; `priority` becomes the graph priority, defaulting to `1` |

Graph-level `estimated_cost` may record `total_tokens`, `total_cpu_time_ms`, and `total_data_volume_bytes`. These keys are optional, and the loader does not automatically check that they match the sums over nodes.

## Node objects

| Field | Required | Default / meaning |
| --- | --- | --- |
| `node_id` | Yes | Unique identifier within the graph |
| `node_type` | No | `custom`; runtime semantic type or compatible CPU/GPU label |
| `hardware` | No | Explicit value takes precedence; otherwise inferred from the CPU/GPU type, falling back to `any` |
| `prompt` | No | A prompt requesting a short placeholder response |
| `tokens` | No | Structured token estimate, such as `{"tokens": 512}` |
| `estimated_tokens` | No | Used when no higher-precedence token field is provided; defaults to `0` |
| `cpu` | No | Explicit duration in milliseconds, such as `{"time_ms": 20}` |
| `estimated_cost` | No | `{}`; supports estimates such as `tokens`, `latency_ms`, and `memory_mb` |
| `resource_requirement` | No | Resource requirements object; see below |
| `layer` | No | Visualization layer number stored in node metadata |
| `metadata` | No | `{}`; caller-supplied information |

Runtime semantic types include `llm_infer`, `tool_call`, `retrieval`, `aggregate`, `condition`, `backtrack`, and `custom`. Unrecognized types map to `NodeType.CUSTOM`. `CPU` / `GPU` are compatibility labels: they become `cpu` / `gpu` when hardware is not explicitly specified, while the runtime node type is CUSTOM.

Explicit `hardware` values are stripped of surrounding whitespace and converted to lowercase. `layer` is metadata only: the loader does not require edges to connect lower layers to higher layers. Cycle detection uses actual dependencies. Node type names do not imply integration with real tools or model services.

### Estimate precedence

| Runtime field | Sources, from highest to lowest precedence |
| --- | --- |
| `estimated_tokens` | `tokens.tokens` → `estimated_cost.tokens` → `estimated_tokens` → `0` |
| `resource_requirement.estimated_time_ms` | `cpu.time_ms` → `resource_requirement.estimated_time_ms` → `estimated_cost.latency_ms` → `0` |
| `resource_requirement.memory_mb` | `resource_requirement.memory_mb` → `estimated_cost.memory_mb` → `0` |
| `resource_requirement.estimated_gpu_memory_mb` | Field of the same name → `resource_requirement.gpu_memory_mb` → `0` |

If a higher-precedence field is present but zero, the loader does not search lower-precedence fields for a positive value. Use values convertible to numbers; all estimates should be nonnegative. Director checks for negative values at submission.

### Resource requirements

| Field | Default | Unit / meaning |
| --- | --- | --- |
| `cpu_cores` | `0` | CPU core count |
| `gpu_count` | `1` for GPU-labeled nodes; `0` otherwise | Required GPU count |
| `memory_mb` | `0` | Memory in MB |
| `gpu_memory_mb` | `0` | GPU memory in MB |
| `estimated_gpu_memory_mb` | See above | Estimated GPU memory in MB |
| `estimated_compute_flops` | `0` | Floating-point operation count |
| `estimated_time_ms` | See above | Estimated execution duration in milliseconds |

These fields describe requirements and estimates, not allocated physical resources. See [Execution](user-guide/workflows.md) for the constraints currently enforced by worker matching.

## Edge objects

| Field | Required | Default / meaning |
| --- | --- | --- |
| `source` | Yes | ID of an upstream node defined in the same graph |
| `target` | Yes | ID of a downstream node defined in the same graph |
| `edge_id` | No | Generated as `e_{source}_{target}` |
| `dependency_type` | No | `control`; dependency category metadata |
| `data_volume_bytes` | No | `0`; estimated transfer size in bytes |
| `data_mapping` | No | `{}`; currently not interpreted by PromptResolver |
| `metadata` | No | `{}`; additional edge metadata |

Endpoints must exist, and self-loops are rejected. Builder ignores repeated additions with the same edge ID and endpoints; reusing an ID with different endpoints fails. Edges with different IDs but identical endpoints are not uniformly rejected as duplicates by this loader. To avoid ambiguity, describe each endpoint pair once and use unique edge IDs.

The loader adds all nodes before connecting edges and checking for cycles, so JSON nodes need not appear in predecessor-first order. In contrast, Python `DAGBuilder.add_node(dependencies=...)` requires the specified predecessors to exist already.

## Minimal runnable input

```json
{
  "dags": [
    {
      "dag_id": "example",
      "nodes": [
        {"node_id": "first", "node_type": "GPU", "tokens": {"tokens": 512}},
        {"node_id": "second", "node_type": "CPU", "cpu": {"time_ms": 20}}
      ],
      "edges": [
        {"source": "first", "target": "second"}
      ]
    }
  ]
}
```

Save the example as `workflow.json`, load it with `SyntheticDAGLoader.load_from_file()`, and execute it through a Scheduler configured with an execution backend. See the [separate benchmark](experiments/real-datasets.md) for paper experiments using virtual time.

## Validation scope

Loading validates JSON structure, identifiers, endpoints, self-loops, cycles, and dependency reference consistency. Admission additionally checks for nonempty graphs, supported versions, nonnegative resource estimates, and retry parameters. The external benchmark entry point validates manifest hashes, shard counts, and total work for frozen inputs; SyntheticDAGLoader does not perform these checks.
