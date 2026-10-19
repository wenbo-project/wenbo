# DAG Fields

Each agent task is represented as a directed acyclic graph (DAG): **nodes** are computation steps, and **edges** state which steps must finish before others can run. This page describes the JSON fields accepted by the system's `SyntheticDAGLoader`. The separate [ARGB benchmark](experiments/real-datasets.md) owns the frozen dataset format used in the paper experiments.

## Example

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

Here, `second` cannot start until `first` completes. The loader accepts either this object with a `dags` array or the array itself. Save the example as `workflow.json` and call `SyntheticDAGLoader.load_from_file("workflow.json")` to obtain runtime graph and source-specification pairs.

## DAG fields

| Field | Meaning |
| --- | --- |
| `dag_id` | Identifier for one task graph; unique among active DAGs in a registry. |
| `nodes` | Computation steps belonging to the DAG. At least one node is needed for execution. |
| `edges` | Directed dependencies between nodes; an empty list means the nodes are independent. |
| `metadata.priority` | Optional task priority used by priority-aware scheduling; default is `1` for this loader. |
| `dag_type`, `topology`, `graph_stats` | Optional classification and graph statistics for describing the workload; they do not define dependencies. |

## Node fields

| Field | Meaning |
| --- | --- |
| `node_id` | Identifier unique within its DAG. Edges refer to this value. |
| `node_type` | Semantic operation type, such as `llm_infer`, `tool_call`, or `retrieval`. The `GPU` and `CPU` compatibility labels select hardware but map to the generic runtime node type. |
| `hardware` | Optional explicit worker preference: `gpu`, `cpu`, or `any`. If omitted, `GPU` or `CPU` in `node_type` supplies the preference; otherwise it defaults to `any`. |
| `prompt` | Input text sent to the execution backend. |
| `tokens` / `estimated_tokens` | Estimated token work, used by policies that order nodes by size. |
| `cpu.time_ms` / `resource_requirement.estimated_time_ms` | Estimated execution duration in milliseconds. These values are estimates, not physical resource reservations. |
| `resource_requirement` | Optional requirements such as `cpu_cores`, `gpu_count`, `memory_mb`, and `gpu_memory_mb`; memory is measured in MB. |
| `metadata` | Optional application-specific information carried with the node. |

## Edge fields

| Field | Meaning |
| --- | --- |
| `source` | ID of the predecessor node. |
| `target` | ID of the successor node; it becomes eligible after its predecessors complete. |
| `edge_id` | Optional edge identifier; generated from the endpoints when omitted. |
| `dependency_type` | Optional dependency label, defaulting to `control`. |
| `data_volume_bytes` | Optional estimated data transferred along the edge, in bytes. |
| `data_mapping` | Optional metadata; the current prompt resolver does not interpret it. |

The loader checks node references and rejects cycles. Worker placement uses the hardware label and declared requirements; the backend performs the actual computation. See [System Usage](user-guide/workflows.md) for the Director, Scheduler, and Workers flow.
