# Wenbo DAG IR Field Reference

This document describes the DAG JSON shape accepted by Wenbo workload loaders. A complete,
runnable input is available at `examples/dags/quickstart.json`.

## 1. Top-Level Object

```json
{
  "dags": []
}
```

| Field | Required | Type | Description |
| --- | --- | --- | --- |
| `dags` | yes | array | List of generated DAG objects. |

## 2. dags[]

Each element in `dags[]` represents one directed acyclic graph.

```json
{
  "dag_id": "dag_000001",
  "dag_type": "reasoning",
  "estimated_cost": {
    "total_tokens": 1408,
    "total_cpu_time_ms": 20,
    "total_data_volume_bytes": 8192
  },
  "nodes": [],
  "edges": []
}
```

| Field | Required | Type | Description | Allowed Values |
| --- | --- | --- | --- | --- |
| `dag_id` | yes | string | Unique DAG identifier within the dataset. | - |
| `dag_type` | yes | string | DAG task category. | `reasoning`, `qa`, `tool`, `retrieval` |
| `estimated_cost` | yes | object | DAG-level aggregate workload estimate. | - |
| `nodes` | yes | array | Executable nodes. Edges reference nodes by `node_id`. | - |
| `edges` | yes | array | Directed dependency edges. | - |

## 3. estimated_cost

`estimated_cost` records aggregate workload estimates for the full DAG.

```json
{
  "total_tokens": 1408,
  "total_cpu_time_ms": 20,
  "total_data_volume_bytes": 8192
}
```

| Field | Required | Type | Description |
| --- | --- | --- | --- |
| `total_tokens` | yes | number | Sum of token estimates across GPU nodes. CPU nodes without `tokens` contribute 0. |
| `total_cpu_time_ms` | yes | number | Sum of `cpu.time_ms` estimates across CPU nodes. GPU nodes without `cpu` contribute 0. |
| `total_data_volume_bytes` | yes | number | Sum of `data_volume_bytes` across all edges. |

## 4. nodes[]

Each node represents one independently schedulable execution step.

GPU node:

```json
{
  "node_id": "N_0_0",
  "node_type": "GPU",
  "layer": 0,
  "tokens": {
    "tokens": 512
  }
}
```

CPU node:

```json
{
  "node_id": "N_1_0",
  "node_type": "CPU",
  "layer": 1,
  "cpu": {
    "time_ms": 20
  }
}
```

| Field | Required | Type | Description | Allowed Values |
| --- | --- | --- | --- | --- |
| `node_id` | yes | string | Unique node identifier within the parent DAG. | - |
| `node_type` | yes | string | Execution resource class used by the generator, visualizer, and scheduler-facing examples. | `GPU`, `CPU` |
| `layer` | yes | integer | Topological layer used by the generator and visualizer. Edges must point from lower layers to higher layers. | `>= 0` |
| `tokens` | no | object | Token estimate. Emitted for GPU nodes by the current generator. | - |
| `cpu` | no | object | CPU runtime estimate. Emitted for CPU nodes by the current generator. | - |

The current template does not use a separate `hardware` field. Resource class is represented directly by `node_type`.

## 5. tokens

```json
{
  "tokens": 512
}
```

| Field | Required | Type | Description |
| --- | --- | --- | --- |
| `tokens` | yes, when the object exists | number | Estimated token count for one GPU node. |

## 6. cpu

```json
{
  "time_ms": 20
}
```

| Field | Required | Type | Description |
| --- | --- | --- | --- |
| `time_ms` | yes, when the object exists | number | Estimated runtime for one CPU node, measured in milliseconds. |

## 7. edges[]

Each edge represents one directed dependency from `source` to `target`.

```json
{
  "edge_id": "E0",
  "source": "N_0_0",
  "target": "N_1_0",
  "data_volume_bytes": 2048
}
```

| Field | Required | Type | Description |
| --- | --- | --- | --- |
| `edge_id` | yes | string | Unique edge identifier within the parent DAG. |
| `source` | yes | string | Upstream node id. It must exist in the same DAG. |
| `target` | yes | string | Downstream node id. It must exist in the same DAG. |
| `data_volume_bytes` | yes | number | Estimated data transferred along this edge, measured in bytes. |

Edges must not be self-loops, duplicate the same `source -> target` pair, or point from the same/higher layer to a lower layer.

## 8. Minimal Valid Dataset

```json
{
  "dags": [
    {
      "dag_id": "dag_000001",
      "dag_type": "reasoning",
      "estimated_cost": {
        "total_tokens": 512,
        "total_cpu_time_ms": 0,
        "total_data_volume_bytes": 0
      },
      "nodes": [
        {
          "node_id": "N_0_0",
          "node_type": "GPU",
          "layer": 0,
          "tokens": {
            "tokens": 512
          }
        }
      ],
      "edges": []
    }
  ]
}
