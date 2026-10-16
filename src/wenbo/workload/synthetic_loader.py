"""Loader for synthetic DAG IR datasets.

This loader accepts the JSON shape produced by ``design/run.py`` and converts it
to Wenbo runtime ``DynamicDAG`` objects without using prompt text for cost
estimation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from wenbo.dag.builder import DAGBuilder
from wenbo.dag.model import DynamicDAG, ModelConfig, NodeType, ResourceReq


@dataclass
class SyntheticDAGSpec:
    dag_id: str
    dag_type: str = "synthetic"
    topology: str = "custom"
    nodes: list[dict[str, Any]] = field(default_factory=list)
    edges: list[dict[str, Any]] = field(default_factory=list)
    estimated_cost: dict[str, Any] = field(default_factory=dict)
    graph_stats: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


def _load_json(file_path: str | Path) -> list[dict[str, Any]]:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("dags"), list):
        return data["dags"]
    raise ValueError(f"Unexpected synthetic DAG JSON structure in {file_path}")


def parse_synthetic_specs(raw_list: list[dict[str, Any]]) -> list[SyntheticDAGSpec]:
    specs = []
    for item in raw_list:
        specs.append(
            SyntheticDAGSpec(
                dag_id=item["dag_id"],
                dag_type=item.get("dag_type", "synthetic"),
                topology=item.get("topology", "custom"),
                nodes=item.get("nodes", []),
                edges=item.get("edges", []),
                estimated_cost=item.get("estimated_cost", {}),
                graph_stats=item.get("graph_stats") or _compute_graph_stats(item),
                metadata=item.get("metadata", {}),
            )
        )
    return specs


class SyntheticDAGLoader:
    """Convert synthetic DAG JSON into executable Wenbo DAG objects."""

    def __init__(
        self,
        default_model: str = "synthetic-model",
        default_sampling_params: Optional[dict] = None,
    ) -> None:
        self._default_model = default_model
        self._default_sampling_params = default_sampling_params or {
            "temperature": 0.0,
            "top_p": 1.0,
            "max_tokens": 16,
        }

    def load_from_file(
        self,
        file_path: str | Path,
        limit: Optional[int] = None,
    ) -> list[tuple[DynamicDAG, SyntheticDAGSpec]]:
        raw_list = _load_json(file_path)
        specs = parse_synthetic_specs(raw_list)
        if limit:
            specs = specs[:limit]

        # 将 JSON 描述转换为运行时 DAG，保留原始规格供结果汇总使用。
        builder = DAGBuilder()
        return [(self._build_dag(builder, spec), spec) for spec in specs]

    def _build_dag(self, builder: DAGBuilder, spec: SyntheticDAGSpec) -> DynamicDAG:
        dag = builder.create_dag(
            dag_id=spec.dag_id,
            agent_id="synthetic_agent",
            priority=int(spec.metadata.get("priority", 1)),
            metadata={
                **spec.metadata,
                "dag_type": spec.dag_type,
                "topology": spec.topology,
                "estimated_cost": spec.estimated_cost,
                "graph_stats": spec.graph_stats,
            },
        )

        model_config = ModelConfig(
            model=self._default_model,
            sampling_params=self._default_sampling_params.copy(),
        )

        for node_def in spec.nodes:
            node_id = node_def["node_id"]
            hardware = _parse_hardware(node_def)
            builder.add_node(
                dag,
                node_id=node_id,
                node_type=_parse_node_type(node_def.get("node_type", "custom")),
                prompt=node_def.get(
                    "prompt",
                    f"Synthetic node {node_id}. Return a short placeholder result.",
                ),
                hardware=hardware,
                model_config=model_config,
                estimated_tokens=_extract_estimated_tokens(node_def),
                estimated_cost=node_def.get("estimated_cost", {}),
                resource_requirement=_build_resource_requirement(node_def, hardware),
                metadata=_build_node_metadata(node_def),
            )

        for edge_def in spec.edges:
            builder.add_dependency(
                dag,
                edge_def["source"],
                edge_def["target"],
                edge_id=edge_def.get("edge_id"),
                dependency_type=edge_def.get("dependency_type", "control"),
                data_volume_bytes=int(edge_def.get("data_volume_bytes", 0) or 0),
                data_mapping=edge_def.get("data_mapping", {}),
                metadata=edge_def.get("metadata", {}),
            )

        builder.validate(dag)
        return dag


def _parse_node_type(value: str) -> NodeType:
    try:
        return NodeType(value)
    except ValueError:
        return NodeType.CUSTOM


def _parse_hardware(node_def: dict[str, Any]) -> str:
    explicit = str(node_def.get("hardware", "")).strip().lower()
    if explicit:
        return explicit

    node_type = str(node_def.get("node_type", "")).strip().lower()
    if node_type == "gpu":
        return "gpu"
    if node_type == "cpu":
        return "cpu"
    return "any"


def _extract_estimated_tokens(node_def: dict[str, Any]) -> int:
    tokens = node_def.get("tokens")
    if isinstance(tokens, dict) and "tokens" in tokens:
        return int(tokens["tokens"] or 0)

    estimated_cost = node_def.get("estimated_cost")
    if isinstance(estimated_cost, dict) and "tokens" in estimated_cost:
        return int(estimated_cost["tokens"] or 0)

    return int(node_def.get("estimated_tokens", 0) or 0)


def _build_resource_requirement(node_def: dict[str, Any], hardware: str) -> ResourceReq:
    raw = node_def.get("resource_requirement") or {}
    estimated_cost = node_def.get("estimated_cost") or {}
    estimated_time_ms = raw.get("estimated_time_ms", estimated_cost.get("latency_ms", 0.0))
    cpu = node_def.get("cpu")
    if isinstance(cpu, dict) and "time_ms" in cpu:
        estimated_time_ms = cpu["time_ms"]

    return ResourceReq(
        cpu_cores=float(raw.get("cpu_cores", 0.0) or 0.0),
        gpu_count=int(raw.get("gpu_count", 1 if hardware == "gpu" else 0) or 0),
        memory_mb=float(raw.get("memory_mb", estimated_cost.get("memory_mb", 0.0)) or 0.0),
        gpu_memory_mb=float(raw.get("gpu_memory_mb", 0.0) or 0.0),
        estimated_gpu_memory_mb=float(raw.get("estimated_gpu_memory_mb", raw.get("gpu_memory_mb", 0.0)) or 0.0),
        estimated_compute_flops=float(raw.get("estimated_compute_flops", 0.0) or 0.0),
        estimated_time_ms=float(estimated_time_ms or 0.0),
    )


def _build_node_metadata(node_def: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(node_def.get("metadata", {}))
    if "layer" in node_def:
        metadata["layer"] = node_def["layer"]
    if "tokens" in node_def:
        metadata["tokens"] = node_def["tokens"]
    if "cpu" in node_def:
        metadata["cpu"] = node_def["cpu"]
    return metadata


def _compute_graph_stats(dag_def: dict[str, Any]) -> dict[str, int]:
    nodes = dag_def.get("nodes", [])
    edges = dag_def.get("edges", [])
    node_ids = {node["node_id"] for node in nodes}
    in_degree = {node_id: 0 for node_id in node_ids}
    out_degree = {node_id: 0 for node_id in node_ids}

    for edge in edges:
        source = edge.get("source")
        target = edge.get("target")
        if source in out_degree:
            out_degree[source] += 1
        if target in in_degree:
            in_degree[target] += 1

    return {
        "num_nodes": len(nodes),
        "num_edges": len(edges),
        "num_roots": sum(1 for degree in in_degree.values() if degree == 0),
        "num_sinks": sum(1 for degree in out_degree.values() if degree == 0),
    }
