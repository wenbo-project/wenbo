"""Throughput metrics for benchmark runs."""

from __future__ import annotations

from typing import Any

from agentx.dag.model import DynamicDAG


def summarize_throughput(
    dags: list[DynamicDAG],
    timeline: list[dict[str, Any]],
    wall_clock_s: float,
    max_concurrent: int = 1,
    resource_limits: dict[str, int] | None = None,
) -> dict[str, Any]:
    completed_dags = sum(1 for dag in dags if dag.state.value == "completed")
    failed_dags = sum(1 for dag in dags if dag.state.value == "failed")
    completed_nodes = 0
    completed_gpu_nodes = 0
    completed_tokens = 0

    for dag in dags:
        for node in dag.nodes.values():
            if node.state.value != "completed":
                continue
            completed_nodes += 1
            completed_tokens += int(node.estimated_tokens or 0)
            if node.hardware == "gpu":
                completed_gpu_nodes += 1

    durations_by_node = _node_durations(timeline)
    durations = list(durations_by_node.values())
    hardware_by_node = _hardware_by_node(dags)
    hardware_busy_s: dict[str, float] = {}
    for node_key, duration in durations_by_node.items():
        hardware = hardware_by_node.get(node_key, "any")
        hardware_busy_s[hardware] = hardware_busy_s.get(hardware, 0.0) + duration

    sum_node_exec_s = sum(durations)
    safe_wall = max(wall_clock_s, 0.001)
    parallelism = sum_node_exec_s / safe_wall
    capacity_time = safe_wall * max(max_concurrent, 1)
    resource_limits = resource_limits or {}

    result = {
        "total_dags": len(dags),
        "completed_dags": completed_dags,
        "failed_dags": failed_dags,
        "completed_nodes": completed_nodes,
        "completed_gpu_nodes": completed_gpu_nodes,
        "completed_tokens": completed_tokens,
        "wall_clock_s": round(wall_clock_s, 3),
        "dags_per_second": round(completed_dags / safe_wall, 3),
        "nodes_per_second": round(completed_nodes / safe_wall, 3),
        "tokens_per_second": round(completed_tokens / safe_wall, 3),
        "gpu_nodes_per_second": round(completed_gpu_nodes / safe_wall, 3),
        "sum_node_exec_s": round(sum_node_exec_s, 3),
        "avg_node_exec_s": round(sum_node_exec_s / max(len(durations), 1), 3),
        "parallelism": round(parallelism, 3),
        "resource_utilization_pct": round(sum_node_exec_s / capacity_time * 100.0, 1),
        "gpu_busy_s": round(hardware_busy_s.get("gpu", 0.0), 3),
        "cpu_busy_s": round(hardware_busy_s.get("cpu", 0.0), 3),
    }

    for hardware, limit in resource_limits.items():
        hardware_capacity = safe_wall * max(limit, 1)
        result[f"{hardware}_utilization_pct"] = round(
            hardware_busy_s.get(hardware, 0.0) / hardware_capacity * 100.0,
            1,
        )

    return result


def _node_durations(timeline: list[dict[str, Any]]) -> dict[str, float]:
    started: dict[str, float] = {}
    durations: dict[str, float] = {}
    for entry in timeline:
        key = f"{entry['dag_id']}:{entry['node_id']}"
        if entry["event"] == "started":
            started[key] = entry["time"]
        elif entry["event"] == "finished" and key in started:
            durations[key] = entry["time"] - started[key]
    return durations


def _hardware_by_node(dags: list[DynamicDAG]) -> dict[str, str]:
    hardware = {}
    for dag in dags:
        for node_id, node in dag.nodes.items():
            hardware[f"{dag.dag_id}:{node_id}"] = node.hardware
    return hardware
