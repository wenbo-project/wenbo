"""Shared throughput experiment helpers."""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any

from agentx.benchmark.runners import (
    AgentXGlobalRunner,
    DAGSequentialRunner,
    NaiveSequentialRunner,
    PerDAGLocalRunner,
    TaskParallelResourcePairRunner,
)
from agentx.executor.synthetic_backend import SyntheticBackend, SyntheticBackendConfig
from agentx.scheduler.policy import (
    FIFOPolicy,
    build_policy as build_registered_policy,
)
from agentx.workload.synthetic_loader import SyntheticDAGLoader

logger = logging.getLogger("throughput-experiment")

DEFAULT_DAG_FILE = "dataset/DAG_Example.json"

RUNNER_KEYS = [
    "naive_sequential",
    "dag_sequential",
    "per_dag_local",
    "agentx_global",
]

CSV_FIELDS = [
    "dag_count",
    "runner",
    "policy",
    "wall_clock_s",
    "completed_dags",
    "completed_nodes",
    "completed_tokens",
    "dags_per_second",
    "nodes_per_second",
    "tokens_per_second",
    "gpu_nodes_per_second",
    "parallelism",
    "resource_utilization_pct",
    "gpu_utilization_pct",
    "cpu_utilization_pct",
    "speedup_nodes_vs_naive",
    "speedup_tokens_vs_naive",
    "speedup_nodes_vs_dag_sequential",
    "speedup_tokens_vs_dag_sequential",
    "speedup_nodes_vs_per_dag_local",
    "speedup_tokens_vs_per_dag_local",
]


def build_policy(name: str):
    return build_registered_policy(name)


def parse_dag_counts(value: str) -> list[int]:
    counts = [int(item.strip()) for item in value.split(",") if item.strip()]
    if not counts or any(count <= 0 for count in counts):
        raise ValueError("--dag-counts must contain positive integers")
    return counts


def load_dags(file_path: str, count: int):
    loader = SyntheticDAGLoader()
    return [dag for dag, _ in loader.load_from_file(file_path, limit=count)]


def build_backend(args) -> SyntheticBackend:
    return SyntheticBackend(
        SyntheticBackendConfig(
            gpu_tokens_per_second=args.gpu_token_rate,
            time_scale=args.time_scale,
            min_latency_ms=getattr(args, "min_latency_ms", 0.1),
        )
    )


def build_resource_limits(args) -> dict[str, int]:
    limits = {}
    if args.gpu_slots > 0:
        limits["gpu"] = args.gpu_slots
    if args.cpu_slots > 0:
        limits["cpu"] = args.cpu_slots
    return limits


async def run_for_count(args, dag_count: int) -> dict[str, Any]:
    logger.info("Running throughput benchmark | dag_count=%d", dag_count)
    resource_limits = build_resource_limits(args)

    naive_dags = load_dags(args.dag_file, dag_count)
    naive = NaiveSequentialRunner(
        executor=build_backend(args),
        node_timeout=args.node_timeout,
    )
    naive_result = await naive.run(naive_dags)
    log_result("naive", naive_result)

    dag_sequential_dags = load_dags(args.dag_file, dag_count)
    dag_sequential = DAGSequentialRunner(
        executor_factory=lambda: build_backend(args),
        policy=build_policy(args.policy),
        max_concurrent=args.max_concurrent,
        debounce_ms=args.debounce_ms,
        heartbeat_ms=args.heartbeat_ms,
        node_timeout=args.node_timeout,
        resource_limits=resource_limits,
    )
    dag_sequential_result = await dag_sequential.run(
        dag_sequential_dags,
        timeout_s=args.timeout_s,
    )
    log_result("dag-seq", dag_sequential_result)

    per_dag_dags = load_dags(args.dag_file, dag_count)
    per_dag = PerDAGLocalRunner(
        executor=build_backend(args),
        max_concurrent=args.max_concurrent,
        node_timeout=args.node_timeout,
        resource_limits=resource_limits,
    )
    per_dag_result = await per_dag.run(per_dag_dags, timeout_s=args.timeout_s)
    log_result("per-dag", per_dag_result)

    agentx_dags = load_dags(args.dag_file, dag_count)
    agentx = AgentXGlobalRunner(
        executor=build_backend(args),
        policy=build_policy(args.policy),
        max_concurrent=args.max_concurrent,
        debounce_ms=args.debounce_ms,
        heartbeat_ms=args.heartbeat_ms,
        node_timeout=args.node_timeout,
        resource_limits=resource_limits,
    )
    agentx_result = await agentx.run(agentx_dags, timeout_s=args.timeout_s)
    log_result("agentx", agentx_result)

    return {
        "dag_count": dag_count,
        "policy": getattr(args, "policy", "fifo"),
        "naive_sequential": result_to_dict(naive_result),
        "dag_sequential": result_to_dict(dag_sequential_result),
        "per_dag_local": result_to_dict(per_dag_result),
        "agentx_global": result_to_dict(agentx_result),
        "speedup": build_speedups(agentx_result, [
            naive_result,
            dag_sequential_result,
            per_dag_result,
        ]),
    }


def build_metadata(args, dag_counts: list[int]) -> dict[str, Any]:
    return {
        "dag_file": args.dag_file,
        "dag_counts": dag_counts,
        "policy": getattr(args, "policy", "fifo"),
        "max_concurrent": args.max_concurrent,
        "resource_limits": build_resource_limits(args),
        "gpu_token_rate": args.gpu_token_rate,
        "time_scale": args.time_scale,
        "min_latency_ms": getattr(args, "min_latency_ms", 0.1),
        "max_active_dags": getattr(args, "max_active_dags", None),
    }


def rows_from_result(result: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    speedup = result.get("speedup", {})
    for runner_key in RUNNER_KEYS:
        runner_result = result[runner_key]
        metrics = runner_result["metrics"]
        row = {
            "dag_count": result["dag_count"],
            "runner": runner_key,
            "policy": result["policy"],
        }
        for field in CSV_FIELDS:
            if field in {"dag_count", "runner", "policy"}:
                continue
            row[field] = metrics.get(field, "")

        if runner_key == "agentx_global":
            row.update(flatten_agentx_speedups(speedup))
        rows.append(row)
    return rows


def flatten_agentx_speedups(speedup: dict[str, Any]) -> dict[str, Any]:
    return {
        "speedup_nodes_vs_naive": nested(speedup, "vs_naive_sequential", "nodes_per_second"),
        "speedup_tokens_vs_naive": nested(speedup, "vs_naive_sequential", "tokens_per_second"),
        "speedup_nodes_vs_dag_sequential": nested(speedup, "vs_dag_sequential", "nodes_per_second"),
        "speedup_tokens_vs_dag_sequential": nested(speedup, "vs_dag_sequential", "tokens_per_second"),
        "speedup_nodes_vs_per_dag_local": nested(speedup, "vs_per_dag_local", "nodes_per_second"),
        "speedup_tokens_vs_per_dag_local": nested(speedup, "vs_per_dag_local", "tokens_per_second"),
    }


def build_speedups(agentx_result, baselines: list) -> dict[str, Any]:
    speedups = {}
    for baseline in baselines:
        speedups[f"vs_{baseline.name}"] = {
            "nodes_per_second": round(
                ratio(
                    agentx_result.metrics["nodes_per_second"],
                    baseline.metrics["nodes_per_second"],
                ),
                3,
            ),
            "tokens_per_second": round(
                ratio(
                    agentx_result.metrics["tokens_per_second"],
                    baseline.metrics["tokens_per_second"],
                ),
                3,
            ),
        }
    return speedups


def ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def nested(data: dict[str, Any], first: str, second: str) -> Any:
    value = data.get(first, {})
    if isinstance(value, dict):
        return value.get(second, "")
    return ""


async def run_fcfs_comparison_for_count(args, dag_count: int) -> dict[str, Any]:
    logger.info("Running FCFS comparison | dag_count=%d", dag_count)
    resource_limits = build_resource_limits(args)

    naive_dags = load_dags(args.dag_file, dag_count)
    naive = NaiveSequentialRunner(
        executor=build_backend(args),
        node_timeout=args.node_timeout,
    )
    naive_result = await naive.run(naive_dags)
    log_result("sequential", naive_result)

    task_parallel_dags = load_dags(args.dag_file, dag_count)
    task_parallel = TaskParallelResourcePairRunner(
        executor=build_backend(args),
        max_active_tasks=args.max_concurrent,
        node_timeout=args.node_timeout,
        resource_limits=resource_limits,
    )
    task_parallel_result = await task_parallel.run(
        task_parallel_dags,
        timeout_s=args.timeout_s,
    )
    log_result("parallel", task_parallel_result)

    agentx_dags = load_dags(args.dag_file, dag_count)
    agentx_fcfs = AgentXGlobalRunner(
        executor=build_backend(args),
        policy=FIFOPolicy(),
        max_concurrent=args.max_concurrent,
        debounce_ms=args.debounce_ms,
        heartbeat_ms=args.heartbeat_ms,
        node_timeout=args.node_timeout,
        resource_limits=resource_limits,
        max_active_dags=getattr(args, "max_active_dags", None),
    )
    agentx_result = await agentx_fcfs.run(agentx_dags, timeout_s=args.timeout_s)
    log_result("agentx", agentx_result)

    speedup = build_speedups(agentx_result, [naive_result, task_parallel_result])
    return {
        "dag_count": dag_count,
        "policy": "fifo",
        "sequential": result_to_dict_as(naive_result, "sequential"),
        "parallel": result_to_dict_as(task_parallel_result, "parallel"),
        "agentx": result_to_dict_as(agentx_result, "agentx"),
        "speedup": speedup,
    }


FCFS_CSV_FIELDS = [
    "dag_count",
    "runner",
    "policy",
    "wall_clock_s",
    "completed_dags",
    "completed_nodes",
    "completed_tokens",
    "dags_per_second",
    "nodes_per_second",
    "tokens_per_second",
    "gpu_nodes_per_second",
    "parallelism",
    "resource_utilization_pct",
    "gpu_utilization_pct",
    "cpu_utilization_pct",
    "speedup_nodes_vs_sequential",
    "speedup_tokens_vs_sequential",
    "speedup_nodes_vs_parallel",
    "speedup_tokens_vs_parallel",
]


def fcfs_rows_from_result(result: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    speedup = result.get("speedup", {})
    for runner_key in ("sequential", "parallel", "agentx"):
        runner_result = result[runner_key]
        metrics = runner_result["metrics"]
        row = {
            "dag_count": result["dag_count"],
            "runner": runner_key,
            "policy": result["policy"],
        }
        for field in FCFS_CSV_FIELDS:
            if field in {"dag_count", "runner", "policy"}:
                continue
            row[field] = metrics.get(field, "")
        if runner_key == "agentx":
            row["speedup_nodes_vs_sequential"] = nested(
                speedup,
                "vs_sequential",
                "nodes_per_second",
            )
            row["speedup_tokens_vs_sequential"] = nested(
                speedup,
                "vs_sequential",
                "tokens_per_second",
            )
            row["speedup_nodes_vs_parallel"] = nested(
                speedup,
                "vs_parallel",
                "nodes_per_second",
            )
            row["speedup_tokens_vs_parallel"] = nested(
                speedup,
                "vs_parallel",
                "tokens_per_second",
            )
        rows.append(row)
    return rows


def write_fcfs_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FCFS_CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def log_result(label: str, result) -> None:
    logger.info(
        "  %-8s wall=%.3fs nodes/s=%.3f tokens/s=%.3f util=%.1f%% gpu_util=%s cpu_util=%s",
        label,
        result.metrics["wall_clock_s"],
        result.metrics["nodes_per_second"],
        result.metrics["tokens_per_second"],
        result.metrics["resource_utilization_pct"],
        fmt_optional_pct(result.metrics.get("gpu_utilization_pct")),
        fmt_optional_pct(result.metrics.get("cpu_utilization_pct")),
    )


def fmt_optional_pct(value) -> str:
    return "n/a" if value is None else f"{value:.1f}%"


def result_to_dict(result) -> dict[str, Any]:
    return {
        "name": result.name,
        "metrics": result.metrics,
        "decision_count": len(result.decisions),
        "timeline_count": len(result.timeline),
        "diagnostics": result.diagnostics,
    }


def result_to_dict_as(result, name: str) -> dict[str, Any]:
    data = result_to_dict(result)
    data["name"] = name
    return data


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
