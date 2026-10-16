"""Reproducible discrete-event experiments for the Wenbo paper.

This module deliberately separates modeled virtual time from the host wall
clock. It compares scheduling semantics, not real GPU kernels or RPC systems.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import platform
import random
import statistics
import subprocess
import sys
import time
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from wenbo.benchmark.discrete_simulator import (
    DiscreteResult,
    DurationConfig,
    POLICY_VERSIONS,
    SimDAG,
    load_sim_dags,
    run_wenbo,
    run_parallel,
    validate_sim_dags as validate_discrete_dags,
)


SCHEMA_VERSION = "wenbo.paper-real-dag.v2"
DEFAULT_SEEDS = (11, 29, 47, 71, 101)
RUNNER_ORDER = ("parallel", "wenbo")
RUNNER_LABELS = {
    "parallel": "Flash-Searcher (parallel)",
    "wenbo": "Wenbo",
}


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    dag_file: str
    resource_estimate_file: str
    role: str


@dataclass(frozen=True)
class ResourceConfig:
    scenario: str
    gpu_count: int
    cpu_count: int

    @property
    def x_value(self) -> int:
        if self.scenario == "gpu_scarce":
            return self.cpu_count
        return self.gpu_count


DATASET_SPECS = {
    "strategyqa": DatasetSpec(
        name="StrategyQA",
        dag_file=(
            "data/converted_dags/strategyqa/"
            "strategyqa_dags_llm_full_rerun.json"
        ),
        resource_estimate_file=(
            "data/converted_dags/strategyqa/"
            "strategyqa_dags_llm_full_rerun_"
            "resource_estimates_local_sim_v2.json"
        ),
        role="parallel_real_qa",
    ),
    "musique": DatasetSpec(
        name="MuSiQue",
        dag_file=(
            "data/converted_dags/musique/"
            "musique_dags_llm_full_optimized_prompt_rerun.json"
        ),
        resource_estimate_file=(
            "data/converted_dags/musique/"
            "musique_dags_llm_full_optimized_prompt_rerun_"
            "resource_estimates_local_sim_v2.json"
        ),
        role="large_multihop_qa",
    ),
    "toolbench": DatasetSpec(
        name="ToolBench",
        dag_file=(
            "data/converted_dags/toolbench/toolbench_dags_3000_deepseek.json"
        ),
        resource_estimate_file=(
            "data/converted_dags/toolbench/"
            "toolbench_dags_3000_deepseek_"
            "resource_estimates_local_sim_v2.json"
        ),
        role="agent_tool_workflow",
    ),
    "toolbench_full": DatasetSpec(
        name="ToolBench-Full-LLM",
        dag_file=(
            "data/converted_dags/toolbench2/"
            "toolbench_dags_all_26999_deepseek_merged_20260913.json"
        ),
        resource_estimate_file=(
            "data/converted_dags/toolbench2/"
            "toolbench_dags_all_26999_deepseek_merged_20260913_"
            "resource_estimates_local_sim_v2.json"
        ),
        role="agent_tool_workflow_full_llm",
    ),
    "tau2": DatasetSpec(
        name="Tau2",
        dag_file="data/converted_dags/tau2/tau2_dags_all.json",
        resource_estimate_file=(
            "data/converted_dags/tau2/"
            "tau2_dags_all_resource_estimates_local_sim_v2.json"
        ),
        role="serial_negative_control",
    ),
    "problems_merged_all": DatasetSpec(
        name="ProblemsMerged-All",
        dag_file=(
            "data/converted_dags/problems_merged/"
            "problems_merged_dags_all_gpu.json"
        ),
        resource_estimate_file=(
            "data/converted_dags/problems_merged/"
            "problems_merged_dags_all_gpu_resource_estimates_local_sim_v2.json"
        ),
        role="gpu_dense_large_dag",
    ),
    "alfworld": DatasetSpec(
        name="ALFWorld",
        dag_file="data/converted_dags/alfworld/alfworld_dags_100_deepseek.json",
        resource_estimate_file=(
            "data/converted_dags/alfworld/"
            "alfworld_dags_100_deepseek_resource_estimates_local_sim_v2.json"
        ),
        role="interactive_heterogeneous_agent",
    ),
    "alfworld_full": DatasetSpec(
        name="ALFWorld-Full-LLM",
        dag_file="data/converted_dags/alfworld/alfworld_dags_all_deepseek.json",
        resource_estimate_file=(
            "data/converted_dags/alfworld/"
            "alfworld_dags_all_deepseek_resource_estimates_local_sim_v2.json"
        ),
        role="interactive_heterogeneous_agent_full_llm",
    ),
    "alfworld_all_rule": DatasetSpec(
        name="ALFWorld-All-Rule",
        dag_file="data/converted_dags/alfworld/alfworld_dags_all_rule.json",
        resource_estimate_file=(
            "data/converted_dags/alfworld/"
            "alfworld_dags_all_rule_resource_estimates_local_sim_v2.json"
        ),
        role="interactive_heterogeneous_agent_full_rule",
    ),
    "synthetic_cpu_fanout": DatasetSpec(
        name="CPU-Fanout-GPU-Join",
        dag_file=(
            "data/converted_dags/stress/"
            "cpu_fanout_gpu_join_dags_1000_f32_cpu3000_gpu960k.json"
        ),
        resource_estimate_file=(
            "data/converted_dags/stress/"
            "cpu_fanout_gpu_join_dags_1000_f32_cpu3000_gpu960k_"
            "resource_estimates.json"
        ),
        role="synthetic_cpu_fanout",
    ),
    "synthetic_gpu_fanout": DatasetSpec(
        name="GPU-Fanout-CPU-Join",
        dag_file=(
            "data/converted_dags/stress/"
            "gpu_fanout_cpu_join_dags_1000_f32.json"
        ),
        resource_estimate_file=(
            "data/converted_dags/stress/"
            "gpu_fanout_cpu_join_dags_1000_f32_resource_estimates.json"
        ),
        role="synthetic_gpu_fanout",
    ),
}


def resource_configs(profile: str) -> list[ResourceConfig]:
    """Return unique resource pairs for a quick, core, or full paper sweep."""
    counts_by_profile = {
        "quick": (1, 4, 16),
        "core": (1, 4, 16, 64),
        "paper": (1, 2, 4, 8, 16, 32, 64, 128),
    }
    try:
        counts = counts_by_profile[profile]
    except KeyError as exc:
        raise ValueError(f"unknown experiment profile: {profile}") from exc

    configs = [ResourceConfig("balanced", count, count) for count in counts]
    configs.extend(ResourceConfig("gpu_scarce", 1, count) for count in counts[1:])
    configs.extend(ResourceConfig("cpu_scarce", count, 1) for count in counts[1:])
    return configs


def fixed_resource_configs(
    counts: Sequence[int],
    *,
    fixed_gpu_count: int = 1,
    fixed_cpu_count: int = 1,
) -> list[ResourceConfig]:
    """Create two sweeps while holding each resource type at a fixed count."""
    normalized = list(dict.fromkeys(int(count) for count in counts))
    if not normalized or any(count <= 0 for count in normalized):
        raise ValueError("resource counts must contain positive integers")
    if fixed_gpu_count <= 0 or fixed_cpu_count <= 0:
        raise ValueError("fixed resource counts must be positive integers")
    return [
        *(
            ResourceConfig("gpu_scarce", fixed_gpu_count, count)
            for count in normalized
        ),
        *(
            ResourceConfig("cpu_scarce", count, fixed_cpu_count)
            for count in normalized
        ),
    ]


def validate_sim_dags(dags: Sequence[SimDAG]) -> None:
    """Reject malformed inputs before a simulator can return partial results."""
    validate_discrete_dags(dags)


def summarize_dataset(
    spec: DatasetSpec,
    dags: Sequence[SimDAG],
    dag_sha256: str,
    resource_sha256: str,
) -> dict[str, Any]:
    """Summarize scale, topology, resource mix, and modeled work."""
    node_counts: list[int] = []
    depths: list[int] = []
    widths: list[int] = []
    critical_paths: list[float] = []
    parallelism_values: list[float] = []
    hardware_counts: Counter[str] = Counter()
    edge_count = 0
    gpu_work_s = 0.0
    cpu_work_s = 0.0

    for dag in dags:
        depth, width, critical_path = _topology_stats(dag)
        node_count = len(dag.nodes)
        total_work = sum(node.duration_s for node in dag.nodes.values())
        node_counts.append(node_count)
        depths.append(depth)
        widths.append(width)
        critical_paths.append(critical_path)
        parallelism_values.append(total_work / critical_path if critical_path else 0.0)
        edge_count += sum(len(node.successors) for node in dag.nodes.values())
        for node in dag.nodes.values():
            hardware_counts[node.hardware] += 1
            if node.hardware == "gpu":
                gpu_work_s += node.duration_s
            else:
                cpu_work_s += node.duration_s

    total_nodes = sum(node_counts)
    total_work_s = gpu_work_s + cpu_work_s
    return {
        "dataset": spec.name,
        "role": spec.role,
        "dag_file": spec.dag_file,
        "resource_estimate_file": spec.resource_estimate_file,
        "dag_sha256": dag_sha256,
        "resource_estimate_sha256": resource_sha256,
        "dag_count": len(dags),
        "node_count": total_nodes,
        "edge_count": edge_count,
        "nodes_per_dag_mean": _round(_mean(node_counts)),
        "nodes_per_dag_p95": _round(_percentile(node_counts, 95)),
        "depth_mean": _round(_mean(depths)),
        "depth_p95": _round(_percentile(depths, 95)),
        "max_width_mean": _round(_mean(widths)),
        "max_width_p95": _round(_percentile(widths, 95)),
        "parallelism_mean": _round(_mean(parallelism_values)),
        "critical_path_s_mean": _round(_mean(critical_paths)),
        "gpu_nodes": hardware_counts["gpu"],
        "cpu_nodes": hardware_counts["cpu"],
        "gpu_node_ratio": _round(hardware_counts["gpu"] / total_nodes),
        "cpu_node_ratio": _round(hardware_counts["cpu"] / total_nodes),
        "gpu_work_s": _round(gpu_work_s),
        "cpu_work_s": _round(cpu_work_s),
        "total_work_s": _round(total_work_s),
        "gpu_work_ratio": _round(gpu_work_s / total_work_s),
        "cpu_work_ratio": _round(cpu_work_s / total_work_s),
    }


def run_experiment(
    specs: Sequence[DatasetSpec],
    configs: Sequence[ResourceConfig],
    seeds: Sequence[int],
    duration_config: DurationConfig,
    output_dir: str | Path,
    *,
    max_dags: int | None = None,
    command: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Run all datasets and write the complete reproducibility bundle."""
    if not specs:
        raise ValueError("at least one dataset is required")
    if not configs:
        raise ValueError("at least one resource configuration is required")
    if not seeds:
        raise ValueError("at least one seed is required")
    for config in configs:
        if config.gpu_count <= 0 or config.cpu_count <= 0:
            raise ValueError("GPU and CPU counts must both be positive")

    input_hashes = _validate_and_hash_inputs(specs)

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    raw_rows: list[dict[str, Any]] = []
    dataset_rows: list[dict[str, Any]] = []
    input_files: list[dict[str, Any]] = []
    started = time.perf_counter()

    for spec in specs:
        dag_hash = input_hashes[spec.dag_file]
        resource_hash = input_hashes[spec.resource_estimate_file]
        dags = load_sim_dags(
            spec.dag_file,
            limit=max_dags,
            duration_config=duration_config,
            resource_estimate_file=spec.resource_estimate_file,
        )
        validate_sim_dags(dags)
        print(
            f"[{spec.name}] loaded {len(dags)} DAGs and "
            f"{sum(len(dag.nodes) for dag in dags)} nodes",
            flush=True,
        )
        dataset_row = summarize_dataset(
            spec,
            dags,
            dag_sha256=dag_hash,
            resource_sha256=resource_hash,
        )
        dataset_rows.append(dataset_row)
        input_files.extend(
            [
                _input_manifest(spec.dag_file, dag_hash),
                _input_manifest(spec.resource_estimate_file, resource_hash),
            ]
        )
        expected = {
            "dags": len(dags),
            "nodes": sum(len(dag.nodes) for dag in dags),
            "tokens": sum(node.tokens for dag in dags for node in dag.nodes.values()),
            "work_s": sum(
                node.duration_s for dag in dags for node in dag.nodes.values()
            ),
        }

        for seed in seeds:
            ordered_dags = list(dags)
            random.Random(seed).shuffle(ordered_dags)
            for config in configs:
                timed_results = _run_methods_timed(
                    ordered_dags,
                    gpu_count=config.gpu_count,
                    cpu_count=config.cpu_count,
                )
                results = [result for result, _ in timed_results]
                _validate_results(results, expected, spec.name, seed, config)
                result_rows = [result.to_row() for result, _ in timed_results]
                _add_paired_speedups(result_rows)
                runtimes_by_runner = {
                    result.runner: runtime_s for result, runtime_s in timed_results
                }
                for result_row in result_rows:
                    raw_rows.append(
                        {
                            "dataset": spec.name,
                            "role": spec.role,
                            "seed": seed,
                            "dag_order": "seeded_shuffle",
                            "scenario": config.scenario,
                            "scenario_x": config.x_value,
                            "virtual_makespan_s": result_row["wall_clock_s"],
                            "simulator_runtime_s": _round(
                                runtimes_by_runner[str(result_row["runner"])]
                            ),
                            **result_row,
                        }
                    )
            print(
                f"[{spec.name}] completed seed {seed} "
                f"({len(configs)} resource configurations)",
                flush=True,
            )
        _write_csv(output_path / "raw_results.csv", raw_rows)
        _write_csv(output_path / "dataset_summary.csv", dataset_rows)
        _write_json(
            output_path / "progress.json",
            {
                "schema_version": SCHEMA_VERSION,
                "status": "running",
                "completed_datasets": [row["dataset"] for row in dataset_rows],
                "raw_rows": len(raw_rows),
            },
        )

    summary_rows = aggregate_results(raw_rows)
    elapsed = time.perf_counter() - started
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "experiment_kind": "deterministic_discrete_event_simulation",
        "warning": (
            "Virtual makespan and throughput are modeled values; they are not "
            "measurements from real GPU or CPU hardware."
        ),
        "command": list(command or sys.argv),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "git_revision": _git_value(["rev-parse", "HEAD"]),
        "git_dirty": bool(_git_value(["status", "--porcelain"])),
        "duration_config": asdict(duration_config),
        "trace_mode": "summary",
        "scheduling_policies": {
            runner: {
                "name": POLICY_VERSIONS[runner][0],
                "version": POLICY_VERSIONS[runner][1],
                "id": f"{POLICY_VERSIONS[runner][0]}@{POLICY_VERSIONS[runner][1]}",
            }
            for runner in RUNNER_ORDER
        },
        "seeds": list(seeds),
        "max_dags_per_dataset": max_dags,
        "resource_configs": [asdict(config) for config in configs],
        "datasets": [asdict(spec) for spec in specs],
        "inputs": input_files,
        "host_experiment_runtime_s": _round(elapsed),
        "result_counts": {
            "raw_rows": len(raw_rows),
            "summary_rows": len(summary_rows),
        },
    }

    _write_csv(output_path / "raw_results.csv", raw_rows)
    _write_csv(output_path / "summary.csv", summary_rows)
    _write_csv(output_path / "dataset_summary.csv", dataset_rows)
    _write_json(
        output_path / "summary.json",
        {"schema_version": SCHEMA_VERSION, "results": summary_rows},
    )
    _write_json(
        output_path / "dataset_summary.json",
        {"schema_version": SCHEMA_VERSION, "datasets": dataset_rows},
    )
    _write_json(
        output_path / "raw_results.json",
        {"schema_version": SCHEMA_VERSION, "results": raw_rows},
    )
    _write_json(output_path / "manifest.json", manifest)
    plot_results(summary_rows, output_path)
    report = render_report(manifest, dataset_rows, summary_rows)
    (output_path / "EXPERIMENT_REPORT.md").write_text(report, encoding="utf-8")
    _write_json(
        output_path / "progress.json",
        {
            "schema_version": SCHEMA_VERSION,
            "status": "completed",
            "completed_datasets": [row["dataset"] for row in dataset_rows],
            "raw_rows": len(raw_rows),
            "summary_rows": len(summary_rows),
        },
    )
    return {
        "manifest": manifest,
        "datasets": dataset_rows,
        "raw_results": raw_rows,
        "summary": summary_rows,
        "output_dir": str(output_path),
    }


def regenerate_artifacts(output_dir: str | Path) -> None:
    """Regenerate plots and the report from a completed result bundle."""
    output_path = Path(output_dir)
    with (output_path / "manifest.json").open("r", encoding="utf-8") as stream:
        manifest = json.load(stream)
    dataset_rows = _load_result_rows(
        output_path / "dataset_summary.json",
        output_path / "dataset_summary.csv",
        "datasets",
    )
    summary_rows = _load_result_rows(
        output_path / "summary.json",
        output_path / "summary.csv",
        "results",
    )
    _write_json(
        output_path / "dataset_summary.json",
        {"schema_version": SCHEMA_VERSION, "datasets": dataset_rows},
    )
    _write_json(
        output_path / "summary.json",
        {"schema_version": SCHEMA_VERSION, "results": summary_rows},
    )
    plot_results(summary_rows, output_path)
    report = render_report(manifest, dataset_rows, summary_rows)
    (output_path / "EXPERIMENT_REPORT.md").write_text(report, encoding="utf-8")


def aggregate_results(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate repeated seeded orderings by dataset, resources, and runner."""
    group_fields = (
        "dataset",
        "role",
        "scenario",
        "scenario_x",
        "gpu_count",
        "cpu_count",
        "dag_count",
        "runner",
    )
    metric_fields = (
        "virtual_makespan_s",
        "dags_per_second",
        "nodes_per_second",
        "tokens_per_second",
        "resource_utilization_pct",
        "gpu_utilization_pct",
        "cpu_utilization_pct",
        "simulator_runtime_s",
        "speedup_makespan_vs_parallel",
        "speedup_nodes_vs_parallel",
    )
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row[field] for field in group_fields)].append(row)

    summaries: list[dict[str, Any]] = []
    for key, group_rows in groups.items():
        summary = dict(zip(group_fields, key))
        summary["repetitions"] = len(group_rows)
        for field in ("policy_name", "policy_version", "policy_id"):
            values = {str(row[field]) for row in group_rows if row.get(field)}
            if len(values) == 1:
                summary[field] = values.pop()
        for field in metric_fields:
            values = [
                float(row[field])
                for row in group_rows
                if row.get(field) not in (None, "")
            ]
            if not values:
                continue
            summary[f"{field}_mean"] = _round(_mean(values))
            summary[f"{field}_std"] = _round(
                statistics.stdev(values) if len(values) > 1 else 0.0
            )
            summary[f"{field}_p50"] = _round(_percentile(values, 50))
            summary[f"{field}_p95"] = _round(_percentile(values, 95))
        summaries.append(summary)

    scenario_order = {"balanced": 0, "gpu_scarce": 1, "cpu_scarce": 2}
    runner_order = {name: index for index, name in enumerate(RUNNER_ORDER)}
    return sorted(
        summaries,
        key=lambda row: (
            str(row["dataset"]),
            scenario_order.get(str(row["scenario"]), 99),
            int(row["scenario_x"]),
            runner_order.get(str(row["runner"]), 99),
        ),
    )


def plot_results(summary_rows: Sequence[dict[str, Any]], output_dir: Path) -> None:
    """Create the paper-oriented throughput, speedup, and utilization figures."""
    import matplotlib.pyplot as plt

    datasets = list(dict.fromkeys(str(row["dataset"]) for row in summary_rows))
    for scenario in ("balanced", "gpu_scarce", "cpu_scarce"):
        scenario_rows = [row for row in summary_rows if row["scenario"] == scenario]
        if not scenario_rows:
            continue
        fig, axes = plt.subplots(
            len(datasets),
            1,
            figsize=(8.2, max(3.2, 2.8 * len(datasets))),
            dpi=160,
            squeeze=False,
        )
        for axis, dataset in zip(axes[:, 0], datasets):
            for runner in RUNNER_ORDER:
                values = sorted(
                    (
                        row
                        for row in scenario_rows
                        if row["dataset"] == dataset and row["runner"] == runner
                    ),
                    key=lambda row: int(row["scenario_x"]),
                )
                if not values:
                    continue
                axis.plot(
                    [row["scenario_x"] for row in values],
                    [row["nodes_per_second_mean"] for row in values],
                    marker="o",
                    linewidth=1.8,
                    label=RUNNER_LABELS[runner],
                )
            axis.set_title(dataset)
            axis.set_xscale("log", base=2)
            ticks = sorted(
                {
                    int(row["scenario_x"])
                    for row in scenario_rows
                    if row["dataset"] == dataset
                }
            )
            axis.set_xticks(ticks, labels=[str(tick) for tick in ticks])
            axis.grid(True, linestyle="--", linewidth=0.6, alpha=0.45)
            axis.set_ylabel("Nodes / virtual second")
        axes[-1, 0].set_xlabel(_scenario_xlabel(scenario, scenario_rows))
        axes[0, 0].legend(ncol=3, fontsize=8)
        fig.suptitle(f"DAG Workload Node Throughput: {_scenario_title(scenario)}")
        fig.tight_layout()
        _save_figure(fig, output_dir / f"throughput_{scenario}")
        plt.close(fig)

    wenbo_rows = [
        row
        for row in summary_rows
        if row["runner"] == "wenbo" and "speedup_nodes_vs_parallel_mean" in row
    ]
    for scenario in ("balanced", "gpu_scarce", "cpu_scarce"):
        values = [row for row in wenbo_rows if row["scenario"] == scenario]
        if not values:
            continue
        fig, axis = plt.subplots(figsize=(8.2, 4.8), dpi=160)
        for dataset in datasets:
            dataset_values = sorted(
                (row for row in values if row["dataset"] == dataset),
                key=lambda row: int(row["scenario_x"]),
            )
            if not dataset_values:
                continue
            axis.plot(
                [row["scenario_x"] for row in dataset_values],
                [row["speedup_nodes_vs_parallel_mean"] for row in dataset_values],
                marker="o",
                linewidth=1.8,
                label=dataset,
            )
        axis.axhline(1.0, color="#555555", linewidth=1.0, linestyle="--")
        axis.set_xscale("log", base=2)
        ticks = sorted({int(row["scenario_x"]) for row in values})
        axis.set_xticks(ticks, labels=[str(tick) for tick in ticks])
        axis.set_xlabel(_scenario_xlabel(scenario, values))
        axis.set_ylabel("Wenbo speedup over Flash-Searcher (parallel)")
        axis.set_title(f"Node Throughput Speedup: {_scenario_title(scenario)}")
        axis.grid(True, linestyle="--", linewidth=0.6, alpha=0.45)
        axis.legend(fontsize=8)
        fig.tight_layout()
        _save_figure(fig, output_dir / f"speedup_{scenario}")
        plt.close(fig)

    for scenario in ("balanced", "gpu_scarce", "cpu_scarce"):
        scenario_rows = [row for row in summary_rows if row["scenario"] == scenario]
        if not scenario_rows:
            continue
        fig, axes = plt.subplots(
            len(datasets),
            1,
            figsize=(8.2, max(3.2, 2.8 * len(datasets))),
            dpi=160,
            squeeze=False,
        )
        for axis, dataset in zip(axes[:, 0], datasets):
            for runner in ("parallel", "wenbo"):
                values = sorted(
                    (
                        row
                        for row in scenario_rows
                        if row["dataset"] == dataset and row["runner"] == runner
                    ),
                    key=lambda row: int(row["scenario_x"]),
                )
                if not values:
                    continue
                axis.plot(
                    [row["scenario_x"] for row in values],
                    [row["resource_utilization_pct_mean"] for row in values],
                    marker="o",
                    linewidth=1.8,
                    label=RUNNER_LABELS[runner],
                )
            ticks = sorted(
                {
                    int(row["scenario_x"])
                    for row in scenario_rows
                    if row["dataset"] == dataset
                }
            )
            axis.set_title(dataset)
            axis.set_xscale("log", base=2)
            axis.set_xticks(ticks, labels=[str(tick) for tick in ticks])
            axis.set_ylim(0, 105)
            axis.grid(True, linestyle="--", linewidth=0.6, alpha=0.45)
            axis.set_ylabel("Utilization (%)")
        axes[-1, 0].set_xlabel(_scenario_xlabel(scenario, scenario_rows))
        axes[0, 0].legend(ncol=2, fontsize=8)
        fig.suptitle(f"Resource Utilization: {_scenario_title(scenario)}")
        fig.tight_layout()
        _save_figure(fig, output_dir / f"utilization_{scenario}")
        plt.close(fig)


def render_report(
    manifest: dict[str, Any],
    dataset_rows: Sequence[dict[str, Any]],
    summary_rows: Sequence[dict[str, Any]],
) -> str:
    """Render a concise report whose claims are derived from result rows."""
    available_scenarios = tuple(
        scenario
        for scenario in ("balanced", "gpu_scarce", "cpu_scarce")
        if any(row["scenario"] == scenario for row in summary_rows)
    )
    synthetic_only = all(
        str(row["role"]).startswith("synthetic_") for row in dataset_rows
    )
    report_title = (
        "# Wenbo 合成 DAG 固定资源扩展实验报告"
        if synthetic_only
        else "# Wenbo 已有 DAG 离散事件模拟实验报告"
    )
    lines = [
        report_title,
        "",
        f"> 结果模式：`{manifest['schema_version']}`  ",
        "> 重要说明：下文的时间和吞吐均来自确定性离散事件虚拟时钟，不是对真实 GPU、CPU、网络、RPC、模型服务或存储系统的性能测量。",
        "",
        "## 实验问题",
        "",
        "本实验检验：与 Flash-Searcher (parallel) 中按任务绑定资源、已获槽位保留至 DAG 完成的调度方式相比，把所有 DAG 的就绪节点放入全局 GPU/CPU 共享资源池，能否提高系统整体吞吐。",
        "",
        "## 数据集概况",
        "",
        "| 数据集 | 实验角色 | DAG 数 | 节点数 | 平均深度 | 平均最大宽度 | GPU/CPU 节点占比 | GPU/CPU 工作量占比 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in dataset_rows:
        lines.append(
            "| {dataset} | {role} | {dag_count} | {node_count} | {depth_mean:.2f} | "
            "{max_width_mean:.2f} | {gpu_node_ratio:.3f}/{cpu_node_ratio:.3f} | "
            "{gpu_work_ratio:.3f}/{cpu_work_ratio:.3f} |".format(**row)
        )

    lines.extend(
        [
            "",
            "## 方法与公平性控制",
            "",
            "- `Flash-Searcher (parallel)`：最多准入 `max(GPU, CPU)` 个 DAG；接入时绑定数量较多资源的一个槽位，另一类资源首次使用时申请，所有已获得槽位保留至整个任务完成；每个 DAG 每种资源最多执行一个节点。",
            "- `Wenbo`：把全部 DAG 的就绪节点放入由共享槽支撑的全局 GPU/CPU 队列。",
            "- 每组配对运行使用完全相同的 DAG、输入顺序、节点耗时、token 数和 GPU/CPU 槽位数。",
            "- 如果任一方法完成的 DAG、节点、token 或模拟总工作量不同，实验立即失败，不写出不完整结论。",
            f"- 重复实验使用以下 DAG 顺序随机种子：`{manifest['seeds']}`；模拟器本身是确定性的。",
            "",
            "## 资源场景",
            "",
        ]
    )
    fixed_gpu_count = _fixed_resource_count("gpu_scarce", summary_rows)
    fixed_cpu_count = _fixed_resource_count("cpu_scarce", summary_rows)
    scenario_descriptions = {
        "balanced": "- `balanced`：GPU 与 CPU 槽位数同步增加。",
        "gpu_scarce": f"- `gpu_scarce`：固定 {fixed_gpu_count} 个 GPU 槽并增加 CPU 槽。",
        "cpu_scarce": f"- `cpu_scarce`：固定 {fixed_cpu_count} 个 CPU 槽并增加 GPU 槽。",
    }
    lines.extend(scenario_descriptions[scenario] for scenario in available_scenarios)
    if synthetic_only:
        lines.extend(
            [
                "",
                "## 吞吐率随资源变化",
                "",
                "节点吞吐单位为 `nodes / virtual second`，DAG 吞吐单位为 `DAGs / virtual second`。相对吞吐以同一数据集、同一扫描方向下可变资源的最小扫描点为 1.000x。",
                "",
                "| 数据集 | 固定资源 | 增加资源 | GPU/CPU | Flash-Searcher (parallel) 节点吞吐 | Wenbo DAG 吞吐 | Wenbo 节点吞吐 | Wenbo 相对吞吐 |",
                "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        synthetic_saturation_rows: list[tuple[str, str, str, int, float]] = []
        for dataset in (row["dataset"] for row in dataset_rows):
            for scenario in available_scenarios:
                if scenario not in ("gpu_scarce", "cpu_scarce"):
                    continue
                values = sorted(
                    (
                        row
                        for row in summary_rows
                        if row["dataset"] == dataset
                        and row["scenario"] == scenario
                        and row["runner"] == "wenbo"
                    ),
                    key=lambda row: int(row["scenario_x"]),
                )
                if not values:
                    continue
                origin_throughput = float(values[0]["nodes_per_second_mean"])
                max_throughput = max(
                    float(row["nodes_per_second_mean"]) for row in values
                )
                saturation_threshold = 0.95 * max_throughput
                saturation = next(
                    row
                    for row in values
                    if float(row["nodes_per_second_mean"])
                    >= saturation_threshold
                )
                fixed_resource = (
                    f"GPU={values[0]['gpu_count']}"
                    if scenario == "gpu_scarce"
                    else f"CPU={values[0]['cpu_count']}"
                )
                varied_resource = "CPU" if scenario == "gpu_scarce" else "GPU"
                synthetic_saturation_rows.append(
                    (
                        str(dataset),
                        fixed_resource,
                        varied_resource,
                        int(saturation["scenario_x"]),
                        max_throughput,
                    )
                )
                for row in values:
                    baseline = next(
                        baseline_row
                        for baseline_row in summary_rows
                        if baseline_row["dataset"] == dataset
                        and baseline_row["scenario"] == scenario
                        and baseline_row["gpu_count"] == row["gpu_count"]
                        and baseline_row["cpu_count"] == row["cpu_count"]
                        and baseline_row["runner"] == "parallel"
                    )
                    relative_throughput = (
                        float(row["nodes_per_second_mean"]) / origin_throughput
                    )
                    lines.append(
                        "| {dataset} | {fixed} | {varied}={scenario_x} | "
                        "{gpu_count}/{cpu_count} | {parallel:.6f} | {dags:.6f} | "
                        "{nodes:.6f} | {relative:.3f}x |".format(
                            dataset=dataset,
                            fixed=fixed_resource,
                            varied=varied_resource,
                            scenario_x=row["scenario_x"],
                            gpu_count=row["gpu_count"],
                            cpu_count=row["cpu_count"],
                            parallel=baseline["nodes_per_second_mean"],
                            dags=row["dags_per_second_mean"],
                            nodes=row["nodes_per_second_mean"],
                            relative=relative_throughput,
                        )
                    )
        lines.extend(
            [
                "",
                "### 实测饱和点",
                "",
                "这里把首次达到本次扫描中 Wenbo 最大平均节点吞吐 95% 的资源点记为实测饱和点；它只描述本次合成负载和扫描范围。",
                "",
                "| 数据集 | 固定资源 | 增加资源 | 95% 实测饱和点 | 本次最大节点吞吐 |",
                "| --- | --- | --- | ---: | ---: |",
            ]
        )
        for dataset, fixed, varied, point, max_throughput in synthetic_saturation_rows:
            lines.append(
                f"| {dataset} | {fixed} | {varied} | {varied}={point} | "
                f"{max_throughput:.6f} |"
            )
    lines.extend(
        [
            "",
            "## 汇总结果",
            "",
            "下表给出每个场景内 Wenbo 相对 Flash-Searcher (parallel) 基线的最大平均节点吞吐加速比，同时披露产生该最大值的资源配置。最大值不能脱离数据集拓扑和资源工作量比例单独解释。",
            "",
            "| 数据集 | 场景 | 最佳 GPU/CPU | 平均加速 | DAG 顺序 P95 | Wenbo 总利用率 | 基线总利用率 |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    wenbo_rows = [
        row
        for row in summary_rows
        if row["runner"] == "wenbo" and "speedup_nodes_vs_parallel_mean" in row
    ]
    for dataset in (row["dataset"] for row in dataset_rows):
        for scenario in ("balanced", "gpu_scarce", "cpu_scarce"):
            candidates = [
                row
                for row in wenbo_rows
                if row["dataset"] == dataset and row["scenario"] == scenario
            ]
            if not candidates:
                continue
            best = max(
                candidates,
                key=lambda row: float(row["speedup_nodes_vs_parallel_mean"]),
            )
            baseline = next(
                row
                for row in summary_rows
                if row["dataset"] == dataset
                and row["scenario"] == scenario
                and row["gpu_count"] == best["gpu_count"]
                and row["cpu_count"] == best["cpu_count"]
                and row["runner"] == "parallel"
            )
            lines.append(
                "| {dataset} | {scenario} | {gpu_count}/{cpu_count} | {speedup:.3f}x | "
                "{p95:.3f}x | {wenbo_util:.2f}% | {baseline_util:.2f}% |".format(
                    dataset=dataset,
                    scenario=scenario,
                    gpu_count=best["gpu_count"],
                    cpu_count=best["cpu_count"],
                    speedup=best["speedup_nodes_vs_parallel_mean"],
                    p95=best["speedup_nodes_vs_parallel_p95"],
                    wenbo_util=best["resource_utilization_pct_mean"],
                    baseline_util=baseline["resource_utilization_pct_mean"],
                )
            )

    scenario_best = {
        scenario: [
            max(
                (
                    row
                    for row in wenbo_rows
                    if row["dataset"] == dataset
                    and row["scenario"] == scenario
                ),
                key=lambda row: float(row["speedup_nodes_vs_parallel_mean"]),
            )
            for dataset in (row["dataset"] for row in dataset_rows)
        ]
        for scenario in available_scenarios
    }
    cpu_work_ratios = [float(row["cpu_work_ratio"]) for row in dataset_rows]
    serial_control = next(
        (
            row
            for row in dataset_rows
            if row["role"] == "serial_negative_control"
        ),
        None,
    )
    lines.extend(
        [
            "",
            "## 结果解读",
            "",
            f"- 当前资源估算模型下，{len(dataset_rows)} 个数据集的 CPU 工作量占比为 "
            f"{min(cpu_work_ratios) * 100:.1f}% 到 "
            f"{max(cpu_work_ratios) * 100:.1f}%。资源扩展曲线必须结合这一工作量比例解释。",
        ]
    )
    if synthetic_only:
        for dataset in (row["dataset"] for row in dataset_rows):
            for scenario in available_scenarios:
                if scenario not in ("gpu_scarce", "cpu_scarce"):
                    continue
                values = sorted(
                    (
                        row
                        for row in summary_rows
                        if row["dataset"] == dataset
                        and row["scenario"] == scenario
                        and row["runner"] == "wenbo"
                    ),
                    key=lambda row: int(row["scenario_x"]),
                )
                if not values:
                    continue
                origin = float(values[0]["nodes_per_second_mean"])
                maximum = max(float(row["nodes_per_second_mean"]) for row in values)
                saturation = next(
                    row
                    for row in values
                    if float(row["nodes_per_second_mean"]) >= 0.95 * maximum
                )
                fixed_resource = (
                    f"GPU={values[0]['gpu_count']}"
                    if scenario == "gpu_scarce"
                    else f"CPU={values[0]['cpu_count']}"
                )
                varied_resource = "CPU" if scenario == "gpu_scarce" else "GPU"
                limiting_resource = "GPU" if scenario == "gpu_scarce" else "CPU"
                lines.append(
                    f"- `{dataset}` 固定 {fixed_resource} 并增加 {varied_resource}："
                    f"Wenbo 节点虚拟吞吐从 {origin:.6f} 增至本次扫描最大值 "
                    f"{maximum:.6f}（{maximum / origin:.3f}x）；"
                    f"{varied_resource}={saturation['scenario_x']} 首次达到最大值的 "
                    f"95%，此后曲线主要受固定的 {limiting_resource} 资源限制。"
                )
    if "balanced" in scenario_best:
        lines.append(
            "- 均衡资源场景中，各数据集的最佳平均加速范围为 "
            f"{min(float(row['speedup_nodes_vs_parallel_mean']) for row in scenario_best['balanced']):.3f}x 到 "
            f"{max(float(row['speedup_nodes_vs_parallel_mean']) for row in scenario_best['balanced']):.3f}x。"
        )
    if "gpu_scarce" in scenario_best:
        strongest = max(
            scenario_best["gpu_scarce"],
            key=lambda row: float(row["speedup_nodes_vs_parallel_mean"]),
        )
        lines.append(
            "- 固定 GPU、增加 CPU 时，最佳平均加速范围为 "
            f"{min(float(row['speedup_nodes_vs_parallel_mean']) for row in scenario_best['gpu_scarce']):.3f}x 到 "
            f"{max(float(row['speedup_nodes_vs_parallel_mean']) for row in scenario_best['gpu_scarce']):.3f}x。最强结果来自 "
            f"{strongest['dataset']} 的 GPU/CPU={strongest['gpu_count']}/{strongest['cpu_count']}，为 "
            f"{strongest['speedup_nodes_vs_parallel_mean']:.3f}x。"
        )
    if "cpu_scarce" in scenario_best:
        lines.append(
            "- 固定 CPU、增加 GPU 时，最佳平均加速范围为 "
            f"{min(float(row['speedup_nodes_vs_parallel_mean']) for row in scenario_best['cpu_scarce']):.3f}x 到 "
            f"{max(float(row['speedup_nodes_vs_parallel_mean']) for row in scenario_best['cpu_scarce']):.3f}x。"
        )
    if serial_control is not None and "gpu_scarce" in scenario_best:
        serial_best = next(
            row
            for row in scenario_best["gpu_scarce"]
            if row["dataset"] == serial_control["dataset"]
        )
        lines.append(
            "- 串行对照 "
            f"{serial_control['dataset']} 的平均最大宽度为 "
            f"{serial_control['max_width_mean']:.2f}，但在 GPU 稀缺场景仍达到 "
            f"{serial_best['speedup_nodes_vs_parallel_mean']:.3f}x。这说明该收益主要来自跨 DAG 资源池化，而不是单个 DAG 内部的宽并行。"
        )

    command = " ".join(str(item) for item in manifest["command"])
    lines.extend(
        [
            "",
            "## 解释边界",
            "",
            "- 结果只隔离固定节点耗时模型下的调度影响，不包含 Scheduler RPC 延迟、队列竞争、数据传输、模型批处理、KV cache 复用、故障、重试和持久化开销。",
            "- Flash-Searcher (parallel) 使用任务级资源绑定模型：数量较多的资源在准入时绑定，另一类资源首次使用时申请，已获得槽位保留至任务完成。等待依赖期间的槽位不能借给其他 DAG；结果只对应这里实现的调度模型。",
            "- 随机种子方差衡量 DAG 准入顺序敏感性，不是硬件噪声，也不是针对新抽样任务的统计置信区间。",
            "- 虚拟吞吐绝对值不能写成真实 requests/s。在接入真实 Worker 前，论文应使用相对加速比和利用率趋势。",
            "",
            "## 复现实验",
            "",
            "```powershell",
            command,
            "```",
            "",
            "`manifest.json` 记录准确的输入哈希、耗时参数、Git 版本和资源配置；`raw_results.csv` 保存每次配对运行，`summary.csv` 保存聚合统计。",
            "",
        ]
    )
    return "\n".join(lines)


def _topology_stats(dag: SimDAG) -> tuple[int, int, float]:
    indegree = {node_id: len(node.predecessors) for node_id, node in dag.nodes.items()}
    queue = deque(
        sorted(
            (node_id for node_id, degree in indegree.items() if degree == 0),
            key=dag.node_order.__getitem__,
        )
    )
    level: dict[str, int] = {}
    critical_path: dict[str, float] = {}
    visited = 0
    while queue:
        node_id = queue.popleft()
        node = dag.nodes[node_id]
        level[node_id] = (
            max((level[pred] + 1 for pred in node.predecessors), default=0)
        )
        critical_path[node_id] = node.duration_s + max(
            (critical_path[pred] for pred in node.predecessors),
            default=0.0,
        )
        visited += 1
        for successor_id in sorted(node.successors, key=dag.node_order.__getitem__):
            indegree[successor_id] -= 1
            if indegree[successor_id] == 0:
                queue.append(successor_id)
    if visited != len(dag.nodes):
        raise ValueError(f"DAG {dag.dag_id} contains a cycle")
    width_counts = Counter(level.values())
    return (
        max(level.values(), default=-1) + 1,
        max(width_counts.values(), default=0),
        max(critical_path.values(), default=0.0),
    )


def _validate_results(
    results: Sequence[DiscreteResult],
    expected: dict[str, int | float],
    dataset: str,
    seed: int,
    config: ResourceConfig,
) -> None:
    for result in results:
        context = (
            f"dataset={dataset}, seed={seed}, scenario={config.scenario}, "
            f"gpu={config.gpu_count}, cpu={config.cpu_count}, runner={result.runner}"
        )
        if result.completed_dags != expected["dags"]:
            raise RuntimeError(f"incomplete DAG count: {context}")
        if result.completed_nodes != expected["nodes"]:
            raise RuntimeError(f"incomplete node count: {context}")
        if result.completed_tokens != expected["tokens"]:
            raise RuntimeError(f"inconsistent token count: {context}")
        if not math.isclose(
            result.sum_node_exec_s,
            float(expected["work_s"]),
            rel_tol=1e-9,
            abs_tol=1e-9,
        ):
            raise RuntimeError(f"inconsistent modeled work: {context}")


def _run_methods_timed(
    dags: list[SimDAG],
    *,
    gpu_count: int,
    cpu_count: int,
) -> list[tuple[DiscreteResult, float]]:
    resource_count = max(gpu_count, cpu_count)
    calls = [
        lambda: run_parallel(
            dags,
            resource_count,
            gpu_count,
            cpu_count,
            validate_input=False,
        ),
        lambda: run_wenbo(
            dags,
            resource_count,
            gpu_count,
            cpu_count,
            validate_input=False,
        ),
    ]
    timed_results = []
    for call in calls:
        started = time.perf_counter()
        result = call()
        timed_results.append((result, time.perf_counter() - started))
    return timed_results


def _add_paired_speedups(rows: list[dict[str, Any]]) -> None:
    wenbo = next((row for row in rows if row["runner"] == "wenbo"), None)
    if not wenbo:
        return
    for baseline_name in ("parallel",):
        baseline = next(
            (row for row in rows if row["runner"] == baseline_name),
            None,
        )
        if not baseline:
            continue
        wenbo[f"speedup_makespan_vs_{baseline_name}"] = _round(
            float(baseline["wall_clock_s"]) / float(wenbo["wall_clock_s"])
        )
        wenbo[f"speedup_nodes_vs_{baseline_name}"] = _round(
            float(wenbo["nodes_per_second"])
            / float(baseline["nodes_per_second"])
        )


def _input_manifest(path: str, digest: str) -> dict[str, Any]:
    file_path = Path(path)
    return {
        "path": path,
        "bytes": file_path.stat().st_size,
        "sha256": digest,
    }


def _validate_and_hash_inputs(specs: Sequence[DatasetSpec]) -> dict[str, str]:
    paths = list(
        dict.fromkeys(
            path
            for spec in specs
            for path in (spec.dag_file, spec.resource_estimate_file)
        )
    )
    missing = [path for path in paths if not Path(path).is_file()]
    if missing:
        joined = ", ".join(missing)
        raise FileNotFoundError(f"experiment input files do not exist: {joined}")
    return {path: _sha256_file(path) for path in paths}


def _sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    fieldnames: list[str] = []
    for row in rows:
        for field in row:
            if field not in fieldnames:
                fieldnames.append(field)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2)


def _load_result_rows(
    json_path: Path,
    csv_path: Path,
    payload_key: str,
) -> list[dict[str, Any]]:
    if json_path.is_file():
        with json_path.open("r", encoding="utf-8") as stream:
            return list(json.load(stream)[payload_key])
    with csv_path.open("r", encoding="utf-8", newline="") as stream:
        return [
            {
                key: _coerce_csv_value(value)
                for key, value in row.items()
                if value != ""
            }
            for row in csv.DictReader(stream)
        ]


def _coerce_csv_value(value: str) -> str | int | float:
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def _save_figure(figure: Any, base_path: Path) -> None:
    figure.savefig(base_path.with_suffix(".png"))
    figure.savefig(base_path.with_suffix(".pdf"))


def _scenario_xlabel(
    scenario: str, rows: Sequence[dict[str, Any]] = ()
) -> str:
    fixed_gpu_count = _fixed_resource_count("gpu_scarce", rows)
    fixed_cpu_count = _fixed_resource_count("cpu_scarce", rows)
    return {
        "balanced": "GPU slots = CPU slots",
        "gpu_scarce": f"CPU slots (GPU slots fixed at {fixed_gpu_count})",
        "cpu_scarce": f"GPU slots (CPU slots fixed at {fixed_cpu_count})",
    }[scenario]


def _scenario_title(scenario: str) -> str:
    return {
        "balanced": "Balanced Scaling",
        "gpu_scarce": "GPU Fixed",
        "cpu_scarce": "CPU Fixed",
    }[scenario]


def _fixed_resource_count(
    scenario: str, rows: Sequence[dict[str, Any]]
) -> str:
    field = "gpu_count" if scenario == "gpu_scarce" else "cpu_count"
    counts = sorted(
        {
            int(row[field])
            for row in rows
            if row.get("scenario") == scenario and row.get(field) is not None
        }
    )
    if not counts:
        return "unknown"
    return "/".join(str(count) for count in counts)


def _git_value(args: Sequence[str]) -> str:
    try:
        completed = subprocess.run(
            ["git", *args],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except OSError:
        return ""
    return completed.stdout.strip() if completed.returncode == 0 else ""


def _mean(values: Iterable[float | int]) -> float:
    materialized = list(values)
    return statistics.fmean(materialized) if materialized else 0.0


def _percentile(values: Iterable[float | int], percentile: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * percentile / 100.0
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    fraction = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _round(value: float, digits: int = 6) -> float:
    return round(float(value), digits)
