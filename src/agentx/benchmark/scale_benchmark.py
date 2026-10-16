"""Repeatable host-resource benchmarks for large streaming DAG datasets.

Unlike :mod:`agentx.benchmark.discrete_simulator`, this module measures real
host time and memory. Each scheduling method runs in a fresh Python process so
allocator state from one method cannot contaminate the next measurement.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import pickle
import platform
import subprocess
import sys
import tempfile
import time
import tracemalloc
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence

from agentx.benchmark.discrete_simulator import (
    DEFAULT_AGENTX_ACTIVE_DAGS,
    DiscreteResult,
    SimDAG,
    iter_sim_dags,
    run_agentx,
    run_parallel,
    run_sequential,
)


SCHEMA_VERSION = "agentx.scale-benchmark.v2"
RUNNERS = ("sequential", "parallel", "agentx")
DEFAULT_RUNNERS = ("parallel", "agentx")
PREPARED_STREAM_FORMAT = "agentx.sim-dag-pickle.v2"
_PREPARED_STREAM_MAGIC = b"AGENTX_SIM_DAG_PICKLE_V2\n"
_PREPARED_BATCH_TAG = "batch"
_PREPARED_END_TAG = "end"
_PREPARED_BATCH_DAGS = 256
_PREPARED_BATCH_NODES = 8192


class ScaleBenchmarkError(RuntimeError):
    """Raised when an isolated benchmark worker cannot produce a result."""


@dataclass(frozen=True)
class ScaleBenchmarkConfig:
    """Inputs shared by every isolated scheduling-method worker."""

    dag_file: str | Path
    runners: tuple[str, ...] = DEFAULT_RUNNERS
    gpu_count: int = 4
    cpu_count: int = 4
    max_active_dags: int = DEFAULT_AGENTX_ACTIVE_DAGS
    parallel_active_limit: int | None = None
    limit: int | None = None
    resource_estimate_file: str | Path | None = None
    chunk_size: int = 1024 * 1024
    validate_unique_ids: bool = False
    check_invariants: bool = False
    worker_timeout_s: float | None = None

    @property
    def resource_count(self) -> int:
        return max(self.gpu_count, self.cpu_count)

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["dag_file"] = str(Path(self.dag_file).resolve())
        if self.resource_estimate_file is not None:
            payload["resource_estimate_file"] = str(
                Path(self.resource_estimate_file).resolve()
            )
        payload["runners"] = list(self.runners)
        payload["resource_count"] = self.resource_count
        return payload


@dataclass(frozen=True)
class ThresholdConfig:
    """Absolute limits and allowed ratios against a prior result file."""

    max_runtime_s: float | None = None
    max_peak_rss_mb: float | None = None
    max_python_peak_mb: float | None = None
    min_nodes_per_second: float | None = None
    max_runtime_ratio: float = 1.2
    max_peak_rss_ratio: float = 1.2
    min_nodes_per_second_ratio: float = 0.8


@dataclass(frozen=True)
class ScaleBenchmarkResult:
    runner: str
    policy_id: str
    worker_pid: int
    host_wall_clock_s: float
    peak_rss_mb: float
    peak_rss_source: str
    python_peak_allocated_mb: float
    completed_dags: int
    completed_nodes: int
    completed_tokens: int
    peak_active_dags: int
    host_dags_per_second: float
    host_nodes_per_second: float
    host_tokens_per_second: float
    simulated_makespan_s: float
    trace_event_count: int

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


def generate_scale_dataset(
    path: str | Path,
    dag_count: int,
    *,
    nodes_per_dag: int = 6,
) -> dict[str, Any]:
    """Atomically write a deterministic, heterogeneous DAG dataset as JSONL."""
    _positive_int("dag_count", dag_count)
    _positive_int("nodes_per_dag", nodes_per_dag)
    if nodes_per_dag < 2:
        raise ValueError("nodes_per_dag must be at least 2")

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = _temporary_path(output_path)
    digest = hashlib.sha256()
    try:
        with temporary_path.open("x", encoding="utf-8", newline="\n") as stream:
            for dag_index in range(dag_count):
                line = json.dumps(
                    _generated_dag(dag_index, dag_count, nodes_per_dag),
                    ensure_ascii=True,
                    separators=(",", ":"),
                )
                encoded = f"{line}\n".encode("utf-8")
                stream.write(encoded.decode("utf-8"))
                digest.update(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, output_path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise

    return {
        "path": str(output_path.resolve()),
        "format": "jsonl",
        "dag_count": dag_count,
        "node_count": dag_count * nodes_per_dag,
        "nodes_per_dag": nodes_per_dag,
        "size_bytes": output_path.stat().st_size,
        "sha256": digest.hexdigest(),
    }


def run_scale_benchmark(
    config: ScaleBenchmarkConfig,
    output_dir: str | Path,
    *,
    thresholds: ThresholdConfig | None = None,
    baseline_path: str | Path | None = None,
    python_executable: str | Path | None = None,
) -> dict[str, Any]:
    """Run isolated workers, evaluate gates, and atomically write JSON and CSV."""
    _validate_config(config)
    thresholds = thresholds or ThresholdConfig()
    _validate_thresholds(thresholds)

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    input_manifest = {
        "dag_file": _input_manifest(Path(config.dag_file)),
        "resource_estimate_file": (
            _input_manifest(Path(config.resource_estimate_file))
            if config.resource_estimate_file is not None
            else None
        ),
    }
    config_payload = config.to_payload()
    baseline_results = _load_baseline(
        baseline_path,
        config.runners,
        expected_input=input_manifest,
        expected_configuration=config_payload,
    )
    executable = str(python_executable or sys.executable)

    results: list[ScaleBenchmarkResult] = []
    with tempfile.TemporaryDirectory(prefix=".scale-workers-", dir=output_path) as temp_dir:
        temporary_directory = Path(temp_dir)
        prepared_path = temporary_directory / "prepared-dags.bin"
        try:
            preprocessing = _prepare_dag_stream(config, prepared_path)
        except Exception as exc:
            raise ScaleBenchmarkError(f"DAG preprocessing failed: {exc}") from exc
        for runner in config.runners:
            results.append(
                _run_isolated_worker(
                    config,
                    runner,
                    temporary_directory,
                    executable,
                    prepared_path,
                )
            )

    gate = evaluate_gate(results, thresholds, baseline_results=baseline_results)
    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "parent_pid": os.getpid(),
        },
        "input": input_manifest,
        "preprocessing": preprocessing,
        "configuration": config_payload,
        "thresholds": asdict(thresholds),
        "baseline": (
            {"path": str(Path(baseline_path).resolve())}
            if baseline_path is not None
            else None
        ),
        "results": [result.to_payload() for result in results],
        "gate": gate,
    }
    _atomic_write_json(output_path / "scale_benchmark.json", report)
    _atomic_write_csv(output_path / "scale_benchmark.csv", results, gate)
    return report


def evaluate_gate(
    results: Sequence[ScaleBenchmarkResult],
    thresholds: ThresholdConfig,
    *,
    baseline_results: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Evaluate absolute and optional historical-relative regression limits."""
    _validate_thresholds(thresholds)
    if not results:
        raise ValueError("at least one benchmark result is required")
    violations: list[dict[str, Any]] = []
    baseline_results = baseline_results or {}

    absolute_limits = (
        ("host_wall_clock_s", thresholds.max_runtime_s, "max"),
        ("peak_rss_mb", thresholds.max_peak_rss_mb, "max"),
        ("python_peak_allocated_mb", thresholds.max_python_peak_mb, "max"),
        ("host_nodes_per_second", thresholds.min_nodes_per_second, "min"),
    )
    relative_limits = (
        ("host_wall_clock_s", thresholds.max_runtime_ratio, "max"),
        ("peak_rss_mb", thresholds.max_peak_rss_ratio, "max"),
        (
            "host_nodes_per_second",
            thresholds.min_nodes_per_second_ratio,
            "min",
        ),
    )

    reference = results[0] if results else None
    for result in results:
        row = result.to_payload()
        if reference is not None and result.runner != reference.runner:
            for metric in ("completed_dags", "completed_nodes", "completed_tokens"):
                actual = int(row[metric])
                expected = int(getattr(reference, metric))
                if actual != expected:
                    violations.append(
                        {
                            "kind": "consistency",
                            "runner": result.runner,
                            "metric": metric,
                            "comparison": "==",
                            "actual": actual,
                            "limit": expected,
                            "reference_runner": reference.runner,
                        }
                    )
        for metric, limit, direction in absolute_limits:
            if limit is not None:
                _append_violation(
                    violations,
                    runner=result.runner,
                    metric=metric,
                    actual=float(row[metric]),
                    limit=float(limit),
                    direction=direction,
                    kind="absolute",
                )

        baseline = baseline_results.get(result.runner)
        if baseline is None:
            continue
        for metric, ratio, direction in relative_limits:
            if metric not in baseline:
                raise ValueError(
                    f"baseline result for {result.runner!r} is missing {metric!r}"
                )
            baseline_value = float(baseline[metric])
            if not math.isfinite(baseline_value) or baseline_value < 0:
                raise ValueError(
                    f"baseline {result.runner}.{metric} must be finite and non-negative"
                )
            limit = baseline_value * ratio
            _append_violation(
                violations,
                runner=result.runner,
                metric=metric,
                actual=float(row[metric]),
                limit=limit,
                direction=direction,
                kind="relative",
                baseline=baseline_value,
                ratio=ratio,
            )

    return {
        "passed": not violations,
        "violation_count": len(violations),
        "violations": violations,
    }


def _generated_dag(
    dag_index: int,
    dag_count: int,
    nodes_per_dag: int,
) -> dict[str, Any]:
    width = max(6, len(str(max(dag_count - 1, 0))))
    nodes = []
    for node_index in range(nodes_per_dag):
        if node_index == 0 or node_index == nodes_per_dag - 1:
            hardware = "cpu"
        else:
            hardware = "gpu" if (dag_index + node_index) % 3 else "cpu"
        nodes.append(
            {
                "node_id": f"node-{node_index:03d}",
                "hardware": hardware,
                "estimated_tokens": 32 + ((dag_index * 17 + node_index * 29) % 224),
            }
        )

    if nodes_per_dag == 2:
        edges = [{"source": "node-000", "target": "node-001"}]
    else:
        edges = [
            {"source": "node-000", "target": f"node-{index:03d}"}
            for index in range(1, nodes_per_dag - 1)
        ]
        edges.extend(
            {
                "source": f"node-{index:03d}",
                "target": f"node-{nodes_per_dag - 1:03d}",
            }
            for index in range(1, nodes_per_dag - 1)
        )
    return {
        "dag_id": f"scale-dag-{dag_index:0{width}d}",
        "nodes": nodes,
        "edges": edges,
    }


def _run_isolated_worker(
    config: ScaleBenchmarkConfig,
    runner: str,
    temporary_directory: Path,
    python_executable: str,
    prepared_path: Path,
) -> ScaleBenchmarkResult:
    worker_config = config.to_payload()
    worker_config["runner"] = runner
    worker_config["prepared_dag_file"] = str(prepared_path.resolve())
    config_path = temporary_directory / f"{runner}.config.json"
    result_path = temporary_directory / f"{runner}.result.json"
    _atomic_write_json(config_path, worker_config)

    command = [
        python_executable,
        "-m",
        "agentx.benchmark.scale_benchmark",
        "_worker",
        "--config",
        str(config_path),
        "--output",
        str(result_path),
    ]
    environment = os.environ.copy()
    source_root = str(Path(__file__).resolve().parents[2])
    existing_pythonpath = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        source_root
        if not existing_pythonpath
        else os.pathsep.join((source_root, existing_pythonpath))
    )
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=config.worker_timeout_s,
            check=False,
            env=environment,
        )
    except subprocess.TimeoutExpired as exc:
        raise ScaleBenchmarkError(
            f"{runner} worker exceeded {config.worker_timeout_s} seconds"
        ) from exc
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "no output"
        raise ScaleBenchmarkError(
            f"{runner} worker exited with code {completed.returncode}: {detail}"
        )
    if not result_path.is_file():
        raise ScaleBenchmarkError(f"{runner} worker did not produce a result file")
    try:
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        return ScaleBenchmarkResult(**payload)
    except (OSError, TypeError, ValueError) as exc:
        raise ScaleBenchmarkError(f"{runner} worker produced an invalid result") from exc


def _worker(config_path: Path, output_path: Path) -> None:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    runner = str(config["runner"])
    prepared_dag_file = config.get("prepared_dag_file")
    if prepared_dag_file is not None:
        source = _iter_prepared_dag_stream(Path(prepared_dag_file))
    else:
        source = iter_sim_dags(
            config["dag_file"],
            limit=config.get("limit"),
            resource_estimate_file=config.get("resource_estimate_file"),
            chunk_size=int(config["chunk_size"]),
            validate_unique_ids=bool(config["validate_unique_ids"]),
        )

    tracemalloc.start()
    started = time.perf_counter()
    result = _run_method(runner, source, config)
    host_wall_clock_s = time.perf_counter() - started
    _, python_peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    peak_rss_bytes, peak_rss_source = _peak_rss(python_peak_bytes)

    safe_wall_clock = max(host_wall_clock_s, 1e-9)
    payload = ScaleBenchmarkResult(
        runner=runner,
        policy_id=result.policy_id,
        worker_pid=os.getpid(),
        host_wall_clock_s=_rounded(host_wall_clock_s),
        peak_rss_mb=_bytes_to_mb(peak_rss_bytes),
        peak_rss_source=peak_rss_source,
        python_peak_allocated_mb=_bytes_to_mb(python_peak_bytes),
        completed_dags=result.completed_dags,
        completed_nodes=result.completed_nodes,
        completed_tokens=result.completed_tokens,
        peak_active_dags=result.peak_active_dags,
        host_dags_per_second=_rounded(result.completed_dags / safe_wall_clock),
        host_nodes_per_second=_rounded(result.completed_nodes / safe_wall_clock),
        host_tokens_per_second=_rounded(result.completed_tokens / safe_wall_clock),
        simulated_makespan_s=_rounded(result.wall_clock_s),
        trace_event_count=int(result.trace_summary.get("event_count", 0)),
    )
    _atomic_write_json(output_path, payload.to_payload())


def _prepare_dag_stream(
    config: ScaleBenchmarkConfig,
    output_path: Path,
) -> dict[str, Any]:
    """Validate and serialize the shared input once for all isolated workers."""
    started = time.perf_counter()
    temporary_path = _temporary_path(output_path)
    dag_count = 0
    node_count = 0
    batch: list[SimDAG] = []
    batch_nodes = 0
    try:
        source = iter_sim_dags(
            config.dag_file,
            limit=config.limit,
            resource_estimate_file=config.resource_estimate_file,
            chunk_size=config.chunk_size,
            validate_unique_ids=config.validate_unique_ids,
        )
        with temporary_path.open("xb") as stream:
            stream.write(_PREPARED_STREAM_MAGIC)
            for dag in source:
                dag_nodes = len(dag.nodes)
                if batch and (
                    len(batch) >= _PREPARED_BATCH_DAGS
                    or batch_nodes + dag_nodes > _PREPARED_BATCH_NODES
                ):
                    pickle.dump(
                        (_PREPARED_BATCH_TAG, batch),
                        stream,
                        protocol=pickle.HIGHEST_PROTOCOL,
                    )
                    batch = []
                    batch_nodes = 0
                batch.append(dag)
                batch_nodes += dag_nodes
                dag_count += 1
                node_count += dag_nodes
            if batch:
                pickle.dump(
                    (_PREPARED_BATCH_TAG, batch),
                    stream,
                    protocol=pickle.HIGHEST_PROTOCOL,
                )
            pickle.dump(
                (
                    _PREPARED_END_TAG,
                    {"dag_count": dag_count, "node_count": node_count},
                ),
                stream,
                protocol=pickle.HIGHEST_PROTOCOL,
            )
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, output_path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise

    return {
        "format": PREPARED_STREAM_FORMAT,
        "host_wall_clock_s": _rounded(time.perf_counter() - started),
        "dag_count": dag_count,
        "node_count": node_count,
        "size_bytes": output_path.stat().st_size,
    }


def _iter_prepared_dag_stream(path: Path) -> Iterator[SimDAG]:
    """Read the trusted temporary stream produced by ``_prepare_dag_stream``."""
    with path.open("rb") as stream:
        magic = stream.read(len(_PREPARED_STREAM_MAGIC))
        if magic != _PREPARED_STREAM_MAGIC:
            raise ValueError("prepared DAG stream has an invalid format header")

        observed_dags = 0
        observed_nodes = 0
        while True:
            try:
                tag, payload = pickle.load(stream)
            except EOFError as exc:
                raise ValueError("prepared DAG stream ended before its trailer") from exc
            except (pickle.PickleError, TypeError, ValueError) as exc:
                raise ValueError("prepared DAG stream contains an invalid record") from exc

            if tag == _PREPARED_BATCH_TAG:
                if not isinstance(payload, list) or not payload:
                    raise ValueError("prepared DAG stream contains an invalid batch")
                for dag in payload:
                    if not isinstance(dag, SimDAG):
                        raise ValueError(
                            "prepared DAG stream contains a non-DAG record"
                        )
                    observed_dags += 1
                    observed_nodes += len(dag.nodes)
                    yield dag
                continue
            if tag != _PREPARED_END_TAG or not isinstance(payload, dict):
                raise ValueError("prepared DAG stream contains an unknown record")
            if payload != {
                "dag_count": observed_dags,
                "node_count": observed_nodes,
            }:
                raise ValueError("prepared DAG stream trailer does not match its records")
            if stream.read(1):
                raise ValueError("prepared DAG stream has trailing content")
            return


def _run_method(runner: str, source: Any, config: dict[str, Any]) -> DiscreteResult:
    common = {
        "resource_count": int(config["resource_count"]),
        "gpu_count": int(config["gpu_count"]),
        "cpu_count": int(config["cpu_count"]),
        "trace_mode": "summary",
        "check_invariants": bool(config["check_invariants"]),
        "validate_input": False,
    }
    if runner == "sequential":
        return run_sequential(source, **common)
    if runner == "parallel":
        return run_parallel(
            source,
            active_limit=config.get("parallel_active_limit"),
            **common,
        )
    if runner == "agentx":
        return run_agentx(
            source,
            active_limit=int(config["max_active_dags"]),
            **common,
        )
    raise ValueError(f"unsupported runner: {runner}")


def _peak_rss(fallback_bytes: int = 0) -> tuple[int, str]:
    if sys.platform == "win32":
        return _windows_peak_rss(), "windows.PeakWorkingSetSize"
    try:
        import resource
    except ImportError:
        return fallback_bytes, "tracemalloc-fallback"
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    if sys.platform != "darwin":
        value *= 1024
    return value, "resource.ru_maxrss"


def _windows_peak_rss() -> int:
    import ctypes
    from ctypes import wintypes

    class ProcessMemoryCounters(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    kernel32.GetCurrentProcess.argtypes = []
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    psapi.GetProcessMemoryInfo.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(ProcessMemoryCounters),
        wintypes.DWORD,
    ]
    psapi.GetProcessMemoryInfo.restype = wintypes.BOOL

    counters = ProcessMemoryCounters()
    counters.cb = ctypes.sizeof(counters)
    process = kernel32.GetCurrentProcess()
    succeeded = psapi.GetProcessMemoryInfo(
        process,
        ctypes.byref(counters),
        counters.cb,
    )
    if not succeeded:
        raise OSError(ctypes.get_last_error(), "GetProcessMemoryInfo failed")
    return int(counters.PeakWorkingSetSize)


def _load_baseline(
    path: str | Path | None,
    runners: Sequence[str],
    *,
    expected_input: dict[str, Any],
    expected_configuration: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    if path is None:
        return {}
    baseline_path = Path(path)
    try:
        payload = json.loads(baseline_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read baseline file: {baseline_path}") from exc
    if not isinstance(payload, dict):
        raise ValueError("baseline must contain a JSON object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(
            f"baseline must use schema_version {SCHEMA_VERSION!r}"
        )
    raw_results = payload.get("results")
    if not isinstance(raw_results, list):
        raise ValueError("baseline results must be an array")
    indexed: dict[str, dict[str, Any]] = {}
    for row in raw_results:
        if not isinstance(row, dict) or not row.get("runner"):
            raise ValueError("each baseline result must identify its runner")
        runner = str(row["runner"])
        if runner in indexed:
            raise ValueError(f"baseline contains duplicate runner: {runner}")
        indexed[runner] = row
    missing = [runner for runner in runners if runner not in indexed]
    if missing:
        raise ValueError(f"baseline is missing runners: {', '.join(missing)}")
    _validate_baseline_compatibility(
        payload,
        expected_input=expected_input,
        expected_configuration=expected_configuration,
    )
    return indexed


def _validate_baseline_compatibility(
    baseline: dict[str, Any],
    *,
    expected_input: dict[str, Any],
    expected_configuration: dict[str, Any],
) -> None:
    baseline_input = baseline.get("input")
    if not isinstance(baseline_input, dict):
        raise ValueError("baseline is missing its input manifest")
    for input_name in ("dag_file", "resource_estimate_file"):
        expected_manifest = expected_input.get(input_name)
        baseline_manifest = baseline_input.get(input_name)
        if expected_manifest is None and baseline_manifest is None:
            continue
        if not isinstance(expected_manifest, dict) or not isinstance(
            baseline_manifest, dict
        ):
            raise ValueError(f"baseline {input_name} does not match the current run")
        if baseline_manifest.get("sha256") != expected_manifest.get("sha256"):
            raise ValueError(
                f"baseline {input_name} SHA-256 does not match the current run"
            )

    baseline_configuration = baseline.get("configuration")
    if not isinstance(baseline_configuration, dict):
        raise ValueError("baseline is missing its benchmark configuration")
    comparable_fields = (
        "gpu_count",
        "cpu_count",
        "max_active_dags",
        "parallel_active_limit",
        "limit",
        "chunk_size",
        "validate_unique_ids",
        "check_invariants",
    )
    mismatches = [
        name
        for name in comparable_fields
        if baseline_configuration.get(name) != expected_configuration.get(name)
    ]
    if mismatches:
        raise ValueError(
            "baseline configuration does not match the current run: "
            + ", ".join(mismatches)
        )


def _append_violation(
    violations: list[dict[str, Any]],
    *,
    runner: str,
    metric: str,
    actual: float,
    limit: float,
    direction: str,
    kind: str,
    baseline: float | None = None,
    ratio: float | None = None,
) -> None:
    failed = actual > limit if direction == "max" else actual < limit
    if not failed:
        return
    violation = {
        "kind": kind,
        "runner": runner,
        "metric": metric,
        "comparison": "<=" if direction == "max" else ">=",
        "actual": _rounded(actual),
        "limit": _rounded(limit),
    }
    if baseline is not None:
        violation["baseline"] = _rounded(baseline)
    if ratio is not None:
        violation["allowed_ratio"] = ratio
    violations.append(violation)


def _input_manifest(path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    digest = hashlib.sha256()
    try:
        with resolved.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise ValueError(f"cannot read DAG input: {resolved}") from exc
    return {
        "path": str(resolved),
        "size_bytes": resolved.stat().st_size,
        "sha256": digest.hexdigest(),
    }


def _validate_config(config: ScaleBenchmarkConfig) -> None:
    if not config.runners:
        raise ValueError("at least one runner is required")
    unknown = [runner for runner in config.runners if runner not in RUNNERS]
    if unknown:
        raise ValueError(f"unsupported runners: {', '.join(unknown)}")
    if len(set(config.runners)) != len(config.runners):
        raise ValueError("runners must not contain duplicates")
    for name, value in (
        ("gpu_count", config.gpu_count),
        ("cpu_count", config.cpu_count),
        ("max_active_dags", config.max_active_dags),
        ("chunk_size", config.chunk_size),
    ):
        _positive_int(name, value)
    if config.limit is not None:
        _positive_int("limit", config.limit)
    if config.parallel_active_limit is not None:
        _positive_int("parallel_active_limit", config.parallel_active_limit)
        if config.parallel_active_limit > max(config.gpu_count, config.cpu_count):
            raise ValueError(
                "parallel_active_limit cannot exceed max(gpu_count, cpu_count) "
                "for the task-lifetime parallel baseline"
            )
    if config.worker_timeout_s is not None and (
        not math.isfinite(config.worker_timeout_s) or config.worker_timeout_s <= 0
    ):
        raise ValueError("worker_timeout_s must be finite and positive")
    if not isinstance(config.validate_unique_ids, bool):
        raise ValueError("validate_unique_ids must be a boolean")
    if not isinstance(config.check_invariants, bool):
        raise ValueError("check_invariants must be a boolean")
    if config.resource_estimate_file is not None:
        if not Path(config.resource_estimate_file).is_file():
            raise ValueError(
                f"resource estimate file does not exist: {config.resource_estimate_file}"
            )


def _validate_thresholds(thresholds: ThresholdConfig) -> None:
    for name, value in (
        ("max_runtime_s", thresholds.max_runtime_s),
        ("max_peak_rss_mb", thresholds.max_peak_rss_mb),
        ("max_python_peak_mb", thresholds.max_python_peak_mb),
        ("min_nodes_per_second", thresholds.min_nodes_per_second),
    ):
        if value is not None and (not math.isfinite(value) or value < 0):
            raise ValueError(f"{name} must be finite and non-negative")
    for name, value in (
        ("max_runtime_ratio", thresholds.max_runtime_ratio),
        ("max_peak_rss_ratio", thresholds.max_peak_rss_ratio),
        ("min_nodes_per_second_ratio", thresholds.min_nodes_per_second_ratio),
    ):
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be finite and positive")


def _positive_int(name: str, value: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = _temporary_path(path)
    try:
        with temporary_path.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def _atomic_write_csv(
    path: Path,
    results: Sequence[ScaleBenchmarkResult],
    gate: dict[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    failures_by_runner: dict[str, list[str]] = {}
    for violation in gate["violations"]:
        failures_by_runner.setdefault(violation["runner"], []).append(
            str(violation["metric"])
        )
    rows = []
    for result in results:
        row = result.to_payload()
        failed_metrics = failures_by_runner.get(result.runner, [])
        row["gate_passed"] = not failed_metrics
        row["failed_metrics"] = ";".join(failed_metrics)
        rows.append(row)
    fieldnames = list(rows[0]) if rows else []
    temporary_path = _temporary_path(path)
    try:
        with temporary_path.open("x", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def _temporary_path(path: Path) -> Path:
    return path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")


def _bytes_to_mb(value: int) -> float:
    return round(value / (1024 * 1024), 6)


def _rounded(value: float) -> float:
    return round(float(value), 9)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Measure host time and memory for large AgentX DAG simulations."
    )
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--dag-file", type=Path)
    inputs.add_argument(
        "--generate-jsonl",
        type=Path,
        metavar="PATH",
        help="Generate a deterministic JSONL dataset at PATH before benchmarking.",
    )
    parser.add_argument("--dag-count", type=int)
    parser.add_argument("--nodes-per-dag", type=int, default=6)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--runners",
        nargs="+",
        choices=RUNNERS,
        default=list(DEFAULT_RUNNERS),
    )
    parser.add_argument("--gpu-count", type=int, default=4)
    parser.add_argument("--cpu-count", type=int, default=4)
    parser.add_argument(
        "--max-active-dags",
        type=int,
        default=DEFAULT_AGENTX_ACTIVE_DAGS,
    )
    parser.add_argument("--parallel-active-limit", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--resource-estimate-file", type=Path)
    parser.add_argument("--chunk-size", type=int, default=1024 * 1024)
    parser.add_argument("--validate-unique-ids", action="store_true")
    parser.add_argument("--check-invariants", action="store_true")
    parser.add_argument("--worker-timeout-s", type=float)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--max-runtime-s", type=float)
    parser.add_argument("--max-rss-mb", type=float)
    parser.add_argument("--max-python-peak-mb", type=float)
    parser.add_argument("--min-nodes-per-second", type=float)
    parser.add_argument("--max-runtime-ratio", type=float, default=1.2)
    parser.add_argument("--max-rss-ratio", type=float, default=1.2)
    parser.add_argument("--min-nodes-per-second-ratio", type=float, default=0.8)
    return parser


def cli_main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.generate_jsonl is not None:
            if args.dag_count is None:
                parser.error("--dag-count is required with --generate-jsonl")
            generated = generate_scale_dataset(
                args.generate_jsonl,
                args.dag_count,
                nodes_per_dag=args.nodes_per_dag,
            )
            dag_file = args.generate_jsonl
            print(
                f"generated {generated['dag_count']} DAGs / "
                f"{generated['node_count']} nodes at {dag_file}",
                flush=True,
            )
        else:
            if args.dag_count is not None:
                parser.error("--dag-count is only valid with --generate-jsonl")
            dag_file = args.dag_file

        config = ScaleBenchmarkConfig(
            dag_file=dag_file,
            runners=tuple(args.runners),
            gpu_count=args.gpu_count,
            cpu_count=args.cpu_count,
            max_active_dags=args.max_active_dags,
            parallel_active_limit=args.parallel_active_limit,
            limit=args.limit,
            resource_estimate_file=args.resource_estimate_file,
            chunk_size=args.chunk_size,
            validate_unique_ids=args.validate_unique_ids,
            check_invariants=args.check_invariants,
            worker_timeout_s=args.worker_timeout_s,
        )
        thresholds = ThresholdConfig(
            max_runtime_s=args.max_runtime_s,
            max_peak_rss_mb=args.max_rss_mb,
            max_python_peak_mb=args.max_python_peak_mb,
            min_nodes_per_second=args.min_nodes_per_second,
            max_runtime_ratio=args.max_runtime_ratio,
            max_peak_rss_ratio=args.max_rss_ratio,
            min_nodes_per_second_ratio=args.min_nodes_per_second_ratio,
        )
        report = run_scale_benchmark(
            config,
            args.output_dir,
            thresholds=thresholds,
            baseline_path=args.baseline,
        )
    except (OSError, ScaleBenchmarkError, ValueError) as exc:
        print(f"scale benchmark failed: {exc}", file=sys.stderr)
        return 2

    for result in report["results"]:
        print(
            f"{result['runner']}: {result['completed_dags']} DAGs, "
            f"{result['host_nodes_per_second']:.2f} nodes/s, "
            f"{result['peak_rss_mb']:.2f} MiB RSS, "
            f"{result['host_wall_clock_s']:.3f} s",
            flush=True,
        )
    gate = report["gate"]
    if gate["passed"]:
        print("scale benchmark gate: PASS", flush=True)
        return 0
    print("scale benchmark gate: FAIL", file=sys.stderr)
    for violation in gate["violations"]:
        print(
            f"  {violation['runner']}.{violation['metric']}: "
            f"{violation['actual']} {violation['comparison']} {violation['limit']}",
            file=sys.stderr,
        )
    return 1


def _worker_main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    _worker(args.config, args.output)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == "_worker":
        return _worker_main(arguments[1:])
    return cli_main(arguments)


if __name__ == "__main__":
    raise SystemExit(main())
