"""Run a self-contained AgentX scheduling example with MockBackend.

Example:
    python examples/quickstart.py

"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

from agentx.dag.model import DAGState
from agentx.dag.registry import DAGRegistry
from agentx.executor.mock_backend import MockBackend, MockMode
from agentx.runtime.event import EventBus
from agentx.runtime.state import DAGStateTracker
from agentx.scheduler import Scheduler
from agentx.scheduler.policy import (
    FairRoundRobinPolicy,
    FIFOPolicy,
    PriorityPolicy,
    ShortestJobFirstPolicy,
)
from agentx.workload.synthetic_loader import SyntheticDAGLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("agentx-quickstart")

DEFAULT_DAG_FILE = Path(__file__).resolve().parent / "dags" / "quickstart.json"
DEFAULT_OUTPUT_FILE = Path(".local") / "quickstart" / "result.json"
ALL_POLICIES = ["fifo", "priority", "fair_rr", "sjf"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the AgentX quickstart with MockBackend")
    parser.add_argument(
        "--dag-file",
        type=Path,
        default=DEFAULT_DAG_FILE,
        help="Path to synthetic DAG JSON file",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=DEFAULT_OUTPUT_FILE,
        help="Path to save results JSON",
    )
    parser.add_argument("--policy", choices=ALL_POLICIES, default="fifo", help="Scheduling policy")
    parser.add_argument("--max-dags", type=int, default=1, help="Maximum DAGs to load")
    parser.add_argument("--max-concurrent", type=int, default=8, help="Maximum concurrent running nodes")
    parser.add_argument(
        "--mock-mode",
        choices=[mode.value for mode in MockMode],
        default=MockMode.INSTANT.value,
        help="Mock backend latency mode",
    )
    parser.add_argument("--base-latency-ms", type=float, default=10.0, help="Base latency for MockBackend")
    parser.add_argument("--random-range-ms", type=float, default=5.0, help="Random range for RANDOM mode")
    parser.add_argument("--debounce-ms", type=float, default=5.0, help="Scheduler debounce window")
    parser.add_argument("--heartbeat-ms", type=float, default=50.0, help="Scheduler heartbeat interval")
    parser.add_argument("--node-timeout", type=float, default=30.0, help="Node execution timeout in seconds")
    parser.add_argument("--timeout-s", type=float, default=120.0, help="Overall wait timeout in seconds")
    return parser.parse_args()


def build_policy(name: str):
    if name == "priority":
        return PriorityPolicy()
    if name == "fair_rr":
        return FairRoundRobinPolicy()
    if name == "sjf":
        return ShortestJobFirstPolicy()
    return FIFOPolicy()


async def wait_all_dags(dags_with_spec: list[tuple], timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if all(dag.state in (DAGState.COMPLETED, DAGState.FAILED) for dag, _ in dags_with_spec):
            return
        await asyncio.sleep(0.05)
    raise TimeoutError(f"Not all DAGs finished within {timeout_s}s")


def collect_dag_result(dag, spec, submit_time: float) -> dict[str, Any]:
    nodes = []
    for node_id, node in dag.nodes.items():
        nodes.append({
            "node_id": node_id,
            "node_type": node.node_type.value,
            "state": node.state.value,
            "hardware": node.hardware,
            "estimated_tokens": node.estimated_tokens,
            "resource_requirement": (
                node.resource_requirement.__dict__
                if node.resource_requirement is not None
                else {}
            ),
        })

    return {
        "dag_id": spec.dag_id,
        "dag_type": spec.dag_type,
        "topology": spec.topology,
        "state": dag.state.value,
        "time_s": round(time.monotonic() - submit_time, 3),
        "graph_stats": spec.graph_stats,
        "estimated_cost": spec.estimated_cost,
        "nodes": nodes,
    }


def summarize_timeline(timeline: list[dict[str, Any]], total_elapsed: float, max_concurrent: int) -> dict[str, Any]:
    started: dict[str, float] = {}
    durations = []
    for entry in timeline:
        key = f"{entry['dag_id']}:{entry['node_id']}"
        if entry["event"] == "started":
            started[key] = entry["time"]
        elif entry["event"] == "finished" and key in started:
            durations.append(entry["time"] - started[key])

    sum_node_time = sum(durations)
    return {
        "total_nodes": len(durations),
        "sum_node_exec_s": round(sum_node_time, 3),
        "avg_node_exec_s": round(sum_node_time / max(len(durations), 1), 3),
        "parallelism": round(sum_node_time / max(total_elapsed, 0.001), 3),
        "concurrency_efficiency_pct": round(
            (sum_node_time / max(total_elapsed, 0.001)) / max(max_concurrent, 1) * 100,
            1,
        ),
    }


async def main_async(args: argparse.Namespace) -> None:
    executor = MockBackend(
        mode=MockMode(args.mock_mode),
        base_latency_ms=args.base_latency_ms,
        random_range_ms=args.random_range_ms,
    )
    event_bus = EventBus()
    registry = DAGRegistry()
    state_tracker = DAGStateTracker(event_bus, registry)
    scheduler = Scheduler(
        event_bus=event_bus,
        registry=registry,
        state_tracker=state_tracker,
        executor=executor,
        policy=build_policy(args.policy),
        max_concurrent=args.max_concurrent,
        debounce_ms=args.debounce_ms,
        heartbeat_ms=args.heartbeat_ms,
        node_timeout=args.node_timeout,
    )
    await scheduler.start()

    try:
        loader = SyntheticDAGLoader()
        dags_with_spec = loader.load_from_file(args.dag_file, limit=args.max_dags)
        logger.info(
            "Loaded %d synthetic DAGs from %s | policy=%s | max_concurrent=%d",
            len(dags_with_spec),
            args.dag_file,
            args.policy,
            args.max_concurrent,
        )

        submit_times: dict[str, float] = {}
        total_start = time.monotonic()
        for dag, spec in dags_with_spec:
            submit_times[spec.dag_id] = time.monotonic()
            await scheduler.submit_dag(dag)
            logger.info(
                "Submitted %s | nodes=%d edges=%d",
                spec.dag_id,
                len(dag.nodes),
                len(dag.edges),
            )

        await wait_all_dags(dags_with_spec, timeout_s=args.timeout_s)
        total_elapsed = time.monotonic() - total_start
        logger.info("All synthetic DAGs finished in %.3fs", total_elapsed)

        results = [
            collect_dag_result(dag, spec, submit_times[spec.dag_id])
            for dag, spec in dags_with_spec
        ]
        timing_summary = summarize_timeline(scheduler.timeline, total_elapsed, args.max_concurrent)
        output_data = {
            "metadata": {
                "dag_file": str(args.dag_file),
                "policy": args.policy,
                "mock_mode": args.mock_mode,
                "max_dags": args.max_dags,
                "max_concurrent": args.max_concurrent,
                "total_dags": len(results),
                "completed": sum(1 for item in results if item["state"] == "completed"),
                "failed": sum(1 for item in results if item["state"] == "failed"),
                "total_wall_clock_s": round(total_elapsed, 3),
                **timing_summary,
            },
            "results": results,
            "timeline": scheduler.timeline,
            "decisions": scheduler.decisions,
        }

        output_path = Path(args.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        logger.info("Saved results to %s", output_path)
        print(
            "AgentX quickstart finished: "
            f"completed={output_data['metadata']['completed']}, "
            f"failed={output_data['metadata']['failed']}, "
            f"output={output_path}"
        )
    finally:
        await scheduler.stop()


def main() -> None:
    asyncio.run(main_async(parse_args()))


if __name__ == "__main__":
    main()
