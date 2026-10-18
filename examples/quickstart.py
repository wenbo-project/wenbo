"""Launch the independent benchmark's real-task paper experiment.

Run from the system checkout:
    python -u examples/quickstart.py --benchmark-root ../benchmark

The benchmark owns datasets, unit-cost paper protocol, the discrete-event simulator and
result validation. Its quickstart profile fixes GPU=2, CPU=16, with all four
real datasets and seeds 11, 29, 47 by default.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENTRY_POINT = Path("examples/paper_real_datasets.py")
DEFAULT_OUTPUT_DIR = ROOT / ".local/quickstart-gpu2-cpu16"
DATASETS = ("strategyqa", "musique", "toolbench", "alfworld")


def find_benchmark_root() -> Path | None:
    """Find a local checkout named benchmark, preferring the nearest ancestor."""
    for parent in ROOT.parents:
        candidate = parent / "benchmark"
        if (candidate / ENTRY_POINT).is_file():
            return candidate
    return None


def positive_int(value: str) -> int:
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return result


def main() -> int:
    """Forward documented options to a separate, isolated benchmark process."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--benchmark-root",
        type=Path,
        default=find_benchmark_root(),
        help="Independent benchmark checkout (auto-detects a sibling named benchmark)",
    )
    parser.add_argument(
        "--data-root", type=Path, help="Override the benchmark's datasets directory"
    )
    parser.add_argument("--datasets", nargs="+", choices=DATASETS, default=list(DATASETS))
    parser.add_argument("--seeds", nargs="+", type=int, default=[11, 29, 47])
    parser.add_argument("--limit", type=positive_int, help="Smoke run: first N DAGs per dataset")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Results directory (existing files are overwritten)",
    )
    args = parser.parse_args()
    if len(set(args.seeds)) != len(args.seeds) or len(set(args.datasets)) != len(args.datasets):
        parser.error("datasets and seeds must not contain duplicates")
    if args.benchmark_root is None:
        parser.error(
            "Benchmark checkout not found. Clone https://github.com/wenbo-project/ARGB "
            "and pass --benchmark-root <checkout>."
        )
    benchmark = args.benchmark_root.resolve()
    script = benchmark / ENTRY_POINT
    if not script.is_file():
        parser.error(f"Benchmark entry point not found: {script}")

    # The benchmark script prepends its own src/, preventing an installed system
    # package with the same import name from shadowing its experiment modules.
    command = [
        sys.executable,
        "-I",
        "-X",
        "utf8",
        "-u",
        str(script),
        "--profile",
        "quickstart",
        "--datasets",
        *args.datasets,
        "--seeds",
        *(str(seed) for seed in args.seeds),
        "--output-dir",
        str(args.output_dir.resolve()),
    ]
    if args.data_root is not None:
        command.extend(["--data-root", str(args.data_root.resolve())])
    if args.limit is not None:
        command.extend(["--limit", str(args.limit)])
    print(
        "Wenbo Quick Start | real-task paper experiment | GPU=2, CPU=16\n"
        f"Benchmark: {benchmark}\n"
        "Execution: deterministic discrete-event virtual time\n",
        flush=True,
    )
    try:
        # Inherit stdout/stderr so progress and tables remain live during recording.
        return subprocess.run(command, cwd=benchmark, check=False).returncode
    except KeyboardInterrupt:
        return 130
    except OSError as exc:
        print(f"Could not launch benchmark: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
