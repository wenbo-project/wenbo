"""Check the cross-repository launcher without bundling benchmark code or data."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.fixture
def benchmark_checkout(tmp_path: Path) -> Path:
    """A protocol probe records subprocess arguments, paths and isolation."""
    root = tmp_path / "benchmark with spaces"
    entry = root / "examples/paper_real_datasets.py"
    entry.parent.mkdir(parents=True)
    entry.write_text(
        "import json, sys\n"
        "from pathlib import Path\n"
        "output = Path(sys.argv[sys.argv.index('--output-dir') + 1])\n"
        "output.mkdir(parents=True, exist_ok=True)\n"
        "(output / 'forwarded.json').write_text(json.dumps({\n"
        "    'args': sys.argv[1:], 'cwd': str(Path.cwd()),\n"
        "    'isolated': sys.flags.isolated, 'utf8': sys.flags.utf8_mode,\n"
        "}), encoding='utf-8')\n"
        "print('BENCHMARK TERMINAL OUTPUT')\n",
        encoding="utf-8",
    )
    return root


def run_launcher(examples: Path, cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-I", "-X", "utf8", str(examples / "quickstart.py"), *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
    )


def test_default_protocol_and_live_output(
    installed_examples: Path, benchmark_checkout: Path, tmp_path: Path
) -> None:
    output = tmp_path / "results"
    completed = run_launcher(
        installed_examples,
        tmp_path,
        "--benchmark-root",
        str(benchmark_checkout),
        "--output-dir",
        str(output),
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    report = json.loads((output / "forwarded.json").read_text(encoding="utf-8"))
    assert report["args"] == [
        "--profile",
        "quickstart",
        "--datasets",
        "strategyqa",
        "musique",
        "toolbench",
        "alfworld",
        "--seeds",
        "11",
        "29",
        "47",
        "--output-dir",
        str(output),
    ]
    assert Path(report["cwd"]) == benchmark_checkout
    assert report["isolated"] == report["utf8"] == 1
    assert "BENCHMARK TERMINAL OUTPUT" in completed.stdout
    assert "GPU=2, CPU=16" in completed.stdout


def test_sibling_discovery_and_relative_options_survive_child_cwd(
    installed_examples: Path, benchmark_checkout: Path, tmp_path: Path
) -> None:
    sibling = installed_examples.parent.parent / "benchmark"
    benchmark_checkout.rename(sibling)
    completed = run_launcher(
        installed_examples,
        tmp_path,
        "--data-root",
        "custom data",
        "--datasets",
        "toolbench",
        "--seeds",
        "29",
        "--limit",
        "3",
        "--output-dir",
        "relative output",
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    report = json.loads((tmp_path / "relative output/forwarded.json").read_text(encoding="utf-8"))
    assert report["args"] == [
        "--profile",
        "quickstart",
        "--datasets",
        "toolbench",
        "--seeds",
        "29",
        "--output-dir",
        str(tmp_path / "relative output"),
        "--data-root",
        str(tmp_path / "custom data"),
        "--limit",
        "3",
    ]
    assert Path(report["cwd"]) == sibling


def test_benchmark_failure_is_propagated(
    installed_examples: Path, benchmark_checkout: Path, tmp_path: Path
) -> None:
    (benchmark_checkout / "examples/paper_real_datasets.py").write_text(
        "import sys\nprint('input checksum mismatch', file=sys.stderr)\nraise SystemExit(7)\n",
        encoding="utf-8",
    )
    completed = run_launcher(
        installed_examples, tmp_path, "--benchmark-root", str(benchmark_checkout)
    )
    assert completed.returncode == 7
    assert "input checksum mismatch" in completed.stderr


def test_missing_entry_reports_actionable_error(installed_examples: Path, tmp_path: Path) -> None:
    completed = run_launcher(installed_examples, tmp_path, "--benchmark-root", "missing checkout")
    assert completed.returncode == 2
    assert "Benchmark entry point not found" in completed.stderr


@pytest.mark.parametrize(
    "args",
    [
        ["--limit", "0"],
        ["--datasets", "strategyqa", "strategyqa"],
        ["--seeds", "11", "11"],
        ["--profile", "paper"],
    ],
)
def test_invalid_options_do_not_launch_or_replace_results(
    installed_examples: Path, benchmark_checkout: Path, tmp_path: Path, args: list[str]
) -> None:
    output = tmp_path / "results"
    output.mkdir()
    previous = output / "forwarded.json"
    previous.write_text("previous result", encoding="utf-8")
    completed = run_launcher(
        installed_examples,
        tmp_path,
        "--benchmark-root",
        str(benchmark_checkout),
        "--output-dir",
        str(output),
        *args,
    )
    assert completed.returncode == 2
    assert previous.read_text(encoding="utf-8") == "previous result"


def test_help_needs_no_benchmark(installed_examples: Path, tmp_path: Path) -> None:
    completed = run_launcher(installed_examples, tmp_path, "--help")
    assert completed.returncode == 0
    assert "--benchmark-root" in completed.stdout
