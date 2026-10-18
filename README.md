# Wenbo

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776ab?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

[Paper (PDF)](paper/Wenbo%EF%BC%9AScaling%20Agent%20Reasoning%20via%20Dynamic%20DAG%20Scheduling.pdf) · [Documentation](https://wenbo.readthedocs.io/en/latest/)

The latest manuscript is temporarily hosted here while the arXiv version is being updated.

Wenbo is an event-driven scheduler for multiple DAG-shaped LLM workflows with bounded concurrency. It provides dependency tracking, logical resource slots, pluggable scheduling policies, execution backends, and runtime records.

## Features

Wenbo addresses resource fragmentation in large-scale agent serving by dynamically scheduling individual computation nodes across shared GPUs and CPUs, preserving task dependencies while improving resource utilization and throughput.

- **DAG-centric agent execution:** Represent reasoning and computation steps as nodes with explicit dependencies, exposing parallelism within and across agent tasks.
- **Dynamic reasoning graphs:** Incrementally materialize executable DAG flows and refine the reasoning graph as intermediate results become available, as described in the paper design.
- **Global dependency-aware scheduling:** Coordinate ready nodes across active DAGs and dispatch them according to their resource requirements and worker availability.
- **Shared GPU/CPU resources:** Allocate resources to individual computation nodes, allowing operations from different tasks to share heterogeneous resources without reserving a fixed resource pair for each task's lifetime.
- **Decoupled system architecture:** Separate reasoning orchestration (Director), resource scheduling (Scheduler), and node execution (Workers).

## Architecture

![Wenbo architecture: Director, Scheduler, and Workers](docs/_static/wenbo_architecture.png)

Wenbo separates reasoning orchestration, global node scheduling, and heterogeneous execution through the Director, Scheduler, and Workers. 

## System and benchmark

This repository contains the Wenbo workflow runtime. The independent [ARGB repository](https://github.com/wenbo-project/ARGB) maintains the paper datasets, discrete-event simulator, baselines, and experiments. The Quick Start below launches an ARGB experiment in an isolated subprocess.

Both distributions use the import name wenbo. Keep benchmark development in a separate environment; the Quick Start runs its source checkout directly and does not require installing the benchmark distribution.

## Quick start

Requires **Python 3.10 or newer**. Clone the system repository and create a virtual environment:

`ash
git clone https://github.com/wenbo-project/wenbo.git
cd wenbo
python -m venv .venv
`

Activate it on macOS / Linux:

`ash
source .venv/bin/activate
`

Or on Windows PowerShell:

`powershell
.venv\Scripts\Activate.ps1
`

Install Wenbo, obtain the benchmark checkout, and run the experiment:

`ash
python -m pip install -e .
git clone https://github.com/wenbo-project/ARGB ../benchmark
python -u examples/quickstart.py --benchmark-root ../benchmark
`

If you already have either checkout, use its existing directory. The benchmark must include the matching datasets and support --profile quickstart; see the [setup requirements](docs/quickstart.md#prepare-the-environment-and-benchmark).

The default run compares Wenbo with the task-level baseline on StrategyQA, MuSiQue, ToolBench, and ALFWorld, using **2 GPU slots, 16 CPU slots, and three seeds** (24 simulations). It uses discrete-event virtual time and requires no physical GPU, model download, or API key.

The terminal reports throughput, resource utilization, and speedup. Reports are saved to .local/quickstart-gpu2-cpu16/; reusing an output directory overwrites its reports. For a smaller first run:

`ash
python -u examples/quickstart.py --benchmark-root ../benchmark --limit 3 --seeds 11 --output-dir .local/paper-smoke
`

See the [Quick Start guide](docs/quickstart.md) for the experiment protocol, command-line options, result validation, and troubleshooting.

## Documentation

Browse the [online documentation](https://wenbo.readthedocs.io/en/latest/) or use the source guides below.

| Topic | Guide |
| --- | --- |
| Run the four-dataset paper experiment | [Quick start](docs/quickstart.md) |
| Repository responsibilities and experiment protocol | [Paper experiments](docs/experiments/real-datasets.md) |
| Build and run a workflow in Python | [Workflows](docs/user-guide/workflows.md) |
| Built-in runtime policies and extensions | [Scheduling](docs/user-guide/scheduling.md) |
| Backends and logical Workers | [Execution](docs/user-guide/execution.md) |
| JSON workload fields | [DAG format](docs/dag-schema.md) |
| Components and source directories | [Architecture](docs/architecture.md), [project layout](docs/reference/project-layout.md) |


## License

Released under the [MIT License](LICENSE).
