# Quick Start

Run Wenbo's paper workload demonstration against the separate [ARGB benchmark](https://github.com/wenbo-project/ARGB). It compares Wenbo with the Flash-Searcher baseline on four real-task datasets. The run uses virtual time, so no physical GPU, model download, or API key is required.

## Set up

Python 3.10 or newer is required. From the Wenbo repository root, create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

On Windows PowerShell, activate it with `.venv\Scripts\Activate.ps1` instead.

Install Wenbo and clone the benchmark alongside it:

```bash
python -m pip install -e .
git clone https://github.com/wenbo-project/ARGB ../benchmark
```

If you already have the benchmark checkout, use its path in the command below.

## Run

```bash
python -u examples/quickstart.py --benchmark-root ../benchmark
```

The terminal shows completion time, throughput, resource utilization, and speedup for each dataset. Results are saved in `.local/quickstart-gpu2-cpu16/`. The benchmark output may label the Flash-Searcher baseline as `parallel`.

For a short trial run, add `--limit 3 --seeds 11`. Run `python -u examples/quickstart.py --help` for other options.

The [experiment guide](experiments/real-datasets.md) explains the protocol, resource configuration, and full paper evaluations. To construct and execute your own DAG, see [System Usage](user-guide/workflows.md).
