# Quick Start

This page runs the paper's real-task experiments on StrategyQA, MuSiQue, ToolBench, and ALFWorld with **GPU=2 and CPU=16**, comparing Flash-Searcher with Wenbo. The system's [quickstart.py](https://github.com/wenbo-project/wenbo/blob/main/examples/quickstart.py) only launches the separate benchmark's paper entry point. The benchmark maintains datasets, the unit-node-cost protocol, simulation, and result validation.

Experiments use discrete-event virtual time and require no API key, model download, or physical GPU. They do not perform real model inference or environment interaction. This page follows the unit-node-cost and lifetime resource-pair baseline protocol in paper §§3.1 and 5.1. See [Experiments](experiments/real-datasets.md) for other resource configurations and full sweeps.

## Prepare the environment and benchmark

Python 3.10 or later is required. Run these commands from the system repository root.

```bash
python -m venv .venv
```

Activate on Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Activate on macOS / Linux:

```bash
source .venv/bin/activate
```

Install the system package and obtain the separate [ARGB repository](https://github.com/wenbo-project/ARGB):

```bash
python -m pip install -e .
git clone https://github.com/wenbo-project/ARGB ../benchmark
```

If a checkout already exists, use its path. The benchmark version must contain `examples/paper_real_datasets.py`, support `--profile quickstart`, and provide a matching `datasets/manifest.json`, four frozen DAG datasets, and resource files. These datasets are not copied into the system source tree.

The launcher executes benchmark source through an isolated subprocess; installing `wenbo-dag-benchmark` in the system environment is unnecessary. Both distributions use the import name `wenbo`, so use a separate virtual environment to install or develop the benchmark. The standalone paper entry point and launcher require only the standard library; installing the system package primarily enables subsequent Python API use.

## Full run and recording

```bash
python -u examples/quickstart.py --benchmark-root ../benchmark
```

`-u` displays progress promptly. By default, all four datasets run without a DAG limit using seeds `11, 29, 47`, for **4 × 3 × 2 = 24 simulations**. GPU/CPU is fixed at `2/16`. Every CPU/GPU node takes one virtual time unit. The Flash-Searcher baseline (`parallel_theory_pair`) reserves a private CPU/GPU pair at admission and holds it until the entire DAG completes, admitting at most `min(GPU, CPU)` tasks concurrently. Wenbo admits all selected DAGs into its global ready queue and releases resources after each node completes. Full-batch admission is this entry point's implementation choice for finite-batch throughput experiments; the paper does not specify a shared active window.

Without `--benchmark-root`, the script searches for a checkout named `benchmark`, starting with the nearest parent directory. Specify the path explicitly for other layouts or multiple checkouts.

The terminal prints two method rows per dataset, followed by the mean speedup and sample standard deviation for each of the four datasets:

| Terminal column | Meaning |
| --- | --- |
| `GPU/CPU` | Simulated resource slots, fixed at `2/16` |
| `Method` | Flash-Searcher or Wenbo; the baseline is recorded as `parallel` in benchmark output |
| `Time(v-s)` | Virtual completion time of all selected tasks, in seconds |
| `Nodes/s`, `DAG/s` | Node and DAG throughput calculated using virtual time |
| `GPU%`, `CPU%`, `All%` | Resource utilization based on node busy time |
| `Speedup` | `T_Flash-Searcher / T_method` for each paired seed, averaged over seeds |

Host elapsed time is displayed separately and must not be confused with `Time(v-s)`. Smoke checks can produce speedups below `1`; results reflect the actual calculation.

## Smoke checks and arguments

First run the first three DAGs from each dataset with one seed:

```bash
python -u examples/quickstart.py --benchmark-root ../benchmark --limit 3 --seeds 11 --output-dir .local/paper-smoke
```

This performs eight small simulations, labeled `SMOKE ONLY` in the terminal. Omit `--limit` for a full run.

```bash
python -u examples/quickstart.py --help
```

| Argument | Default | Meaning |
| --- | --- | --- |
| `--benchmark-root` | Automatically discovered | Root of the separate benchmark checkout |
| `--data-root` | Discovered by the benchmark | Data directory containing `manifest.json`, usually its `datasets/` |
| `--datasets` | All four real datasets | Choices: `strategyqa musique toolbench alfworld` |
| `--seeds` | `11 29 47` | Distinct random seeds for task ordering |
| `--limit` | Unlimited | Read the first N DAGs per dataset; must be a positive integer |
| `--output-dir` | `.local/quickstart-gpu2-cpu16` | Results directory |

The system entry point always invokes the benchmark's `quickstart` profile and exposes no GPU/CPU or runtime policy options. Use other benchmark profiles for resource sweeps.

The default output path is resolved relative to the system repository. Explicit relative paths are resolved against the current command-line directory, then passed to the subprocess as absolute paths. Changing the subprocess working directory therefore does not change input or output locations.

## Save and validate results

The default output directory is `.local/quickstart-gpu2-cpu16/`:

| File | Contents |
| --- | --- |
| `console.log` | Benchmark progress, method tables, and final summary |
| `raw_results.csv` | One simulation result per dataset, seed, and method |
| `summary.csv` | Per-method means and sample standard deviations across seeds |
| `results.json` | Dataset sizes, raw results, and summaries |
| `manifest.json` | Input and code hashes, configuration, policy versions, and completion status |

A successful default full run has exit code `0`, manifest `status=completed`, `completed_runs=24`, resource configuration `[[2,16]]`, and `validation=PASS` for every raw result. Validation checks input SHA-256 hashes (including ToolBench shards), completed DAG/node/token counts, resource work, completion-time lower bounds, and method-specific admission rules.

Reusing a directory overwrites reports with the same names. Use distinct `--output-dir` values to retain multiple recordings or experiments. Failures or interruptions may leave partial results; check both the exit code and completion status.

## Troubleshooting

| Symptom | Action |
| --- | --- |
| Benchmark or experiment entry point not found | Point `--benchmark-root` to a checkout containing `examples/paper_real_datasets.py` |
| `--profile quickstart` not recognized | Use a compatible benchmark version supporting this profile; the launcher does not substitute another experiment |
| Manifest not found | Point `--data-root` to the `datasets/` directory containing the matching frozen inputs |
| Input hash mismatch | Check that data and manifest belong to the same release; do not skip validation |
| Paths contain spaces or Chinese characters | Quote paths; use `python -X utf8 -u` if needed |
| PowerShell cannot activate the environment | Run installation and execution commands directly with `.venv\Scripts\python.exe` |

Argument or entry point errors return `2`. Once the experiment starts, the launcher preserves the benchmark's exit code and does not substitute inputs on failure. Ctrl+C returns `130`.

## Runtime API

See [Building and Running Workflows](user-guide/workflows.md) for graph construction and backend integration.
