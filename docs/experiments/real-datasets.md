# Experiments

The [ARGB benchmark repository](https://github.com/wenbo-project/ARGB) owns the paper datasets, discrete-event simulator, baselines, and evaluation scripts. This system repository provides the runtime and a [Quick Start](../quickstart.md) launcher for one fixed-resource demonstration.

## Real-task workloads

ARGB-R contains DAGs derived from StrategyQA, MuSiQue, ToolBench, and ALFWorld. Each DAG node represents CPU or GPU work, and edges encode dependencies. The experiment uses unit execution cost per node and virtual time; it does not run model inference or require physical GPUs.

The comparison follows the paper's task-level baseline: Flash-Searcher reserves one CPU/GPU pair for a DAG's lifetime. Wenbo schedules ready nodes from different DAGs on shared resources and releases a resource after each node. Both methods process the same DAGs and task order.

## Run the demonstration

After [Quick Start setup](../quickstart.md), run from the system repository:

```bash
python -u examples/quickstart.py --benchmark-root ../benchmark
```

This launcher uses two GPU slots and sixteen CPU slots across all four datasets. The benchmark may label Flash-Searcher as `parallel` in raw output. Reports are saved under `.local/quickstart-gpu2-cpu16/`; `summary.csv` contains per-method summaries, and `manifest.json` records configuration and completion status.

The launcher uses the benchmark's `quickstart` profile. Its full-batch admission for Wenbo is an implementation choice for this finite-batch demonstration; the paper does not specify a shared active window. The paper's unit-node costs and lifetime-held baseline pair are retained.

## Full evaluation

The fixed-resource demonstration does not reproduce every paper figure. From the separate ARGB checkout, use its `table` and `paper` profiles for broader resource sweeps:

```bash
python -u examples/paper_real_datasets.py --profile table
python -u examples/paper_real_datasets.py --profile paper
```

Synthetic workloads and priority experiments have separate benchmark entry points. See the [ARGB evaluation protocol](https://github.com/wenbo-project/ARGB/blob/main/docs/evaluation.md) for inputs, options, validation, and exact reproduction commands.
