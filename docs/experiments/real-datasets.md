# Separate Benchmark and Real Dataset Experiments

The separate [ARGB benchmark repository](https://github.com/wenbo-project/ARGB) maintains paper experiments. The system's `examples/quickstart.py` launches that repository's `examples/paper_real_datasets.py --profile quickstart` through an isolated subprocess.

## Repository responsibilities

| Content | Wenbo system repository | Separate ARGB benchmark repository |
| --- | --- | --- |
| DAG model, asynchronous Scheduler, workers, and backend protocol | Maintains | Uses the experiment-specific version |
| Quick Start | Discovers checkout, forwards arguments and exit codes, and displays subprocess output | Executes paper experiments |
| Real dataset conversion, frozen DAGs, resource costs, and manifests | Does not distribute | Maintains |
| Discrete-event simulator, baselines, and experiment metrics | Does not distribute | Maintains |
| Experiment configurations, input validation, and result files | Invokes the companion entry point | Maintains |


The system Quick Start does not import the system's `wenbo` package or copy the simulator into the system distribution.

## Four real datasets and the protocol

Inputs for StrategyQA, MuSiQue, ToolBench, and ALFWorld retain DAG structures derived from real tasks. The paper entry point assigns every node a duration of 1; frozen resource costs are used only for integrity checks. The discrete-event experiment advances virtual time using unit costs and performs no model inference or interaction with the original environments.

See [Quick Start](../quickstart.md) for the fixed-resource demonstration, available arguments, and output fields:

```bash
python -u examples/quickstart.py --benchmark-root ../benchmark
```

This entry point fixes GPU=2, CPU=16, and node duration=`1`, with seeds `11, 29, 47`. By default it runs all four datasets in full, totaling 24 simulations. Both methods use the same DAGs, unit costs, and task order shuffled with the same seed. Every CPU/GPU node takes one virtual time unit. The `parallel_theory_pair` baseline reserves a private CPU/GPU pair at admission and holds it until the entire DAG completes, admitting at most `min(GPU, CPU)` tasks concurrently. Wenbo admits all selected DAGs into its global ready queue and releases resources after each node completes. Full-batch admission is this entry point's implementation choice for finite-batch throughput experiments; the paper does not specify a shared active window.

The paper baseline `parallel_theory_pair` uses `theory_private_pair_fifo@1`, while Wenbo uses `global_fifo@1`. Speedups are ratios of paired virtual completion times, aggregated as means and sample standard deviations. The benchmark's [evaluation protocol](https://github.com/wenbo-project/ARGB/blob/main/docs/evaluation.md) and recorded policy versions define the simulation semantics and their correspondence to other paper mechanisms. Running this demonstration does not reproduce every paper mechanism, main table, or figure.

Full inputs must pass manifest SHA-256 checks. ToolBench loads shards in index order; `--limit` is intended only for smoke checks. A default full run finishes with `status=completed` and `completed_runs=24`.

## Other paper configurations

Run these commands in the separate benchmark repository:

```bash
python -u examples/paper_real_datasets.py --profile table
python -u examples/paper_real_datasets.py --profile paper
```

`table` uses four resource configurations for 96 simulations; `paper` uses 21 configurations for 504 simulations. The fixed `2/16` setting is intended for the recorded demonstration and does not replace these sweeps. Priority P99 measurements and synthetic figures require their respective scripts and inputs.

## Environment and versions

The companion benchmark must support `--profile quickstart` and provide all four datasets matching its manifest. The system launcher does not automatically fetch repositories, download data, or rewrite experiment configurations.

Both distributions currently use the Python import name `wenbo`. The launcher uses an isolated subprocess, and the benchmark entry point prioritizes its own `src/`, so the benchmark need not be installed in the system environment. Use separate virtual environments if installing both distributions. The output manifest records hashes of the actual entry point, simulator, and input files to identify the version used.
