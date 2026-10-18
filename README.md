# Wenbo

[![Python](https://img.shields.io/badge/Python-3.10+-3776ab?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)
[![Documentation](https://img.shields.io/badge/Documentation-latest-blue?style=flat-square)](https://wenbo.readthedocs.io/en/latest)

Wenbo is an event-driven scheduler for running multiple DAG-shaped LLM workflows with bounded concurrency. 

Wenbo 是一个面向 LLM 工作流的多 DAG 调度系统。

## Highlights

- Event-driven execution of multiple DAGs with dependency tracking.
- Pluggable FIFO, priority, shortest-job-first, and fair round-robin policies.
- Bounded worker pool with timeout and failure propagation.
- Mock and synthetic executors for runnable examples and experiments.
- Timeline, scheduling-decision, and trace collection for experiments.
- A self-contained quickstart that runs without an API key or model download.

## Quickstart

Requirements: Python 3.10 or newer.

```bash
git clone <your-github-repository-url>
cd wenbo
python -m pip install -e .
python examples/quickstart.py
```

The example schedules the DAG in `examples/dags/quickstart.json` with the mock executor and writes its result to `.local/quickstart/result.json`. A successful run ends with `completed=1, failed=0`.

For complete setup and verification instructions, see the [Getting Started guide](https://wenbo.readthedocs.io/en/latest/getting_started.html).

Try another scheduling policy or simulated latency:

```bash
python examples/quickstart.py --policy priority --mock-mode estimated --base-latency-ms 20
python examples/quickstart.py --help
```

No real LLM is contacted by this example.

## Architecture

```text
DAG input
   │
   ▼
Director / admission control
   │
   ▼
Scheduler ── SchedulingPolicy
   │
   ▼
WorkerPool ── ExecutorBackend
   │
   └──────── EventBus ── State / Timeline / Trace
```

The public code is organized as follows:

| Path                  | Purpose                                                  |
| --------------------- | -------------------------------------------------------- |
| `src/wenbo/dag`       | DAG model, builder, registry, and validation             |
| `src/wenbo/director`  | Admission control                                        |
| `src/wenbo/scheduler` | Ready queue, policies, and scheduling loop               |
| `src/wenbo/executor`  | Worker pool and execution backends                       |
| `src/wenbo/runtime`   | Events, state transitions, prompts, timeline, and traces |
| `src/wenbo/workload`  | Synthetic DAG workload loader                            |
| `examples`            | Minimal runnable example and input DAG                   |

The JSON input format is documented in [docs/dag-schema.md](docs/dag-schema.md).

Benchmark scripts and paper experiments are managed separately. This submission focuses on the scheduler implementation and quickstart.

## Development

Install the development dependencies and run the public smoke checks:

```bash
python -m pip install -e ".[dev]"
python -m ruff check --select E9,F63,F7,F82 src/wenbo
python -m ruff check examples/quickstart.py tests/test_quickstart.py
python -m pytest -q tests/test_quickstart.py
```

The GitHub Actions workflow runs the same checks on Python 3.10 and 3.12.

## Scope of the public repository

Generated datasets, experiment outputs, paper drafts, local archives, real-model adapters, model files, and credentials are excluded from version control. This keeps the competition submission focused on the implementation and its reproducible example.

## License

Released under the [MIT License](LICENSE).
