# AgentX

<p align="center">
|  <a href="https://wenbo.readthedocs.io/en/latest/introduction.html"><b>Documentation</b></a>  |
</p>

AgentX is an event-driven scheduler for running multiple DAG-shaped LLM workflows with bounded concurrency. 

AgentX 是一个面向 LLM 工作流的多 DAG 调度系统。

## Highlights

- Event-driven execution of multiple DAGs with dependency tracking.
- Pluggable FIFO, priority, shortest-job-first, and fair round-robin policies.
- Bounded worker pool with timeout and failure propagation.
- Mock, synthetic, llama.cpp CLI, and llama.cpp server executors.
- Timeline, scheduling-decision, and trace collection for experiments.
- A self-contained quickstart that runs without an API key or model download.

## Quickstart

Requirements: Python 3.10 or newer.

```bash
git clone <your-github-repository-url>
cd agentx
python -m pip install -e .
python examples/quickstart.py
```

The example schedules the DAG in `examples/dags/quickstart.json` with the mock executor and writes its result to `.local/quickstart/result.json`. A successful run ends with `completed=1, failed=0`.

Try another scheduling policy or simulated latency:

```bash
python examples/quickstart.py --policy priority --mock-mode fixed --base-latency-ms 20
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

| Path | Purpose |
| --- | --- |
| `src/agentx/dag` | DAG model, builder, registry, and validation |
| `src/agentx/director` | Admission control |
| `src/agentx/scheduler` | Ready queue, policies, and scheduling loop |
| `src/agentx/executor` | Worker pool and execution backends |
| `src/agentx/runtime` | Events, state transitions, prompts, timeline, and traces |
| `src/agentx/workload` | Workload loaders and converters |
| `src/agentx/benchmark` | Reproducible simulation and benchmark utilities |
| `examples` | Minimal runnable example and input DAG |

The JSON input format is documented in [docs/dag-schema.md](docs/dag-schema.md).

## Development

Install the development dependencies and run the public smoke checks:

```bash
python -m pip install -e ".[dev]"
python -m ruff check --select E9,F63,F7,F82 src/agentx
python -m ruff check examples/quickstart.py tests/test_quickstart.py
python -m pytest -q tests/test_quickstart.py
```

The GitHub Actions workflow runs the same checks on Python 3.10 and 3.12.

## Using a real model

The quickstart intentionally uses `MockBackend`. The executor package also contains adapters for llama.cpp command-line and server deployments. For the HTTP server adapter, install the optional dependency:

```bash
python -m pip install -e ".[server]"
```

Model processes and weights are not bundled with this repository.

## Scope of the public repository

Generated datasets, experiment outputs, paper drafts, local archives, model files, and credentials are excluded from version control. This keeps the competition submission focused on the implementation and its reproducible example.

## License

Released under the [MIT License](LICENSE).
