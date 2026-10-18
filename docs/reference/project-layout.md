# Project Layout

The tree below starts at the Wenbo system repository root prepared for publication. The Python package uses a `src` layout, and documentation lives in `docs`.

```text
wenbo/
├── README.md                      # System overview, Quick Start, and development checks
├── CONTRIBUTING.md                # Issues, contributions, and validation conventions
├── MANIFEST.in                    # Source distribution file list
├── LICENSE                        # MIT license
├── pyproject.toml                 # Package metadata, build, and development dependencies
├── ruff.toml                      # Lint and formatting rules
├── .pre-commit-config.yaml        # Pre-commit checks
├── .github/workflows/ci.yml       # Continuous integration checks
├── paper/                         # Published manuscript (original filename retained)
├── .readthedocs.yaml               # Read the Docs build configuration
├── src/wenbo/
│   ├── dag/                       # Graph models, construction, registry, and validation
│   ├── director/                  # Input normalization and admission limits
│   ├── scheduler/                 # Scheduling loop, ready queue, candidates, and policies
│   ├── executor/                  # Backend protocol, synthetic backend, and worker pool
│   ├── runtime/                   # Events, state, execution coordination, and tracing
│   └── workload/                  # Runtime JSON workload loader
├── examples/
│   └── quickstart.py              # Launcher for separate benchmark paper experiments
├── tests/
│   ├── conftest.py                # Copy examples for isolated installed-package tests
│   ├── test_package.py            # Installed package imports and package boundaries
│   ├── test_quickstart.py         # Subprocess isolation, argument forwarding, and exit codes
│   └── test_runtime_regressions.py # Graph validation, state, cancellation, and retry regressions
└── docs/
    ├── index.rst                  # Documentation home and navigation
    ├── introduction.rst           # Project introduction
    ├── conf.py                    # Sphinx configuration
    ├── requirements.txt           # Pinned documentation dependencies
    ├── _static/                   # Images and stylesheets
    ├── _templates/                # Theme template overrides
    ├── Makefile                   # POSIX Sphinx build wrapper
    ├── make.bat                   # Windows Sphinx build wrapper
    ├── quickstart.md              # Installation, result tables, and example arguments
    ├── architecture.md            # Runtime responsibilities and lifecycle
    ├── dag-schema.md              # Runtime JSON format
    ├── user-guide/
    │   ├── workflows.md           # Graph construction, execution, retries, and cancellation
    │   ├── scheduling.md          # Policies and extension interfaces
    │   ├── execution.md           # Backends and worker resource matching
    │   └── observability.md       # Timelines, events, and tracing
    ├── experiments/
    │   └── real-datasets.md       # Separate benchmark and reproduction entry points
    ├── reference/
    │   ├── api.md                 # Python interfaces and source index
    │   └── project-layout.md      # This page
    └── development.md             # Development checks and documentation conventions
```

## Where to start reading the source

| Topic | Suggested entry point |
| --- | --- |
| Run paper experiments on real tasks | `examples/quickstart.py` |
| Describe nodes and dependencies | `dag/model.py`, `dag/builder.py` |
| Check graph admission requirements | `director/director.py`, `dag/validator.py` |
| Understand a scheduling round | `scheduler/scheduler.py` → `candidate.py` → `policy.py` |
| Understand resource selection | `executor/worker.py` |
| Integrate an execution backend | `executor/base.py`, `runtime/coordinator.py` |
| Follow retries and dependency resolution | `runtime/state.py` |
| Export events and results | `runtime/timeline.py`, `runtime/trace.py` |

Except for examples, these source files are under `src/wenbo/`. See the [API index](api.md) for source links and import paths.

## Examples and benchmark scope

The official entry point, `examples/quickstart.py`, invokes the separate benchmark's paper entry point. Defaults are four real datasets, GPU=2 / CPU=16, and three seeds, totaling 24 simulations.

The separate [ARGB repository](https://github.com/wenbo-project/ARGB) maintains paper datasets, the discrete-event simulator, baselines, experiment configurations, and figures. See [Experiments](../experiments/real-datasets.md).

## Local artifacts

`.local/` holds example results and maintenance artifacts and is excluded by `.gitignore`. Virtual environments (`.venv/`), build output, caches, external datasets, paper drafts, and model files are also excluded from public source contents. The manuscript intentionally published in `paper/` is a separate exception.

Paper experiment results default to `.local/quickstart-gpu2-cpu16/`. Use different `--output-dir` values for separate runs. Reusing a path overwrites results with the same names; interrupted experiments may leave partial reports. Do not copy datasets into `src/` or generated results into documentation source directories.
