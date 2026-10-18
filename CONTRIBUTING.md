# Contributing to Wenbo

Use Python 3.10 or newer and create a virtual environment before installing the development tools:

```bash
python -m pip install -e ".[dev]"
python -m ruff check src examples tests
python -m ruff format --check src examples tests
python -m pytest -q
python -m build
```

Tests include isolated subprocess imports and require the package to be installed. They run without API keys, GPUs, external datasets, or the benchmark checkout. CI runs on Python 3.10 and 3.12.

Before proposing a change, search existing issues and pull requests. For a bug report, include the Python and Wenbo versions, operating system, a minimal reproducer, expected behavior, and the traceback. Remove credentials and private inputs from reports.

Keep pull requests focused. Explain the problem, the resulting behavior, and the checks performed. Add regression tests for behavioral fixes and update the relevant documentation for API or output changes. Preserve Python 3.10 compatibility and follow the existing Ruff rules. The [development guide](docs/development.md) describes docstrings, units, lifecycle boundaries, and documentation conventions.

The system repository owns the runtime, backend interfaces, examples, and system tests. Datasets, the discrete-event simulator, experiment protocols, and paper figures belong in the independent [ARGB benchmark](https://github.com/wenbo-project/ARGB). Describe measured behavior accurately and distinguish simulated resource slots from physical CPU/GPU execution.

Do not commit `.local/`, virtual environments, caches, generated reports, datasets, model files, or secrets. Use Git to select the public files; uploading the whole working directory as an archive can include ignored local artifacts.

Contributions are distributed under this repository's [MIT License](LICENSE).
