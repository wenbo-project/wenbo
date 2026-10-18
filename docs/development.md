# Development and Documentation

This page describes development checks and documentation conventions. Run commands from the repository root with a virtual environment activated.

## Set up the development environment

Create and activate an environment as described in [Quick Start](quickstart.md), then install development dependencies:

```bash
python -m pip install -e ".[dev]"
```

The core package, Quick Start launcher, and companion paper entry point depend only on the Python standard library. Development extras include pytest, build, and a pinned Ruff version. The separate benchmark repository manages paper experiment dependencies.

## Local checks

```bash
python -m ruff check src examples tests
python -m ruff format --check src examples tests
python -m pytest -q
python -m build
python -I examples/quickstart.py --help
```

Tests cover launcher argument forwarding and failure exits, installed package imports, graph validation, state propagation, cancellation, and retries. They do not require external frozen datasets.

CI installs the package and runs checks on Python 3.10 and 3.12. New syntax and standard library usage must remain compatible with the declared minimum, Python 3.10.

## Build the documentation

Read the Docs builds the Markdown documentation using `.readthedocs.yaml`, `docs/conf.py`, and `docs/requirements.txt`, with the Furo theme.

```bash
python -m pip install -r docs/requirements.txt
python -m sphinx -b html -W --keep-going docs .local/docs-html
```

The entry point is `docs/index.rst`. Update its toctree when adding pages. `docs/_static/` contains images and stylesheets; `docs/_templates/` contains template overrides. Keep generated output under `.local/`, outside the documentation sources.

The convenience wrappers use the same source root: run `make -C docs html` on macOS / Linux or `docs\make.bat html` on Windows. Their HTML output is `.local/docs-build/html/`; CI and the direct command above use `.local/docs-html/`.

## Contributions and release files

See the [contribution guide](https://github.com/wenbo-project/wenbo/blob/main/CONTRIBUTING.md) for issues and pull requests. `MANIFEST.in` defines source distribution contents, including examples, documentation, test fixtures, and check configuration. CI builds the sdist and wheel, installs the built wheel, and runs tests. Publish public files through Git; `.gitignore` does not filter local artifacts when an entire directory is compressed directly.
