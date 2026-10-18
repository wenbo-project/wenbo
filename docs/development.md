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

## Source documentation conventions

Use English for public module, type, and method docstrings, implementation comments, and documentation. Explain what callers need to know:

- The responsibility of an object or method and the runtime components it works with.
- Nonobvious parameter meanings, time and resource units, defaults, and precedence.
- Return values, possible exceptions, in-place input changes, and other side effects.
- Actual constraints on concurrency, state, cancellation, leases, and output preservation.

Do not repeat parameters already clear from the signature. Implementation comments should explain constraints and reasoning rather than narrate each line, record conversation history, or describe unimplemented features as available behavior.

Clearly distinguish Unix timestamps, monotonic runtime clocks, and simulator virtual time. Explain when resource values represent logical slots or estimates.

## Navigation and updates

The [documentation home](index.rst) lists pages, the [project layout](reference/project-layout.md) explains directory responsibilities, and the [API index](reference/api.md) lists module paths and common interfaces. Update these entry points and references when adding, renaming, or removing pages. Use relative Markdown links.

When changing interfaces, review source docstrings, the relevant guides, and runnable examples together. For JSON fields, check the loader's actual defaults, precedence, and validation scope. For experiments, verify script arguments, units, input requirements, and reproduction scope.

Run complete Python examples against the installed current package. Link real dataset experiments to the separate benchmark documentation, which maintains commands, prerequisites, and smoke checks. Launcher documentation describes only the companion quickstart profile's interface contract; the benchmark defines simulation semantics and other profiles. Do not rerun full experiments solely to validate prose.

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
