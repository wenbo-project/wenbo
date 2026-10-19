# Development

Install development dependencies from the Wenbo repository root with Python 3.10 or newer:

```bash
python -m pip install -e ".[dev]"
```

Run the local checks before contributing:

```bash
python -m ruff check src examples tests
python -m ruff format --check src examples tests
python -m pytest -q
```

To build the documentation locally:

```bash
python -m pip install -r docs/requirements.txt
python -m sphinx -b html -W --keep-going docs .local/docs-html
```

See [Contributing](https://github.com/wenbo-project/wenbo/blob/main/CONTRIBUTING.md) for issue and pull request guidance. Paper datasets and evaluation code belong to the separate [ARGB benchmark](https://github.com/wenbo-project/ARGB).
