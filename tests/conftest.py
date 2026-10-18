from __future__ import annotations

import shutil
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def installed_examples(tmp_path: Path) -> Path:
    """Copy examples without src so their imports use the installed package."""
    examples = tmp_path / "installed-example" / "examples"
    shutil.copytree(
        ROOT / "examples", examples, ignore=shutil.ignore_patterns("__pycache__", "*.pyc")
    )
    return examples
