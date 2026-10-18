"""Check that the installed core package works outside the source checkout."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_installed_package_imports_in_isolated_python(tmp_path: Path) -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            "-X",
            "utf8",
            "-c",
            "import importlib, importlib.metadata, importlib.util, pkgutil, wenbo; "
            "assert wenbo.__version__ == importlib.metadata.version('wenbo'); "
            "assert importlib.util.find_spec('wenbo.benchmark') is None; "
            "[importlib.import_module(module.name) "
            "for module in pkgutil.walk_packages(wenbo.__path__, wenbo.__name__ + '.')]; "
            "print('All Wenbo modules imported')",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "All Wenbo modules imported" in completed.stdout
