from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_quickstart_completes_example_dag(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[1]
    output_file = tmp_path / "result.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(project_root / "examples" / "quickstart.py"),
            "--output-file",
            str(output_file),
        ],
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    result = json.loads(output_file.read_text(encoding="utf-8"))
    assert result["metadata"]["completed"] == 1
    assert result["metadata"]["failed"] == 0
    assert result["results"][0]["state"] == "completed"
