"""Meta test: the dependency-governance example must run green."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEMO = ROOT / "examples" / "dependency-governance" / "run_demo.py"


def test_dependency_governance_example_runs_green():
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    proc = subprocess.run(
        [sys.executable, str(DEMO)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(ROOT),
        timeout=120,
    )
    assert proc.returncode == 0, (
        f"dependency-governance example failed:\n{proc.stdout}\n{proc.stderr}"
    )
    assert "DEMO OK" in proc.stdout
    assert "0 violations" in proc.stdout