"""Meta test: the MVP example must run green on a bare interpreter.

Guards against example rot: examples are not covered by the package test
suites, so an API change could silently break the demo. This test drives
the demo end-to-end in a subprocess (same pattern as
test_schema_generator.py) and requires the demo's own OK marker plus a
clean data-rules verdict.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEMO = ROOT / "examples" / "mvp" / "run_demo.py"


def test_mvp_example_runs_green():
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    proc = subprocess.run(
        [sys.executable, str(DEMO)],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(ROOT),
        timeout=180,
    )
    assert proc.returncode == 0, (
        f"mvp example failed:\n{proc.stdout}\n{proc.stderr}"
    )
    assert "DEMO OK" in proc.stdout
    assert "0 violations" in proc.stdout