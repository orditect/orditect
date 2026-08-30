"""Meta test: the schema generator must run on a bare interpreter.

The script injects the protocol src/ into sys.path itself; a top-level import
of orditect.* placed BEFORE that injection makes the script crash with
ModuleNotFoundError on a fresh clone / bare CI runner (v0.1.6, issue #4).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GENERATOR = ROOT / "packages" / "protocol" / "scripts" / "generate_schemas.py"


def test_generate_schemas_runs_without_package_install():
    """Run the generator in a subprocess with no src/ on sys.path."""
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    proc = subprocess.run(
        [sys.executable, str(GENERATOR), "--check"],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(ROOT),
    )
    assert proc.returncode == 0, (
        f"generator failed on a bare interpreter:\n{proc.stdout}\n{proc.stderr}"
    )