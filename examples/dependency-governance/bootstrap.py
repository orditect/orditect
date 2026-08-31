"""Import bootstrap for running the example without installing any package.

Injects every required package's src/ directory into sys.path, mirroring
the per-package conftest pattern used by the repository test suites.
Import this module BEFORE any orditect import.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

_PACKAGES = (
    "protocol",
    "core",
    "flow",
    "stream",
    "adapter-local",
    "adapter-ui",
    "bridge-openai",
)

for _pkg in _PACKAGES:
    _src = REPO_ROOT / "packages" / _pkg / "src"
    if _src.is_dir() and str(_src) not in sys.path:
        sys.path.insert(0, str(_src))