"""pytest configuration for repository-level integration tests.

Injects every package's src/ into sys.path so cross-package imports work
without installing anything (mirrors the per-package conftest pattern).
"""

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]

_PACKAGES = (
    "protocol",
    "core",
    "flow",
    "stream",
    "adapter-memory",
    "adapter-local",
    "adapter-ui",
    "bridge-openai",
)

for _pkg in _PACKAGES:
    _src = _REPO_ROOT / "packages" / _pkg / "src"
    if _src.is_dir() and str(_src) not in sys.path:
        sys.path.insert(0, str(_src))