"""pytest configuration for orditect-adapter-ui tests."""

import sys
from pathlib import Path

_SRC = Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_REPO = Path(__file__).resolve().parents[3]
for pkg in ("protocol", "flow", "adapter-local", "adapter-memory"):
    src = _REPO / "packages" / pkg / "src"
    if src.is_dir() and str(src) not in sys.path:
        sys.path.insert(0, str(src))