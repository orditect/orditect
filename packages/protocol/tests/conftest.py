"""pytest configuration and shared fixtures for orditect-protocol tests."""

import sys
from pathlib import Path

# Ensure src/ is on sys.path before pytest applies pythonpath config
# (conftest is loaded before pytest.ini_options.pythonpath takes effect).
_SRC = Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))