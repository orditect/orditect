"""Schema drift gate: model changes must regenerate artifacts in the same commit.

Re-runs the generator (scripts/generate_schemas.py) in memory and diffs the
rendered text against the checked-in artifacts byte for byte. A failure means
a model changed without regenerating — fix by running:

    python packages/protocol/scripts/generate_schemas.py
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.golden

_PKG_ROOT = Path(__file__).resolve().parents[2]
_GENERATOR = _PKG_ROOT / "scripts" / "generate_schemas.py"
_SCHEMA_DIR = _PKG_ROOT / "schemas"


def _load_generator():
    """Import the generator module from its script path (not a package)."""
    spec = importlib.util.spec_from_file_location("generate_schemas", _GENERATOR)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("generate_schemas", module)
    spec.loader.exec_module(module)
    return module


class TestSchemaDrift:
    """Checked-in artifacts must equal a fresh regeneration, byte for byte."""

    def test_artifacts_match_regeneration(self):
        generator = _load_generator()
        artifacts = generator.generate_all()
        assert artifacts, "generator produced no artifacts"

        drifted: list[str] = []
        for rel_id, rendered in artifacts.items():
            path = _SCHEMA_DIR / rel_id
            if not path.is_file():
                drifted.append(f"{rel_id} (missing)")
                continue
            if path.read_text(encoding="utf-8") != rendered:
                drifted.append(rel_id)

        assert not drifted, (
            "schema drift detected:\n"
            + "\n".join(f"  - schemas/{d}" for d in drifted)
            + "\nrun: python packages/protocol/scripts/generate_schemas.py"
        )

    def test_no_orphan_artifacts(self):
        """No artifact on disk may lack a generator target (stale files)."""
        generator = _load_generator()
        expected = set(generator.generate_all())
        on_disk = {
            p.relative_to(_SCHEMA_DIR).as_posix()
            for p in _SCHEMA_DIR.rglob("*.json")
        }
        orphans = on_disk - expected
        assert not orphans, (
            "orphan schema artifacts (no generator target): "
            + ", ".join(sorted(orphans))
        )