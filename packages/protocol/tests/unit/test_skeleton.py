"""B0 skeleton pinning tests.

Pin the package skeleton invariants:
- Version string is readable and well-formed.
- Package layout is importable.
- PEP 420 namespace coexistence holds (orditect.* must not own the top-level
  `orditect` package; multiple distributions share the namespace).
"""

import importlib
import re
import sys

import pytest


@pytest.mark.unit
class TestPackageSkeleton:
    """Skeleton invariants for orditect-protocol."""

    def test_version_is_well_formed(self):
        """__version__ matches semver-ish pattern or dev fallback."""
        import orditect.protocol as proto

        assert isinstance(proto.__version__, str)
        # Accept "0.1.0" style or "0.0.0.dev0" dev fallback
        assert re.match(r"^\d+\.\d+\.\d+(\.dev\d+)?$", proto.__version__), (
            f"unexpected version format: {proto.__version__}"
        )

    def test_package_importable(self):
        """orditect.protocol imports cleanly."""
        mod = importlib.import_module("orditect.protocol")
        assert mod is not None
        assert hasattr(mod, "__version__")

    def test_orditect_is_namespace_package(self):
        """`orditect` must be a PEP 420 namespace package (no __init__.py).

        If orditect/__init__.py exists, other orditect-* distributions
        (core / flow / stream) cannot share the namespace -> coexistence breaks.
        """
        import orditect

        # Namespace packages have __file__ == None and __path__ as _NamespacePath
        assert getattr(orditect, "__file__", None) is None, (
            "orditect/__init__.py detected: must be a PEP 420 namespace package "
            "(remove orditect/__init__.py)"
        )
        assert hasattr(orditect, "__path__")

    def test_protocol_subpackage_is_regular_package(self):
        """orditect.protocol is a regular package (has __init__.py)."""
        import orditect.protocol as proto

        assert proto.__file__ is not None
        assert proto.__file__.endswith("__init__.py")


@pytest.mark.unit
class TestNamespaceCoexistence:
    """PEP 420 coexistence: multiple orditect-* distributions share namespace."""

    def test_coexistence_with_sibling_namespace_portion(self, tmp_path, monkeypatch):
        """A second distribution contributing orditect.other imports alongside.

        Simulates the future scenario where orditect-core, orditect-flow, etc.
        are installed alongside orditect-protocol. All must import successfully.
        """
        # Create a sibling namespace portion: orditect/other/__init__.py
        sibling_src = tmp_path / "sibling_src"
        sibling_pkg = sibling_src / "orditect" / "other"
        sibling_pkg.mkdir(parents=True)
        (sibling_pkg / "__init__.py").write_text(
            '"""Sibling namespace portion (simulates orditect-core)."""\n'
            'MARKER = "sibling-present"\n',
            encoding="utf-8",
        )

        # Invalidate caches so the new path is scanned
        importlib.invalidate_caches()
        monkeypatch.syspath_prepend(str(sibling_src))

        # Both portions of the namespace must import
        import orditect.protocol  # noqa: F401
        other = importlib.import_module("orditect.other")

        assert other.MARKER == "sibling-present"

        # Namespace path must contain both roots
        import orditect

        paths = list(orditect.__path__)
        assert any("sibling_src" in p for p in paths), (
            f"sibling namespace root not in orditect.__path__: {paths}"
        )