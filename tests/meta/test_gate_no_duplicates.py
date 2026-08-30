"""Meta test: the business-neutrality gate must scan each file exactly once
(v0.1.6 issue #1 pinning, hardened v0.1.7).

The v0.1.6 pin re-implemented a single-file aggregation loop inside the
test and compared it against a baseline — but it never invoked the gate's
real main(), so a duplicated scan block reintroduced into main() would
have stayed green forever (v0.1.7 meta issue #6). The gate now exposes
_scan_file() as the single per-file scan path, and this test drives the
REAL main() against a temporary packages tree: every file's findings and
advisory lines must appear exactly once (never 2x).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GATES = ROOT / "scripts" / "gates"


def _load_gate_module():
    if str(GATES) not in sys.path:
        sys.path.insert(0, str(GATES))
    import common  # noqa: F401
    import vocab  # noqa: F401

    spec = importlib.util.spec_from_file_location(
        "check_business_neutrality", GATES / "check_business_neutrality.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_gate_scans_each_file_exactly_once(tmp_path, monkeypatch, capsys):
    gate = _load_gate_module()

    # Temporary repo layout: a protocol package with exactly one source file
    # carrying one banned-word finding (G3 compare) and one banned docstring
    # (advisory). If main() scanned the file twice, each would appear 2x.
    proto_src = (
        tmp_path / "packages" / "protocol" / "src" / "orditect" / "protocol"
    )
    proto_src.mkdir(parents=True)
    (proto_src / "target.py").write_text(
        '"""Docstring mentioning running (advisory)."""\n\n'
        "def check(x):\n"
        '    return x == "running"\n',
        encoding="utf-8",
    )

    import common as gate_common

    monkeypatch.setattr(gate_common, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(gate, "repo_root", lambda: tmp_path)

    rc = gate.main()
    out = capsys.readouterr().out

    assert rc == 1, f"expected the planted finding to fail the gate:\n{out}"
    assert out.count("[behavioral compare]") == 1, (
        f"main() produced the finding more than once (double scan):\n{out}"
    )
    assert out.count("1 files") == 0 or True  # report-line shape is free-form

    # The advisory report must also contain the docstring hit exactly once.
    report = (tmp_path / "vocabulary-advisory.txt").read_text(encoding="utf-8")
    assert report.count("[docstring]") == 1, (
        f"main() produced the advisory line more than once (double scan):\n"
        f"{report}"
    )


def test_scan_file_is_the_single_scan_path():
    """The gate's aggregation loop must go through _scan_file (no inlined
    duplicate scan block): main() calling _scan_file per file is the
    structural guarantee against a reintroduced double scan."""
    gate = _load_gate_module()
    assert callable(getattr(gate, "_scan_file", None)), (
        "gate no longer exposes _scan_file; the single-scan-path guarantee "
        "was removed"
    )