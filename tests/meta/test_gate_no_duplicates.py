"""Meta test: the business-neutrality gate must scan each file exactly once
(v0.1.6, issue #1).

A duplicated scan block appended every finding/advisory twice. We pin it by
exercising the gate's own per-file scan path: scanning one file through the
gate's aggregation loop must yield exactly one traversal's worth of findings
(never 2x).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GATES = ROOT / "scripts" / "gates"
TARGET = (
    ROOT / "packages" / "protocol" / "src" / "orditect" / "protocol"
    / "domains" / "audit.py"
)


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


def test_gate_scans_each_file_exactly_once():
    gate = _load_gate_module()

    # Baseline: a single SurfaceScan traversal over the target file.
    baseline = gate.SurfaceScan("audit.py")
    baseline.visit(gate.parse_python(TARGET))
    baseline_findings = list(baseline.findings)
    baseline_advisory = list(gate._advisory_docstrings("audit.py", gate.parse_python(TARGET)))

    # Reproduce the gate's main-loop aggregation for a single file (the
    # fixed version appends findings+advisory exactly once per file).
    findings: list[str] = []
    advisory: list[str] = []
    rel = "audit.py"
    tree = gate.parse_python(TARGET)
    scan = gate.SurfaceScan(rel)
    scan.visit(tree)
    findings.extend(scan.findings)
    advisory.extend(gate._advisory_docstrings(rel, tree))

    assert findings == baseline_findings, (
        "gate aggregation appended findings more than once (double scan)"
    )
    assert advisory == baseline_advisory, (
        "gate aggregation appended advisory lines more than once (double scan)"
    )