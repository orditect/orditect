"""Self-pinning tests for the data rules (DR-*).

Two properties are pinned per rule:
- detection: deliberately violating data MUST be caught;
- no false positives: legal data (incl. T1-expiry-legitimized states and
  identical-payload dedup) MUST NOT produce violations.
"""

from __future__ import annotations

import pytest

from orditect.protocol.rules.runner import RULES, run_rules

pytestmark = pytest.mark.unit


def _snap(task_id="t1", eid="e1", status="running", op="save", **extra):
    data = {
        "task_id": task_id, "step": "execute", "execution_id": eid,
        "status": status, "created_at": "2026-08-28T10:00:00Z",
    }
    data.update(extra)
    return {"v": 1, "op": op, "ts": "2026-08-28T10:00:00Z", "data": data}


def _audit(event_id="ev1", task_id="t1", payload=None):
    return {"v": 1, "op": "append", "ts": "2026-08-28T10:00:00Z",
            "data": {"event_id": event_id, "task_id": task_id,
                     "payload": payload or {},
                     "created_at": "2026-08-28T10:00:00Z"}}


def _edge(child="c", parent="p"):
    return {"v": 1, "op": "edge_write", "ts": "2026-08-28T10:00:00Z",
            "data": {"child_id": child, "parent_id": parent,
                     "registered_at": "2026-08-28T10:00:00Z"}}


def _put(key="sha256/ab/cdef"):
    return {"v": 1, "op": "put", "ts": "2026-08-28T10:00:00Z",
            "data": {"key": key}}


class TestSnapshotRules:
    def test_legal_generation_progression_clean(self):
        lines = [
            _snap(eid="e1", status="running"),
            _snap(eid="e1", status="done", op="save_terminal"),
            _snap(eid="e2", status="running"),  # new generation: always legal
        ]
        report = run_rules(lines)
        assert report.ok, report.summary()

    def test_drift_after_terminal_detected(self):
        lines = [
            _snap(eid="e1", status="done", op="save_terminal"),
            _snap(eid="e1", status="running"),
        ]
        report = run_rules(lines)
        assert not report.ok
        rules = {f.rule for f in report.findings}
        assert "DR-SNP-001" in rules
        assert "DR-SNP-002" in rules

    def test_degraded_mode_without_op(self):
        """No op on any row: DR-SNP-001 still catches drift, marked degraded;
        DR-SNP-002 is skipped (not degraded)."""
        lines = [
            _snap(eid="e1", status="done", op=None),
            _snap(eid="e1", status="running", op=None),
        ]
        for line in lines:
            line.pop("op")
        report = run_rules(lines)
        snp_001 = [f for f in report.findings if f.rule == "DR-SNP-001"]
        assert len(snp_001) == 1
        assert snp_001[0].degraded is True
        assert not any(f.rule == "DR-SNP-002" for f in report.findings)

    def test_missing_execution_id_detected(self):
        lines = [_snap(eid="")]
        report = run_rules(lines, select={"DR-SNP-003"})
        assert any(f.rule == "DR-SNP-003" for f in report.findings)


class TestAuditRules:
    def test_identical_repeat_is_dedup_not_conflict(self):
        lines = [_audit(payload={"a": 1}), _audit(payload={"a": 1})]
        report = run_rules(lines, select={"DR-AUD-001"})
        assert report.ok

    def test_same_id_different_payload_detected(self):
        lines = [_audit(payload={"v": 1}), _audit(payload={"v": 2})]
        report = run_rules(lines, select={"DR-AUD-001"})
        assert any(f.rule == "DR-AUD-001" for f in report.findings)


class TestContentRules:
    def test_resolving_pointer_clean(self):
        lines = [
            _put(key="sha256/ab/cdef"),
            _snap(input_pointer={"backend": "fs", "key": "sha256/ab/cdef"}),
        ]
        report = run_rules(lines, select={"DR-CTT-001"})
        assert report.ok

    def test_dangling_pointer_detected(self):
        lines = [_snap(output_pointer={"backend": "fs", "key": "ghost"})]
        report = run_rules(lines, select={"DR-CTT-001"})
        assert any(f.rule == "DR-CTT-001" for f in report.findings)

    def test_registered_dangling_exempt(self):
        lines = [
            {"meta": "dangling_pointers", "keys": ["ghost"]},
            _snap(output_pointer={"backend": "fs", "key": "ghost"}),
        ]
        report = run_rules(lines, select={"DR-CTT-001"})
        assert report.ok


class TestClockRules:
    def test_z_and_offset_both_accepted(self):
        lines = [
            _snap(created_at="2026-08-28T10:00:00Z"),
            _snap(eid="e2", created_at="2026-08-28T10:00:00+00:00"),
        ]
        report = run_rules(lines, select={"DR-ALL-001"})
        assert report.ok

    def test_naive_datetime_detected(self):
        lines = [_snap(created_at="2026-08-28 10:00:00")]
        report = run_rules(lines, select={"DR-ALL-001"})
        assert any(f.rule == "DR-ALL-001" for f in report.findings)


class TestReferenceWarnings:
    """T1 exemption matrix: legitimately dangling references must never be
    violations — warnings at most."""

    def test_audit_referencing_expired_task_is_warning_only(self):
        lines = [_audit(task_id="ghost-task")]
        report = run_rules(lines, select={"DR-ALL-002"})
        assert report.ok  # warnings never fail
        assert any(
            f.rule == "DR-ALL-002" and f.level == "warning"
            for f in report.findings
        )

    def test_edge_endpoint_without_snapshot_is_warning_only(self):
        lines = [_edge(child="ghost-c", parent="ghost-p")]
        report = run_rules(lines, select={"DR-DEP-001"})
        assert report.ok
        warnings = [f for f in report.findings if f.rule == "DR-DEP-001"]
        assert len(warnings) == 2  # both endpoints
        assert all(f.level == "warning" for f in warnings)

    def test_task_ref_suffix_mismatch_is_warning(self):
        lines = [{
            "v": 1, "op": "save", "ts": "2026-08-28T10:00:00Z",
            "data": {"placeholders": [
                {"placeholder_id": "ph_abc", "task_ref": "tf:enrich-ph_OTHER"},
            ]},
        }]
        report = run_rules(lines, select={"DR-ALL-003"})
        assert report.ok
        assert any(f.rule == "DR-ALL-003" for f in report.findings)

    def test_task_ref_suffix_match_clean(self):
        lines = [{
            "v": 1, "op": "save", "ts": "2026-08-28T10:00:00Z",
            "data": {"placeholders": [
                {"placeholder_id": "ph_abc", "task_ref": "tf:enrich-ph_abc"},
            ]},
        }]
        report = run_rules(lines, select={"DR-ALL-003"})
        assert report.ok


class TestRulesRegistry:
    def test_all_registered_rules_have_dr_prefix(self):
        for rule_id in RULES:
            assert rule_id.startswith("DR-")

    def test_expected_rule_set(self):
        assert set(RULES) == {
            "DR-SNP-001", "DR-SNP-002", "DR-SNP-003",
            "DR-AUD-001", "DR-CTT-001",
            "DR-ALL-001", "DR-ALL-002", "DR-ALL-003", "DR-DEP-001",
        }