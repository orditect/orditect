"""Self-pinning for the reference rule executor (run_rules)."""

from __future__ import annotations

import pytest

from orditect.protocol.rules import RuleReport, run_rules

pytestmark = pytest.mark.unit


class TestRunRules:
    def test_empty_input_ok(self):
        report = run_rules([])
        assert report.ok
        assert report.violation_count == 0
        assert report.warning_count == 0

    def test_select_subset(self):
        bad = [{"v": 1, "op": "save", "ts": "2026-08-28T10:00:00Z",
                "data": {"task_id": "t", "step": "s", "execution_id": "",
                         "created_at": "naive-no-offset"}}]
        only_clock = run_rules(bad, select={"DR-ALL-001"})
        assert any(f.rule == "DR-ALL-001" for f in only_clock.findings)
        assert not any(f.rule == "DR-SNP-003" for f in only_clock.findings)

        only_snp = run_rules(bad, select={"DR-SNP-003"})
        assert any(f.rule == "DR-SNP-003" for f in only_snp.findings)
        assert not any(f.rule == "DR-ALL-001" for f in only_snp.findings)

    def test_warnings_do_not_fail_ok(self):
        lines = [{"v": 1, "op": "append", "ts": "2026-08-28T10:00:00Z",
                  "data": {"event_id": "e", "task_id": "ghost",
                           "created_at": "2026-08-28T10:00:00Z"}}]
        report = run_rules(lines)
        assert report.ok
        assert report.warning_count > 0

    def test_summary_renders(self):
        report = run_rules([])
        assert "0 violations" in report.summary()