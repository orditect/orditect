"""Reference executor for the data rules (library-level, stdlib inputs).

Accepts an iterable of envelope dicts; applies the registered rules; returns
a RuleReport. CLI / file traversal / bundle reading live in the ADAPTER
layer — never here.
"""

from __future__ import annotations

from typing import Callable, Iterable

from orditect.protocol.rules._types import Finding, RuleReport
from orditect.protocol.rules.all import (
    dr_all_001,
    dr_all_002,
    dr_all_003,
    dr_dep_001,
)
from orditect.protocol.rules.audit import dr_aud_001
from orditect.protocol.rules.content import dr_ctt_001
from orditect.protocol.rules.snapshot import dr_snp_001, dr_snp_002, dr_snp_003

#: rule id -> implementation (single source for runner + traceability gate)
RULES: dict[str, Callable[[Iterable[dict]], list[Finding]]] = {
    "DR-SNP-001": dr_snp_001,
    "DR-SNP-002": dr_snp_002,
    "DR-SNP-003": dr_snp_003,
    "DR-AUD-001": dr_aud_001,
    "DR-CTT-001": dr_ctt_001,
    "DR-ALL-001": dr_all_001,
    "DR-ALL-002": dr_all_002,
    "DR-DEP-001": dr_dep_001,
    "DR-ALL-003": dr_all_003,
}


def run_rules(
    lines: Iterable[dict],
    *,
    select: set[str] | None = None,
) -> RuleReport:
    """Apply the rule set to an envelope stream.

    Args:
        lines: envelope dicts ({"v","op","ts","data"}; op optional).
        select: optional subset of rule ids (None = all registered rules).

    Returns:
        RuleReport; ok == True iff zero violations (warnings never fail).
    """
    rows = list(lines)
    chosen = RULES if select is None else {
        rid: fn for rid, fn in RULES.items() if rid in select
    }
    findings: list[Finding] = []
    for fn in chosen.values():
        findings.extend(fn(rows))
    return RuleReport(findings=findings)