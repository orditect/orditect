"""Snapshot-domain data rules (DR-SNP-*)."""

from __future__ import annotations

from typing import Iterable

from orditect.protocol.rules._types import Finding
from orditect.protocol.rules.common import iter_domain_rows

_SNAPSHOT_OPS = ("save", "save_terminal")


def dr_snp_001(lines: Iterable[dict]) -> list[Finding]:
    """DR-SNP-001 (T3, violation): status drift within a terminal generation.

    With op: after save_terminal for a key, any later row for the same key
    whose status differs from the terminal status is a violation.
    Without op: any status drift for the same key is a violation, degraded.

    Empty-status rows carry no state intent (adjudicated v0.1.5): they
    never overwrite the recorded baseline and never count as drift.
    """
    rows = list(lines)
    findings: list[Finding] = []
    terminal: dict[tuple, str] = {}     # key -> terminal status (private state)
    seen_status: dict[tuple, str] = {}  # key -> last non-empty status (degraded mode)

    for i, line in iter_domain_rows(rows, "snapshot"):
        data = line.get("data", {})
        key = (data.get("task_id"), data.get("step"), data.get("execution_id"))
        status = data.get("status", "")
        op = line.get("op")

        if op in _SNAPSHOT_OPS:
            if key in terminal and status and status != terminal[key]:
                findings.append(Finding(
                    rule="DR-SNP-001", level="violation",
                    location=f"snapshots[{i}].data",
                    message=f"status drift after terminal: "
                            f"{terminal[key]!r} -> {status!r}",
                    term="T3",
                ))
            if op == "save_terminal" and status:
                terminal[key] = status
        else:
            # degraded mode: no op available, only drift is checkable
            if key in seen_status and status and status != seen_status[key]:
                findings.append(Finding(
                    rule="DR-SNP-001", level="violation",
                    location=f"snapshots[{i}].data",
                    message=f"status drift within one generation "
                            f"(degraded: op absent): "
                            f"{seen_status[key]!r} -> {status!r}",
                    term="T3",
                    degraded=True,
                ))
            if status:
                seen_status[key] = status
    return findings


def dr_snp_002(lines: Iterable[dict]) -> list[Finding]:
    """DR-SNP-002 (T3, violation): illegal op sequence after terminal.

    Any save/save_terminal for a key whose status differs from a previously
    recorded terminal status for that key violates op-sequence legality.
    Requires op — skipped entirely (not degraded) when op is absent.

    Empty-status rows carry no state intent (adjudicated v0.1.5): they are
    neither a violation nor a new terminal baseline.
    """
    rows = list(lines)
    if not any(line.get("op") in _SNAPSHOT_OPS for _, line in iter_domain_rows(rows, "snapshot")):
        return []  # op absent: rule does not apply (not a degradation)

    findings: list[Finding] = []
    terminal: dict[tuple, str] = {}
    for i, line in iter_domain_rows(rows, "snapshot"):
        op = line.get("op")
        if op not in _SNAPSHOT_OPS:
            continue
        data = line.get("data", {})
        key = (data.get("task_id"), data.get("step"), data.get("execution_id"))
        status = data.get("status", "")
        if key in terminal and status and status != terminal[key]:
            findings.append(Finding(
                rule="DR-SNP-002", level="violation",
                location=f"snapshots[{i}]",
                message=f"op {op!r} after save_terminal changes status: "
                        f"{terminal[key]!r} -> {status!r}",
                term="T3",
            ))
        if op == "save_terminal" and status:
            terminal[key] = status
    return findings


def dr_snp_003(lines: Iterable[dict]) -> list[Finding]:
    """DR-SNP-003 (T11, violation): execution_id must be present and non-empty."""
    findings: list[Finding] = []
    for i, line in iter_domain_rows(list(lines), "snapshot"):
        data = line.get("data", {})
        if not data.get("execution_id"):
            findings.append(Finding(
                rule="DR-SNP-003", level="violation",
                location=f"snapshots[{i}].data",
                message="snapshot row has empty or missing execution_id",
                term="T11",
            ))
    return findings