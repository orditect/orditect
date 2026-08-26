"""Cross-domain data rules (DR-ALL-*, DR-DEP-*)."""

from __future__ import annotations

from typing import Iterable

from orditect.protocol.rules._types import Finding
from orditect.protocol.rules.common import has_explicit_offset, iter_domain_rows

_DATETIME_FIELDS = ("created_at", "updated_at", "expire_at", "timestamp",
                    "registered_at", "ts")


def dr_all_001(lines: Iterable[dict]) -> list[Finding]:
    """DR-ALL-001 (T7, violation): every datetime carries an explicit offset."""
    findings: list[Finding] = []
    for i, line in enumerate(lines):
        if not isinstance(line, dict) or "meta" in line:
            continue
        data = line.get("data", line)
        if not isinstance(data, dict):
            continue
        for field_name in _DATETIME_FIELDS:
            value = data.get(field_name)
            if value is None:
                continue
            if not has_explicit_offset(value):
                findings.append(Finding(
                    rule="DR-ALL-001", level="violation",
                    location=f"rows[{i}].data.{field_name}",
                    message=f"datetime {field_name} lacks an explicit offset: "
                            f"{value!r}",
                    term="T7",
                ))
    return findings


def dr_all_002(lines: Iterable[dict]) -> list[Finding]:
    """DR-ALL-002 (refs, warning): audit.task_id without a snapshot row.

    Dangling references can be legitimate (T1 expiry) — warning, never
    violation.
    """
    rows = list(lines)
    snapshot_tasks = {
        line.get("data", {}).get("task_id")
        for _, line in iter_domain_rows(rows, "snapshot")
    }
    findings: list[Finding] = []
    reported: set[str] = set()
    for i, line in iter_domain_rows(rows, "audit"):
        task_id = line.get("data", {}).get("task_id")
        if task_id and task_id not in snapshot_tasks and task_id not in reported:
            reported.add(task_id)
            findings.append(Finding(
                rule="DR-ALL-002", level="warning",
                location=f"audit[{i}].data",
                message=f"audit references task {task_id!r} with no snapshot "
                        f"row (may be T1-expired)",
                term="T6",
            ))
    return findings


def dr_dep_001(lines: Iterable[dict]) -> list[Finding]:
    """DR-DEP-001 (T12, warning): edge endpoints without a snapshot row."""
    rows = list(lines)
    snapshot_tasks = {
        line.get("data", {}).get("task_id")
        for _, line in iter_domain_rows(rows, "snapshot")
    }
    findings: list[Finding] = []
    reported: set[str] = set()
    for i, line in iter_domain_rows(rows, "edge"):
        data = line.get("data", {})
        for field_name in ("child_id", "parent_id"):
            task_id = data.get(field_name)
            if (task_id and task_id not in snapshot_tasks
                    and task_id not in reported):
                reported.add(task_id)
                findings.append(Finding(
                    rule="DR-DEP-001", level="warning",
                    location=f"edges[{i}].data.{field_name}",
                    message=f"edge endpoint {task_id!r} has no snapshot row "
                            f"(may be T1-expired)",
                    term="T12",
                ))
    return findings


def dr_all_003(lines: Iterable[dict]) -> list[Finding]:
    """DR-ALL-003 (deterministic-ID, warning): manifest placeholder task_ref
    suffix must equal placeholder_id (tf:enrich-{pid} convention)."""
    findings: list[Finding] = []
    for i, line in enumerate(lines):
        if not isinstance(line, dict) or "meta" in line:
            continue
        data = line.get("data", line)
        if not isinstance(data, dict):
            continue
        for ph in data.get("placeholders", []) or []:
            if not isinstance(ph, dict):
                continue
            pid = ph.get("placeholder_id")
            ref = ph.get("task_ref", "")
            if pid and isinstance(ref, str) and ref and not ref.endswith(pid):
                findings.append(Finding(
                    rule="DR-ALL-003", level="warning",
                    location=f"rows[{i}].data.placeholders",
                    message=f"task_ref {ref!r} suffix does not match "
                            f"placeholder_id {pid!r}",
                    term="T1",
                ))
    return findings