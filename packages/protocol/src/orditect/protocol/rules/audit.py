"""Audit-domain data rules (DR-AUD-*)."""

from __future__ import annotations

import json
from typing import Iterable

from orditect.protocol.rules._types import Finding
from orditect.protocol.rules.common import iter_domain_rows
from orditect.protocol.mechanism import IDEMPOTENCY_EXCLUDED_FIELDS

def _canonical_payload(data: dict) -> str:
    """Canonical payload form for identity comparison (T4).

    Mechanism clock fields (producer-clock values, T7) are excluded: two
    writes with identical business content but different producer timestamps
    are the SAME write, not a conflict.
    """
    stripped = {
        k: v for k, v in data.items() if k not in IDEMPOTENCY_EXCLUDED_FIELDS
    }
    return json.dumps(stripped, sort_keys=True, ensure_ascii=False, default=str)


def dr_aud_001(lines: Iterable[dict]) -> list[Finding]:
    """DR-AUD-001 (T4, violation): same event_id with different payload.

    Identical payload repeats are legal dedup and are NOT reported.
    """
    findings: list[Finding] = []
    seen: dict[str, str] = {}  # event_id -> canonical full row payload
    for i, line in iter_domain_rows(list(lines), "audit"):
        data = line.get("data", {})
        event_id = data.get("event_id")
        if not event_id:
            continue
        canonical = _canonical_payload(data)
        if event_id in seen and seen[event_id] != canonical:
            findings.append(Finding(
                rule="DR-AUD-001", level="violation",
                location=f"audit[{i}].data",
                message=f"event_id {event_id!r} re-used with a different payload",
                term="T4",
            ))
        else:
            seen.setdefault(event_id, canonical)
    return findings