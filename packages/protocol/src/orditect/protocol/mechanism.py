"""Mechanism-field whitelists for contract read interfaces (contract data).

This module is contract data, not implementation: adapters may reference
these tables or embed an equal set. Inputs outside the whitelist MUST be
rejected explicitly (InvalidQueryError) — never silently fall back.

Business fields (payload / cost / error / any business-defined content) are
never whitelisted: the mechanism/business boundary is the business-isolation
boundary itself.
"""

from __future__ import annotations
from collections.abc import Iterable

#: Allowed Sort.field values per query-capable domain.
SORT_FIELDS: dict[str, frozenset[str]] = {
    "audit": frozenset({"created_at", "event_id"}),
    "snapshot": frozenset({"created_at", "updated_at", "expire_at"}),
}

#: The record field each domain's TimeRange filter applies to.
TIME_RANGE_TARGET: dict[str, str] = {
    "audit": "created_at",
    "snapshot": "created_at",
}

#: Allowed group_by values per aggregating domain.
GROUP_BY_FIELDS: dict[str, frozenset[str]] = {
    "snapshot": frozenset({"status", "model"}),
}
#: Mechanism clock fields excluded from idempotency comparison (T4).
#: These are producer-clock values (T7) generated at write time, not part of
#: the business content. Two writes with identical business fields but
#: different producer timestamps are the SAME write for idempotency purposes.
IDEMPOTENCY_EXCLUDED_FIELDS: frozenset[str] = frozenset({
    "created_at",
    "updated_at",
    "registered_at",
})


def idempotent_payload_equal(a: dict, b: dict) -> bool:
    """T4 content comparison excluding mechanism clock fields.

    Args:
        a: first payload dict (e.g. model_dump / to_payload output)
        b: second payload dict

    Returns:
        True when the two payloads carry identical business content
        (all fields except IDEMPOTENCY_EXCLUDED_FIELDS match).
    """
    strip = lambda d: {
        k: v for k, v in d.items() if k not in IDEMPOTENCY_EXCLUDED_FIELDS
    }
    return strip(a) == strip(b)

#: Non-state fields merged to complete the record (T3 second face).
SNAPSHOT_MERGE_FIELDS: frozenset[str] = frozenset({
    "parent_task_id", "input_pointer", "output_pointer",
    "error", "cost", "model", "expire_at",
})


def fold_snapshot_rows(rows: Iterable[dict]) -> dict | None:
    """Reconstruct THE record for one (task_id, step, execution_id) generation
    from an ordered stream of save/save_terminal payload dicts.

    Single executable definition of the snapshot merge semantics
    (adjudicated v0.1.5); adapters call this or embed an equal
    implementation:

    - status: the LAST row whose status is non-empty. An empty status is
      the absence of status intent — never a mutation, never a regression.
      (save_terminal with an empty status asserts finality WITHOUT
      asserting a new state: on an already-terminal key it conflicts if
      the recorded terminal content differs.)
    - created_at: the FIRST row's value (record creation instant).
    - updated_at: the LAST row's value.
    - SNAPSHOT_MERGE_FIELDS: a non-None value overwrites; None or absent
      preserves (merging completes the record, never erases it).
    """
    record: dict | None = None
    for row in rows:
        if record is None:
            record = {
                k: v for k, v in row.items() if v is not None
            }
            continue
        for field in SNAPSHOT_MERGE_FIELDS:
            value = row.get(field)
            if value is not None:
                record[field] = value
        status = row.get("status")
        if status:
            record["status"] = status
        updated_at = row.get("updated_at")
        if updated_at is not None:
            record["updated_at"] = updated_at
    return record

__all__ = [
    "SORT_FIELDS",
    "TIME_RANGE_TARGET",
    "GROUP_BY_FIELDS",
    "IDEMPOTENCY_EXCLUDED_FIELDS",
    "idempotent_payload_equal",
    "SNAPSHOT_MERGE_FIELDS",
    "fold_snapshot_rows",
]