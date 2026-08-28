"""Mechanism-field whitelists for contract read interfaces (contract data).

This module is contract data, not implementation: adapters may reference
these tables or embed an equal set. Inputs outside the whitelist MUST be
rejected explicitly (InvalidQueryError) — never silently fall back.

Business fields (payload / cost / error / any business-defined content) are
never whitelisted: the mechanism/business boundary is the business-isolation
boundary itself.
"""

from __future__ import annotations

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

__all__ = [
    "SORT_FIELDS",
    "TIME_RANGE_TARGET",
    "GROUP_BY_FIELDS",
    "IDEMPOTENCY_EXCLUDED_FIELDS",
    "idempotent_payload_equal",
]