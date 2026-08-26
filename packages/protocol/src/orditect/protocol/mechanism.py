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

__all__ = ["SORT_FIELDS", "TIME_RANGE_TARGET", "GROUP_BY_FIELDS"]