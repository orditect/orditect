"""Shared helpers for rule implementations."""

from __future__ import annotations

import re
from typing import Any

#: Accepted explicit offsets: "Z" or "+HH:MM"/"-HH:MM" (T7, DR-ALL-001).
_EXPLICIT_OFFSET_RE = re.compile(r"(Z|[+-]\d{2}:\d{2})$")


def has_explicit_offset(value: Any) -> bool:
    """True when a datetime string carries an explicit timezone offset."""
    if not isinstance(value, str):
        return False
    return bool(_EXPLICIT_OFFSET_RE.search(value))


def iter_domain_rows(lines: list[dict], domain: str) -> list[tuple[int, dict]]:
    """Yield (index, envelope) pairs whose data looks like the given domain.

    Domain inference is structural (op names first, payload shape as
    fallback) — never business-vocabulary based.
    """
    out: list[tuple[int, dict]] = []
    for i, line in enumerate(lines):
        if not isinstance(line, dict) or "meta" in line:
            continue
        op = line.get("op")
        data = line.get("data", {})
        if domain == "snapshot":
            if op in ("save", "save_terminal") or (
                isinstance(data, dict) and "execution_id" in data
            ):
                out.append((i, line))
        elif domain == "audit":
            if op == "append" or (
                isinstance(data, dict) and "event_id" in data
            ):
                out.append((i, line))
        elif domain == "edge":
            if op == "edge_write" or (
                isinstance(data, dict)
                and "child_id" in data
                and "parent_id" in data
            ):
                out.append((i, line))
    return out