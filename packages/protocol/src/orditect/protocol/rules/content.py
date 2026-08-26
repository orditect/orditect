"""Content-domain data rules (DR-CTT-*)."""

from __future__ import annotations

from typing import Iterable

from orditect.protocol.rules._types import Finding
from orditect.protocol.rules.common import iter_domain_rows

_POINTER_FIELDS = ("input_pointer", "output_pointer")


def dr_ctt_001(lines: Iterable[dict]) -> list[Finding]:
    """DR-CTT-001 (T5, violation): a recorded pointer must resolve.

    A snapshot row's pointer key absent from the content rows AND not
    registered via a dangling_pointers metadata row is a violation.
    """
    rows = list(lines)

    content_keys: set[str] = set()
    registered_dangling: set[str] = set()
    for line in rows:
        if not isinstance(line, dict):
            continue
        if line.get("meta") == "dangling_pointers":
            registered_dangling.update(line.get("keys", []))
            continue
        if line.get("op") == "put":
            data = line.get("data", {})
            if isinstance(data, dict) and data.get("key"):
                content_keys.add(data["key"])

    findings: list[Finding] = []
    for i, line in iter_domain_rows(rows, "snapshot"):
        data = line.get("data", {})
        for field_name in _POINTER_FIELDS:
            pointer = data.get(field_name)
            if not isinstance(pointer, dict):
                continue
            key = pointer.get("key")
            if not key:
                continue
            if key not in content_keys and key not in registered_dangling:
                findings.append(Finding(
                    rule="DR-CTT-001", level="violation",
                    location=f"snapshots[{i}].data.{field_name}",
                    message=f"pointer {key!r} does not resolve and is not "
                            f"registered as dangling",
                    term="T5",
                ))
    return findings