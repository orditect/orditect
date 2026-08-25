"""Content-domain conformance cases (CF-CTT-*)."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from orditect.protocol.models import TaskPointer

CaseFn = Callable[[Any], Awaitable[None]]
CASES: list[tuple[str, str, CaseFn]] = []


def case(case_id: str):
    def deco(fn: CaseFn) -> CaseFn:
        CASES.append((case_id, "content_sink", fn))
        return fn
    return deco


@case("CF-CTT-001")
async def pointer_round_trip(adapter: Any) -> None:
    """CF-CTT-001 (T5): put returns a pointer that resolves to the same bytes."""
    data = b"orditect-conformance-\x00\x01\x02"
    ptr = await adapter.put(data, content_type="application/octet-stream")
    assert isinstance(ptr, TaskPointer) and ptr.backend and ptr.key
    got = await adapter.get(ptr)
    assert got == data, "round-trip content mismatch"


@case("CF-CTT-003")
async def content_immutable_under_pointer(adapter: Any) -> None:
    """CF-CTT-003 (T5): re-putting different content never corrupts an earlier pointer."""
    p1 = await adapter.put(b"first")
    p2 = await adapter.put(b"second")
    assert await adapter.get(p1) == b"first", "earlier pointer content changed"
    assert await adapter.get(p2) == b"second"