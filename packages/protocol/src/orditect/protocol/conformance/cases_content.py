"""Content-domain conformance cases (CF-CTT-*).

Authoring discipline: one adapter instance runs the WHOLE suite in a single
event loop — every case MUST use case-unique task_id / event_id / key
prefixes (e.g. "cf-ctt-005/..."), never shared placeholders.
"""

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


@case("CF-CTT-004")
async def metadata_round_trip(adapter: Any) -> None:
    """CF-CTT-004 (T5): get_metadata returns the metadata supplied at put."""
    if not adapter.capabilities.supports("content_query"):
        return
    meta = {"content_type": "text/plain", "origin": "cf-ctt-004"}
    ptr = await adapter.put(b"with-meta", metadata=meta)
    got = await adapter.get_metadata(ptr)
    for key, value in meta.items():
        assert got.get(key) == value, f"metadata lost: {key}"


@case("CF-CTT-005")
async def missing_pointer_semantics(adapter: Any) -> None:
    """CF-CTT-005 (T5): exists() is False and get() raises for a missing pointer."""
    if not adapter.capabilities.supports("content_query"):
        return
    from orditect.protocol.errors import ContentNotFoundError

    ghost = TaskPointer(backend="mem", key="cf-ctt-005/never-written")
    assert await adapter.exists(ghost) is False
    try:
        await adapter.get(ghost)
    except ContentNotFoundError:
        return
    raise AssertionError("expected ContentNotFoundError, got success")