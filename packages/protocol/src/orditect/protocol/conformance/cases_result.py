"""Result-domain conformance cases (CF-RST-*)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Awaitable, Callable

CaseFn = Callable[[Any], Awaitable[None]]
CASES: list[tuple[str, str, CaseFn]] = []


def case(case_id: str):
    def deco(fn: CaseFn) -> CaseFn:
        CASES.append((case_id, "result_sink", fn))
        return fn
    return deco


def _requires_query(adapter: Any) -> bool:
    return adapter.capabilities.supports("result_query")


@case("CF-RST-002")
async def expired_read_returns_none(adapter: Any) -> None:
    """CF-RST-002 (T1/T7): a record past its expire_at is invisible to readers."""
    if not _requires_query(adapter):
        return  # read-side verification only when query declared
    past = datetime.now(UTC) - timedelta(seconds=1)
    await adapter.save("cf-rst-002", {"k": "v"}, expire_at=past)
    got = await adapter.get("cf-rst-002")
    assert got is None, "expired record must be invisible (lazy expiry, T1)"


@case("CF-RST-003")
async def concurrent_save_single_record(adapter: Any) -> None:
    """CF-RST-003 (T10): concurrent saves of one stream_id leave one clean record."""
    if not _requires_query(adapter):
        return
    import asyncio

    async def w(i: int) -> None:
        await adapter.save(
            "cf-rst-003", {"n": i},
            expire_at=datetime.now(UTC) + timedelta(hours=1),
        )

    await asyncio.gather(*(w(i) for i in range(8)))
    got = await adapter.get("cf-rst-003")
    assert got is not None and isinstance(got.get("n"), int), (
        "record missing or partially written after concurrent save"
    )