"""Single-event-loop pinning for the conformance runner (WI-1.9).

Pre-fix, each case ran in its own asyncio.run, so an adapter holding a
loop-bound resource (e.g. an asyncio.Lock created in an earlier case) broke
in a later case. Post-fix, one loop runs the whole suite.
"""

from __future__ import annotations

from typing import Any

import pytest

from orditect.protocol import CapabilitySet
from orditect.protocol.conformance import run_conformance
from orditect.protocol.models import TaskPointer


class _LoopBoundResourceAdapter:
    """Declares content_sink only; stashes an asyncio.Lock at first use and
    re-uses it later — the lock is bound to the creating loop."""

    def __init__(self) -> None:
        self._data: dict[str, bytes] = {}
        self._meta: dict[str, dict] = {}
        self._lock = None

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(content_sink=True, content_query=True)

    async def put(self, content: bytes, **kw: Any) -> TaskPointer:
        import asyncio
        if self._lock is None:
            self._lock = asyncio.Lock()  # binds to the running loop
        meta = dict(kw.get("metadata") or {})
        if kw.get("content_type") is not None:
            meta["content_type"] = kw["content_type"]
        async with self._lock:
            key = f"mem://{len(self._data)}"
            self._data[key] = content
            self._meta[key] = meta
        return TaskPointer(backend="mem", key=key)

    async def get(self, pointer: TaskPointer) -> bytes:
        from orditect.protocol.errors import ContentNotFoundError
        try:
            return self._data[pointer.key]
        except KeyError:
            raise ContentNotFoundError(pointer.key) from None
    async def delete(self, pointer: TaskPointer) -> bool:
        return self._data.pop(pointer.key, None) is not None

    async def exists(self, pointer: TaskPointer) -> bool:
        return pointer.key in self._data

    async def get_metadata(self, pointer: TaskPointer) -> dict[str, Any]:
        from orditect.protocol.errors import ContentNotFoundError
        try:
            return self._meta[pointer.key]
        except KeyError:
            raise ContentNotFoundError(pointer.key) from None

@pytest.mark.unit
class TestRunnerSingleLoop:
    def test_loop_bound_resource_survives_across_cases(self):
        """CF-CTT-001 and CF-CTT-003 both put(); the second put re-uses the
        lock created by the first — green only inside one shared loop."""
        report = run_conformance(_LoopBoundResourceAdapter())
        assert report.failed == 0, report.summary()