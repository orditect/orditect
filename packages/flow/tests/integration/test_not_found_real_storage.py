"""Contract pinning for not-found behavior against the REAL TaskRedisDB.

The flow test suite runs almost entirely on in-memory FakeStorage doubles,
whose get_task() raises TaskNotFoundError for missing tasks. The real
TaskRedisDB returns {} instead (the documented storage contract). This file
pins the aligned not-found behavior on the real backend.

Requires a real Redis (default db15, integration-suite convention).
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from orditect.core import TaskRedisDB
from orditect.flow import TaskOrchestrator
from orditect.flow.exceptions import TaskNotFoundError

REDIS_URL = "redis://localhost:6379/15"

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def orchestrator():
    storage = TaskRedisDB(redis_url=REDIS_URL)
    await storage.connect()
    await storage.client.flushdb()
    orch = TaskOrchestrator(storage, governor=None)
    yield orch
    await storage.client.flushdb()
    await storage.close()


async def test_cancel_ghost_returns_false(orchestrator):
    """FLIP(v0.1.4): cancel() on a ghost task returns False on the real
    backend (was KeyError) — ghost is treated as not-found per contract."""
    ok = await orchestrator.cancel("ghost-task")
    assert ok is False


async def test_terminate_ghost_returns_false(orchestrator):
    """FLIP(v0.1.4): terminate() on a ghost task returns False on the real
    backend (was KeyError)."""
    ok = await orchestrator.terminate("ghost-task")
    assert ok is False


async def test_get_status_ghost_raises_not_found(orchestrator):
    """FLIP(v0.1.4): get_status() on a ghost task raises TaskNotFoundError
    on the real backend (was KeyError)."""
    with pytest.raises(TaskNotFoundError):
        await orchestrator.get_status("ghost-task")


async def test_wait_terminal_ghost_raises_not_found(orchestrator):
    """FLIP(v0.1.4): wait_terminal() on a ghost task raises TaskNotFoundError
    on the real backend (was KeyError)."""
    with pytest.raises(TaskNotFoundError):
        await orchestrator.wait_terminal("ghost-task", timeout=0.5)