"""orditect-core adapter: enrich task concurrency governance.

Available only when orditect-core is installed.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class TaskbaseGovernorAdapter:
    """Reuse taskbase governor to control enrich task concurrency.

        Usage:
            governor = get_default_governor(redis_client)
            adapter = TaskbaseGovernorAdapter(governor, resource="enrich")
            # Before dispatching EnrichManager, call adapter.acquire(), and call adapter.release() upon completion.
        """

    def __init__(self, governor: Any, resource: str = "taskstream_enrich"):
        self._governor = governor
        self._resource = resource

    async def acquire(self, timeout: float = 30.0) -> str:
        return await self._governor.acquire(self._resource, timeout=timeout)

    async def release(self, token: str) -> None:
        await self._governor.release(self._resource, token)