"""CancellationToken — task cancellation token (v0.3.0: B6 polling cache)."""
from __future__ import annotations

import time
from typing import TYPE_CHECKING, Optional

from orditect.core.errors import CancelledByUser

if TYPE_CHECKING:
    from orditect.core.redis.task_db import TaskRedisDB


class CancellationToken:
    """Task cancellation token: queries cancel_requested via TaskRedisDB,
    for long-process segmented checks.

    v0.3.0 (B6): new min_interval local cache. Returns last query result
    within window, queries Redis only when expired. Semantics unchanged
    (cancel_requested is monotonically increasing flag, cache only delays
    "discovering cancellation" time, never misses cancellation), data
    cleaning 10k-level polling no longer hits hot path.
    """

    def __init__(
        self,
        task_id: str,
        task_redis_db: "TaskRedisDB",
        *,
        min_interval: float = 0.1,
    ):
        """
        Args:
            task_id: task ID
            task_redis_db: task store instance
            min_interval: polling cache window (seconds). 0=disable cache
                (must query Redis every time).
        """
        self.task_id = str(task_id)
        self.task_redis_db = task_redis_db
        self._min_interval = float(min_interval)
        self._cached_result: Optional[bool] = None
        self._cached_at: float = 0.0

    async def is_cancelled(self) -> bool:
        """Query if task is requested for cancellation (with local cache)."""
        if self._min_interval > 0 and self._cached_result is not None:
            elapsed = time.monotonic() - self._cached_at
            if elapsed < self._min_interval:
                return self._cached_result

        rec = await self.task_redis_db.get_task(self.task_id)
        result = bool(rec and rec.get("cancel_requested", False))

        if self._min_interval > 0:
            self._cached_result = result
            self._cached_at = time.monotonic()

        return result

    async def raise_if_cancelled(self):
        """Raise CancelledByUser exception if cancelled (for task internal
        active interruption)."""
        if await self.is_cancelled():
            raise CancelledByUser("CancelledByUser")