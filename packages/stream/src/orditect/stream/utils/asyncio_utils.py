"""Enqueue with backpressure policy."""
from __future__ import annotations

import asyncio
from typing import TypeVar

from orditect.stream.config import BackpressurePolicy
from orditect.stream.exceptions import BackpressureError

T = TypeVar("T")


async def queue_put_with_policy(
    queue: asyncio.Queue[T],
    item: T,
    policy: BackpressurePolicy,
) -> None:
    """Enqueue with backpressure policy.

        block: wait for available slot (backpressure upstream)
        fail:  immediately fail with BackpressureError
        """
    if policy is BackpressurePolicy.BLOCK:
        await queue.put(item)
        return
    # FAIL
    if queue.full():
        raise BackpressureError(
            f"mux queue full (maxsize={queue.maxsize}), policy=fail"
        )
    queue.put_nowait(item)