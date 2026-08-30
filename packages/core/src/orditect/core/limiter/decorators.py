"""@limited decorator: declarative resource governance.

Usage example:
    from orditect.core.limiter.decorators import limited

    @limited(resource="llm", mode="wait", timeout=5.0)
    async def call_llm(prompt: str):
        # business logic
        pass

    @limited(resource="gpu_pool", mode="reject")
    async def submit_heavy_task(data: dict):
        # failure to acquire immediately raises AcquireTimeoutError (app side maps 429)
        pass

    @limited(resource="api_qps", resource_type="bucket", mode="wait")
    async def call_external_api():
        # rate control
        pass
"""
from __future__ import annotations

import asyncio
import functools
from typing import Literal, Optional

from orditect.core.errors import AcquireTimeoutError
from orditect.core.limiter.registry import get_registry

import logging
logger = logging.getLogger(__name__)

#: Strong references for shielded release tasks created by @limited wrappers
#: (module-level: the decorator's wrapper has no owning instance to hold
#: them). An orphaned shield task must never be GC-collected mid-release.
_release_tasks: set[asyncio.Task] = set()


def _retrieve_release_error(task: "asyncio.Task") -> None:
    """Retrieve exceptions from shielded release tasks (prevents
    'exception was never retrieved' warnings at GC time)."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.debug(f"@limited release finished with error: {exc}")

def limited(
    resource: str,
    mode: Literal["wait", "reject"] = "wait",
    timeout: Optional[float] = None,
    resource_type: Literal["semaphore", "bucket"] = "semaphore",
):
    """Declarative resource governance decorator.

        Args:
            resource: resource name (must be registered in registry)
            mode: wait mode
                - "wait": queue wait (timeout raises AcquireTimeoutError)
                - "reject": immediate rejection (failure to acquire raises AcquireTimeoutError)
            timeout: maximum wait seconds (only mode="wait" valid, None=infinite wait)
            resource_type: resource type
                - "semaphore": concurrency control (AsyncLeaseSemaphore)
                - "bucket": rate control (AsyncTokenBucket)

        Raises:
            AcquireTimeoutError: timeout or rejected (app side maps 429/503)
            KeyError: resource not registered

        Example:
            @limited(resource="llm", mode="wait", timeout=5.0)
            async def call_llm(prompt: str):
                return await llm_client.chat(prompt)

            @limited(resource="gpu_pool", mode="reject")
            async def submit_task(data: dict):
                return await heavy_computation(data)
        """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            registry = get_registry()

            if resource_type == "semaphore":
                limiter = registry.get_semaphore(resource)

                if mode == "wait":
                    token = await limiter.acquire(timeout=timeout)
                else:  # reject
                    token = await limiter.try_acquire()
                    if token is None:
                        raise AcquireTimeoutError(
                            f"resource '{resource}' rejected (mode=reject)"
                        )

                try:
                    return await func(*args, **kwargs)
                finally:
                    # R12: shield prevents a second cancellation from
                    # swallowing the release; the task is strong-referenced
                    # so it survives GC even when the shield await is itself
                    # interrupted, with a RuntimeError fallback for
                    # loop-teardown windows (v0.1.7 — mirrors the executor's
                    # _shielded_finalize discipline).
                    release_coro = limiter.release(token)
                    try:
                        release_task = asyncio.create_task(release_coro)
                    except RuntimeError:
                        release_coro.close()
                        logger.warning(
                            "release skipped: no running event loop "
                            "(teardown phase)"
                        )
                    else:
                        _release_tasks.add(release_task)
                        release_task.add_done_callback(_release_tasks.discard)
                        release_task.add_done_callback(_retrieve_release_error)
                        await asyncio.shield(release_task)

            else:  # bucket
                limiter = registry.get_bucket(resource)

                if mode == "wait":
                    # v0.3.0: max_sleep contract correction — bucket.acquire(max_sleep=0.0)
                    # is "immediate rejection" semantics, not "infinite wait". None → pass huge value for infinite wait.
                    max_sleep = timeout if timeout is not None else 999999.0
                    await limiter.acquire(max_sleep=max_sleep)
                else:  # reject
                    ok = await limiter.try_acquire()
                    if not ok:
                        raise AcquireTimeoutError(
                            f"resource '{resource}' rejected (mode=reject)"
                        )

                # bucket no release needed (reservation is consumption)
                return await func(*args, **kwargs)

        return wrapper
    return decorator