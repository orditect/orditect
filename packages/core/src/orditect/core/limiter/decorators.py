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
                    # shield prevents CancelledError from swallowing release (D3 fix)
                    await asyncio.shield(limiter.release(token))

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