"""asyncio utility functions"""
import asyncio
from typing import Any, Coroutine, List, TypeVar

T = TypeVar("T")


async def run_with_timeout(
        coro: Coroutine[Any, Any, T],
        timeout: float,
        default: T | None = None,
) -> T | None:
    """Run a coroutine with a timeout.

        Args:
            coro: Coroutine
            timeout: Timeout in seconds
            default: Default return value on timeout

        Returns:
            Coroutine result or default value

        Example:
            result = await run_with_timeout(
                fetch_data(),
                timeout=5.0,
                default={"error": "timeout"},
            )
        """
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        return default


async def gather_with_concurrency(
        limit: int,
        *coros: Coroutine[Any, Any, T],
) -> List[T]:
    """Run gather with concurrency limit.

        Args:
            limit: Concurrency limit
            *coros: List of coroutines

        Returns:
            List of results

        Example:
            results = await gather_with_concurrency(
                5,  # max 5 concurrent
                fetch_data_1(),
                fetch_data_2(),
                fetch_data_3(),
                # ... more coroutines
            )
        """
    semaphore = asyncio.Semaphore(limit)

    async def run_with_semaphore(coro):
        async with semaphore:
            return await coro

    return await asyncio.gather(*[run_with_semaphore(coro) for coro in coros])