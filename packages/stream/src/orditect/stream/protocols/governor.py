"""Resource governance protocol (defined by taskstream, implemented by taskbase).

Loose coupling: taskstream does not import taskbase, only defines the protocol.
Taskbase's AsyncLeaseSemaphore and other implementations naturally satisfy the protocol (duck typing).
"""
from __future__ import annotations

from typing import Protocol


class ResourceGovernorProtocol(Protocol):
    """Resource governance protocol (defined by taskstream, implemented by taskbase).

    Responsibilities:
    - Control concurrent access to resources
    - Acquire/release resource tokens
    - Timeout control
    """

    async def acquire(
        self,
        resource: str,
        timeout: float | None = None,
    ) -> str:
        """Acquire a resource token (blocking wait).

        Args:
            resource: Resource name (e.g., "default_stream_llm", "vector_search")
            timeout: Maximum wait time in seconds, None means infinite wait

        Returns:
            Resource token (for subsequent release)

        Raises:
            AcquireTimeoutError: Timeout occurred before acquiring the resource
        """
        ...

    async def try_acquire(self, resource: str) -> str | None:
        """Attempt to acquire a resource token (non-blocking).

        Args:
            resource: Resource name

        Returns:
            Resource token (success) or None (failure)
        """
        ...

    async def release(self, resource: str, token: str) -> None:
        """Release a resource token.

        Args:
            resource: Resource name
            token: Resource token (value returned by acquire)
        """
        ...

    async def get_usage(self, resource: str) -> int:
        """Get current resource usage (approximate).

        Args:
            resource: Resource name

        Returns:
            Current usage (approximate, for monitoring only)
        """
        ...