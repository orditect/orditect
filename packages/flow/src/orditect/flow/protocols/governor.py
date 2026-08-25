"""Abstract interface for resource governance."""
from typing import Protocol, Optional


class ResourceGovernorProtocol(Protocol):
    """Resource governance protocol (defined by taskflow, implemented by taskbase).

    Responsibilities:
    - Control concurrent access to resources
    - Acquire/release resource tokens
    - Timeout control
    """

    async def acquire(
            self,
            resource: str,
            timeout: Optional[float] = None,
    ) -> str:
        """Acquire a resource token (blocking wait).

        Args:
            resource: Resource name (e.g. "llm", "task_execution")
            timeout: Maximum wait time in seconds, None means infinite wait

        Returns:
            Resource token (for subsequent release)

        Raises:
            AcquireTimeoutError: Timeout while acquiring the resource
        """
        ...

    async def try_acquire(self, resource: str) -> Optional[str]:
        """Try to acquire a resource token (non-blocking).

        Args:
            resource: Resource name

        Returns:
            Resource token if successful, or None if failed
        """
        ...

    async def release(self, resource: str, token: str) -> None:
        """Release a resource token.

        Args:
            resource: Resource name
            token: Resource token (returned by acquire)
        """
        ...

    async def get_usage(self, resource: str) -> int:
        """Get the current usage of a resource (approximate).

        Args:
            resource: Resource name

        Returns:
            Current usage (approximate, for monitoring purposes only)
        """
        ...