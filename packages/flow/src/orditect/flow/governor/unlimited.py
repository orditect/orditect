"""Unlimited resource governance (for testing)"""
import uuid
from typing import Optional

from orditect.flow.protocols.governor import ResourceGovernorProtocol


class UnlimitedGovernor(ResourceGovernorProtocol):
    """Unlimited resource governance (for testing)

        Features:
        - No concurrency control
        - All acquire calls succeed immediately
        - All try_acquire calls succeed immediately
        - release is a no-op

        Applicable scenarios:
        - Unit tests
        - Performance tests
        - Scenarios that do not require concurrency control
        """

    async def acquire(
            self,
            resource: str,
            timeout: Optional[float] = None,
    ) -> str:
        """Acquire a resource token (succeeds immediately)"""
        return f"unlimited-{resource}-{uuid.uuid4().hex[:8]}"

    async def try_acquire(self, resource: str) -> Optional[str]:
        """Attempt to acquire a resource token (succeeds immediately)"""
        return f"unlimited-{resource}-{uuid.uuid4().hex[:8]}"

    async def release(self, resource: str, token: str) -> None:
        """Release a resource token (no-op)"""
        pass

    async def get_usage(self, resource: str) -> int:
        """Get current usage of the resource (always returns 0)"""
        return 0