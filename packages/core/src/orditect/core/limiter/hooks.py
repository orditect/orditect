"""Observation hook protocol.

Design discipline (corresponding to observability goals 4/5):
1. limiter calls hooks must be wrapped in try/except — monitoring never
   allowed to block or crash business;
2. package doesn't know Prometheus / Langfuse existence, app side injects
   implementation;
3. in_use_approx is non-atomic approximate value, forbidden for alerting.
"""
from typing import Protocol, runtime_checkable


@runtime_checkable
class LimiterHooks(Protocol):
    """Rate limiter observation hooks (app side injects implementation, package
    internal calls wrapped in try/except)."""

    async def on_acquired(self, resource: str, waited: float, in_use_approx: int) -> None:
        """Acquisition succeeded. waited=wait seconds, in_use_approx=instant
        water level after acquisition (approximate)."""
        ...

    async def on_timeout(self, resource: str, waited: float) -> None:
        """acquire timeout abandoned."""
        ...

    async def on_rejected(self, resource: str) -> None:
        """try_acquire / bucket rejected (app side maps 429)."""
        ...

    async def on_released(self, resource: str, held: float) -> None:
        """Release succeeded. held=hold seconds (monotonic measurement)."""
        ...