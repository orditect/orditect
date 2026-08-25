"""Lease + watchdog shared infrastructure.

Design goals:
  - LeaseGuard: watchdog renewal coroutine (periodically calls sem_refresh.lua)
  - Auto-stop on renewal failure (token already recycled)
  - Integrated with AsyncLeaseSemaphore, achieving "lease doesn't expire during holding"
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from orditect.core.limiter.semaphore import AsyncLeaseSemaphore, LeaseToken

logger = logging.getLogger(__name__)


class LeaseGuard:
    """Lease guard: periodically renews, prevents lease expiry during holding.

    Usage example:
        guard = LeaseGuard(semaphore, token, renew_interval=lease_time/3)
        await guard.start()
        # ... business logic ...
        await guard.stop()
    """

    def __init__(
        self,
        semaphore: "AsyncLeaseSemaphore",
        token: "LeaseToken",
        renew_interval: float,
    ):
        """
        Args:
            semaphore: AsyncLeaseSemaphore instance
            token: LeaseToken to renew
            renew_interval: renewal interval (seconds), recommended lease_time / 3
        """
        self.semaphore = semaphore
        self.token = token
        self.renew_interval = float(renew_interval)

        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self):
        """Start watchdog coroutine."""
        if self._task is not None:
            raise RuntimeError("LeaseGuard already started")

        self._task = asyncio.create_task(self._watchdog_loop())
        logger.debug(
            f"LeaseGuard started: resource={self.token.resource} "
            f"token={self.token.value[:8]}... interval={self.renew_interval}s"
        )

    async def stop(self):
        """Stop watchdog coroutine (idempotent)."""
        if self._task is None:
            return

        self._stop_event.set()
        try:
            await asyncio.wait_for(self._task, timeout=1.0)
        except asyncio.TimeoutError:
            logger.warning(
                f"LeaseGuard stop timeout: resource={self.token.resource} "
                f"token={self.token.value[:8]}..."
            )
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        finally:
            self._task = None
            logger.debug(
                f"LeaseGuard stopped: resource={self.token.resource} "
                f"token={self.token.value[:8]}..."
            )

    async def _watchdog_loop(self):
        """watchdog main loop: periodic renewal.

        v0.3.3 (S5): renewal wait changed from asyncio.sleep to stop_event.wait
        timeout — stop() sets immediate wake-up exit (previously sleep didn't
        respond to event, stop worst waited full renew_interval period, plus
        1s wait_for strong cancel fallback path).
        """
        try:
            while not self._stop_event.is_set():
                try:
                    # Event-driven wait: stop() sets immediate wake; timeout expiry renews
                    try:
                        await asyncio.wait_for(
                            self._stop_event.wait(),
                            timeout=self.renew_interval,
                        )
                        break  # stop_event set, exit
                    except asyncio.TimeoutError:
                        pass  # normal expiry, execute renewal

                    result = await self.semaphore._refresh_script(
                        keys=[self.semaphore.key],
                        args=[self.token.value, str(self.semaphore.lease_time)],
                    )

                    if result == 0:
                        logger.warning(
                            f"LeaseGuard: token expired, stopping watchdog: "
                            f"resource={self.token.resource} token={self.token.value[:8]}..."
                        )
                        break

                    logger.debug(
                        f"LeaseGuard: renewed lease: resource={self.token.resource} "
                        f"token={self.token.value[:8]}..."
                    )

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(
                        f"LeaseGuard: renew failed (will retry): {e}",
                        exc_info=True,
                    )
        finally:
            # v0.3.2 (#10): watchdog exit (stop/recycle/exception) auto-deregisters.
            # release's pop and this pop idempotent coexist (first to take effect wins).
            self.semaphore._guards.pop(self.token.value, None)

    async def __aenter__(self):
        """async with support."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """async with exit auto-stop."""
        await self.stop()