"""AsyncTokenBucket — reservation + client sleep token bucket.

Algorithm pattern references redis-rate-limiters, fixes its three defects:
  1. Clock changed to server-side redis.call('TIME'), rejects client clock pollution of shared state
  2. max_sleep passed to Lua, rejection atomically decided in script — rejected requests don't burn future quota
  3. Removed +20ms unfounded magic compensation

Usage: llm_gate / ocr_gate RPM/QPS rate limiting (stacked with concurrency semaphore, each serving its own purpose).

Core advantage: zero Redis connection occupancy during wait (Lua reserves future slot, client local sleep).
"""
from __future__ import annotations

import asyncio
import time
from typing import Sequence

import redis.asyncio as aioredis

from orditect.core._lua import load_lua
from orditect.core.errors import AcquireTimeoutError
from orditect.core.limiter.hooks import LimiterHooks


class AsyncTokenBucket:
    """Distributed token bucket (reservation style)."""

    def __init__(
        self,
        client: aioredis.Redis,
        name: str,
        capacity: int,
        refill_amount: int,
        refill_frequency: float,
        *,
        hooks: Sequence[LimiterHooks] = (),
        key_prefix: str = "{ftb}:token-bucket",
    ):
        """
        Args:
            client: Redis client
            name: resource name
            capacity: bucket capacity (allowed burst)
            refill_amount: tokens added per refill
            refill_frequency: refill interval (seconds)
            hooks: observation hook list
            key_prefix: key prefix
        """
        self.client = client
        self.name = name
        self.capacity = int(capacity)
        self.refill_amount = int(refill_amount)
        self.refill_frequency = float(refill_frequency)
        self.hooks = list(hooks)
        self.key_prefix = key_prefix

        self._acquire_script = client.register_script(load_lua("bucket_acquire.lua"))

    @property
    def key(self) -> str:
        return f"{self.key_prefix}:{self.name}"

    async def _call_hooks(self, method: str, *args):
        """Call hooks (wrapped in try/except)."""
        for hook in self.hooks:
            try:
                fn = getattr(hook, method, None)
                if fn:
                    await fn(*args)
            except Exception:
                pass

    async def acquire(self, max_sleep: float = 0.0) -> float:
        """Reserve slot and local sleep until that moment (zero Redis connection occupancy during wait).

        Args:
            max_sleep: maximum allowed wait seconds, 0.0 means no wait (immediate rejection)

        Returns:
            actual wait seconds

        Raises:
            AcquireTimeoutError: estimated wait > max_sleep, Lua rejects (doesn't commit reservation)
        """
        # max_sleep=0 means no wait (immediate rejection), pass 0 to Lua script
        # max_sleep>0 means maximum allowed wait seconds
        # max_sleep=None means infinite wait (pass huge value)
        if max_sleep == 0.0:
            max_sleep_ms = 0  # immediate rejection
        elif max_sleep is None:
            max_sleep_ms = 999999999  # infinite wait
        else:
            max_sleep_ms = int(max_sleep * 1000)

        # Call Lua script to reserve slot
        result = await self._acquire_script(
            keys=[self.key],
            args=[
                str(self.capacity),
                str(self.refill_amount),
                str(self.refill_frequency),
                str(max_sleep_ms),
            ],
        )

        # v0.3.2 (#15): triple, server_now_ms same source as slot (server-side clock)
        status, slot_ms = int(result[0]), int(result[1])
        server_now_ms = int(result[2])

        if status == 0:
            await self._call_hooks("on_rejected", self.name)
            raise AcquireTimeoutError(
                f"token bucket rejected: {self.name} (estimated wait exceeds max_sleep)"
            )

        # Wait duration calculated with server-side clock — rejects client clock drift pollution (same clock source as script)
        wait_ms = max(0, slot_ms - server_now_ms)
        wait_sec = wait_ms / 1000.0

        if wait_sec > 0:
            await asyncio.sleep(wait_sec)

        await self._call_hooks("on_acquired", self.name, wait_sec, 0)
        return wait_sec


    async def try_acquire(self) -> bool:
        """Equivalent to acquire(max_sleep=0), returns False on rejection and triggers on_rejected."""
        try:
            await self.acquire(max_sleep=0.0)
            return True
        except AcquireTimeoutError:
            return False

    async def __aenter__(self):
        """async with support (infinite wait)."""
        await self.acquire(max_sleep=999999.0)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Token bucket no release needed (reservation is consumption)."""
        pass