"""AsyncLeaseSemaphore — distributed semaphore with ZSET + per-token lease.

P2 surgery completed:
  - ✅ watchdog renewal (fixes false mutual exclusion: lease=10s vs task 5h)
  - ✅ try_acquire (@limited mode="reject" → 429)
  - ✅ in_use() (water level: Prometheus collector / frontend dashboard)
  - ✅ hooks (observation hooks, call site must try/except)
  - ✅ {hashtag} key naming (free cluster slot compatibility)
  - ✅ monotonic timing (D1: NTP time synchronization doesn't affect timeout)
  - ⏳ shield release (D3: CancelledError release not swallowed, caller responsible)

Data structure:
  - key: ZSET, member=token, score=acquisition timestamp (milliseconds)
  - Expiry determination: score < now - lease_ms → slot recycled
  - watchdog periodically refreshes score, prevents expiry during holding

Acceptance tests (tests/integration/test_lease_semaphore.py):
  A crash recovery: kill holding task, slot must auto-recover within lease time ✅
  B no doubling: full load + hold exceeding lease + watchdog alive, capacity no doubling no failure ✅
  C no leakage: timeout path zero leakage, zero false holding ✅
"""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Any, Sequence

import redis.asyncio as aioredis

from orditect.core._lua import load_lua
from orditect.core.errors import AcquireTimeoutError
from orditect.core.limiter.hooks import LimiterHooks
from orditect.core.limiter.lease import LeaseGuard


@dataclass(frozen=True)
class LeaseToken:
    """Credential for one successful acquisition. acquired_at uses monotonic, for held duration and hooks."""
    resource: str
    value: str
    acquired_at: float


class AsyncLeaseSemaphore:
    """Distributed lease semaphore (ZSET scheme + watchdog renewal)."""

    def __init__(
        self,
        client: aioredis.Redis,
        name: str,
        limit: int,
        lease_time: float = 30.0,
        *,
        renew_interval: float | None = None,
        hooks: Sequence[LimiterHooks] = (),
        key_prefix: str = "{ftb}:semaphore",
    ):
        """
        Args:
            client: Redis client (directly takes redis.asyncio.Redis, doesn't depend on app wrapper class)
            name: resource name (e.g. "llm", "ocr")
            limit: concurrency limit
            lease_time: lease duration (seconds), slots not released within timeout auto-recycled
            renew_interval: watchdog renewal interval (seconds), default lease_time / 3
            hooks: observation hook list (called wrapped in try/except)
            key_prefix: key prefix ({ftb} is hashtag, ensures cluster same slot)
        """
        self.client = client
        self.name = name
        self.limit = int(limit)
        self.lease_time = float(lease_time)
        self.renew_interval = float(renew_interval) if renew_interval else lease_time / 3.0
        self.hooks = list(hooks)
        self.key_prefix = key_prefix

        self._acquire_script = client.register_script(load_lua("sem_acquire.lua"))
        self._release_script = client.register_script(load_lua("sem_release.lua"))
        self._refresh_script = client.register_script(load_lua("sem_refresh.lua"))

        # Store LeaseGuard corresponding to each token (for stopping watchdog on release)
        self._guards: dict[str, LeaseGuard] = {}

    @property
    def key(self) -> str:
        return f"{self.key_prefix}:{self.name}"

    async def _call_hooks(self, method: str, *args):
        """Call hooks (wrapped in try/except, monitoring never blocks business)."""
        for hook in self.hooks:
            try:
                fn = getattr(hook, method, None)
                if fn:
                    await fn(*args)
            except Exception:
                pass  # monitoring failure doesn't affect business

    @staticmethod
    def _is_match(result: Any, token_value: str) -> bool:
        """Normalize a Lua acquire-script return value for comparison with
        the expected token (S4: bytes clients, decode_responses=False).

        Args:
            result: raw value returned by the acquire Lua script
                (str | bytes | bytearray | None)
            token_value: the token we attempted to acquire with

        Returns:
            True when the script granted this token (after bytes/str
            normalization); False when the slot was full (None or mismatch).
        """
        if isinstance(result, (bytes, bytearray)):
            result = result.decode("utf-8", errors="ignore")
        return result == token_value

    async def acquire(self, timeout: float | None = None) -> LeaseToken:
        """Blocking wait for acquisition. Timeout raises AcquireTimeoutError. Starts watchdog on success.

        Lua return value normalized via _is_match (bytes/str) — fixes decode_responses=False client acquire infinite loop to timeout.
        Post-Lua steps (in_use network / guard creation start) have overall fallback: any failure immediately returns slot then raises — caller failure to acquire token can't release, no fallback means slot wasted for one lease period.

        Args:
            timeout: maximum wait seconds, None means infinite wait

        Returns:
            LeaseToken (includes resource/value/acquired_at)

        Raises:
            AcquireTimeoutError: still not acquired after timeout
        """
        token_value = str(uuid.uuid4())
        start = time.monotonic()
        delay = 0.05
        max_delay = 0.5

        while True:
            # Try acquisition (Lua atomic operation: clean expired + ZCARD + ZADD)
            result = await self._acquire_script(
                keys=[self.key],
                args=[str(self.limit), str(self.lease_time), token_value],
            )

            if self._is_match(result, token_value):
                waited = time.monotonic() - start
                token = LeaseToken(
                    resource=self.name,
                    value=token_value,
                    acquired_at=time.monotonic(),
                )
                try:
                    in_use = await self.in_use()
                    # Start watchdog renewal
                    guard = LeaseGuard(self, token, self.renew_interval)
                    await guard.start()
                except BaseException:
                    # S3: slot already written to ZSET but caller cannot obtain token — immediately return.
                    # BaseException covers CancelledError window (after Lua returns, before _guards registration is canceled);
                    # when release script itself fails, exceptions are exposed cumulatively, not silent.
                    await self._release_script(keys=[self.key], args=[token_value])
                    raise
                self._guards[token_value] = guard

                await self._call_hooks("on_acquired", self.name, waited, in_use)
                return token

            # Not acquired, check timeout
            if timeout is not None:
                waited = time.monotonic() - start
                if waited >= timeout:
                    await self._call_hooks("on_timeout", self.name, waited)
                    raise AcquireTimeoutError(
                        f"acquire semaphore timeout: {self.name} (waited {waited:.2f}s)"
                    )

            # Backoff wait
            await asyncio.sleep(delay)
            delay = min(max_delay, delay * 2)

    async def try_acquire(self) -> LeaseToken | None:
        """Non-blocking acquisition. Returns None immediately if failure to acquire (triggers on_rejected).

        Synchronized with acquire() S3/S4 fixes (normalized comparison + failure fallback return).

        Returns:
            LeaseToken (success) / None (failure)
        """
        token_value = str(uuid.uuid4())
        result = await self._acquire_script(
            keys=[self.key],
            args=[str(self.limit), str(self.lease_time), token_value],
        )

        if not self._is_match(result, token_value):
            await self._call_hooks("on_rejected", self.name)
            return None

        token = LeaseToken(
            resource=self.name,
            value=token_value,
            acquired_at=time.monotonic(),
        )
        try:
            in_use = await self.in_use()
            # Start watchdog renewal
            guard = LeaseGuard(self, token, self.renew_interval)
            await guard.start()
        except BaseException:
            await self._release_script(keys=[self.key], args=[token_value])
            raise
        self._guards[token_value] = guard

        await self._call_hooks("on_acquired", self.name, 0.0, in_use)
        return token

    async def release(self, token: LeaseToken) -> None:
        """Release slot (idempotent). Stop watchdog first then release.

        (D3 concluded): shield protection carried by call path — sem.hold()'s
        __aexit__ already has built-in shield; manual acquire/release paired callers,
        at potentially interrupted positions should asyncio.shield(sem.release(token)) themselves.
        """
        # Stop watchdog
        guard = self._guards.pop(token.value, None)
        if guard:
            await guard.stop()

        # Release slot
        await self._release_script(keys=[self.key], args=[token.value])
        held = time.monotonic() - token.acquired_at
        await self._call_hooks("on_released", self.name, held)

    async def in_use(self) -> int:
        """Current water level approximate value: clean expired tokens first then count.

        Expiry determination uses server-side clock (same clock source as acquire/refresh),
        avoids client clock drift causing monitoring water level and determination logic inconsistency.

        Note: non-atomic operation, only for monitoring display, forbidden for alerting.
        """
        # Server-side clock (redis TIME command)
        server_time = await self.client.time()
        now_ms = int(server_time[0]) * 1000 + int(server_time[1]) // 1000
        lease_ms = int(self.lease_time * 1000)
        await self.client.zremrangebyscore(self.key, "-inf", now_ms - lease_ms)
        return await self.client.zcard(self.key)

    def hold(self, timeout: float | None = None) -> "SemaphoreHold":
        """Return independent async context manager (new object per call).

        Replaces deleted bare `async with sem:`.
        Semaphore instance shared by multiple coroutines, context state must be
        hung on independent object produced per call — token stored in respective
        instance, naturally concurrency-safe.

        Usage:
            async with sem.hold():
                await long_running_llm_call()

            async with sem.hold(timeout=5.0) as token:
                ...
        """
        return SemaphoreHold(self, timeout)

class SemaphoreHold:
    """Independent context object returned by sem.hold() (single use).

    (S1/S2):
    - token stored in this instance (not shared sem instance), concurrent calls do not overwrite each other.
    - __aexit__ uses shield(release): when outer coroutine is cancelled, release is not swallowed.
      shield semantics: inner release coroutine continues completion, CancelledError still
      raised to with caller — release and cancel propagation both handled.
    """

    def __init__(self, sem: AsyncLeaseSemaphore, timeout: float | None):
        self._sem = sem
        self._timeout = timeout
        self._token: LeaseToken | None = None

    async def __aenter__(self) -> LeaseToken:
        self._token = await self._sem.acquire(timeout=self._timeout)
        return self._token

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._token is not None:
            await asyncio.shield(self._sem.release(self._token))
            self._token = None