"""Resource registry: manages global limiter instances (singleton pattern).

Usage scenarios:
- app startup registers resources (register_semaphore / register_bucket)
- @limited decorator gets resource instance from registry
- Avoids repeated limiter instance creation (saves Redis connections)
- Monitoring endpoint gets semaphore water level via status query interface (get_semaphore_status etc.)

Status query interface (core data plane unified outlet):
- get_semaphore_limit(name): preset limit (sync, pure memory read)
- get_semaphore_usage(name): real-time usage (async, approximate value)
- get_semaphore_status(name): single semaphore full status (async)
- get_all_semaphore_status(): all semaphore status aggregation (async)

Discipline: usage field is approximate value (clean expired tokens first then count, non-atomic),
only for monitoring display, forbidden for alerting. Upper framework (flow GovernorManager /
stream StreamGovernorManager) provides unified format resource status via this interface.
"""
from __future__ import annotations

import logging
from typing import Dict

import redis.asyncio as aioredis

from orditect.core.limiter.semaphore import AsyncLeaseSemaphore
from orditect.core.limiter.bucket import AsyncTokenBucket

logger = logging.getLogger(__name__)


class LimiterRegistry:
    """Global limiter registry (singleton).

    Responsibilities:
    - Register/get distributed semaphore and token bucket instances (idempotent registration)
    - Provide semaphore status query interface (preset limit + real-time water level)

    Status query return format (aligned with flow/stream):
        {
            "name": "default_stream_llm",
            "limit": 30,
            "usage": 15,           # approximate value, forbidden for alerting
            "available": 15,
            "utilization": "50.0%",
        }
    """

    def __init__(self):
        self._semaphores: Dict[str, AsyncLeaseSemaphore] = {}
        self._buckets: Dict[str, AsyncTokenBucket] = {}

    def register_semaphore(
        self,
        name: str,
        client: aioredis.Redis,
        limit: int,
        lease_time: float = 30.0,
        **kwargs,
    ) -> AsyncLeaseSemaphore:
        """Register semaphore (idempotent).

        Args:
            name: resource name (e.g. "llm", "ocr")
            client: Redis client
            limit: concurrency limit
            lease_time: lease duration (seconds)
            **kwargs: other parameters (renew_interval, hooks, key_prefix)

        Returns:
            AsyncLeaseSemaphore instance
        """
        if name in self._semaphores:
            existing = self._semaphores[name]
            # v0.3.3 (B4): parameter drift explicit — idempotent semantics unchanged (first wins),
            # but "dynamic capacity adjustment" drift intent no longer silently swallowed
            if existing.limit != int(limit) or existing.lease_time != float(lease_time):
                logger.warning(
                    f"Re-registration with different params ignored: {name} "
                    f"(limit {existing.limit} vs {limit}, "
                    f"lease_time {existing.lease_time} vs {lease_time}). "
                    f"First registration wins; call clear() first if "
                    f"reconfiguration is intended."
                )
            return existing
        self._semaphores[name] = AsyncLeaseSemaphore(
            client, name, limit, lease_time, **kwargs
        )
        return self._semaphores[name]

    def register_bucket(
        self,
        name: str,
        client: aioredis.Redis,
        capacity: int,
        refill_amount: int,
        refill_frequency: float,
        **kwargs,
    ) -> AsyncTokenBucket:
        """Register token bucket (idempotent).

        Args:
            name: resource name (e.g. "llm_rpm", "api_qps")
            client: Redis client
            capacity: bucket capacity (allowed burst)
            refill_amount: tokens added per refill
            refill_frequency: refill interval (seconds)
            **kwargs: other parameters (hooks, key_prefix)

        Returns:
            AsyncTokenBucket instance
        """
        if name in self._buckets:
            existing = self._buckets[name]
            if (existing.capacity != int(capacity)
                    or existing.refill_amount != int(refill_amount)
                    or existing.refill_frequency != float(refill_frequency)):
                logger.warning(
                    f"Re-registration with different params ignored: {name} "
                    f"(capacity {existing.capacity} vs {capacity}, "
                    f"refill_amount {existing.refill_amount} vs {refill_amount}, "
                    f"refill_frequency {existing.refill_frequency} vs {refill_frequency}). "
                    f"First registration wins; call clear() first if "
                    f"reconfiguration is intended."
                )
            return existing
        self._buckets[name] = AsyncTokenBucket(
            client, name, capacity, refill_amount, refill_frequency, **kwargs
        )
        return self._buckets[name]

    def get_semaphore(self, name: str) -> AsyncLeaseSemaphore:
        """Get semaphore (raises KeyError if not registered)."""
        return self._semaphores[name]

    def get_bucket(self, name: str) -> AsyncTokenBucket:
        """Get token bucket (raises KeyError if not registered)."""
        return self._buckets[name]

    def has_semaphore(self, name: str) -> bool:
        """Check if semaphore is registered."""
        return name in self._semaphores

    def has_bucket(self, name: str) -> bool:
        """Check if token bucket is registered."""
        return name in self._buckets

    # ---------- Status query interface ----------

    def get_semaphore_limit(self, name: str) -> int:
        """Get preset limit for specified semaphore (sync, pure memory read, no Redis round trip).

        Args:
            name: resource name

        Returns:
            Concurrency limit set at registration (limit)

        Raises:
            KeyError: semaphore not registered
        """
        return self._semaphores[name].limit

    async def get_semaphore_usage(self, name: str) -> int:
        """Get real-time usage for specified semaphore (approximate value).

        Args:
            name: resource name

        Returns:
            Current occupied slot count (clean expired tokens first then count)

        Raises:
            KeyError: semaphore not registered

        Note: return value is non-atomic approximate value, only for monitoring
        display, forbidden for alerting.
        """
        return await self._semaphores[name].in_use()

    async def get_semaphore_status(self, name: str) -> dict:
        """Get full status for single semaphore (preset limit + real-time water level).

        Args:
            name: resource name

        Returns:
            {
                "name": "default_stream_llm",
                "limit": 30,
                "usage": 15,           # approximate value, forbidden for alerting
                "available": 15,
                "utilization": "50.0%",
            }

        Raises:
            KeyError: semaphore not registered

        Note: usage/available/utilization based on approximate water level,
        only for monitoring display.
        """
        sem = self._semaphores[name]
        usage = await sem.in_use()
        limit = sem.limit
        utilization = f"{usage / limit * 100:.1f}%" if limit > 0 else "0.0%"
        return {
            "name": name,
            "limit": limit,
            "usage": usage,
            "available": max(0, limit - usage),
            "utilization": utilization,
        }

    async def get_all_semaphore_status(self) -> Dict[str, dict]:
        """Get status list for all registered semaphores.

        Returns:
            {name: status_dict} dictionary; returns {} when no semaphore registered.
            Token buckets not included in this result (reservation is consumption, no water level concept).

        Note: each status_dict's usage is approximate value, only for monitoring display.
        """
        result: Dict[str, dict] = {}
        for name in self._semaphores:
            result[name] = await self.get_semaphore_status(name)
        return result

    def clear(self):
        """Clear registry (for testing)."""
        self._semaphores.clear()
        self._buckets.clear()


# Global singleton
_registry = LimiterRegistry()


def get_registry() -> LimiterRegistry:
    """Get global registry instance."""
    return _registry