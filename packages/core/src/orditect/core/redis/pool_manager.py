"""Redis connection pool unified manager (global singleton).

Solves problems:
1. Multiple connection pool creations, scattered config → unified management
2. Cannot uniformly monitor connection pool status → get_pool_stats()
3. Connection pool capacity planning difficult → total connections controllable

Design principles:
- Singleton pattern: only one manager instance globally
- Idempotent registration: repeated registration returns same instance
- Dependency injection: all modules share same connection pool
- Unified monitoring: get_pool_stats() queries water level (approximate value)
"""
from __future__ import annotations

import logging
from typing import Dict, Optional

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class RedisPoolManager:
    """Redis connection pool manager (singleton pattern).

    Responsibilities:
    - Unified management of all Redis connection pools
    - Provide connection pool water level query (approximate value)
    - Support multiple Redis instances (e.g. cache/queue/task separation)

    Usage example:
        # app startup
        pool_manager = get_pool_manager()
        redis_client = pool_manager.register_pool(
            "default",
            redis_url="redis://localhost:6379/0",
            max_connections=200,
        )

        # All modules share this connection pool
        task_db = TaskRedisDB(client=redis_client)
        sem = AsyncLeaseSemaphore(client=redis_client, name="llm", limit=30)

        # app shutdown
        await pool_manager.close_all()
    """

    def __init__(self):
        self._pools: Dict[str, aioredis.ConnectionPool] = {}
        self._clients: Dict[str, aioredis.Redis] = {}

    def register_pool(
        self,
        name: str,
        redis_url: str,
        max_connections: int = 200,
        **kwargs,
    ) -> aioredis.Redis:
        """Register connection pool (idempotent).

        v0.3.3:
        - B3: add health_check_interval=30 (B9 previously only covered
          self-managed path, PoolManager path's NAT/LB silent disconnection
          risk remains). kwargs explicitly passed respects caller.
        - B4: re-registration parameter drift logger.warning — idempotent
          semantics unchanged (first registration wins), but drift intent no
          longer silently swallowed.

        Args:
            name: connection pool name (e.g. "default", "cache", "queue")
            redis_url: Redis connection address
            max_connections: pool capacity
            **kwargs: other parameters (socket_timeout, socket_keepalive,
                health_check_interval, etc.)

        Returns:
            aioredis.Redis client
        """
        if name in self._pools:
            existing = self._pools[name]
            if existing.max_connections != max_connections:
                logger.warning(
                    f"Re-registration with different max_connections ignored: "
                    f"pool '{name}' ({existing.max_connections} vs {max_connections}). "
                    f"First registration wins; call clear() + re-register "
                    f"if reconfiguration is intended."
                )
            return self._clients[name]

        # B3: Supplement B9 discipline to PoolManager path
        kwargs.setdefault("health_check_interval", 30)
        self._pools[name] = aioredis.ConnectionPool.from_url(
            redis_url,
            max_connections=max_connections,
            decode_responses=True,
            **kwargs,
        )
        self._clients[name] = aioredis.Redis(connection_pool=self._pools[name])
        logger.info(
            f"Redis pool registered: name={name} "
            f"max_connections={max_connections} url={redis_url}"
        )
        return self._clients[name]

    def get_client(self, name: str = "default") -> aioredis.Redis:
        """Get Redis client (raises KeyError if not registered).

        Args:
            name: connection pool name

        Returns:
            aioredis.Redis client

        Raises:
            KeyError: connection pool not registered
        """
        return self._clients[name]

    def has_pool(self, name: str) -> bool:
        """Check if connection pool is registered."""
        return name in self._pools

    async def get_pool_stats(self, name: str = "default") -> dict:
        """Get connection pool statistics (approximate value).

        Args:
            name: connection pool name

        Returns:
            {
                "name": "default",
                "max_connections": 200,
                "in_use": 15,  # approximate value
                "available": 185,  # approximate value
                "utilization": "7.5%",
            }

        Raises:
            KeyError: connection pool not registered

        Note:
            in_use is approximate value (redis-py doesn't directly expose),
            only for monitoring display, not for alerting.
        """
        if name not in self._pools:
            raise KeyError(f"Pool '{name}' not registered")

        pool = self._pools[name]

        # redis-py doesn't directly expose in_use connection count, using heuristic estimation here
        # Note: this is approximate value, only for monitoring display, not for alerting
        in_use = len(pool._in_use_connections) if hasattr(pool, "_in_use_connections") else 0
        max_conn = pool.max_connections

        return {
            "name": name,
            "max_connections": max_conn,
            "in_use": in_use,
            "available": max_conn - in_use,
            "utilization": f"{in_use / max_conn * 100:.1f}%",
        }

    async def close_all(self):
        """Close all connection pools (called on app shutdown)."""
        for name, pool in self._pools.items():
            await pool.disconnect(inuse_connections=True)
            logger.info(f"Redis pool closed: name={name}")
        self._pools.clear()
        self._clients.clear()

    def clear(self):
        """Clear registry (for testing, doesn't synchronously close connections)."""
        self._pools.clear()
        self._clients.clear()


# Global singleton
_pool_manager = RedisPoolManager()


def get_pool_manager() -> RedisPoolManager:
    """Get global connection pool manager instance."""
    return _pool_manager