"""RedisDB base class.

Lua scripts lazy registration, DI mode contract fulfilled.

Changes:
  - #1: _json_merge_script changed to lazy registration on first use
    (_get_json_merge_script), update() no longer fails in DI mode
    (RedisDB(client=...)), fulfilling "dependency injection no need to connect()"
    contract. connect() in self-managed mode still warm-up registers
    (behavior unchanged).

Preserved changes:
  - reconnect() physically deleted (R3 wrap-up): redis-py comes with automatic
    reconnection, manual reconnect would kill in-flight commands of other
    gates.
  - DI mode close() raises InvalidUsageError (pool lifecycle belongs to
    RedisPoolManager).
  - Follows previous version: R5 (expiry is not None) / B9 (health_check_interval) /
    json_merge.
"""
from __future__ import annotations

import json

import redis.asyncio as aioredis

from orditect.core._lua import load_lua
from orditect.core.errors import InvalidUsageError


class RedisDB:
    """Redis client wrapper base class (supports dependency injection).

    Two usage modes:
    1. Dependency injection (recommended): RedisDB(client=redis_client)
       - Connection pool managed by RedisPoolManager
       - No need to call connect() (script methods lazy register,
         truly connect-free)
       - close() raises InvalidUsageError (pool lifecycle belongs to PoolManager)
    2. Self-managed connection pool (backward compatible): RedisDB(redis_url="...")
       - Creates own connection pool
       - Needs to call connect() / close()

    Reconnection handled automatically by redis-py (retry_on_timeout
    default on). Business layer needing retry mechanism should use tenacity
    or custom retry decorator.
    """


    def __init__(
        self,
        redis_url: str | None = None,
        client: aioredis.Redis | None = None,
        max_connections: int = 200,
        timeout: int = 300,
        default_expire_time: int = 604800,
    ):
        if client is not None:
            self.client = client
            self.pool = None
            self._owns_pool = False
            self.redis_url = None
            self.max_connections = 0
        elif redis_url is not None:
            self.redis_url = redis_url
            self.max_connections = int(max_connections)
            self.pool = None
            self.client = None
            self._owns_pool = True
        else:
            raise ValueError("Must provide redis_url or client")

        self.timeout = int(timeout)
        self.default_expire_time = int(default_expire_time)
        self._json_merge_script = None

    async def connect(self):
        """Establish connection pool + client (only self-managed mode needs).

        DI mode is no-op: scripts lazy register on first use (see
        _get_json_merge_script), fulfilling "dependency injection no need to connect()"
        contract.
        """
        if not self._owns_pool:
            return

        if self.pool is None:
            self.pool = aioredis.ConnectionPool.from_url(
                self.redis_url,
                decode_responses=True,
                max_connections=self.max_connections,
                health_check_interval=30,  # B9
            )
        self.client = aioredis.Redis(connection_pool=self.pool)
        # warm-up (self-managed mode behavior unchanged); DI mode lazy registration fallback
        self._json_merge_script = self.client.register_script(load_lua("json_merge.lua"))

    def _get_json_merge_script(self):
        """Script lazy registration — update() usable in DI mode."""
        if self._json_merge_script is None:
            if self.client is None:
                raise RuntimeError("RedisDB not connected: call connect() first")
            self._json_merge_script = self.client.register_script(load_lua("json_merge.lua"))
        return self._json_merge_script

    async def close(self):
        """Close connection pool.

        DI mode changed from silent skip to raising InvalidUsageError —
        connection pool lifecycle belongs to RedisPoolManager, business side
        mistakenly calls close should immediately expose.
        """
        if not self._owns_pool:
            raise InvalidUsageError(
                "close() on dependency-injected client: "
                "pool lifecycle is managed by RedisPoolManager, "
                "use pool_manager.close_all() instead"
            )
        if self.pool:
            await self.pool.disconnect(inuse_connections=True)

    async def set_with_expiry(self, key, value, expiry=None):
        """Write key-value pair and set expiry (value auto JSON serialized)."""
        ex = int(expiry if expiry is not None else self.default_expire_time)  # R5
        await self.client.set(key, json.dumps(value, ensure_ascii=False), ex=ex)

    async def get(self, key):
        """Read key value (returns raw string, caller JSON deserializes)."""
        return await self.client.get(key)

    async def update(self, key, new_data: dict, expiry=None):
        """Atomic read-modify-write (json_merge.lua, concurrency-safe).

        Raises:
            KeyError: key does not exist
            ValueError: existing value is not JSON object / input not serializable
            RuntimeError: not connected (self-managed mode not connect)
        """
        ex = int(expiry if expiry is not None else self.default_expire_time)  # R5
        try:
            payload = json.dumps(new_data, ensure_ascii=False)
        except (TypeError, ValueError) as e:
            raise ValueError(f"new_data not JSON-serializable: {e}") from e

        res_raw = await self._get_json_merge_script()(
            keys=[key],
            args=[payload, str(ex)],
        )

        try:
            res = json.loads(res_raw) if isinstance(res_raw, str) else res_raw
        except Exception:
            res = {"ok": False, "err": f"BAD_SCRIPT_RESPONSE:{res_raw}"}

        if not res.get("ok"):
            err = res.get("err", "UNKNOWN")
            if err == "NOT_FOUND":
                raise KeyError(f"key not exists: {key}")
            if err in ("NOT_A_JSON_OBJECT", "BAD_UPDATES_JSON"):
                raise ValueError(f"update failed: {err} (key={key})")
            raise RuntimeError(f"update failed: {err}")