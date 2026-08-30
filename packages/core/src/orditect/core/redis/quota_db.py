"""AdmissionQuotaRedisDB — generic admission quota (scope + units).

P2 fix: ZSET lease + expiry cleanup, prevents task crash causing pending_units inflation.

Changes:
  - #1: all Lua scripts lazy register, DI mode fulfills "no connect() required" contract.
  - #22: task_ttl_sec's or default swallows explicit 0 → is not None (R5 discipline complemented).
  - #21: pending_key TTL fallback (quota_reserve.lua bumps after write, quota_release.lua preserves).
  - Cluster C preparation: quota_reserve allows units=0 (accounting semantics, taskflow BudgetLedger.open consumes).
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

import redis.asyncio as aioredis

from orditect.core._lua import load_lua
from orditect.core.redis.base import RedisDB


class AdmissionQuotaRedisDB(RedisDB):
    """
        Quota control model:
        - Current occupancy: admission:{scope}:pending_units (integer, atomic inc/dec, with TTL fallback)
        - Lease record: admission:{scope}:leases (ZSET, member=task_id, score=reservation timestamp)
        - Lease details: admission:{scope}:leases:units (HASH, field=task_id, value=units)
        """

    def __init__(
        self,
        redis_url: str | None = None,
        client: aioredis.Redis | None = None,
        max_connections: int = 200,
        timeout: int = 300,
        default_expire_time: int = 604800,
        key_prefix: str = "admission",
    ):
        super().__init__(
            redis_url=redis_url,
            client=client,
            max_connections=max_connections,
            timeout=timeout,
            default_expire_time=default_expire_time,
        )
        self.key_prefix = key_prefix
        self._reserve_script = None  # lazy registration (#1)
        self._release_script = None

    async def connect(self):
        """Establish connection + register Lua scripts (warm-up; lazy registration serves as fallback for DI path)."""
        await super().connect()
        if self.client is not None:
            self._reserve_script = self.client.register_script(load_lua("quota_reserve.lua"))
            self._release_script = self.client.register_script(load_lua("quota_release.lua"))

    # ---------- Script lazy registration (#1) ----------
    def _get_reserve_script(self):
        if self._reserve_script is None:
            if self.client is None:
                raise RuntimeError("AdmissionQuotaRedisDB not connected: call connect() first")
            self._reserve_script = self.client.register_script(load_lua("quota_reserve.lua"))
        return self._reserve_script

    def _get_release_script(self):
        if self._release_script is None:
            if self.client is None:
                raise RuntimeError("AdmissionQuotaRedisDB not connected: call connect() first")
            self._release_script = self.client.register_script(load_lua("quota_release.lua"))
        return self._release_script

    def _pending_key(self, scope: str) -> str:
        s = str(scope or "").strip()
        if not s:
            raise ValueError("scope cannot be empty")
        return f"{self.key_prefix}:{s}:pending_units"

    def _leases_key(self, scope: str) -> str:
        """ZSET lease key name."""
        s = str(scope or "").strip()
        if not s:
            raise ValueError("scope cannot be empty")
        return f"{self.key_prefix}:{s}:leases"

    async def reserve_units(
        self,
        *,
        scope: str,
        task_id: str,
        units: int,
        max_units: int,
        task_ttl_sec: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Reserve quota (atomic, idempotent).

        ZSET lease + expiry cleanup, prevents task crash causing pending_units inflation.
        units=0 legal (accounting semantics: register lease slot but don't consume quota), units<0 rejected.

        Return examples:
        - Success: {"ok": True, "reason": "", "current": 30, "reserved": 10}
        - Limit exceeded: {"ok": False, "reason": "limit_exceeded", "current": 95, "reserved": 0}
        - Idempotent: {"ok": True, "reason": "already_reserved", ...}
        """
        # v0.3.2 (#22): or → is not None (explicit 0 not swallowed by default, then <=0
        ttl = int(task_ttl_sec if task_ttl_sec is not None else self.default_expire_time)
        if ttl <= 0:
            ttl = self.default_expire_time

        raw = await self._get_reserve_script()(
            keys=[self._pending_key(scope), self._leases_key(scope)],
            args=[str(int(units)), str(int(max_units)), str(int(ttl)), task_id],
        )
        return self._parse_lua_result(raw)

    async def release_units(self, *, scope: str, task_id: str) -> Dict[str, Any]:
        """Release quota (atomic, idempotent).

        Return examples:
        - Success: {"ok": True, "reason": "", "current": 20, "released": 10}
        - Idempotent: {"ok": True, "reason": "not_reserved", "current": 20, "released": 0}
        """
        raw = await self._get_release_script()(
            keys=[self._pending_key(scope), self._leases_key(scope)],
            # ARGV[2]: fallback TTL for the eternal-key guard (the release
            # side does not know the original task_ttl; a safe default).
            args=[task_id, str(self.default_expire_time)],
        )
        return self._parse_lua_result(raw)

    async def get_pending_units(self, *, scope: str) -> int:
        """Query current occupancy (direct read pending_units)."""
        v = await self.client.get(self._pending_key(scope))
        try:
            return int(v or 0)
        except Exception:
            return 0

    async def get_task_reserved_units(self, *, scope: str, task_id: str) -> int:
        """Query reserved amount for a task (read from HASH)."""
        leases_key = self._leases_key(scope)
        v = await self.client.hget(f"{leases_key}:units", task_id)
        try:
            return int(v or 0)
        except Exception:
            return 0

    @staticmethod
    def _parse_lua_result(raw: Any) -> Dict[str, Any]:
        """Parse Lua returned cjson string."""
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")
        if isinstance(raw, str):
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                pass
        if isinstance(raw, dict):
            return raw
        return {"ok": False, "reason": f"bad_lua_result:{raw}"}