"""In-memory hot-path doubles (demo only, zero infrastructure).

Same three doubles as examples/mvp: swapping them for the production
Redis trio (TaskRedisDB / AsyncLeaseSemaphore / AdmissionQuotaRedisDB)
requires no business-code change. See examples/mvp/infra.py for the
full rationale.
"""

from __future__ import annotations

import uuid
from typing import Any

from orditect.flow.exceptions import AcquireTimeoutError


class InMemoryGovernor:
    """In-memory ResourceGovernorProtocol (per-resource capacity)."""

    def __init__(self, capacity: int = 2) -> None:
        self.capacity = capacity
        self._in_use: dict[str, int] = {}
        self._tokens: dict[str, str] = {}

    async def acquire(self, resource: str, timeout: float | None = None) -> str:
        if self._in_use.get(resource, 0) >= self.capacity:
            raise AcquireTimeoutError(f"resource full: {resource}")
        self._in_use[resource] = self._in_use.get(resource, 0) + 1
        token = f"tok-{resource}-{uuid.uuid4().hex[:8]}"
        self._tokens[token] = resource
        return token

    async def try_acquire(self, resource: str) -> str | None:
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        if self._tokens.pop(token, None) is not None:
            self._in_use[resource] = max(0, self._in_use.get(resource, 0) - 1)

    async def get_usage(self, resource: str) -> int:
        return self._in_use.get(resource, 0)


class InMemoryQuota:
    """In-memory quota DB matching the BudgetLedger duck type.

    Implements the dual-habitat idempotency discipline: reserving with an
    already-seen task_id (the call_id) returns already_reserved and never
    double-charges.
    """

    def __init__(self) -> None:
        self._pending: dict[str, int] = {}
        self._reserved: dict[str, dict[str, int]] = {}

    async def reserve_units(
        self,
        *,
        scope: str,
        task_id: str,
        units: int,
        max_units: int,
        task_ttl_sec: int | None = None,
    ) -> dict[str, Any]:
        reserved = self._reserved.setdefault(scope, {})
        if task_id in reserved:
            return {"ok": True, "reason": "already_reserved"}
        current = self._pending.get(scope, 0)
        if current + units > max_units:
            return {"ok": False, "reason": "limit_exceeded"}
        reserved[task_id] = units
        self._pending[scope] = current + units
        return {"ok": True, "reason": "", "current": self._pending[scope]}

    async def get_pending_units(self, *, scope: str) -> int:
        return self._pending.get(scope, 0)