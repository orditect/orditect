"""In-memory hot-path doubles (demo only, zero infrastructure).

Same three doubles as examples/mvp, plus the v0.1.1 dependency-governance
primitives DependencyGovernor requires (active-children sets, remaining-deps
counters, cancel-vote sets). Swapping them for the production Redis trio
requires no business-code change.
"""

from __future__ import annotations

import copy
import uuid
from typing import Any

from orditect.flow.exceptions import (
    AcquireTimeoutError,
    InvalidStateTransitionError,
)


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
    """In-memory quota DB matching the BudgetLedger duck type."""

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


class InMemoryTaskStorage:
    """In-memory task store matching the flow TaskStorageProtocol, plus
    the v0.1.1 dependency-governance primitives (DependencyGovernor's
    hot-path substrate).
    """

    TERMINAL: frozenset[str] = frozenset({"succeeded", "failed", "cancelled"})

    def __init__(self) -> None:
        self._tasks: dict[str, dict[str, Any]] = {}
        self._children: dict[str, set[str]] = {}
        # dependency-governance state
        self._active_children: dict[str, set[str]] = {}
        self._remaining_deps: dict[str, int] = {}
        self._cancel_votes: dict[str, set[str]] = {}

    # ----- flow TaskStorageProtocol -----

    async def initialize_task(
        self,
        task_id: str,
        initial_status: str = "pending",
        *,
        parent_task_id: str | None = None,
        if_not_exists: bool = False,
        **kwargs: Any,
    ) -> bool:
        if if_not_exists and task_id in self._tasks:
            return False
        record: dict[str, Any] = {
            "status": initial_status,
            "cancel_requested": False,
            "execution_id": f"exec-{uuid.uuid4().hex[:12]}",
        }
        if parent_task_id is not None:
            record["parent_task_id"] = parent_task_id
            self._children.setdefault(parent_task_id, set()).add(task_id)
        self._tasks[task_id] = record
        return True

    async def update_task(
        self, task_id: str, updates: dict[str, Any], **kwargs: Any
    ) -> None:
        if task_id in self._tasks:
            self._tasks[task_id].update(copy.deepcopy(updates))

    async def get_task(self, task_id: str) -> dict[str, Any]:
        return copy.deepcopy(self._tasks.get(task_id, {}))

    async def request_cancel(self, task_id: str) -> bool:
        record = self._tasks.get(task_id)
        if record is None:
            return False
        record["cancel_requested"] = True
        return True

    async def list_children(self, parent_task_id: str) -> list[str]:
        return sorted(self._children.get(parent_task_id, set()))

    async def list_task_ids_by_status(
        self, status: str, *, limit: int | None = None
    ) -> list[str]:
        ids = sorted(
            tid for tid, rec in self._tasks.items() if rec.get("status") == status
        )
        return ids if limit is None else ids[:limit]

    async def bulk_get_tasks(self, task_ids: list[str]) -> list[dict[str, Any]]:
        return [await self.get_task(tid) for tid in task_ids]

    async def reopen_task(
        self,
        task_id: str,
        *,
        initial_status: str = "pending",
        expiry: Any = None,
    ) -> str:
        record = self._tasks[task_id]
        old_status = record.get("status", "")
        if old_status not in self.TERMINAL:
            raise InvalidStateTransitionError(f"not terminal: {old_status}")
        record.setdefault("previous_execution_ids", []).append(
            record["execution_id"]
        )
        record["execution_id"] = f"exec-{uuid.uuid4().hex[:12]}"
        record["previous_status"] = old_status
        record["status"] = initial_status
        record["cancel_requested"] = False
        record.pop("result", None)
        record.pop("error", None)
        return record["execution_id"]

    # ----- v0.1.1 dependency-governance primitives -----

    async def sadd_active_child(self, parent_id: str, child_id: str) -> None:
        self._active_children.setdefault(parent_id, set()).add(child_id)

    async def srem_active_child(self, parent_id: str, child_id: str) -> None:
        self._active_children.get(parent_id, set()).discard(child_id)

    async def get_active_children(self, parent_id: str) -> list[str]:
        return sorted(self._active_children.get(parent_id, set()))

    async def set_remaining_deps(self, task_id: str, n: int) -> None:
        self._remaining_deps[task_id] = int(n)

    async def decr_remaining_deps(self, task_id: str) -> int:
        new_value = self._remaining_deps.get(task_id, 0) - 1
        self._remaining_deps[task_id] = new_value
        return new_value

    async def get_remaining_deps(self, task_id: str) -> int:
        return self._remaining_deps.get(task_id, 0)

    async def list_ready_dep_tasks(self, *, status: str | None = None) -> list[str]:
        candidates = [tid for tid, n in self._remaining_deps.items() if n <= 0]
        if status is None:
            return sorted(candidates)
        return sorted(
            tid
            for tid in candidates
            if self._tasks.get(tid, {}).get("status") == status
        )

    async def vote_and_check_threshold(
        self, child_id: str, parent_id: str, threshold: int
    ) -> bool:
        votes = self._cancel_votes.setdefault(child_id, set())
        votes.add(parent_id)
        return len(votes) >= threshold

    async def get_cancel_votes(self, child_id: str) -> list[str]:
        return sorted(self._cancel_votes.get(child_id, set()))

    async def clear_cancel_votes(self, child_id: str) -> None:
        self._cancel_votes.pop(child_id, None)