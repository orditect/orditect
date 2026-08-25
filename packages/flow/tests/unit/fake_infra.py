"""In-memory storage double for governance unit tests (no Redis).

Implements the flow TaskStorageProtocol surface plus the v0.1.1
dependency-governance primitives and a simplified reopen_task, mirroring
the core TaskRedisDB semantics closely enough for pure-logic tests.
"""

from __future__ import annotations

import copy
import uuid
from typing import Any

from orditect.flow.exceptions import (
    InvalidStateTransitionError,
    TaskNotFoundError,
)

_FAKE_TERMINAL_WORDS: frozenset[str] = frozenset({"succeeded", "failed", "cancelled"})


class FakeGovernanceStorage:
    """In-memory storage double for DependencyGovernor / executor tests."""

    def __init__(self) -> None:
        self._tasks: dict[str, dict[str, Any]] = {}
        self._children: dict[str, set[str]] = {}  # parent_id -> child ids
        self._active_children: dict[str, set[str]] = {}
        self._remaining_deps: dict[str, int] = {}
        self._cancel_votes: dict[str, set[str]] = {}
        self._result_consumers: dict[str, set[str]] = {}

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
        rec: dict[str, Any] = {
            "status": initial_status,
            "reason": "",
            "payload": {},
            "cancel_requested": False,
            "execution_id": f"exec-{uuid.uuid4().hex[:12]}",
        }
        if parent_task_id is not None:
            rec["parent_task_id"] = parent_task_id
            self._children.setdefault(parent_task_id, set()).add(task_id)
        self._tasks[task_id] = rec
        return True

    async def update_task(
        self,
        task_id: str,
        updates: dict[str, Any],
        validate_status_transfer: bool = True,
        expiry: Any = None,
    ) -> None:
        rec = self._tasks.get(task_id)
        if rec is None:
            raise TaskNotFoundError(f"task_id not found: {task_id}")
        rec.update(copy.deepcopy(updates))

    async def get_task(self, task_id: str) -> dict[str, Any]:
        rec = self._tasks.get(task_id)
        return copy.deepcopy(rec) if rec is not None else {}

    async def request_cancel(self, task_id: str) -> bool:
        rec = self._tasks.get(task_id)
        if rec is None:
            return False
        rec["cancel_requested"] = True
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

    # ----- core reopen primitive (simplified mirror of task_reopen.lua) -----

    async def reopen_task(
        self,
        task_id: str,
        *,
        initial_status: str = "pending",
        expiry: Any = None,
    ) -> str:
        rec = self._tasks.get(task_id)
        if rec is None:
            raise TaskNotFoundError(f"task_id not found: {task_id}")
        old_status = rec.get("status", "")
        if old_status not in _FAKE_TERMINAL_WORDS:
            raise InvalidStateTransitionError(
                f"reopen rejected: task {task_id} is not terminal "
                f"(current: {old_status})"
            )
        prev = rec.setdefault("previous_execution_ids", [])
        old_eid = rec.get("execution_id")
        if old_eid:
            prev.append(old_eid)
            del prev[:-50]
        new_eid = f"exec-{uuid.uuid4().hex[:12]}"
        rec["execution_id"] = new_eid
        rec["previous_status"] = old_status
        rec["status"] = initial_status
        rec["cancel_requested"] = False
        return new_eid

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

    async def sadd_result_consumer(self, task_id: str, consumer_id: str) -> bool:
        consumers = self._result_consumers.setdefault(task_id, set())
        if consumer_id in consumers:
            return False
        consumers.add(consumer_id)
        return True