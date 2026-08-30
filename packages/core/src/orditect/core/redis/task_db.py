"""TaskRedisDB — task store + status index + lineage index.

Index ZSET lease-ification + initialization atomization.

Changes:
  - Status/lineage index changed from SET + key-level TTL to ZSET lease
    (member=task_id, score=expire_at_ms, server-side clock), read path
    ZREMRANGEBYSCORE lazy cleanup. Fixes #8: shared index key TTL refreshed
    by later writers, long TTL active members disappear due to shared TTL. "Index and primary
    record share same expiry" contract corrected from "key-level same TTL"
    to "member-level same expiry instant".
  - initialize_task fully Lua-ified (task_init.lua): idempotency check and
    write atomized, fixes #14 TOCTOU window (exists → status advance →
    concurrent write reset).
  - list_task_ids_by_status removes filter_expired parameter (read path
    necessarily cleans, parameter loses meaning), return order by expiry
    time ascending.
  - All Lua scripts lazy register (#1): DI mode fulfills "no need to connect()"
    contract.
  - Index key-level TTL only increases not decreases (fallback for "never
    read" residue prevention; member-level score is precise semantics).

Preserved changes:
  - B1 state machine hosting: terminal_statuses / transitions instance-level
    injection, Lua terminal protection and Python transfer validation bound
    to same instance source.
  - B3 idempotent initialization: initialize_task(if_not_exists=True).
  - B2 lineage index: initialize_task(parent_task_id=...) + list_children().
  - R5 fix: expiry or default → is not None.
"""
from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from typing import Iterable, Optional

import redis.asyncio as aioredis

from orditect.core._lua import load_lua
from orditect.core.errors import InvalidStatusTransferError, TaskNotFoundError
from orditect.core.redis.base import RedisDB
from orditect.core.task.status import TaskStatus, can_transfer

#: Default terminal status set (vocabulary when using core standalone)
DEFAULT_TERMINAL_STATUSES: tuple[str, ...] = ("completed", "failed", "cancelled")

import logging
logger = logging.getLogger(__name__)

#: Attached-key suffixes for dependency governance (v0.1.1). Every attached
#: key lives under the owning task's hot-record key and shares its expiry
#: instant (synced on write, followed up on update_task).
_ATTACHED_KEY_SUFFIXES: tuple[str, ...] = (
    "active_children",
    "remaining_deps",
    "cancel_votes",
    "result_consumers",
)

class TaskRedisDB(RedisDB):
    """
    Task store model:
    - Task primary record: task:{task_id} (JSON, includes status/reason/payload/timestamp/cancel_requested)
    - Status index:   task_status:{status} -> ZSET(member=task_id, score=expire_at_ms)
    - Lineage index:  {task_key_prefix}_children:{parent_id} -> ZSET(same)
    """

    def __init__(
        self,
        redis_url: str | None = None,
        client: aioredis.Redis | None = None,
        max_connections: int = 200,
        timeout: int = 300,
        default_expire_time: int = 604800,
        task_key_prefix: str = "task",
        status_index_prefix: str = "task_status",
        *,
        terminal_statuses: tuple[str, ...] = DEFAULT_TERMINAL_STATUSES,
        transitions: dict[str, set[str]] | None = None,
    ):
        """
        Args:
            terminal_statuses: terminal status set (instance-level binding).
                Passed to task_update.lua ARGV[6], Lua terminal protection
                based on this. Upper framework (e.g. flow) when integrating declares
                its own vocabulary, e.g. ("succeeded", "failed", "cancelled").
            transitions: status transfer whitelist (None=built-in default table).
                Python side full state machine validation (when
                validate_status_transfer=True). Format: {from_status:
                {to_status, ...}}, "" represents initial empty status.
        """
        super().__init__(
            redis_url=redis_url,
            client=client,
            max_connections=max_connections,
            timeout=timeout,
            default_expire_time=default_expire_time,
        )
        self.task_key_prefix = task_key_prefix
        self.status_index_prefix = status_index_prefix

        # B1: state machine instance-level binding
        self._terminal_statuses = tuple(terminal_statuses)
        self._transitions = transitions  # None falls back to module-level can_transfer

        # Note: no "timestamp" here — it is always set fresh at
        # initialize_task time (a constructor-time value would be a dead,
        # misleading default).
        self.default_task_data = {
            "status": "",
            "reason": "",
            "payload": {},
            "cancel_requested": False,
        }

        self._update_script = None  # lazy registration (#1)
        self._init_script = None    # v0.3.2: task_init.lua
        self._reopen_script = None  # v0.1.0: task_reopen.lua

    async def connect(self):
        """Establish connection + register Lua scripts (warm-up; lazy registration serves as fallback for DI path)."""
        await super().connect()
        if self.client is not None:
            self._update_script = self.client.register_script(load_lua("task_update.lua"))
            self._init_script = self.client.register_script(load_lua("task_init.lua"))
            self._reopen_script = self.client.register_script(load_lua("task_reopen.lua"))
    # ---------- Script lazy registration (#1) ----------
    def _get_update_script(self):
        if self._update_script is None:
            if self.client is None:
                raise RuntimeError("TaskRedisDB not connected: call connect() first")
            self._update_script = self.client.register_script(load_lua("task_update.lua"))
        return self._update_script

    def _get_init_script(self):
        if self._init_script is None:
            if self.client is None:
                raise RuntimeError("TaskRedisDB not connected: call connect() first")
            self._init_script = self.client.register_script(load_lua("task_init.lua"))
        return self._init_script

    def _get_reopen_script(self):
        if self._reopen_script is None:
            if self.client is None:
                raise RuntimeError("TaskRedisDB not connected: call connect() first")
            self._reopen_script = self.client.register_script(load_lua("task_reopen.lua"))
        return self._reopen_script

    async def reopen_task(
        self,
        task_id: str,
        *,
        initial_status: str = TaskStatus.pending.value,
        expiry: Optional[int] = None,
    ) -> str:
        """Reopen a terminal task as a new execution generation.

Recovery-system hot-path primitive: controlled new-generation opening
for terminal tasks, enabling resume/rerun without violating terminal
protection (T3). Terminal protection remains unconditional within one
generation; reopen only produces a new generation.

Atomic actions (task_reopen.lua):
verify terminal -> write new execution_id -> reset state -> migrate
status index -> leave old generation trace (previous_execution_ids,
capped at 50).

Concurrency (T4/T10): concurrent reopen of the same terminal task —
exactly one winner; the loser reads the reset initial state (not
terminal) and is rejected.

Args:
    task_id: task to reopen (must currently be in a terminal state
        declared by this instance's terminal_statuses).
    initial_status: post-reopen initial state word (caller vocabulary).
    expiry: new generation lease; None = keep remaining TTL (B1
        preserve semantics), explicit value advances the lease.

Returns:
    The new execution_id (format "exec-{uuid4hex[:12]}").

Raises:
    TaskNotFoundError: task does not exist.
    InvalidStatusTransferError: current state is not terminal.
"""
        import uuid as _uuid

        new_execution_id = f"exec-{_uuid.uuid4().hex[:12]}"
        ex = int(expiry) if expiry is not None else -1

        res_raw = await self._get_reopen_script()(
            keys=[self._task_key(task_id)],
            args=[
                task_id,
                new_execution_id,
                initial_status,
                str(ex),
                self.status_index_prefix,
                json.dumps(list(self._terminal_statuses)),
                str(self.default_expire_time),
            ],
        )

        try:
            res = json.loads(res_raw) if isinstance(res_raw, str) else res_raw
        except Exception:
            res = {"ok": False, "err": f"BAD_SCRIPT_RESPONSE:{res_raw}"}

        if not res.get("ok"):
            err = res.get("err", "UNKNOWN")
            if err == "NOT_FOUND":
                raise TaskNotFoundError(f"task_id not found: {task_id}")
            if err == "NOT_TERMINAL":
                raise InvalidStatusTransferError(
                    f"reopen rejected: task {task_id} is not terminal "
                    f"(current: {res.get('current_status', '')})"
                )
            raise RuntimeError(f"reopen_task failed: {err}")

        return str(res["execution_id"])

    async def _server_now_ms(self) -> int:
        """Server-side clock (B8 discipline: same clock source as acquire/refresh)."""
        server_time = await self.client.time()
        return int(server_time[0]) * 1000 + int(server_time[1]) // 1000

    # ---------- key helpers ----------
    def _task_key(self, task_id: str) -> str:
        return f"{self.task_key_prefix}:{task_id}"

    def _status_index_key(self, status: str) -> str:
        return f"{self.status_index_prefix}:{status}"

    def _children_key(self, parent_task_id: str) -> str:
        """B2 lineage index key."""
        return f"{self.task_key_prefix}_children:{parent_task_id}"

    @staticmethod
    def _now_str() -> str:
        """UTC-aware ISO timestamp (T7: mechanism-internal hot-record field)."""
        return datetime.now(UTC).isoformat()

    def _can_transfer(self, from_status: str, to_status: str) -> bool:
        """Instance-level transfer validation (transitions injected takes priority, otherwise built-in default table)."""
        if self._transitions is not None:
            return to_status in self._transitions.get(from_status, set())
        return can_transfer(from_status, to_status)

    # ---------- APIs ----------
    async def initialize_task(
        self,
        task_id: str,
        expiry=None,
        initial_status: str = TaskStatus.pending.value,
        *,
        if_not_exists: bool = False,
        parent_task_id: str | None = None,
    ) -> bool:
        """Initialize task record + status index + lineage index (task_init.lua single script atomization).

        Task carries an execution_id from creation (T11 hot-path projection). reopen_task only advances the generation;
        initialize assigns the first one. Idempotent skip (if_not_exists hit)
        keeps the existing execution_id untouched.

        ... (rest of docstring unchanged) ...
        """
        import uuid as _uuid

        execution_id = f"exec-{_uuid.uuid4().hex[:12]}"
        ex = int(expiry if expiry is not None else self.default_expire_time)

        data = copy.deepcopy(self.default_task_data)
        data["timestamp"] = self._now_str()
        data["status"] = initial_status
        if parent_task_id is not None:
            data["parent_task_id"] = parent_task_id

        tkey = self._task_key(task_id)
        skey = self._status_index_key(initial_status) if initial_status else tkey
        ckey = self._children_key(parent_task_id) if parent_task_id is not None else tkey

        result = await self._get_init_script()(
            keys=[tkey, skey, ckey],
            args=[
                json.dumps(data, ensure_ascii=False),
                str(ex),
                task_id,
                "1" if if_not_exists else "0",
                "1" if initial_status else "0",
                parent_task_id or "",
                execution_id,  # ARGV[7]: initial generation identity (v0.1.0)
            ],
        )
        return int(result) == 1

    async def list_children(self, parent_task_id: str) -> list[str]:
        """B2: query all child task IDs of a task (ZSET lease, read path lazy cleanup of expired members).

        Return order: by expiry time ascending (same score by task_id dictionary order).
        """
        key = self._children_key(parent_task_id)
        now_ms = await self._server_now_ms()
        await self.client.zremrangebyscore(key, "-inf", now_ms)
        return list(await self.client.zrange(key, 0, -1))

    async def exists_task(self, task_id: str) -> bool:
        """Check if task exists."""
        return bool(await self.client.exists(self._task_key(task_id)))

    async def get_task(self, task_id: str) -> dict:
        """Read task record (JSON deserialize, failure returns {})."""
        raw = await self.get(self._task_key(task_id))
        if not raw:
            return {}
        if isinstance(raw, dict):
            return raw
        try:
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    async def update_task(
        self,
        task_id: str,
        updates: dict,
        expiry: Optional[int] = None,
        validate_status_transfer: bool = True,
    ):
        """Atomic update task record (Lua script guarantees merge + status index maintenance atomicity).

        Dual-layer state machine validation (vocabulary same source from instance):
        - Python side: full transfer table (self._transitions or built-in default table)
        - Lua side: terminal protection (self._terminal_statuses, unconditional)

        Expiry semantics change —
        - None (default): preserve primary record remaining expiry instant,
          index lease advances with the same measure (fixes "status update extends short
          TTL task to default_expire_time");
        - Explicit value: advance expiry instant (consistent with previous behavior).

        v0.1.1: after a successful update, attached dependency-governance keys
        (active_children / remaining_deps / cancel_votes / result_consumers)
        follow the hot record's expiry instant (best-effort, logged only).
        """
        # B1: None → -1 (Lua side reads TTL to parse remaining expiry, fallback default when no TTL)
        ex = int(expiry) if expiry is not None else -1
        tkey = self._task_key(task_id)

        # Python side full state machine validation
        if validate_status_transfer and "status" in updates:
            rec = await self.get_task(task_id)
            if not rec:
                raise TaskNotFoundError(f"task_id not found: {task_id}")

            old_status = rec.get("status", "")
            new_status = updates.get("status", "")
            if old_status != new_status and not self._can_transfer(old_status, new_status):
                raise InvalidStatusTransferError(
                    f"invalid status transfer: {old_status} -> {new_status}"
                )

        payload = json.dumps(updates, ensure_ascii=False)

        res_raw = await self._get_update_script()(
            keys=[tkey],
            args=[
                payload,
                str(ex),
                self.status_index_prefix,
                "1" if validate_status_transfer else "0",
                task_id,
                json.dumps(list(self._terminal_statuses)),  # ARGV[6]: terminal status set
                str(self.default_expire_time),  # ARGV[7]: B1 preserve mode fallback TTL
            ],
        )

        try:
            res = json.loads(res_raw) if isinstance(res_raw, str) else res_raw
        except Exception:
            res = {"ok": False, "err": f"BAD_SCRIPT_RESPONSE:{res_raw}"}

        if not res.get("ok"):
            err = res.get("err", "UNKNOWN")
            if err == "NOT_FOUND":
                raise TaskNotFoundError(f"task_id not found: {task_id}")
            if err == "INVALID_TRANSFER":
                raise InvalidStatusTransferError(f"invalid status transfer for task_id={task_id}")
            raise RuntimeError(f"update_task failed: {err}")

        # v0.1.1: attached dep-governance keys share the hot record's expiry
        # instant; follow it up (best-effort, failures logged only).
        await self._follow_attached_keys_ttl(task_id, ex)

    async def update_task_field(self, task_id: str, field: str, value, expiry=None):
        """Update single field (convenience wrapper for update_task)."""
        await self.update_task(task_id, {field: value}, expiry=expiry)

    async def request_cancel(self, task_id: str) -> bool:
        """Request task cancellation (set cancel_requested=True, task internal polls this flag)."""
        rec = await self.get_task(task_id)
        if not rec:
            return False
        await self.update_task(task_id, {"cancel_requested": True})
        return True

    async def list_task_ids_by_status(
        self,
        status: str,
        *,
        limit: int | None = None,
    ) -> list[str]:
        """Query task ID list by status (ZSET lease index).

        Index ZSET-ified (score=expire_at), read path first lazy cleans
        expired members — shared index members no longer suffer from TTL contagion (#8 fix).
        filter_expired parameter deleted (read path necessarily cleans,
        parameter loses meaning).
        Return order: by expiry time ascending (expiring soon first; same
        score by task_id dictionary order).

        Args:
            status: status word
            limit: maximum return count (None=all; <=0 returns empty list)
        """
        if limit is not None and limit <= 0:
            return []
        key = self._status_index_key(status)
        now_ms = await self._server_now_ms()
        await self.client.zremrangebyscore(key, "-inf", now_ms)
        # Members cleared key auto-evaporates (aggregate type inherent behavior), no manual empty check deletion needed
        end = -1 if limit is None else limit - 1
        return list(await self.client.zrange(key, 0, end))

    async def list_tasks_by_status(self, status: str) -> list[str]:
        """Compatible old method name (same as list_task_ids_by_status)."""
        return await self.list_task_ids_by_status(status)

    async def bulk_get_tasks(self, task_ids: Iterable[str]) -> list[dict]:
        """Batch read task records (MGET, JSON deserialize failure returns {})."""
        keys = [self._task_key(tid) for tid in task_ids]
        if not keys:
            return []

        raws = await self.client.mget(keys)
        out = []
        for raw in raws:
            if not raw:
                out.append({})
                continue
            try:
                obj = json.loads(raw) if isinstance(raw, str) else raw
                out.append(obj if isinstance(obj, dict) else {})
            except Exception:
                out.append({})
        return out

    # ---------- dependency-governance primitives (v0.1.1) ----------
    # Thin wrappers over plain Redis commands. No Lua involvement: the
    # v0.1.1 frozen-script commitment forbids any change under lua/.

    def _active_children_key(self, parent_task_id: str) -> str:
        return f"{self._task_key(parent_task_id)}:active_children"

    def _remaining_deps_key(self, task_id: str) -> str:
        return f"{self._task_key(task_id)}:remaining_deps"

    def _cancel_votes_key(self, task_id: str) -> str:
        return f"{self._task_key(task_id)}:cancel_votes"

    def _result_consumers_key(self, task_id: str) -> str:
        return f"{self._task_key(task_id)}:result_consumers"

    async def _sync_attached_ttl(self, owner_task_id: str, keys: list[str]) -> None:
        """Best-effort TTL sync for attached keys.

        v0.1.5 (corrected): keyed on the ATTACHED key's own existence, not
        blindly on the owner's:
        - attached key EXISTS: owner alive -> follow the owner's TTL;
          owner gone -> fall back to default_expire_time (the attached data
          is real business state — e.g. result-consumer dedup, cancel votes
          — and must not be dropped, or dedup/vote state would be lost).
        - attached key MISSING: do nothing (a genuine ghost; never create
          or TTL a non-existent key).

        EXPIRE on a missing key is a harmless no-op. Failures are logged,
        never raised — the write itself already succeeded.
        """
        try:
            owner_ttl = await self.client.ttl(self._task_key(owner_task_id))
            async with self.client.pipeline(transaction=False) as pipe:
                for key in keys:
                    if not await self.client.exists(key):
                        continue  # genuine ghost: never materialize it
                    if owner_ttl > 0:
                        pipe.expire(key, owner_ttl)
                    else:
                        pipe.expire(key, self.default_expire_time)
                await pipe.execute()
        except Exception as e:
            logger.warning(
                f"attached-key TTL sync failed (ignored): {owner_task_id}, {e}"
            )

    async def _follow_attached_keys_ttl(self, task_id: str, expiry: int) -> None:
        """Advance attached keys' TTL after update_task moved the hot record's TTL.

        Best-effort: failures are logged, never raised. EXPIRE on missing
        keys is a no-op, so tasks without dependency keys pay no semantic cost.
        """
        try:
            ex = expiry
            if ex < 0:
                # preserve mode: Lua resolved the remaining TTL internally
                ex = await self.client.ttl(self._task_key(task_id))
            if ex <= 0:
                return
            tkey = self._task_key(task_id)
            async with self.client.pipeline(transaction=False) as pipe:
                for suffix in _ATTACHED_KEY_SUFFIXES:
                    pipe.expire(f"{tkey}:{suffix}", ex)
                await pipe.execute()
        except Exception as e:
            logger.warning(
                f"attached-key TTL follow failed (ignored): {task_id}, {e}"
            )

    # ----- active-children notification set (parent side) -----

    async def sadd_active_child(self, parent_id: str, child_id: str) -> None:
        """Add child_id to the parent's active-children notification set."""
        key = self._active_children_key(parent_id)
        await self.client.sadd(key, child_id)
        await self._sync_attached_ttl(parent_id, [key])

    async def srem_active_child(self, parent_id: str, child_id: str) -> None:
        """Remove child_id from the parent's active-children set (idempotent)."""
        await self.client.srem(self._active_children_key(parent_id), child_id)

    async def get_active_children(self, parent_id: str) -> list[str]:
        """List the parent's active-children set (sorted for determinism)."""
        members = await self.client.smembers(self._active_children_key(parent_id))
        return sorted(members)

    # ----- remaining-deps counter (child side) -----

    async def set_remaining_deps(self, task_id: str, n: int) -> None:
        """Initialize the child's remaining-deps counter (full rewrite, retry-safe)."""
        key = self._remaining_deps_key(task_id)
        await self.client.set(key, str(int(n)))
        await self._sync_attached_ttl(task_id, [key])

    async def decr_remaining_deps(self, task_id: str) -> int:
        """Decrement the counter; returns the new value.

        DECR on a missing key creates it at -1 (fault-tolerance contract:
        callers treat negatives as non-ready, log, and move on). A TTL sync
        follows so a ghost-created counter still expires with the hot record.
        """
        key = self._remaining_deps_key(task_id)
        value = int(await self.client.decr(key))
        await self._sync_attached_ttl(task_id, [key])
        return value

    async def get_remaining_deps(self, task_id: str) -> int:
        """Read the counter (0 when absent)."""
        raw = await self.client.get(self._remaining_deps_key(task_id))
        try:
            return int(raw or 0)
        except (TypeError, ValueError):
            return 0

    async def list_ready_dep_tasks(self, *, status: str | None = None) -> list[str]:
        """SCAN remaining-deps counters; return task_ids whose counter <= 0.

        Args:
            status: optional status-word filter applied against the hot
                records (vocabulary-neutral, T6: the word is injected by the
                caller, never embedded here). Missing records and mismatched
                statuses are excluded.

        Performance boundary: SCAN-based; intended for <=10k-scale task sets.
        Callers should poll no faster than ~100ms.
        """
        suffix = ":remaining_deps"
        prefix = f"{self.task_key_prefix}:"
        pattern = f"{prefix}*{suffix}"
        keys: list[str] = []
        async for key in self.client.scan_iter(match=pattern, count=500):
            keys.append(key)
        if not keys:
            return []
        raws = await self.client.mget(keys)
        candidates: list[str] = []
        for key, raw in zip(keys, raws):
            try:
                if int(raw or 0) <= 0:
                    candidates.append(key[len(prefix):-len(suffix)])
            except (TypeError, ValueError):
                continue
        if status is None or not candidates:
            return sorted(candidates)
        records = await self.bulk_get_tasks(candidates)
        return sorted(
            tid for tid, rec in zip(candidates, records)
            if rec.get("status") == status
        )

    # ----- cancel-vote set (child side) -----

    async def vote_and_check_threshold(
        self, child_id: str, parent_id: str, threshold: int
    ) -> bool:
        """Cast one cancel vote and atomically test the threshold.

        SADD + SCARD run inside one MULTI/EXEC transaction (single key,
        cluster-slot safe), so concurrent voters each observe an accurate
        post-vote count: exactly one caller observes the threshold being
        reached. Repeat votes from the same parent are idempotent.
        """
        key = self._cancel_votes_key(child_id)
        async with self.client.pipeline(transaction=True) as pipe:
            pipe.sadd(key, parent_id)
            pipe.scard(key)
            _, count = await pipe.execute()
        await self._sync_attached_ttl(child_id, [key])
        return int(count) >= int(threshold)

    async def get_cancel_votes(self, child_id: str) -> list[str]:
        """List the child's cancel-vote set (sorted for determinism)."""
        members = await self.client.smembers(self._cancel_votes_key(child_id))
        return sorted(members)

    async def clear_cancel_votes(self, child_id: str) -> None:
        """Delete the child's cancel-vote set (called on the child's terminal)."""
        await self.client.delete(self._cancel_votes_key(child_id))

    # ----- result-consumer dedup set -----

    async def sadd_result_consumer(self, task_id: str, consumer_id: str) -> bool:
        """Register a result consumer; True = first time (dedup key)."""
        key = self._result_consumers_key(task_id)
        added = int(await self.client.sadd(key, consumer_id))
        await self._sync_attached_ttl(task_id, [key])
        return added == 1