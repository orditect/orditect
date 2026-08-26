"""Dependency governance plane (v0.1.1): passive multi-parent dependency APIs.

Boundary discipline (orchestration independence):
- This governor only governs dependency relationships: register, query,
  vote, notify, audit. It never creates tasks, never schedules execution,
  never interprets DAG semantics.
- Readiness is driven exclusively by external calls to
  notify_task_terminal(); nothing here auto-drives execution.

Vocabulary neutrality (T6): all status words (success / terminal / ready)
are caller-declared at construction; the framework embeds none of the
caller's business vocabulary beyond documented defaults.
"""

from __future__ import annotations

import logging
from typing import Any

from orditect.flow.exceptions import TaskNotFoundError
from orditect.flow.protocols.storage import TaskStorageProtocol
from orditect.protocol import AuditEvent, UnsupportedCapabilityError

logger = logging.getLogger(__name__)

#: flow-vocabulary defaults (overridable at construction for external
#: orchestration systems with their own vocabularies).
DEFAULT_TERMINAL_WORDS: frozenset[str] = frozenset({"succeeded", "failed", "cancelled"})
DEFAULT_READY_STATUS = "pending"

#: Depth bound for lineage / cycle walks (aligned with the cascade limit).
_MAX_LINEAGE_DEPTH = 32

#: Hard cap on the exemption snapshot (hot-record bloat guard).
MAX_EXEMPT_RESOURCES = 10


class DependencyGovernor:
    """Passive multi-parent dependency governor.

    Args:
        storage: task store (flow TaskStorageProtocol extended with the
            v0.1.1 dependency primitives, e.g. core TaskRedisDB).
        success_words: caller-declared success terminal words (T6). Empty
            raises ValueError. The ONLY auto-vote criterion:
            ``terminal_status not in success_words``.
        terminal_words: caller-declared terminal words (defaults to the
            flow vocabulary). Used to classify parents at registration.
        ready_status: status word a task must hold to be reported by
            get_ready_tasks() (defaults to the flow initial word).
        lifecycle: cancel executor invoked when votes reach threshold;
            None = votes are recorded but never trigger cancellation.
        audit_writer: optional protocol AuditWriter (T9: write failures
            are logged, never raised).
        dep_graph_store: optional cold-path dependency graph store
            (duck-typed: write_dependency / read_graph / all_edges).
            Not injected: the hot path works fully and
            get_dependency_graph raises UnsupportedCapabilityError (T8).
    """

    def __init__(
        self,
        storage: TaskStorageProtocol,
        *,
        success_words: frozenset[str],
        terminal_words: frozenset[str] | None = None,
        ready_status: str = DEFAULT_READY_STATUS,
        lifecycle: Any = None,
        audit_writer: Any = None,
        dep_graph_store: Any = None,
    ):
        if not success_words:
            raise ValueError(
                "DependencyGovernor requires explicit non-empty success_words (T6)"
            )
        self._storage = storage
        self._success_words = frozenset(success_words)
        self._terminal_words = (
            frozenset(terminal_words)
            if terminal_words is not None
            else DEFAULT_TERMINAL_WORDS
        )
        self._ready_status = ready_status
        self._lifecycle = lifecycle
        self._audit = audit_writer
        self._dep_graph_store = dep_graph_store

    # ---------- internal helpers ----------

    def _is_terminal(self, status: str) -> bool:
        return status in self._terminal_words

    def _is_success(self, status: str) -> bool:
        return status in self._success_words

    async def _get_record(self, task_id: str) -> dict:
        """Read a task record; an empty record is treated as not found."""
        rec = await self._storage.get_task(task_id)
        if not rec:
            raise TaskNotFoundError(f"task_id not found: {task_id}")
        return rec

    async def _emit_audit(self, event: AuditEvent) -> None:
        """Best-effort audit write (T9: observation never blocks)."""
        if self._audit is None:
            return
        try:
            await self._audit.append(event)
        except Exception as e:
            logger.warning(f"audit append failed (ignored): {e}")

    # ---------- dependency graph query ----------

    async def get_dependency_graph(self, root_id: str) -> dict:
        """Query the full dependency graph (cold path, never touches Redis).

        Returns:
            {"nodes": [...], "edges": [...]} as produced by the injected
            dep_graph_store.

        Raises:
            UnsupportedCapabilityError: dep_graph_store not injected (T8).
        """
        if self._dep_graph_store is None:
            raise UnsupportedCapabilityError(
                "dependency graph query requires an injected dep_graph_store"
            )
        return await self._dep_graph_store.read_graph(root_id)
    # ---------- dependency registration ----------

    async def register_dependency(
        self,
        child_id: str,
        parents: list[str],
        *,
        primary_parent: str | None = None,
        exempt_resources: list[str] | None = None,
    ) -> None:
        """Register a multi-parent dependency relationship (governance only).

        Called by the external orchestration system after creating the child
        task. Never creates tasks, never schedules execution: readiness is
        only surfaced via get_ready_tasks(), driven by notify_task_terminal().

        Semantics:
        - Terminal parents are legal (registration may lag termination):
          non-terminal parents are counted into remaining_deps and added to
          their active_children; terminal-success parents are ignored;
          terminal-abnormal parents are treated as already-cast cancel votes.
        - Full rewrite: retrying the same (child_id, parents) registration is
          idempotent. Re-registering with a DIFFERENT parent set is not
          supported (stale active_children on removed parents).

        Raises:
            TaskNotFoundError: child_id or any parent does not exist.
            ValueError: empty parents, cycle detected, primary_parent not in
                parents, or exemption snapshot exceeds the cap.
        """
        if not parents:
            raise ValueError("register_dependency requires a non-empty parents list")
        parents = list(dict.fromkeys(parents))  # dedupe, preserve declaration order

        # 1. existence (terminal parents are legal)
        await self._get_record(child_id)
        parent_recs: dict[str, dict] = {}
        for pid in parents:
            parent_recs[pid] = await self._get_record(pid)

        # 2. cycle detection (line 1 of 2; the offline full-graph scan is line 2)
        await self._assert_no_cycle(child_id, parents)

        # 3. primary parent (single chain for lineage and exemption)
        primary = primary_parent or parents[0]
        if primary not in parents:
            raise ValueError(f"primary_parent {primary!r} is not in parents")

        # 4. exemption snapshot (frozen at registration; invalidate on reopen)
        if exempt_resources is not None:
            if len(exempt_resources) > MAX_EXEMPT_RESOURCES:
                raise ValueError(
                    f"exempt_resources exceeds the cap of {MAX_EXEMPT_RESOURCES}"
                )
            snapshot = list(exempt_resources)
        else:
            snapshot = await self._collect_chain_resources(primary)

        # 5. classify parents from the records read at step 1
        counted: list[str] = []    # non-terminal: counted + active_children
        abnormal: list[str] = []   # terminal and not success: already-cast votes
        for pid in parents:
            status = parent_recs[pid].get("status", "")
            if not self._is_terminal(status):
                counted.append(pid)
            elif not self._is_success(status):
                abnormal.append(pid)

        # 6. writes (threshold decisions reuse the same atomic primitive as
        # vote_cancel: the caster of the threshold-reaching vote observes True)
        threshold = len(parents)
        triggered = False
        for pid in counted:
            await self._storage.sadd_active_child(pid, child_id)
        for pid in abnormal:
            if await self._storage.vote_and_check_threshold(child_id, pid, threshold):
                triggered = True
        await self._storage.update_task(
            child_id,
            {
                "depends_on": parents,
                "primary_parent": primary,
                "exempt_resources_snapshot": snapshot,
            },
        )
        await self._storage.set_remaining_deps(child_id, len(counted))

        # 6b. post-write recheck (TOCTOU compensation): a counted parent may
        # have terminated mid-registration, and its terminal notify then
        # missed this child. Compensation is safe in both directions: a
        # duplicate DECR only drives the counter negative (tolerated —
        # readiness is a <=0 threshold) and a duplicate vote is set-idempotent.
        for pid in counted:
            try:
                current = (await self._storage.get_task(pid)).get("status", "")
            except Exception:
                continue
            if not self._is_terminal(current):
                continue
            new_value = await self._storage.decr_remaining_deps(child_id)
            if new_value < 0:
                logger.warning(
                    f"remaining_deps went negative during registration "
                    f"compensation: {child_id} ({new_value})"
                )
            if not self._is_success(current):
                if await self._storage.vote_and_check_threshold(
                    child_id, pid, threshold
                ):
                    triggered = True

        # 7. cold path (optional; failures degrade to log + audit, hot path rules)
        if self._dep_graph_store is not None:
            for pid in parents:
                try:
                    await self._dep_graph_store.write_dependency(
                        child_id, pid, pid == primary
                    )
                except Exception as e:
                    logger.error(
                        f"dep graph write failed (hot path unaffected): "
                        f"{child_id}<-{pid}, {e}"
                    )
                    await self._emit_audit(
                        AuditEvent(
                            event_id=f"dep-index-fail-{child_id}-{pid}",
                            task_id=child_id,
                            event_type="dep_index_write_failed",
                            payload={"parent": pid, "error": str(e)},
                        )
                    )

        # 8. threshold trigger (e.g. every parent terminal-abnormal at/around
        # registration). lifecycle.cancel is idempotent against an already-
        # terminal child, so redundant triggers are harmless.
        if triggered and self._lifecycle is not None:
            try:
                await self._lifecycle.cancel(child_id)
            except Exception as e:
                logger.warning(f"registration-time cancel failed: {child_id}, {e}")

        # 9. remaining_deps == 0 means ready; readiness is ONLY surfaced via
        # get_ready_tasks() — nothing is ever scheduled here.

    async def _assert_no_cycle(self, child_id: str, parents: list[str]) -> None:
        """DFS the ancestors of each parent; child_id among them = cycle."""
        visited: set[str] = set()
        stack: list[tuple[str, int]] = [(pid, 0) for pid in parents]
        while stack:
            node, depth = stack.pop()
            if node == child_id:
                raise ValueError(
                    f"dependency cycle detected: {child_id} is an ancestor "
                    f"of its own parent"
                )
            if node in visited or depth >= _MAX_LINEAGE_DEPTH:
                continue
            visited.add(node)
            try:
                rec = await self._storage.get_task(node)
            except Exception:
                continue
            parent_ref = rec.get("parent_task_id")
            if parent_ref:
                stack.append((parent_ref, depth + 1))
            for dep in rec.get("depends_on") or []:
                stack.append((dep, depth + 1))

    async def _collect_chain_resources(self, start_id: str) -> list[str]:
        """Collect the resource ledger along the primary-parent chain (inclusive).

        Mirrors the executor's ancestor-walk semantics; cycle-safe and
        depth-capped. Truncated at MAX_EXEMPT_RESOURCES with a warning.
        """
        resources: list[str] = []
        visited: set[str] = set()
        current: str | None = start_id
        for _ in range(_MAX_LINEAGE_DEPTH):
            if current is None or current in visited:
                break
            visited.add(current)
            try:
                rec = await self._storage.get_task(current)
            except Exception:
                break
            if not rec:
                break
            resource = rec.get("resource")
            if resource and resource not in resources:
                resources.append(resource)
                if len(resources) >= MAX_EXEMPT_RESOURCES:
                    logger.warning(
                        f"exemption snapshot truncated at "
                        f"{MAX_EXEMPT_RESOURCES}: chain of {start_id}"
                    )
                    break
            current = rec.get("parent_task_id")
        return resources

    # ---------- readiness query ----------

    async def get_ready_tasks(self) -> list[str]:
        """Return task_ids whose remaining_deps <= 0 AND status == ready_status.

        Readiness is a computed view only — nothing is ever scheduled here.
        Performance boundary: SCAN-based; intended for <=10k-scale task sets.
        Callers should poll no faster than ~100ms.
        """
        return await self._storage.list_ready_dep_tasks(status=self._ready_status)

    # ---------- cancel voting ----------

    async def vote_cancel(self, parent_id: str, child_id: str) -> bool:
        """Cast a cancel vote against child_id on behalf of parent_id.

        Returns True when this vote reached the threshold (len(depends_on))
        and cancellation was triggered. Atomicity: SADD + SCARD run in one
        MULTI/EXEC transaction (core vote_and_check_threshold), so exactly
        one concurrent voter observes the threshold being reached.

        Returns False when the child is missing, already terminal, the
        parent is not registered as a dependency, or the threshold is not
        reached.
        """
        rec = await self._storage.get_task(child_id)
        if not rec:
            return False
        status = rec.get("status", "")
        if self._is_terminal(status):
            return False
        depends_on = rec.get("depends_on") or []
        if parent_id not in depends_on:
            return False

        reached = await self._storage.vote_and_check_threshold(
            child_id, parent_id, len(depends_on)
        )
        if reached and self._lifecycle is not None:
            try:
                await self._lifecycle.cancel(child_id)
            except Exception as e:
                logger.warning(f"vote-triggered cancel failed: {child_id}, {e}")
        return reached

    # ---------- terminal notification (unified entry) ----------

    async def notify_task_terminal(self, task_id: str, terminal_status: str) -> None:
        """Unified entry called by the external orchestration system after ANY
        task reaches a terminal state.

        Two directions, both best-effort (T9: failures are logged, never
        raised — a notification failure must not disturb the caller's flow):

        As a parent (drives readiness + hang prevention):
        - for each active child: DECR remaining_deps unconditionally
          (success never auto-votes; pinned), and if
          terminal_status not in success_words: SADD an automatic cancel
          vote on the child's behalf.

        As a child (reverse cleanup):
        - SREM itself from every parent's active_children;
        - clear its own cancel_votes when it had dependencies.

        Discipline: this method is NEVER invoked by the built-in executor —
        wiring it at the task-closure point is the composition root's /
        bridge layer's responsibility.
        """
        success = self._is_success(terminal_status)

        # ----- as a parent -----
        try:
            for child_id in await self._storage.get_active_children(task_id):
                await self._notify_one_child(task_id, child_id, terminal_status, success)
        except Exception as e:
            logger.warning(f"terminal notify (as parent) degraded: {task_id}, {e}")

        # ----- as a child -----
        try:
            rec = await self._storage.get_task(task_id)
            depends_on = rec.get("depends_on") or []
            for parent_id in depends_on:
                await self._storage.srem_active_child(parent_id, task_id)
            if depends_on:
                await self._storage.clear_cancel_votes(task_id)
        except Exception as e:
            logger.warning(f"terminal notify (as child) degraded: {task_id}, {e}")

    async def _notify_one_child(
        self, parent_id: str, child_id: str, terminal_status: str, success: bool
    ) -> None:
        """Per-child parent-side notification (individual failures logged)."""
        try:
            new_value = await self._storage.decr_remaining_deps(child_id)
            if new_value < 0:
                logger.warning(
                    f"remaining_deps went negative after terminal notify: "
                    f"{child_id} ({new_value}); parent={parent_id}"
                )
            if success:
                # success never auto-votes (prevents accidental cancellation)
                return
            rec = await self._storage.get_task(child_id)
            if not rec:
                return
            status = rec.get("status", "")
            if self._is_terminal(status):
                return
            depends_on = rec.get("depends_on") or []
            if parent_id not in depends_on:
                return
            reached = await self._storage.vote_and_check_threshold(
                child_id, parent_id, len(depends_on)
            )
            if reached and self._lifecycle is not None:
                try:
                    await self._lifecycle.cancel(child_id)
                except Exception as e:
                    logger.warning(f"notify-triggered cancel failed: {child_id}, {e}")
        except Exception as e:
            logger.warning(
                f"terminal notify for child degraded: {child_id}, "
                f"parent={parent_id}, {e}"
            )

    # ---------- result consumption audit ----------

    async def result_consumed(self, task_id: str, consumer_id: str) -> None:
        """Audit a result consumption, deduplicated by (task_id, consumer_id).

        Only the first consumption by a given consumer produces an audit
        event; repeats are silent. Internal framework get_task() calls never
        trigger this method — it fires only on explicit invocation.
        """
        first = await self._storage.sadd_result_consumer(task_id, consumer_id)
        if not first:
            return
        await self._emit_audit(
            AuditEvent(
                event_id=f"consume-{task_id}-{consumer_id}",
                task_id=task_id,
                event_type="result_consumed",
                payload={"consumer": consumer_id},
            )
        )

    # ---------- exemption snapshot lifecycle ----------

    async def invalidate_exempt_snapshot(self, task_id: str) -> None:
        """Reset the exemption snapshot to None (falls back to the live walk).

        Call after reopen_task (new generation) and before re-execution.
        RecoveryService's rerun path invokes this when a governor is
        injected; external orchestration systems that reopen on their own
        carry the same responsibility.
        """
        await self._storage.update_task(
            task_id, {"exempt_resources_snapshot": None}
        )