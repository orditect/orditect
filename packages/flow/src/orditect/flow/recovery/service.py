"""RecoveryService: resume / rerun over the recursive task tree.

Per-node decision algorithm (the core of recovery):
- latest generation reached a caller-declared success word AND hot record
  carries a result  -> REUSE (F3 short-circuit, no re-execution)
- otherwise (failed / cancelled / never ran / running-but-lease-expired)
  -> RERUN (core reopen_task opens a new generation, then executor.execute
  drives re-execution directly).

Execution dispatch (design decision): rerun bypasses orchestrator.submit —
reopen_task has already reset the node to its initial state (a fresh
generation), so re-initialization would be wrong (double registration +
state conflict). The service holds the executor and calls executor.execute
directly; the task instance comes from the caller-injected task_factory.

Vocabulary neutrality (T6): success words are caller-declared
(reuse_terminal_words); the framework embeds none.
"""
from __future__ import annotations

import asyncio
import logging
from enum import Enum
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

#: task_factory: rebuild a task instance from a task_id (caller-injected).
TaskFactory = Callable[[str], Awaitable[Any]]


class ReuseDecision(str, Enum):
    """Per-node recovery decision."""

    REUSE = "reuse"
    RERUN = "rerun"

def _retrieve_rerun_error(task: "asyncio.Task") -> None:
    """Retrieve exceptions from background rerun tasks (aligned with the
    orchestrator's F5 discipline; prevents 'exception was never retrieved'
    warnings at GC time)."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.debug(
            f"Recovery rerun finished with error "
            f"(already logged at call site): {exc}"
        )

class RecoveryService:
    """Drives resume / rerun over a task tree.

    Args:
        storage: task store (core TaskRedisDB — reopen_task + hot record).
        snapshot_reader: protocol SnapshotReader (tree/version queries).
        executor: TaskExecutor — drives re-execution of rerun nodes directly
            (bypasses submit; reopen already reset the node state).
        reuse_terminal_words: caller-declared success vocabulary (T6).
        task_factory: caller-injected reconstruction of a task by task_id.
    """

    def __init__(
        self,
        storage: Any,
        snapshot_reader: Any,
        executor: Any,
        reuse_terminal_words: frozenset[str],
        task_factory: TaskFactory,
        dependency_governor: Any = None,  # v0.1.1: snapshot invalidation wiring
    ):
        if not reuse_terminal_words:
            raise ValueError("RecoveryService requires explicit reuse_terminal_words (T6)")
        if executor is None:
            raise ValueError("RecoveryService requires an executor (dispatch target)")
        self._storage = storage
        self._reader = snapshot_reader
        self._executor = executor
        self._reuse_words = frozenset(reuse_terminal_words)
        self._task_factory = task_factory
        # v0.1.1: when injected, the rerun path invalidates the exemption
        # snapshot after reopen (stale snapshots must never survive a new
        # generation). Best-effort: invalidation failure never blocks rerun.
        self._dependency_governor = dependency_governor
        self._bg_tasks: set[asyncio.Task] = set()  # strong refs against GC

    # ---------- per-node decision ----------

    async def decide(self, task_id: str) -> ReuseDecision:
        """Decide reuse vs rerun for one node (latest generation)."""
        try:
            snap = await self._reader.get(task_id, "execute")
        except Exception as e:
            logger.warning(f"recovery decide: snapshot read failed (rerun): {task_id}, {e}")
            return ReuseDecision.RERUN
        if snap is not None and snap.status in self._reuse_words:
            try:
                rec = await self._storage.get_task(task_id)
            except Exception:
                rec = {}
            if "result" in rec:
                # Adjudicated v0.1.7 (#11): key presence, not non-None —
                # a side-effect task that returns None is still reusable,
                # matching the executor's _try_reuse_result rule.
                return ReuseDecision.REUSE
        return ReuseDecision.RERUN

    # ---------- public primitives ----------

    async def resume(self, root_task_id: str) -> dict[str, ReuseDecision]:
        """Resume a task tree from failure points (recover the whole subtree).

        Per node: reuse if latest generation succeeded (with hot-record
        result), else rerun (reopen new generation + re-execute).

        Returns:
            {task_id: ReuseDecision} for every node in the subtree.
        """
        tree_ids = await self._tree_task_ids(root_task_id)
        plan: dict[str, ReuseDecision] = {}
        for tid in tree_ids:
            plan[tid] = await self.decide(tid)

        await self._dispatch(tree_ids, plan)
        return plan

    async def rerun(self, root_task_id: str, scope: set[str]) -> dict[str, ReuseDecision]:
        """Rerun an explicit scope of nodes; others follow normal decide().

        Args:
            root_task_id: tree root (used to resolve the subtree).
            scope: explicit task_id set to force-rerun. Nodes in the subtree
                but not in scope follow decide() (reuse if succeeded).

        Returns:
            {task_id: ReuseDecision} for every node in the subtree.
        """
        tree_ids = await self._tree_task_ids(root_task_id)
        plan: dict[str, ReuseDecision] = {}
        for tid in tree_ids:
            if tid in scope:
                plan[tid] = ReuseDecision.RERUN
            else:
                plan[tid] = await self.decide(tid)

        await self._dispatch(tree_ids, plan)
        return plan

    # ---------- internals ----------

    async def _tree_task_ids(self, root_task_id: str) -> list[str]:
        """Resolve the subtree task_ids via the snapshot domain tree query."""
        tree = await self._reader.get_tree(root_task_id, latest_only=True)
        return [s.task_id for s in tree]

    async def _dispatch(
        self, tree_ids: list[str], plan: dict[str, ReuseDecision]
    ) -> None:
        """Dispatch rerun nodes: reopen (new generation) + executor.execute."""
        for tid in tree_ids:
            if plan.get(tid) is not ReuseDecision.RERUN:
                continue
            try:
                await self._rerun_node(tid)
            except Exception as e:
                # One node's failure must not block the rest of the tree.
                logger.error(f"recovery rerun failed: {tid}, {e}", exc_info=True)

    async def _rerun_node(self, task_id: str) -> None:
        """Reopen a new generation (core) + re-execute via executor.

        Order matters: reopen first (resets state to initial for the new
        generation), then invalidate the exemption snapshot (stale snapshots
        must not survive a new generation), then execute. executor.execute
        drives running settlement -> F3 reuse query -> execution; for a
        reopened node the F3 query sees the NEW generation (no success
        snapshot yet), so it executes.
        """
        # 1. Hot-path new generation (T3-safe: reopen, not state regression)
        await self._storage.reopen_task(task_id)
        # 2. v0.1.1: invalidate the exemption snapshot frozen at registration
        #    (best-effort: the rerun itself must not be blocked by this)
        if self._dependency_governor is not None:
            try:
                await self._dependency_governor.invalidate_exempt_snapshot(task_id)
            except Exception as e:
                logger.warning(
                    f"exempt snapshot invalidation failed (rerun unaffected): "
                    f"{task_id}, {e}"
                )
        # 3. Reconstruct task instance (caller-injected factory)
        task = await self._task_factory(task_id)
        # 4. Drive re-execution directly via executor (bypass submit: reopen
        #    already reset state; re-initialize would be wrong). Strong-ref
        #    the background task against GC.
        bg = asyncio.create_task(self._executor.execute(task_id=task_id, task=task))
        self._bg_tasks.add(bg)
        bg.add_done_callback(self._bg_tasks.discard)
        bg.add_done_callback(_retrieve_rerun_error)