"""Task orchestrator (parent context auto-injection + cascading cancellation)."""
import asyncio
import logging
import uuid
from typing import Any, Dict, Optional

from orditect.flow.protocols.storage import TaskStorageProtocol
from orditect.flow.protocols.governor import ResourceGovernorProtocol
from orditect.flow.core.task import BaseBackEndTask
from orditect.flow.core.state_machine import TaskStateMachine, TaskStatus
from orditect.flow.core.executor import TaskExecutor
from orditect.flow.core.lifecycle import TaskLifecycle
from orditect.flow.core.context import current_task_id
from orditect.flow.exceptions import TaskNotFoundError

logger = logging.getLogger(__name__)

try:
    from orditect.core import InvalidStatusTransferError as _TaskbaseInvalidTransfer
except ImportError:
    _TaskbaseInvalidTransfer = None  # 理论不可达（taskbase 硬依赖），防御保留

# : maximum recursion depth of cascade cancellation (prevents lineage cycles causing infinite loop)
_MAX_CASCADE_DEPTH = 32


def _retrieve_bg_error(task: "asyncio.Task") -> None:
    """F5: background coroutine exception retrieve — eliminate 'exception was never retrieved' noise.

        Business exceptions from task execute have already been written to the task record by the executor (status=failed + error),
        re-raising at the orchestrator layer is meaningless; but not retrieving will cause asyncio to warn during GC.
        """
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.debug(f"Background task finished with error (recorded in task store): {exc}")


class TaskOrchestrator:
    """Task orchestrator (manages the complete lifecycle of tasks).

    Changes:
    - R6-2: submit automatically injects parent_task_id within the task execution context
      (asyncio.create_task copies context, nested submit has zero boilerplate).
      Explicit parent_task_id takes precedence over auto-injection.
    - R6-3: cancel / terminate cascade along lineage (self first, then children recursively).
      Cascade depth limit _MAX_CASCADE_DEPTH prevents cycles.
    - N1: submit supports if_not_exists idempotency.
    - R17-a: background task strong reference prevents GC.
    - dependency_governor: v0.1.1 passive dependency-governance
        hookup. NOTE: injecting it here only ATTACHES it — no
        internal code path uses it automatically (orchestration
        independence: the executor never emits dependency
        notifications). Callers must wire notify_task_terminal()
        at their own task-closure points (composition root /
        bridge layer).
    """

    def __init__(
            self,
            storage: TaskStorageProtocol,
            governor: Optional[ResourceGovernorProtocol] = None,
            state_machine: Optional[TaskStateMachine] = None,
            snapshot_sink: Any = None,
            snapshot_query: Any = None,                 # F3
            reuse_terminal_words: frozenset[str] | None = None,  # F3
            dependency_governor: Any = None,            # v0.1.1: passive dep governance
    ):
        self.storage = storage
        self.governor = governor
        self.state_machine = state_machine or TaskStateMachine()
        self.executor = TaskExecutor(
            storage, governor,
            snapshot_sink=snapshot_sink,
            snapshot_query=snapshot_query,              # F3
            reuse_terminal_words=reuse_terminal_words,  # F3
        )
        self.lifecycle = TaskLifecycle(storage, self.state_machine)
        # v0.1.1: None = every dependency-governance path stays inert.
        self.dependency_governor = dependency_governor
        self._bg_tasks: set = set()

    async def submit(
            self,
            task: BaseBackEndTask,
            task_id: Optional[str] = None,
            metadata: Optional[Dict[str, Any]] = None,
            resource: Optional[str] = None,
            timeout: Optional[float] = None,
            *,
            if_not_exists: bool = False,
            parent_task_id: Optional[str] = None,
            **kwargs,
    ) -> str:
        """Submit a task.

        Args:
            parent_task_id: Parent task ID. When None, automatically reads from current execution context
                (R6-2: auto-register lineage when submitting within parent task's execute());
                explicit parameter takes precedence; top-level call (no context) becomes root task.
        """
        if task_id is None:
            task_id = f"task-{uuid.uuid4().hex[:12]}"

        # R6-2: automatic parent injection (explicit parameter takes priority)
        if parent_task_id is None:
            parent_task_id = current_task_id.get()

        created = await self.lifecycle.initialize(
            task_id, metadata,
            parent_task_id=parent_task_id,
            if_not_exists=if_not_exists,
        )
        if not created:
            logger.info(f"Task already exists, skip submit (idempotent): {task_id}")
            return task_id

        await self.lifecycle.transition_to(task_id, TaskStatus.QUEUED)

        bg = asyncio.create_task(
            self.executor.execute(
                task_id=task_id,
                task=task,
                resource=resource,
                timeout=timeout,
                **kwargs,
            )
        )
        self._bg_tasks.add(bg)
        # F5: supplement exception retrieval besides discard (business exception already in task record, here only prevent alarm noise)
        bg.add_done_callback(self._bg_tasks.discard)
        bg.add_done_callback(_retrieve_bg_error)

        logger.info(f"Task submitted: {task_id} (parent={parent_task_id})")
        return task_id

    async def get_status(self, task_id: str) -> TaskStatus:
        return await self.lifecycle.get_status(task_id)

    async def get_task(self, task_id: str) -> Dict[str, Any]:
        return await self.lifecycle.get_task(task_id)

    def is_running(self, task_id: str) -> bool:
        return self.executor.is_running(task_id)

    # ---------- R6-3: cascade cancellation ----------

    async def _cascade_children(self, task_id: str, mode: str, depth: int) -> None:
        """Cascade cancel child tasks along lineage (recursive, with depth limit to prevent cycles)."""
        if depth >= _MAX_CASCADE_DEPTH:
            logger.warning(
                f"Cascade depth limit reached ({_MAX_CASCADE_DEPTH}), "
                f"possible lineage cycle: {task_id}"
            )
            return

        try:
            children = await self.storage.list_children(task_id)
        except Exception as e:
            logger.warning(f"list_children failed during cascade: {task_id}, error: {e}")
            return

        for child_id in children:
            try:
                child = await self.storage.get_task(child_id)
                child_status = TaskStatus(child["status"])
                if self.state_machine.is_terminal(child_status):
                    continue  # 终态子任务跳过

                # cascade with same mode as itself
                if mode == "terminate":
                    await self._terminate_single(child_id)
                else:
                    await self.lifecycle.cancel(child_id)

                # recurse to next layer
                await self._cascade_children(child_id, mode, depth + 1)
            except Exception as e:
                # single child task failure does not block cascade of other children
                logger.warning(f"Cascade to child failed: {child_id}, error: {e}")

    async def cancel(self, task_id: str) -> bool:
        """Cancel task (graceful mode: mark cancellation, R6-3 cascade along lineage)."""
        ok = await self.lifecycle.cancel(task_id)
        if ok:
            await self._cascade_children(task_id, mode="cancel", depth=0)
        return ok

    async def _terminate_single(self, task_id: str) -> bool:
        """Terminate a single task (without cascading — cascading is driven
        by terminate uniformly).

        Race fallback: when the fallback update to CANCELLED hits taskbase
        Lua terminal protection (the task was concurrently closed to terminal
        right after the check), confirming it reached CANCELLED counts as
        success; other terminal states count as failure — the exception no
        longer escapes.

        Returns:
            True: termination successful (or idempotently confirmed)
            False: task does not exist or is already terminal
        """
        try:
            task = await self.storage.get_task(task_id)
        except TaskNotFoundError:
            return False
        if not task:
            return False

        current_status = TaskStatus(task["status"])
        if self.state_machine.is_terminal(current_status):
            return False

        await self.storage.request_cancel(task_id)

        if self.executor.is_running(task_id):
            await self.executor.cancel(task_id, force=True)
            logger.info(f"Task terminated (coroutine cancelled): {task_id}")
        else:
            try:
                await self.lifecycle.transition_to(task_id, TaskStatus.CANCELLED)
            except Exception as e:
                # F1: same-source race fallback as lifecycle.cancel
                from orditect.flow.exceptions import InvalidStateTransitionError
                is_race = isinstance(e, InvalidStateTransitionError) or (
                    _TaskbaseInvalidTransfer is not None
                    and isinstance(e, _TaskbaseInvalidTransfer)
                )
                if not is_race:
                    raise
                final_status = await self.lifecycle.get_status(task_id)
                if final_status != TaskStatus.CANCELLED:
                    logger.warning(
                        f"Terminate fallback rejected (already terminal): "
                        f"{task_id} (final: {final_status.value})"
                    )
                    return False
            logger.info(
                f"Task terminated (status fallback, coroutine not local): "
                f"{task_id}"
            )
        return True

    async def terminate(self, task_id: str) -> bool:
        """Force terminate task (immediate mode, R6-3 cascade along lineage).

        Difference from cancel():
        - cancel() is graceful marking: waits for task to check and interrupt itself
        - terminate() is forceful: cancels running coroutine, releases resources immediately
        Both cascade to all descendant tasks along parent_task_id lineage.
        """
        ok = await self._terminate_single(task_id)
        if ok:
            await self._cascade_children(task_id, mode="terminate", depth=0)
        return ok

    async def list_tasks(
            self,
            status: Optional[TaskStatus] = None,
            limit: int = 100,
            offset: int = 0,
    ) -> list[Dict[str, Any]]:
        """List tasks (compose taskbase primitives: ids query + batch read).

        Orchestration query is not part of storage protocol responsibilities — this method composes
        list_task_ids_by_status + bulk_get_tasks at orchestration layer.
        Full-table scan without status filter is explicitly rejected (anti-pattern).
        """
        if status is None:
            raise ValueError(
                "list_tasks() requires explicit status filter "
                "(full-table scan is not supported by design)"
            )
        ids = await self.storage.list_task_ids_by_status(status.value)
        ids = ids[offset:offset + limit]
        if not ids:
            return []
        records = await self.storage.bulk_get_tasks(ids)
        return [{"task_id": tid, **rec} for tid, rec in zip(ids, records)]

    async def wait_terminal(
            self,
            task_id: str,
            *,
            timeout: float = 30.0,
            poll_interval: float = 0.05,
    ) -> Dict[str, Any]:
        """Wait for the task to reach a terminal state and return the full
        record (for recursive composition where a parent waits for children).

        The polling interval uses exponential backoff (x1.5, capped at 0.5s),
        so long-running tasks no longer hammer Redis at a fixed 50ms.

        Args:
            task_id: Task ID
            timeout: Maximum wait seconds (explicit upper bound)
            poll_interval: Starting polling interval in seconds

        Returns:
            Full task record (includes status/result/error fields)

        Raises:
            TimeoutError: Task did not reach a terminal state within timeout
            TaskNotFoundError: Task does not exist
        """
        import time
        deadline = time.monotonic() + timeout
        interval = poll_interval
        while time.monotonic() < deadline:
            record = await self.storage.get_task(task_id)
            if not record:
                raise TaskNotFoundError(f"task_id not found: {task_id}")
            status = TaskStatus(record["status"])
            if self.state_machine.is_terminal(status):
                return record
            await asyncio.sleep(interval)
            interval = min(interval * 1.5, 0.5)
        raise TimeoutError(
            f"wait_terminal timeout: task {task_id} did not reach "
            f"terminal state within {timeout}s"
        )


    async def wait_all_finalized(self, timeout: float = 5.0) -> None:
        """Wait for all background coroutines and finalization tasks to complete (for testing/application shutdown).

        After submit's bg task reaches business terminal state, shielded finalization may still be running —
        test/teardown scenarios need explicit draining, otherwise coroutines survive until event loop
        closes and are force-killed by GC ('Task was destroyed but it is pending').
        """
        import time as _time
        deadline = _time.monotonic() + timeout
        while _time.monotonic() < deadline:
            bg_pending = any(not t.done() for t in self._bg_tasks)
            finalize_pending = len(self.executor._finalize_tasks) > 0
            if not bg_pending and not finalize_pending:
                return
            await asyncio.sleep(0.02)
        # timeout fallback: cancel residual background tasks
        for t in self._bg_tasks:
            if not t.done():
                t.cancel()