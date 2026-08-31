"""Task executor (F2: lineage set deduplication + finalization coroutine lifecycle closure).

Changes:
- F2: _find_ancestor_resources returns a set (fixes A-B-A sandwich deadlock:
  root(holds A) → mid(holds B) → leaf(requests A); checking only the nearest ancestor would miss root's A,
  leaf queues for A, root waits for leaf → mutual wait; after set deduplication, any ancestor on the chain
  holding the same resource grants exemption).
- Finalization writes shielded + strong references (_shielded_finalize + _finalize_tasks):
  a) Prevent GC/cancellation from interrupting finalization (in GeneratorExit context, awaiting reports
     'coroutine ignored GeneratorExit');
  b) Register strong reference to the shielded inner task to prevent 'Task was destroyed but it is pending';
  c) When the event loop is closed (teardown phase) and create_task is unavailable, fallback:
     coro.close() prevents 'never awaited', finalization is abandoned (resource release during process exit
     is meaningless).
- contextvar reset defense: when a coroutine is forcibly closed, the context has already switched;
  resetting the old context token raises ValueError — swallow it without affecting correctness
  (the contextvar is naturally reclaimed when the coroutine is destroyed).

Retained changes:
- 1c: tasks marked cancel_requested, after completion, write CANCELLED (backfill) and call on_cancel,
  no longer overwritten by SUCCEEDED/FAILED, and no longer incorrectly call on_success/on_failure.
- R5: check cancel_requested after registering the coroutine and before acquire (plug terminate/submit race).
- R12: release goes through shield (secondary cancellation does not swallow release).
- R17: _ExecutionTimeout sentinel distinguishes execution timeout from business TimeoutError;
  outer cancellation cascades to cancel inner (prevent orphan coroutine).
- #2: acquire queue timeout parameterized (acquire_timeout, default None = infinite queuing).
"""
import asyncio
import logging
from typing import Any, Dict, Optional

from orditect.flow.protocols.storage import TaskStorageProtocol
from orditect.flow.protocols.governor import ResourceGovernorProtocol
from orditect.flow.core.task import BaseBackEndTask
from orditect.flow.core.state_machine import TaskStatus
from orditect.flow.core.context import current_task_id
from orditect.flow.snapshot import NullSnapshotQuery, NullSnapshotSink

_NULL_SINK = NullSnapshotSink()
_NULL_QUERY = NullSnapshotQuery()

logger = logging.getLogger(__name__)

try:
    from orditect.core import InvalidStatusTransferError as _TaskbaseInvalidTransfer
except ImportError:
    _TaskbaseInvalidTransfer = None  # taskbase 未安装时无终态保护兜底


class _ExecutionTimeout(Exception):
    """Execution timeout sentinel (module-private).

        In Python 3.11+, asyncio.TimeoutError is the same class as the built-in TimeoutError,
        so 'except' cannot distinguish between "framework execution timeout" and "business code raising TimeoutError".
        Therefore, _run_with_timeout raises this sentinel when wait expires, and execute() catches it
        and re-raises as asyncio.TimeoutError per the external contract.
        """

def _retrieve_finalize_error(task: "asyncio.Task") -> None:
    """Retrieve exceptions from shielded finalization tasks to prevent
    'exception was never retrieved' warnings at GC time. Business-hook
    failures are already logged at the call site; this only silences the
    asyncio warning channel."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.debug(f"Finalize task finished with error (already logged): {exc}")


class TaskExecutor:
    """Task executor (responsible for executing tasks)."""

    def __init__(
        self,
        storage: TaskStorageProtocol,
        governor: ResourceGovernorProtocol | None = None,
        *,
        acquire_timeout: float | None = None,
        snapshot_sink: Any = None,
        snapshot_query: Any = None,        # F3: reuse short-circuit query
        reuse_terminal_words: frozenset[str] | None = None,  # F3: success words
    ):
        """
        Args:
            ... (existing) ...
            snapshot_sink: F2 snapshot sink (default NullSink, zero behavior change).
            snapshot_query: F3 reuse query (default NullSnapshotQuery — never
                short-circuits). When provided, a node whose latest generation
                already reached a reuse-success word short-circuits: result is
                reused from the core hot record, not re-executed.
            reuse_terminal_words: F3 caller-declared success vocabulary for the
                reuse decision (vocabulary neutrality T6 — the framework embeds
                none). None defaults to frozenset() (no word qualifies → never
                short-circuits, even with a query injected).
        """
        self.storage = storage
        self.governor = governor
        self.acquire_timeout = acquire_timeout
        self._snapshot_sink = snapshot_sink or _NULL_SINK
        self._snapshot_query = snapshot_query or _NULL_QUERY
        self._reuse_words = frozenset(reuse_terminal_words or ())
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._finalize_tasks: set[asyncio.Task] = set()
    # R9/F2: maximum backtracking depth for lineage duplicate check (consistent with orchestrator cascade limit)
    _MAX_LINEAGE_DEPTH = 32

    async def _shielded_finalize(self, coro) -> None:
        """Shielded finalization + strong reference to prevent GC + loop
        closure fallback + exception retrieve.

        - The shielded inner task has no strong reference by default; when
          the main coroutine is cancelled, the inner task becomes a pending
          orphan -> register in _finalize_tasks to hold until completion.
        - The done callback both discards the reference AND retrieves any
          exception (prevents 'exception was never retrieved' warnings).
        - When the event loop is closed (teardown phase, GC forcibly kills
          coroutines), create_task raises RuntimeError -> coro.close()
          prevents 'never awaited' warnings, and finalization is abandoned
          (resource release during process exit is meaningless).
        """
        try:
            task = asyncio.create_task(coro)
        except RuntimeError:
            coro.close()
            logger.warning("Finalize skipped: no running event loop (teardown phase)")
            return
        self._finalize_tasks.add(task)
        task.add_done_callback(self._finalize_tasks.discard)
        task.add_done_callback(_retrieve_finalize_error)
        await asyncio.shield(task)

    async def _find_ancestor_resources(self, task_id: str) -> set[str]:
        """Collect the resource ledger set of all ancestors along the lineage chain (F2).

        Retains the parent_rec reuse optimization (each ancestor queried exactly once).

        Returns:
            Set of resource names held by all ancestors; if no ancestors / query fails → empty set
            (empty set = normal acquire, safe default).
        """
        resources: set[str] = set()
        try:
            rec = await self.storage.get_task(task_id)
        except Exception:
            return resources

        visited = {task_id}
        for _ in range(self._MAX_LINEAGE_DEPTH):
            parent = rec.get("parent_task_id")
            if parent is None:
                return resources
            if parent in visited:
                logger.warning(f"Lineage cycle detected at: {parent}")
                return resources
            visited.add(parent)

            try:
                parent_rec = await self.storage.get_task(parent)
            except Exception:
                return resources

            ancestor_resource = parent_rec.get("resource")
            if ancestor_resource:
                resources.add(ancestor_resource)
            rec = parent_rec
        return resources

    def is_running(self, task_id: str) -> bool:
        """Check whether the task coroutine is running in this process."""
        return task_id in self._running_tasks

    async def cancel(self, task_id: str, force: bool = False) -> None:
        """Cancel task execution (dual mode, semantics same as earlier)."""
        if force:
            task = self._running_tasks.get(task_id)
            if task is not None and not task.done():
                task.cancel()
                logger.info(f"Task coroutine cancelled (force): {task_id}")
            else:
                logger.warning(
                    f"Cannot force-cancel: task coroutine not found "
                    f"(finished or running in another process): {task_id}"
                )
        else:
            try:
                await self.storage.request_cancel(task_id)
                logger.info(f"Task cancel requested (graceful): {task_id}")
            except Exception as e:
                logger.warning(
                    f"request_cancel failed (task may not exist): {task_id}, "
                    f"error: {e}"
                )

    # ---------- internal helpers ----------

    async def _is_cancel_requested(self, task_id: str) -> bool:
        """Read the cancellation flag (on failure treat as not cancelled, do not block main flow)."""
        try:
            task = await self.storage.get_task(task_id)
            return bool(task.get("cancel_requested", False))
        except Exception as e:
            logger.warning(f"cancel_requested check failed (assume not cancelled): {task_id}, error: {e}")
            return False

    async def _write_snapshot(self, task_id: str, status: str, terminal: bool,
                              error: Exception | None = None,
                              execution_id: str = "") -> None:
        """F2: write execution snapshot (T9 non-blocking, T11 execution_id from
        the core hot record). Failures are logged and swallowed — snapshot is
        observation, never blocks the task path."""
        try:
            if execution_id:
                rec = await self.storage.get_task(task_id)
                eid = execution_id
                parent = rec.get("parent_task_id")
            else:
                rec = await self.storage.get_task(task_id)
                eid = rec.get("execution_id", "")
                parent = rec.get("parent_task_id")
            await self._snapshot_sink.write(
                task_id=task_id,
                execution_id=eid,
                parent_task_id=parent,
                status=status,
                terminal=terminal,
                error=str(error) if error else None,
            )
        except Exception as e:
            logger.warning(f"snapshot write failed (execution unaffected): {task_id}, {e}")

    async def _safe_hook(self, hook, *args) -> None:
        """Invoke a business hook with try/except (T9: observation hooks
        must never block or crash the finalization write chain)."""
        try:
            await hook(*args)
        except Exception as e:
            logger.warning(f"Task hook raised (write chain unaffected): {e}")

    async def _try_reuse_result(self, task_id: str) -> tuple[bool, Any]:
        """F3: reuse short-circuit (option B — result lives in the core hot record).

        Returns (reused, result). reused=True means the latest generation already
        reached a caller-declared success word AND the hot record carries a
        result; the caller then short-circuits execution and reuses it.

        None-result semantics (adjudicated v0.1.7, #11): a task that
        legitimately returns None (side-effect-only work) still counts as
        reusable — the check is `"result" not in rec` (key presence), not
        `rec.get("result") is not None`. RecoveryService.decide uses the
        same rule, so executor reuse and recovery rerun agree on
        side-effect tasks.

        Capability degradation (T8): any query/storage failure falls back to
        normal execution with a warning — never silently misjudges.
        """
        if not self._reuse_words:
            return False, None
        try:
            latest = await self._snapshot_query.latest_status(task_id)
        except Exception as e:
            logger.warning(f"reuse query failed (normal execution): {task_id}, {e}")
            return False, None
        if latest not in self._reuse_words:
            return False, None
        try:
            rec = await self.storage.get_task(task_id)
        except Exception as e:
            logger.warning(f"reuse hot-record read failed (normal execution): {task_id}, {e}")
            return False, None
        if "result" not in rec:
            return False, None
        logger.info(f"reusing prior successful result (skip re-execution): {task_id}")
        return True, rec["result"]

    async def _run_with_timeout(self, task: BaseBackEndTask, task_id: str,
                                timeout: float, kwargs: dict) -> Any:
        """Task call with execution timeout (R17).
        - wait timeout → raise _ExecutionTimeout sentinel (distinguish from business TimeoutError)
        - outer cancelled → cascade cancel inner (prevent orphan coroutine, force cancel remains effective)
        """
        inner = asyncio.create_task(task.execute(task_id=task_id, **kwargs))
        try:
            done, pending = await asyncio.wait({inner}, timeout=timeout)
        except asyncio.CancelledError:
            inner.cancel()
            await asyncio.gather(inner, return_exceptions=True)
            raise
        if pending:
            inner.cancel()
            await asyncio.gather(inner, return_exceptions=True)
            raise _ExecutionTimeout(f"Task execution timeout after {timeout}s")
        return inner.result()  # 业务异常（含内置 TimeoutError）原样传播

    async def _update_terminal(self, task_id: str, updates: dict) -> bool:
        """Write terminal state (race fallback: return False when rejected by taskbase terminal protection)."""
        try:
            await self.storage.update_task(
                task_id,
                updates=updates,
                validate_status_transfer=False,
            )
            return True
        except Exception as e:
            if _TaskbaseInvalidTransfer is not None and isinstance(e, _TaskbaseInvalidTransfer):
                logger.warning(
                    f"Terminal write rejected by storage terminal protection "
                    f"(task already in terminal state): {task_id}"
                )
                return False
            raise

    async def _settle_cancelled(self, task_id: str, reason: str) -> None:
        """1c finalization: tasks marked cancel_requested uniformly set to CANCELLED (idempotent write)."""
        written = await self._update_terminal(
            task_id,
            {"status": TaskStatus.CANCELLED.value, "cancel_outcome": reason},
        )
        if not written:
            logger.debug(f"Task already terminal (cancelled by lifecycle): {task_id}")

    async def _finalize_failure(self, task_id: str, task: BaseBackEndTask,
                                error: Exception, outcome: str) -> None:
        """Complete write chain for failure finalization (wrapped by
        _shielded_finalize). Hook calls are wrapped in try/except (T9:
        observation never blocks the write chain)."""
        if await self._is_cancel_requested(task_id):
            await self._settle_cancelled(task_id, outcome)
            await self._write_snapshot(
                task_id, TaskStatus.CANCELLED.value, terminal=True, error=error
            )
            await self._safe_hook(task.on_cancel, task_id)
        else:
            ok = await self._update_terminal(
                task_id,
                {"status": TaskStatus.FAILED.value, "error": str(error)},
            )
            if ok:
                await self._write_snapshot(
                    task_id, TaskStatus.FAILED.value, terminal=True, error=error
                )
                await self._safe_hook(task.on_failure, task_id, error)
            else:
                await self._safe_hook(task.on_cancel, task_id)

    async def _finalize_cancel(self, task_id: str, task: BaseBackEndTask,
                               execution_id: str = "") -> None:
        """Write chain for cancellation finalization (wrapped by
        _shielded_finalize). Hook calls are wrapped in try/except (T9).

        execution_id is captured by the caller at cancel time: by the time
        this shielded background task runs, a concurrent reopen may already
        have advanced the hot record to a new generation, so re-reading it
        here would write the cancelled snapshot into the WRONG generation.
        """
        await self._update_terminal(task_id, {"status": TaskStatus.CANCELLED.value})
        await self._write_snapshot(
            task_id, TaskStatus.CANCELLED.value, terminal=True,
            execution_id=execution_id,
        )
        await self._safe_hook(task.on_cancel, task_id)
    # ---------- main execution flow ----------

    async def execute(
        self,
        task_id: str,
        task: BaseBackEndTask,
        resource: Optional[str] = None,
        timeout: float | None = None,
        **kwargs,
    ) -> Any:
        """Execute task."""
        resource_name = resource or task.resource_type
        kwargs.setdefault("governor", self.governor)

        token = None
        inherited = False  # R9/F2: slot inherited from an ancestor (not released in finally)

        # register running coroutine (for force cancellation)
        self._running_tasks[task_id] = asyncio.current_task()

        # R6-2: set current task context (nested submit auto-inherits parent identity)
        _ctx_token = current_task_id.set(task_id)

        try:
            # 0. R5: check cancel_requested before acquire (block terminate/submit race)
            if await self._is_cancel_requested(task_id):
                logger.info(f"Task cancelled before acquire (TOCTOU guard): {task_id}")
                raise asyncio.CancelledError()

            # F3: reuse short-circuit — an already-succeeded node reuses its prior
            # result. v0.1.6: evaluated BEFORE acquire and BEFORE the running
            # write, so a reused node never occupies a semaphore slot and never
            # leaves the hot record stuck at RUNNING (zombie).
            reused, reused_result = await self._try_reuse_result(task_id)
            if reused:
                logger.info(f"Task reused (short-circuit): {task_id}")
                return reused_result

            # 1. R9/F2 + v0.1.1: exemption check — the snapshot frozen at
            #    registration wins over the live ancestor walk when present.
            if self.governor:
                try:
                    rec = await self.storage.get_task(task_id)
                    snapshot = rec.get("exempt_resources_snapshot")
                except Exception as e:
                    logger.warning(
                        f"exemption snapshot read failed (fallback to walk): "
                        f"{task_id}, {e}"
                    )
                    snapshot = None
                if snapshot is not None:
                    # v0.1.1: exemption snapshot frozen at registration
                    # (invalidated via invalidate_exempt_snapshot on reopen);
                    # an empty list means "explicitly no exemption".
                    ancestor_resources: set[str] = set(snapshot)
                else:
                    ancestor_resources = await self._find_ancestor_resources(task_id)
                if resource_name in ancestor_resources:
                    inherited = True
                    logger.info(
                        f"Resource inherited from ancestor (skip acquire): "
                        f"{task_id} -> {resource_name}"
                    )
                else:
                    logger.debug(f"Acquiring resource: {resource_name}")
                    token = await self.governor.acquire(
                        resource_name,
                        timeout=self.acquire_timeout,
                    )
                    # R9-1: resource ledger registration (for descendant dedup)
                    try:
                        await self.storage.update_task(
                            task_id,
                            updates={"resource": resource_name},
                            validate_status_transfer=False,
                        )
                    except Exception as e:
                        # ledger failure does not affect execution (descendants
                        # degrade to a normal acquire — safe default)
                        logger.warning(
                            f"Resource ledger write failed: {task_id}, error: {e}"
                        )

            # 2. update status to running
            await self.storage.update_task(
                task_id,
                updates={"status": TaskStatus.RUNNING.value},
                validate_status_transfer=False,
            )
            await self._write_snapshot(task_id, TaskStatus.RUNNING.value, terminal=False)  # F2

            # 3. execute task (R17: only a wait expiry counts as an execution timeout)
            if timeout:
                result = await self._run_with_timeout(task, task_id, timeout, kwargs)
            else:
                result = await task.execute(task_id=task_id, **kwargs)

            # 4. 1c: check the cancel flag after the task returns
            if await self._is_cancel_requested(task_id):
                await self._settle_cancelled(task_id, "succeeded_but_cancelled")
                await self._safe_hook(task.on_cancel, task_id)
                logger.info(f"Task completed but was cancelled: {task_id}")
                return result

            # 5. normal success path
            ok = await self._update_terminal(
                task_id,
                {
                    "status": TaskStatus.SUCCEEDED.value,
                    "result": result,
                    "progress": 1.0,
                },
            )
            if not ok:
                await self._safe_hook(task.on_cancel, task_id)
                return result

            await self._write_snapshot(task_id, TaskStatus.SUCCEEDED.value, terminal=True)
            await self._safe_hook(task.on_success, task_id, result)
            logger.info(f"Task succeeded: {task_id}")
            return result

        except _ExecutionTimeout as e:
            logger.error(f"Task execution timeout: {task_id}")
            await self._shielded_finalize(
                self._finalize_failure(task_id, task, e, "timeout_but_cancelled")
            )
            raise asyncio.TimeoutError(str(e)) from e

        except asyncio.CancelledError:
            logger.info(f"Task cancelled: {task_id}")
            # Capture the current generation BEFORE shielding: a concurrent
            # reopen_task may advance the hot record's execution_id while the
            # shielded finalize runs, and the cancelled snapshot must still be
            # written into THIS generation (T11), not the reopened one.
            try:
                _rec = await self.storage.get_task(task_id)
                _eid = _rec.get("execution_id", "")
            except Exception:
                _eid = ""
            await self._shielded_finalize(
                self._finalize_cancel(task_id, task, execution_id=_eid)
            )
            raise

        except Exception as e:
            # task failure (a business TimeoutError also lands here — never
            # mislabeled as an execution timeout)
            logger.error(f"Task failed: {task_id}, error: {e}", exc_info=True)
            await self._shielded_finalize(
                self._finalize_failure(task_id, task, e, "failed_but_cancelled")
            )
            raise

        finally:
            # restore task context (defensive: when the coroutine is force-closed
            # the context has already switched; resetting the old token would
            # raise ValueError — swallow it, correctness is unaffected since the
            # contextvar is reclaimed with the coroutine)
            try:
                current_task_id.reset(_ctx_token)
            except ValueError:
                pass
            # unregister coroutine tracking
            self._running_tasks.pop(task_id, None)

            # R12: release the resource token (shield so a second cancellation
            # does not swallow the release; _shielded_finalize handles the
            # loop-closed case)
            # R9/F2: an inherited slot is never released here (the ancestor owns it)
            if self.governor and token and not inherited:
                logger.debug(f"Releasing resource: {resource_name}")
                await self._shielded_finalize(self.governor.release(resource_name, token))

