"""orditect-flow adapter: TaskflowResultStore + TaskflowEnricher.

Changes:
- TaskflowResultStore.save() uses true TTL (#5 fix):
  initialize_task(expiry=...) + update_task(expiry=...),
  no longer stuffing ttl into data fields (previously default_expire_time 7 days silently took effect).
- TaskflowEnricher implements true taskflow dispatching (#3 fix):
  resolve() internally submits + wait_terminal, replacing the broken skeleton from earlier versions
  (which falsely claimed tf: prefix but dispatched locally with a fake task_id as job_id).
- Deterministic task_id convention (design point 2, framework spec):
  task_id = f"enrich-{placeholder_id}" — shares the same convention with EnrichManager._make_task_ref(),
  aligning dispatcher and reference side zero-channel, so retries/replays for the same placeholder
  converge to the same task (idempotent-friendly, leveraging taskstore idempotent primitives).

Available only when orditect-flow is installed (hard dependency since it became required).
"""
from __future__ import annotations

import logging
from typing import Any, Callable

from orditect.stream.events import PlaceholderState
from orditect.stream.exceptions import EnrichError
from orditect.stream.protocols import (
    EnrichRequest,
    EnrichResult,
    ResultStoreProtocol,
)

logger = logging.getLogger(__name__)


class TaskflowResultStore(ResultStoreProtocol):
    """Reuse taskflow storage to store manifest (natively compatible with task_ref system).

    manifest is stored as a special "task record" with task_id = stream_id.
    #5: save's ttl truly takes effect (record-level EXPIRE),
    get returns None for expired records (underlying key has evaporated).

    Reserved status word: the task record's status is set to the reserved
    word "manifest" (not part of any business state machine). Callers must
    never drive status transitions on these records — only the manifest
    field is updated; a status update on a "manifest" record fails loudly
    with InvalidStatusTransferError by design.
    """

    def __init__(self, storage: Any):
        """
        Args:
            orchestrator: orditect.flow TaskStorageProtocol instance
        """
        self._storage = storage

    async def save(self, stream_id: str, manifest: dict[str, Any], ttl: int) -> None:
        """Store manifest (as a field of the task record, record-level TTL takes effect)."""
        try:
            # v0.3.2 (#5): initialize with real expiry; already exists (duplicate save) skip overwriting accounting
            await self._storage.initialize_task(
                task_id=stream_id,
                expiry=ttl,
                initial_status="manifest",
                if_not_exists=True,
            )
            # update synchronously refreshes expiry (duplicate save scenario: latest ttl wins)
            await self._storage.update_task(
                stream_id,
                {"manifest": manifest},
                expiry=ttl,
            )
        except Exception as e:
            logger.error(f"TaskflowResultStore save failed: {e}", exc_info=True)
            raise

    async def get(self, stream_id: str) -> dict[str, Any] | None:
        """Retrieve manifest (returns None if not found or expired)."""
        try:
            task = await self._storage.get_task(stream_id)
            return task.get("manifest")
        except Exception:
            return None


class TaskflowEnricher:
    """Wrap enrich tasks as taskflow tasks and dispatch via TaskOrchestrator (true dispatching).

    task_ref convention (framework spec, shared with EnrichManager._make_task_ref):
        task_id = f"enrich-{placeholder_id}"
        task_ref = f"tf:enrich-{placeholder_id}"
    Deterministic IDs align the dispatcher (manager) and the reference side (ManifestResolver) zero-channel;
    retries/replays for the same placeholder reuse the same task_id (submit(if_not_exists=True) idempotent).

    Usage example:
        orchestrator = TaskOrchestrator(storage, governor)
        enricher = TaskflowEnricher(
            orchestrator=orchestrator,
            task_factory=lambda req: MyEnrichTask(storage, governor, req),
        )
        runner = StreamRunner(..., enricher=enricher,
                              config=cfg.merge(enrich_mode=EnrichMode.TASKFLOW))
    """

    def __init__(
        self,
        orchestrator: Any,
        task_factory: Callable[[EnrichRequest], Any],
        *,
        wait_timeout: float = 300.0,
    ):
        """
        Args:
            orchestrator: orditect.flow.TaskOrchestrator
            task_factory: Factory from EnrichRequest to BaseBackEndTask instance.
                The task's execute() should return {"url": ...} (dict containing url key).
                The factory is injected by business logic — the framework does not know how to construct specific enrich tasks.
            wait_timeout: Maximum seconds to wait for task terminal state (default 300s,
                consistent with the principle that "any wait has a limit").
        """
        self._orchestrator = orchestrator
        self._task_factory = task_factory
        self._wait_timeout = wait_timeout

    async def resolve(self, request: EnrichRequest) -> EnrichResult:
        """Submit + wait_terminal for true dispatching, map terminal result to EnrichResult.

        Raises:
            EnrichError: task failed/cancelled/timed out
        """
        task_id = f"enrich-{request.placeholder_id}"
        task = self._task_factory(request)

        await self._orchestrator.submit(task, task_id=task_id, if_not_exists=True)
        record = await self._orchestrator.wait_terminal(
            task_id, timeout=self._wait_timeout
        )

        status = record.get("status")
        if status == "succeeded":
            result = record.get("result") or {}
            url = result.get("url") if isinstance(result, dict) else None
            if url:
                return EnrichResult(url=url, state=PlaceholderState.RESOLVED)
            raise EnrichError(
                f"taskflow enrich succeeded but no url in result: {task_id}"
            )
        raise EnrichError(
            f"taskflow enrich task {status}: {task_id} "
            f"error={record.get('error')}"
        )