"""Action sink: protocolized action channel (command-queue form, DD-013).

UI/HITL/MCP/agent write action commands into a queue; flow's ActionDispatcher
consumes and executes them asynchronously. Action records double as audit
events (event_id = action_id), giving idempotency and traceability for free.

This module is the UI-side writer: it builds commands and enqueues them.
The action models (ActionCommand / ActionType / ActionQueue) live in flow
(shared mechanism records).
"""

from __future__ import annotations

import logging
from typing import Any

from orditect.flow.actions.models import (
    ActionReceipt,
    ActionType,
    ActionQueue,
    new_action_id,
)
from orditect.flow.actions.models import ActionCommand
from orditect.protocol import AuditEvent

logger = logging.getLogger(__name__)


class ActionSinkAdapter:
    """Write-side adapter: converts UI/HITL/MCP/agent calls into queue commands.

    Args:
        queue: ActionQueue implementation (hot-path or stub).
        audit_writer: protocol AuditWriter (optional). When set, every
            accepted command also writes an audit event (event_id = action_id)
            for traceability.
    """

    def __init__(self, queue: ActionQueue, audit_writer: Any = None) -> None:
        self._queue = queue
        self._audit = audit_writer

    async def _submit(
        self,
        action_type: ActionType,
        target_task_id: str,
        *,
        root_task_id: str | None = None,
        scope: frozenset[str] = frozenset(),
        actor: str = "",
        params: dict[str, Any] | None = None,
    ) -> ActionReceipt:
        """Build, enqueue, and audit one action command."""
        action_id = new_action_id()
        command = ActionCommand(
            action_id=action_id,
            action_type=action_type,
            target_task_id=target_task_id,
            root_task_id=root_task_id,
            scope=scope,
            actor=actor,
            params=params or {},
        )
        try:
            await self._queue.enqueue(command)
        except Exception as e:
            logger.error(f"action enqueue failed: {action_id}, {e}")
            return ActionReceipt(
                accepted=False, action_id=action_id, detail=str(e)
            )

        # Audit the accepted command (T9: observation never blocks)
        if self._audit is not None:
            try:
                await self._audit.append(
                    AuditEvent(
                        event_id=action_id,
                        task_id=target_task_id,
                        event_type=f"action_{action_type.value}",
                        payload={
                            "action_type": action_type.value,
                            "target": target_task_id,
                            "root": root_task_id,
                            "scope": sorted(scope) if scope else None,
                            "actor": actor,
                            "params": params,
                        },
                    )
                )
            except Exception as e:
                logger.warning(f"action audit failed (accepted): {action_id}, {e}")

        return ActionReceipt(accepted=True, action_id=action_id)

    async def pause_node(
        self, task_id: str, *, actor: str = ""
    ) -> ActionReceipt:
        """Pause a node (cancel + cascade)."""
        return await self._submit(
            ActionType.PAUSE, task_id, actor=actor
        )

    async def retry_node(
        self, task_id: str, root_task_id: str, *, actor: str = ""
    ) -> ActionReceipt:
        """Retry a single node within a tree."""
        return await self._submit(
            ActionType.RETRY,
            task_id,
            root_task_id=root_task_id,
            scope=frozenset({task_id}),
            actor=actor,
        )

    async def retry_scope(
        self, root_task_id: str, scope: set[str], *, actor: str = ""
    ) -> ActionReceipt:
        """Retry an explicit scope of nodes."""
        return await self._submit(
            ActionType.RETRY,
            root_task_id,
            root_task_id=root_task_id,
            scope=frozenset(scope),
            actor=actor,
        )

    async def resume_tree(
        self, root_task_id: str, *, actor: str = ""
    ) -> ActionReceipt:
        """Resume a task tree from failure points."""
        return await self._submit(
            ActionType.RESUME, root_task_id, root_task_id=root_task_id, actor=actor
        )

    async def get_receipt(self, action_id: str) -> dict[str, Any] | None:
        """Query the execution receipt of a previously submitted action."""
        return await self._queue.get_receipt(action_id)