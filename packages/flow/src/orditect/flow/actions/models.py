"""Action command models: the mechanism records for the action-queue form.

These models are shared between the UI adapter (which writes commands) and
the flow ActionDispatcher (which consumes and executes them). They live in
flow because they are mechanism records — not UI-specific, not protocol
contract surface (C1: no active verbs on the protocol surface).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Protocol


class ActionType(str, Enum):
    """Mechanism action types (vocabulary-neutral)."""

    PAUSE = "pause"
    RETRY = "retry"
    RESUME = "resume"


@dataclass(frozen=True)
class ActionCommand:
    """One action command (the queue record)."""

    action_id: str
    action_type: ActionType
    target_task_id: str
    root_task_id: str | None = None
    scope: frozenset[str] = frozenset()
    actor: str = ""  # who triggered (user id / agent id / system)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ActionReceipt:
    """Acceptance receipt returned to the caller after queueing."""

    accepted: bool
    action_id: str
    detail: str = ""


class ActionQueue(Protocol):
    """Queue carrier for action commands (duck-typed).

    Implementations: hot-path Redis list (production) / in-memory list
    (reference/testing). The dispatcher polls this queue.
    """

    async def enqueue(self, command: ActionCommand) -> None:
        """Append one command to the queue."""
        ...

    async def dequeue(self, *, timeout: float = 1.0) -> ActionCommand | None:
        """Pop one command from the queue (None on timeout)."""
        ...

    async def get_receipt(self, action_id: str) -> dict[str, Any] | None:
        """Query the execution receipt of one action (None if not yet executed)."""
        ...


def new_action_id() -> str:
    """Generate a new action ID (deterministic format)."""
    return f"act-{uuid.uuid4().hex[:12]}"