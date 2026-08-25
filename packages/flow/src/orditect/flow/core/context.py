"""Task execution context (parent context auto-injection).

asyncio.create_task copies the current context, so when nested submit is called,
child tasks automatically inherit the parent task identity — zero boilerplate for same-process recursion,
explicit parameter passing for cross-process.
"""
from __future__ import annotations

from contextvars import ContextVar

# : current executing task ID (set when executor enters execute)
current_task_id: ContextVar[str | None] = ContextVar("taskflow_current_task_id", default=None)