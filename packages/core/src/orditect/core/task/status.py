"""Task state machine. P1 original migration from SnapAudit_10 core/task/task_status.py.

Design points:
- TaskStatus inherits str + Enum, ensuring JSON serialization is string
- can_transfer uses whitelist, unlisted transfers all rejected
- Terminal states (completed/failed/cancelled) have empty transfer targets, irreversible
"""
from enum import Enum


class TaskStatus(str, Enum):
    pending = "pending"
    in_progress = "in_progress"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


TERMINAL_STATUSES = {
    TaskStatus.completed.value,
    TaskStatus.failed.value,
    TaskStatus.cancelled.value,
}

# Legal status transfer whitelist (key=source status, value=allowed target status set)
_ALLOWED_TRANSITIONS = {
    "": {TaskStatus.pending.value, TaskStatus.in_progress.value},  # compatible with historical empty status
    TaskStatus.pending.value: {
        TaskStatus.in_progress.value,
        TaskStatus.cancelled.value,
        TaskStatus.failed.value,
    },
    TaskStatus.in_progress.value: {
        TaskStatus.completed.value,
        TaskStatus.failed.value,
        TaskStatus.cancelled.value,
    },
    TaskStatus.completed.value: set(),  # terminal state cannot transfer
    TaskStatus.failed.value: set(),
    TaskStatus.cancelled.value: set(),
}


def is_terminal(status: str) -> bool:
    """Check if terminal state."""
    return status in TERMINAL_STATUSES


def can_transfer(from_status: str, to_status: str) -> bool:
    """Check if status transfer is legal (whitelist)."""
    return to_status in _ALLOWED_TRANSITIONS.get(from_status, set())