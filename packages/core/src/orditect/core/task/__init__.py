"""Task domain primitives: state machine + cancellation token."""
from orditect.core.task.status import TaskStatus, can_transfer, is_terminal
from orditect.core.task.cancel import CancellationToken

__all__ = ["TaskStatus", "can_transfer", "is_terminal", "CancellationToken"]