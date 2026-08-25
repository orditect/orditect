"""Task state machine: manage task state transitions."""
from enum import Enum
from typing import Dict, Set

from orditect.flow.exceptions import InvalidStateTransitionError


class TaskStatus(str, Enum):
    """Task status enumeration."""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskStateMachine:
    """Task state machine (manage state transitions).

    State transition diagram:
    pending → queued → running → succeeded
                        ↓         ↓
                    cancelled   failed
    """

    def __init__(self):
        # define allowed state transitions
        self.allowed_transitions: Dict[TaskStatus, Set[TaskStatus]] = {
            TaskStatus.PENDING: {TaskStatus.QUEUED, TaskStatus.CANCELLED},
            TaskStatus.QUEUED: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
            TaskStatus.RUNNING: {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED},
            TaskStatus.SUCCEEDED: set(),  # 终态
            TaskStatus.FAILED: set(),  # 终态
            TaskStatus.CANCELLED: set(),  # 终态
        }

    def can_transition(self, from_status: TaskStatus, to_status: TaskStatus) -> bool:
        """Check whether the state transition is valid.

        Args:
            from_status: Source status
            to_status: Target status

        Returns:
            True: transition allowed
            False: transition forbidden
        """
        return to_status in self.allowed_transitions.get(from_status, set())

    def validate_transition(self, from_status: TaskStatus, to_status: TaskStatus) -> None:
        """Validate state transition (raise exception on invalid).

        Args:
            from_status: Source status
            to_status: Target status

        Raises:
            InvalidStateTransitionError: Invalid state transition
        """
        if not self.can_transition(from_status, to_status):
            raise InvalidStateTransitionError(
                f"invalid state transition: {from_status.value} -> {to_status.value}"
            )

    def is_terminal(self, status: TaskStatus) -> bool:
        """Check whether the status is terminal.

        Args:
            status: Task status

        Returns:
            True: terminal (succeeded/failed/cancelled)
            False: non-terminal
        """
        return status in {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED}

    def get_allowed_transitions(self, status: TaskStatus) -> Set[TaskStatus]:
        """Get all allowed transition targets for a given status.

        Args:
            status: Task status

        Returns:
            Set of allowed target statuses
        """
        return self.allowed_transitions.get(status, set())