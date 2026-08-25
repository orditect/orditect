"""Framework-neutral exceptions."""

class TaskflowError(Exception):
    """Base exception for all framework exceptions."""


class TaskNotFoundError(TaskflowError):
    """Task does not exist."""


class InvalidStateTransitionError(TaskflowError):
    """Invalid state transition."""


class TaskCancelledError(TaskflowError):
    """Task has been cancelled."""


class AcquireTimeoutError(TaskflowError, TimeoutError):
    """Resource acquisition timeout.

        Also inherits built-in TimeoutError: for backward compatibility.
        """


class DependencyNotSatisfiedError(TaskflowError):
    """Dependency not satisfied."""


class WorkflowExecutionError(TaskflowError):
    """Workflow execution error."""