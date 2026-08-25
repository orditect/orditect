"""Workflow step definition."""
from typing import Callable, Any, Optional, List, Dict
from dataclasses import dataclass, field


@dataclass
class WorkflowStep:
    """Workflow step.

    Defines a single step in a workflow, including:
    - Step name
    - Handler function (execution logic)
    - Dependencies (preceding steps)
    - Rollback function (compensation logic on failure)
    - Retry policy (optional)
    """

    name: str
    """Step name (unique identifier)."""

    handler: Callable
    """Handler function (async function).

        Signature: async def handler(context: Dict[str, Any], results: Dict[str, Any]) -> Any
        - context: Workflow context (input parameters)
        - results: Results of completed steps (keyed by step name)
        - Returns: Step result
        """

    dependencies: List[str] = field(default_factory=list)
    """List of dependent step names (preceding steps).

        Only after all dependent steps are completed can the current step execute.
        """

    rollback_handler: Optional[Callable] = None
    """Rollback function (optional, compensation logic on failure).

        Signature: async def rollback_handler(result: Any) -> None
        - result: Result of the current step (if produced)

        On workflow failure, rollback functions of completed steps are called in reverse order.
        """

    retry_policy: Optional[Any] = None
    """Retry policy (optional, type is RetryPolicy, defined later in the retry module)."""

    timeout: Optional[float] = None
    """Step execution timeout in seconds (optional)."""

    def __post_init__(self):
        """Validate step definition."""
        if not self.name:
            raise ValueError("WorkflowStep name cannot be empty")
        if not callable(self.handler):
            raise ValueError(f"WorkflowStep handler must be callable: {self.name}")
        if self.rollback_handler is not None and not callable(self.rollback_handler):
            raise ValueError(f"WorkflowStep rollback_handler must be callable: {self.name}")