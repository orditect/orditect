"""Base task class: all tasks must inherit from this class."""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

from orditect.flow.protocols.storage import TaskStorageProtocol
from orditect.flow.protocols.governor import ResourceGovernorProtocol


class BaseBackEndTask(ABC):
    """Base task class (all tasks must inherit from this class).

    Usage example:
        class MyTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                # Task logic
                result = await process_data(kwargs["data"])
                return result

        # Create task instance
        task = MyTask(storage, governor)

        # Submit task via orchestrator
        task_id = await orchestrator.submit(task, data={"key": "value"})

    Multi-type governance:
        The class attribute resource_type defines the governance resource name for this task (default "task_execution").
        TaskExecutor acquires a resource token based on the resource parameter or task.resource_type when executing.
        Subclasses can override resource_type to implement multi-type governance:

        class AgentTask(BaseBackEndTask):
            resource_type = "task_agent"

        class OSSBatchTask(BaseBackEndTask):
            resource_type = "task_oss_batch"

    Dual-layer governance:
        - Task governance: task boundary management (resource_type), acquire at task start, release at task end
        - Global governance: call-point management (GovernedClient), acquire at call start, release at call end
        When global governance is needed inside a task, obtain the governor injected by the executor from kwargs:

        class AgentTask(BaseBackEndTask):
            resource_type = "task_agent"

            async def execute(self, task_id: str, **kwargs):
                governor = kwargs.get("governor") or self.governor
                llm = GovernedClient(governor, resource="default_stream_llm",
                                     handler=call_llm)
                return await llm.call(prompt)
    """

    # : task governance resource type (subclass can override for multi-type governance)
    # : TaskExecutor actually uses resource name from submit/execute's resource parameter or this attribute
    resource_type: str = "task_execution"

    def __init__(
            self,
            storage: TaskStorageProtocol,
            governor: Optional[ResourceGovernorProtocol] = None,
    ):
        """
        Args:
            storage: Task storage (for state management)
            governor: Resource governance (optional, for concurrency control)
        """
        self.storage = storage
        self.governor = governor

    @abstractmethod
    async def execute(self, task_id: str, **kwargs) -> Any:
        """Execute the task (must be implemented by subclasses).

        Args:
            task_id: Task ID
            **kwargs: Task parameters (TaskExecutor injects governor,
                obtainable via kwargs.get("governor") for global call-point governance)

        Returns:
            Task result

        Raises:
            Exception: Task execution failed
        """
        raise NotImplementedError

    async def on_success(self, task_id: str, result: Any) -> None:
        """Task success hook (optional to implement).

        Args:
            task_id: Task ID
            result: Task result
        """
        pass

    async def on_failure(self, task_id: str, error: Exception) -> None:
        """Task failure hook (optional to implement).

        Args:
            task_id: Task ID
            error: Exception information
        """
        pass

    async def on_cancel(self, task_id: str) -> None:
        """Task cancellation hook (optional to implement).

        Args:
            task_id: Task ID
        """
        pass

    async def report_progress(self, task_id: str, progress: float) -> None:
        """Report progress (can be called by subclasses).

        Args:
            task_id: Task ID
            progress: Progress (0.0 - 1.0)
        """
        await self.storage.update_task(task_id, {"progress": progress})