"""Abstract interface for task storage (lineage + idempotency extensions)."""
from typing import Protocol, Dict, Any, Optional, List


class TaskStorageProtocol(Protocol):
    """Task storage protocol (defined by taskflow, implemented by taskbase).

    Responsibilities:
    - Initialize task records (supports lineage registration and idempotency)
    - Update task status and metadata
    - Query task information
    - Request task cancellation
    - Lineage query (data foundation for cascading cancellation / observation trees)

    Breaking changes:
    - initialize_task adds keyword-only parameters parent_task_id / if_not_exists,
      return value changes from None to bool (True=initialized successfully, False=idempotent skip).
    - Added list_children method.
      Custom storage implementations need to update accordingly (no compatibility burden as not yet live, CHANGELOG announced).

    validate_status_transfer parameter description (same as before):
    - For taskbase implementation: True enables underlying state machine validation; False skips it (Lua terminal protection still active).
    - For taskflow local implementation: kept for signature compatibility only, actually ignored (state machine validation is handled by TaskStateMachine).

    Signature discipline (established):
    - Cross-implementation calls must use keyword arguments (taskflow protocol and taskbase positional semantics differ:
      protocol second position is initial_status, taskbase second position is expiry).
    - New parameters go in keyword-only position (after *), to prevent positional ambiguity.

    List query note (batch 5):
    list_tasks does not belong to the storage protocol — the orchestration layer (TaskOrchestrator.list_tasks)
    composes list_task_ids_by_status + bulk_get_tasks two data-plane primitives to implement it.
    """

    async def initialize_task(
        self,
        task_id: str,
        initial_status: str,
        *,
        parent_task_id: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> bool:
        """Initialize a task record.

        Args:
            task_id: Task ID
            initial_status: Initial status (e.g. "pending")
            parent_task_id: Parent task ID (lineage registration, data foundation for cascading cancellation/observation trees).
                None indicates root task.
            if_not_exists: Idempotency switch. When True, if the task already exists, skip all writes
                and return False (prevents parent retry from resetting a running child task back to initial status).

        Returns:
            True if initialization succeeded; False if if_not_exists is True and task already exists (idempotent skip).

        Note:
            metadata should be set separately via update_task(), to be compatible with different storage implementations.
        """
        ...

    async def update_task(
        self,
        task_id: str,
        updates: Dict[str, Any],
        validate_status_transfer: bool = True,
    ) -> None:
        """Update a task record.

        Args:
            task_id: Task ID
            updates: Fields to update (e.g. {"status": "running", "progress": 0.5})
            validate_status_transfer: Whether to enable underlying storage state machine validation.
        """
        ...
    async def get_task(self, task_id: str) -> Dict[str, Any]:
        """Get a task record.

        Contract: a MISSING task returns an empty dict ({}), never raises.
        Callers MUST check for emptiness and raise TaskNotFoundError (or
        handle the not-found case) themselves.

        Args:
            task_id: Task ID

        Returns:
            Task record dict, or {} when the task does not exist.
        """
        ...
    async def request_cancel(self, task_id: str) -> bool:
        """Request task cancellation.

        Returns:
            True: cancellation marked successfully.
            False: task does not exist or is already completed.
        """
        ...



    async def list_children(self, parent_task_id: str) -> List[str]:
        """Query all child task IDs of a given task (lineage index read).

        Args:
            parent_task_id: Parent task ID

        Returns:
            List of child task IDs (empty list if no children).
        """
        ...

    async def list_task_ids_by_status(
        self,
        status: str,
        *,
        limit: int | None = None,
    ) -> List[str]:
        """Query task IDs by status (data-plane primitive for orchestration layer list_tasks).

        This method is invoked by TaskOrchestrator.list_tasks().
        Previously not declared in the protocol; custom storage implementations would raise AttributeError.

        Args:
            status: Status term (taskflow vocabulary: pending/queued/running/...)
            limit: Maximum number to return (None = all; <=0 returns empty list)

        Returns:
            List of task IDs (order is implementation-defined; taskbase implementation returns ascending by expiry time).
        """
        ...

    async def bulk_get_tasks(self, task_ids: List[str]) -> List[Dict[str, Any]]:
        """Batch read task records (data-plane primitive for orchestration layer list_tasks).

        Args:
            task_ids: List of task IDs

        Returns:
            List of records of the same length as task_ids; non-existent tasks are represented by {} placeholder.
        """
        ...