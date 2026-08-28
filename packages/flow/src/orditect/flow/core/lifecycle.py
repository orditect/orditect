"""Task lifecycle management: initialization, state transitions, terminal handling."""


import logging

from orditect.flow.protocols.storage import TaskStorageProtocol
from orditect.flow.core.state_machine import TaskStateMachine, TaskStatus
from orditect.flow.exceptions import InvalidStateTransitionError, TaskNotFoundError

logger = logging.getLogger(__name__)

try:
    from orditect.core import InvalidStatusTransferError as _TaskbaseInvalidTransfer
except ImportError:
    _TaskbaseInvalidTransfer = None

class TaskLifecycle:
    """Task lifecycle management.

    Responsibilities:
    - Initialize tasks
    - State transitions (with state machine validation)
    - Cancel tasks
    - Query task status
    """

    def __init__(
        self,
        storage: TaskStorageProtocol,
        state_machine: TaskStateMachine,
    ):
        """
        Args:
            storage: Task storage
            state_machine: State machine
        """
        self.storage = storage
        self.state_machine = state_machine

    async def initialize(
        self,
        task_id: str,
        metadata: dict | None = None,
        *,
        parent_task_id: str | None = None,
        if_not_exists: bool = False,
    ) -> bool:
        """Initialize a task.

        Args:
            task_id: Task ID
            metadata: Task metadata (optional)
            parent_task_id: Parent task ID (lineage registration)
            if_not_exists: Idempotency switch

        Returns:
            True if initialized successfully; False if idempotent skip
        """
        created = await self.storage.initialize_task(
            task_id=task_id,
            initial_status=TaskStatus.PENDING.value,
            parent_task_id=parent_task_id,
            if_not_exists=if_not_exists,
        )

        if not created:
            logger.info(f"Task already exists (idempotent skip): {task_id}")
            return False

        if metadata:
            await self.storage.update_task(task_id, {"metadata": metadata})

        logger.info(f"Task initialized: {task_id} (parent={parent_task_id})")
        return True

    async def transition_to(self, task_id: str, to_status: TaskStatus) -> None:
        """State transition (with state machine validation).

        Args:
            task_id: Task ID
            to_status: Target status

        Raises:
            TaskNotFoundError: Task does not exist
            InvalidStateTransitionError: Invalid state transition
        """
        # 1. get current status
        task = await self.storage.get_task(task_id)
        if not task:
            raise TaskNotFoundError(f"task_id not found: {task_id}")
        from_status = TaskStatus(task["status"])

        # 2. state machine validation
        self.state_machine.validate_transition(from_status, to_status)

        # 3. update status (use validate_status_transfer=False to avoid
        # conflict with the underlying storage's own state machine; we trust
        # orditect-flow's state machine, and the underlying Lua terminal
        # protection still applies unconditionally).
        await self.storage.update_task(
            task_id,
            {"status": to_status.value},
            validate_status_transfer=False,
        )
        logger.info(
            f"Task state transition: {task_id} "
            f"{from_status.value} -> {to_status.value}"
        )

    async def cancel(self, task_id: str) -> bool:
        """Cancel a task.

        Race-condition idempotent finalization covers two exception sources:
        a) taskflow state machine validation rejection (transition_to
           re-reads and finds a terminal state);
        b) taskbase Lua terminal protection rejection (narrow window: after
           validation passes but before the Lua write, the task is closed to
           terminal concurrently).

        Args:
            task_id: Task ID

        Returns:
            True: cancellation successful (including idempotent confirmation
                when the executor already closed it)
            False: task does not exist or is already terminal
        """
        try:
            task = await self.storage.get_task(task_id)
            if not task:
                logger.warning(f"Cannot cancel non-existent task: {task_id}")
                return False
            current_status = TaskStatus(task["status"])

            # terminal task cannot be cancelled
            if self.state_machine.is_terminal(current_status):
                logger.warning(
                    f"Cannot cancel terminal task: {task_id} "
                    f"(status: {current_status.value})"
                )
                return False

            # mark cancellation request
            try:
                # F3: task expired/deleted between get and request_cancel
                # -> treat as non-existent
                requested = await self.storage.request_cancel(task_id)
            except TaskNotFoundError:
                return False
            if not requested:
                return False

            # transition to cancelled
            try:
                await self.transition_to(task_id, TaskStatus.CANCELLED)
            except InvalidStateTransitionError:
                # Path (a): Python state machine validation rejected.
                final_status = await self.get_status(task_id)
                if final_status != TaskStatus.CANCELLED:
                    logger.warning(
                        f"Cancel race lost to concurrent terminal settle: "
                        f"{task_id} (final: {final_status.value})"
                    )
                    return False
            except Exception as e:
                # Path (b): taskbase Lua terminal protection rejected the
                # direct write (validate=False).
                if _TaskbaseInvalidTransfer is not None and isinstance(
                    e, _TaskbaseInvalidTransfer
                ):
                    final_status = await self.get_status(task_id)
                    if final_status != TaskStatus.CANCELLED:
                        logger.warning(
                            f"Cancel rejected by storage terminal protection: "
                            f"{task_id} (final: {final_status.value})"
                        )
                        return False
                else:
                    raise

            logger.info(f"Task cancelled: {task_id}")
            return True

        except TaskNotFoundError:
            logger.warning(f"Cannot cancel non-existent task: {task_id}")
            return False

    async def get_status(self, task_id: str) -> TaskStatus:
        """Get task status.

        Args:
            task_id: Task ID

        Returns:
            Task status

        Raises:
            TaskNotFoundError: Task does not exist
        """
        task = await self.storage.get_task(task_id)
        if not task:
            raise TaskNotFoundError(f"task_id not found: {task_id}")
        return TaskStatus(task["status"])

    async def get_task(self, task_id: str) -> dict:
        """Get full task information.

        Args:
            task_id: Task ID

        Returns:
            Task record

        Raises:
            TaskNotFoundError: Task does not exist
        """
        task = await self.storage.get_task(task_id)
        if not task:
            raise TaskNotFoundError(f"task_id not found: {task_id}")
        return task

    async def is_terminal(self, task_id: str) -> bool:
        """Check whether the task is terminal.

        Args:
            task_id: Task ID

        Returns:
            True: terminal
            False: non-terminal
        """
        status = await self.get_status(task_id)
        return self.state_machine.is_terminal(status)