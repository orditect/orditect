"""Saga pattern (failure rollback)."""
import logging
from typing import Dict, Any, Set

from orditect.flow.workflow.workflow import Workflow

logger = logging.getLogger(__name__)


class SagaPattern:
    """Saga pattern (failure rollback for distributed transactions).

    Responsibilities:
    - On workflow failure, roll back completed steps in reverse order
    - Call each step's rollback_handler if available
    """

    async def rollback(
            self,
            workflow: Workflow,
            results: Dict[str, Any],
            completed: Set[str],
    ) -> None:
        """Roll back the workflow.

        Args:
            workflow: Workflow instance
            results: Results of completed steps
            completed: Set of completed step names
        """
        logger.info(f"Rolling back workflow: {workflow.name}")

        # get execution order (topological sort)
        execution_order = workflow.get_execution_order()

        # rollback in reverse order
        for step in reversed(execution_order):
            if step.name in completed and step.rollback_handler:
                try:
                    logger.info(f"Rolling back step: {step.name}")
                    result = results.get(step.name)
                    await step.rollback_handler(result)
                    logger.info(f"Step rolled back: {step.name}")
                except Exception as e:
                    logger.error(
                        f"Rollback failed for step: {step.name}, error: {e}",
                        exc_info=True,
                    )
                    # rollback failure does not affect other steps' rollback

        logger.info(f"Workflow rolled back: {workflow.name}")