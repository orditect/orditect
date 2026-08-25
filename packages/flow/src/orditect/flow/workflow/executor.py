"""Workflow executor (structured concurrency + retry_policy connected)."""
import asyncio
import logging
from typing import Dict, Any, Optional, Set

from orditect.flow.workflow.workflow import Workflow
from orditect.flow.workflow.step import WorkflowStep
from orditect.flow.workflow.saga import SagaPattern
from orditect.flow.exceptions import WorkflowExecutionError

logger = logging.getLogger(__name__)


class WorkflowExecutor:
    """Workflow executor (executes multi-step workflows).

    Changes:
    - R7: parallel branches changed from bare gather to "cancel and await all in-flight steps
      after failure, then start Saga rollback" — rollback no longer runs concurrently with in-flight steps.
    - R13 partial: WorkflowStep.retry_policy dead field now connected (if present, wraps the handler).
    """

    def __init__(self, saga: Optional[SagaPattern] = None):
        self.saga = saga or SagaPattern()

    async def execute(
            self,
            workflow: Workflow,
            context: Optional[Dict[str, Any]] = None,
            parallel: bool = False,
    ) -> Dict[str, Any]:
        context = context or {}
        results: Dict[str, Any] = {}
        completed: Set[str] = set()

        try:
            if parallel:
                await self._execute_parallel(workflow, context, results, completed)
            else:
                await self._execute_sequential(workflow, context, results, completed)

            logger.info(f"Workflow succeeded: {workflow.name}")
            return results

        except Exception as e:
            logger.error(f"Workflow failed: {workflow.name}, error: {e}", exc_info=True)
            await self.saga.rollback(workflow, results, completed)
            raise WorkflowExecutionError(f"Workflow execution failed: {workflow.name}") from e

    async def _execute_sequential(
            self,
            workflow: Workflow,
            context: Dict[str, Any],
            results: Dict[str, Any],
            completed: Set[str],
    ) -> None:
        for step in workflow.get_execution_order():
            await self._execute_step(step, context, results, completed)

    async def _execute_parallel(
            self,
            workflow: Workflow,
            context: Dict[str, Any],
            results: Dict[str, Any],
            completed: Set[str],
    ) -> None:
        """Parallel execution (R7: structured concurrency — cancel and await survivors on failure before raising)."""
        parallel_groups = workflow.get_parallel_groups()

        for group in parallel_groups:
            tasks = [
                asyncio.create_task(self._execute_step(step, context, results, completed))
                for step in group
            ]
            try:
                await asyncio.gather(*tasks, return_exceptions=False)
            except BaseException:
                # R7: cancel and wait for all in-flight steps to be collected, then re-raise
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise

    async def _execute_step(
            self,
            step: WorkflowStep,
            context: Dict[str, Any],
            results: Dict[str, Any],
            completed: Set[str],
    ) -> None:
        """Execute a single step (R13: retry_policy connected)."""
        logger.info(f"Executing step: {step.name}")

        handler = step.handler
        if step.retry_policy is not None:
            async def with_retry(ctx, res):
                return await step.retry_policy.execute_with_retry(handler, ctx, res)
            effective_handler = with_retry
        else:
            effective_handler = handler

        try:
            if step.timeout:
                result = await asyncio.wait_for(
                    effective_handler(context, results),
                    timeout=step.timeout,
                )
            else:
                result = await effective_handler(context, results)

            results[step.name] = result
            completed.add(step.name)
            logger.info(f"Step succeeded: {step.name}")

        except Exception as e:
            logger.error(f"Step failed: {step.name}, error: {e}", exc_info=True)
            raise