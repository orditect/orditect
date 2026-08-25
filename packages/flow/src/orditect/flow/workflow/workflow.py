"""Workflow definition."""
from typing import List, Dict, Any, Optional
from orditect.flow.workflow.step import WorkflowStep
from orditect.flow.workflow.dag import DAG


class Workflow:
    """Workflow (multi-step task orchestration).

    Defines a complete workflow consisting of multiple steps and their dependencies.

    Usage example:
        workflow = Workflow(
            name="document_processing",
            steps=[
                WorkflowStep(name="parse", handler=parse_document),
                WorkflowStep(name="chunk", handler=chunk_text, dependencies=["parse"]),
                WorkflowStep(name="embed", handler=generate_embeddings, dependencies=["chunk"]),
            ],
        )

        executor = WorkflowExecutor()
        results = await executor.execute(workflow, context={"doc_url": "https://..."})
    """

    def __init__(
            self,
            name: str,
            steps: List[WorkflowStep],
            metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Args:
            name: Workflow name
            steps: List of workflow steps
            metadata: Workflow metadata (optional)
        """
        self.name = name
        self.steps = steps
        self.metadata = metadata or {}
        self.dag = DAG(steps)

    def get_execution_order(self) -> List[WorkflowStep]:
        """Get execution order (topological sort).

        Returns:
            List of steps sorted by dependencies.
        """
        return self.dag.topological_sort()

    def get_parallel_groups(self) -> List[List[WorkflowStep]]:
        """Get parallel execution groups.

        Returns:
            List of parallel execution groups (steps within each group can run in parallel).
        """
        return self.dag.get_parallel_groups()

    def validate(self) -> None:
        """Validate the legality of the workflow.

        Raises:
            WorkflowExecutionError: Workflow definition is invalid.
        """
        pass