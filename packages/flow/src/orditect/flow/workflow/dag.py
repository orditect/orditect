"""DAG (Directed Acyclic Graph): manage dependencies of workflow steps."""
from typing import List, Dict, Set
from orditect.flow.workflow.step import WorkflowStep
from orditect.flow.exceptions import WorkflowExecutionError


class DAG:
    """Directed Acyclic Graph (DAG).

    Used to manage dependencies of workflow steps, supporting:
    - Topological sorting (determine execution order)
    - Circular dependency detection
    - Parallel execution identification
    """

    def __init__(self, steps: List[WorkflowStep]):
        """
        Args:
            steps: List of workflow steps
        """
        self.steps = {step.name: step for step in steps}
        self._validate()

    def _validate(self) -> None:
        """Validate the DAG's legality.

        Raises:
            WorkflowExecutionError: Circular dependency exists or dependency references a non-existent step
        """
        # 1. check if dependent step exists
        for step in self.steps.values():
            for dep in step.dependencies:
                if dep not in self.steps:
                    raise WorkflowExecutionError(
                        f"Step '{step.name}' depends on non-existent step '{dep}'"
                    )

        # 2. check circular dependency
        visited = set()
        rec_stack = set()

        def has_cycle(step_name: str) -> bool:
            visited.add(step_name)
            rec_stack.add(step_name)

            for dep in self.steps[step_name].dependencies:
                if dep not in visited:
                    if has_cycle(dep):
                        return True
                elif dep in rec_stack:
                    return True

            rec_stack.remove(step_name)
            return False

        for step_name in self.steps:
            if step_name not in visited:
                if has_cycle(step_name):
                    raise WorkflowExecutionError(
                        f"Circular dependency detected in workflow"
                    )

    def topological_sort(self) -> List[WorkflowStep]:
        """Topological sort (determine execution order).

        Returns:
            List of steps sorted by dependencies (dependent steps first)
        """
        # Kahn's algorithm
        in_degree: Dict[str, int] = {name: 0 for name in self.steps}

        # compute indegree
        for step in self.steps.values():
            for dep in step.dependencies:
                in_degree[step.name] += 1

        # queue: steps with indegree 0
        queue: List[str] = [name for name, degree in in_degree.items() if degree == 0]
        result: List[WorkflowStep] = []

        while queue:
            # take from head
            current = queue.pop(0)
            result.append(self.steps[current])

            # update adjacent steps' indegree
            for step in self.steps.values():
                if current in step.dependencies:
                    in_degree[step.name] -= 1
                    if in_degree[step.name] == 0:
                        queue.append(step.name)

        # if result count != step count, circular dependency exists
        if len(result) != len(self.steps):
            raise WorkflowExecutionError("Circular dependency detected in workflow")

        return result

    def get_ready_steps(self, completed: Set[str]) -> List[WorkflowStep]:
        """Get ready steps (all dependencies completed).

        Args:
            completed: Set of completed step names

        Returns:
            List of ready steps
        """
        ready = []
        for step in self.steps.values():
            if step.name not in completed:
                # check if all dependencies completed
                if all(dep in completed for dep in step.dependencies):
                    ready.append(step)
        return ready

    def get_parallel_groups(self) -> List[List[WorkflowStep]]:
        """Get parallel execution groups (steps at the same level can run in parallel).

        Returns:
            List of parallel execution groups (steps within each group can run in parallel)
        """
        groups = []
        completed: Set[str] = set()

        while len(completed) < len(self.steps):
            # get currently ready steps
            ready = self.get_ready_steps(completed)
            if not ready:
                break

            # current group can execute in parallel
            groups.append(ready)

            # mark as completed
            for step in ready:
                completed.add(step.name)

        return groups