"""工作流执行集成测试"""
import pytest

from orditect.flow import Workflow, WorkflowStep, WorkflowExecutor


@pytest.mark.integration
class TestWorkflowExecution:
    """工作流执行测试"""

    async def test_sequential_workflow(self, redis_client):
        """串行工作流"""

        async def step1(context, results):
            return {"step1": "result1"}

        async def step2(context, results):
            assert "step1" in results
            return {"step2": "result2"}

        async def step3(context, results):
            assert "step1" in results
            assert "step2" in results
            return {"step3": "result3"}

        workflow = Workflow(
            name="test_workflow",
            steps=[
                WorkflowStep(name="step1", handler=step1),
                WorkflowStep(name="step2", handler=step2, dependencies=["step1"]),
                WorkflowStep(name="step3", handler=step3, dependencies=["step2"]),
            ],
        )

        executor = WorkflowExecutor()
        results = await executor.execute(workflow, context={})

        assert results["step1"] == {"step1": "result1"}
        assert results["step2"] == {"step2": "result2"}
        assert results["step3"] == {"step3": "result3"}

    async def test_parallel_workflow(self, redis_client):
        """并行工作流"""

        async def step1(context, results):
            return {"step1": "result1"}

        async def step2(context, results):
            return {"step2": "result2"}

        async def step3(context, results):
            assert "step1" in results
            assert "step2" in results
            return {"step3": "result3"}

        workflow = Workflow(
            name="test_parallel_workflow",
            steps=[
                WorkflowStep(name="step1", handler=step1),
                WorkflowStep(name="step2", handler=step2),
                WorkflowStep(name="step3", handler=step3, dependencies=["step1", "step2"]),
            ],
        )

        executor = WorkflowExecutor()
        results = await executor.execute(workflow, context={}, parallel=True)

        assert "step1" in results
        assert "step2" in results
        assert "step3" in results