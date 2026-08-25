"""DAG 单元测试"""
import pytest

from orditect.flow import WorkflowStep, DAG, WorkflowExecutionError


async def handler(context, results):
    return "result"


class TestDAG:
    """DAG 测试"""

    def test_topological_sort(self):
        """拓扑排序"""
        steps = [
            WorkflowStep(name="step1", handler=handler),
            WorkflowStep(name="step2", handler=handler, dependencies=["step1"]),
            WorkflowStep(name="step3", handler=handler, dependencies=["step2"]),
        ]
        dag = DAG(steps)
        order = dag.topological_sort()
        assert [s.name for s in order] == ["step1", "step2", "step3"]

    def test_circular_dependency_detection(self):
        """循环依赖检测"""
        steps = [
            WorkflowStep(name="step1", handler=handler, dependencies=["step3"]),
            WorkflowStep(name="step2", handler=handler, dependencies=["step1"]),
            WorkflowStep(name="step3", handler=handler, dependencies=["step2"]),
        ]
        with pytest.raises(WorkflowExecutionError):
            DAG(steps)

    def test_get_ready_steps(self):
        """获取就绪步骤"""
        steps = [
            WorkflowStep(name="step1", handler=handler),
            WorkflowStep(name="step2", handler=handler, dependencies=["step1"]),
            WorkflowStep(name="step3", handler=handler, dependencies=["step1"]),
        ]
        dag = DAG(steps)

        # initially only step1 ready
        ready = dag.get_ready_steps(completed=set())
        assert [s.name for s in ready] == ["step1"]

        # after step1 complete, step2 and step3 ready
        ready = dag.get_ready_steps(completed={"step1"})
        assert {s.name for s in ready} == {"step2", "step3"}