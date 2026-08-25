"""状态机单元测试"""
import pytest

from orditect.flow import TaskStateMachine, TaskStatus, InvalidStateTransitionError


class TestTaskStateMachine:
    """状态机测试"""

    def test_can_transition_valid(self):
        """合法流转"""
        sm = TaskStateMachine()
        assert sm.can_transition(TaskStatus.PENDING, TaskStatus.QUEUED)
        assert sm.can_transition(TaskStatus.QUEUED, TaskStatus.RUNNING)
        assert sm.can_transition(TaskStatus.RUNNING, TaskStatus.SUCCEEDED)
        assert sm.can_transition(TaskStatus.RUNNING, TaskStatus.FAILED)
        assert sm.can_transition(TaskStatus.RUNNING, TaskStatus.CANCELLED)

    def test_can_transition_invalid(self):
        """非法流转"""
        sm = TaskStateMachine()
        assert not sm.can_transition(TaskStatus.PENDING, TaskStatus.RUNNING)
        assert not sm.can_transition(TaskStatus.SUCCEEDED, TaskStatus.RUNNING)
        assert not sm.can_transition(TaskStatus.FAILED, TaskStatus.PENDING)

    def test_validate_transition_valid(self):
        """验证合法流转（不抛异常）"""
        sm = TaskStateMachine()
        sm.validate_transition(TaskStatus.PENDING, TaskStatus.QUEUED)
        sm.validate_transition(TaskStatus.RUNNING, TaskStatus.SUCCEEDED)

    def test_validate_transition_invalid(self):
        """验证非法流转（抛异常）"""
        sm = TaskStateMachine()
        with pytest.raises(InvalidStateTransitionError):
            sm.validate_transition(TaskStatus.SUCCEEDED, TaskStatus.RUNNING)

    def test_is_terminal(self):
        """判断终态"""
        sm = TaskStateMachine()
        assert sm.is_terminal(TaskStatus.SUCCEEDED)
        assert sm.is_terminal(TaskStatus.FAILED)
        assert sm.is_terminal(TaskStatus.CANCELLED)
        assert not sm.is_terminal(TaskStatus.PENDING)
        assert not sm.is_terminal(TaskStatus.RUNNING)