"""任务状态机单元测试（纯逻辑，无需 Redis）。"""
import pytest

from orditect.core import TaskStatus, can_transfer, is_terminal


class TestTaskStatus:
    """TaskStatus 枚举测试。"""

    def test_enum_values(self):
        """状态枚举值正确。"""
        assert TaskStatus.pending.value == "pending"
        assert TaskStatus.in_progress.value == "in_progress"
        assert TaskStatus.completed.value == "completed"
        assert TaskStatus.failed.value == "failed"
        assert TaskStatus.cancelled.value == "cancelled"

    def test_enum_is_str(self):
        """TaskStatus 继承 str，JSON 序列化时为字符串。"""
        assert isinstance(TaskStatus.pending, str)
        assert TaskStatus.pending == "pending"


class TestIsTerminal:
    """is_terminal 测试。"""

    @pytest.mark.parametrize("status", ["completed", "failed", "cancelled"])
    def test_terminal_statuses(self, status):
        """终态判断正确。"""
        assert is_terminal(status)

    @pytest.mark.parametrize("status", ["pending", "in_progress", ""])
    def test_non_terminal_statuses(self, status):
        """非终态判断正确。"""
        assert not is_terminal(status)


class TestCanTransfer:
    """can_transfer 测试（白名单制）。"""

    @pytest.mark.parametrize("from_s,to_s", [
        ("", "pending"),
        ("", "in_progress"),
        ("pending", "in_progress"),
        ("pending", "cancelled"),
        ("pending", "failed"),
        ("in_progress", "completed"),
        ("in_progress", "failed"),
        ("in_progress", "cancelled"),
    ])
    def test_allowed_transitions(self, from_s, to_s):
        """合法流转。"""
        assert can_transfer(from_s, to_s)

    @pytest.mark.parametrize("from_s,to_s", [
        ("pending", "completed"),      # 不允许跳级
        ("completed", "pending"),      # 终态不可逆
        ("completed", "failed"),
        ("failed", "pending"),
        ("cancelled", "in_progress"),
        ("in_progress", "pending"),    # 不允许回退
    ])
    def test_forbidden_transitions(self, from_s, to_s):
        """非法流转。"""
        assert not can_transfer(from_s, to_s)