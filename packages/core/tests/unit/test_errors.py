"""异常继承关系单元测试。"""
import pytest

from orditect.core import (
    TaskbaseError,
    AcquireTimeoutError,
    LimiterUnavailableError,
    TaskNotFoundError,
    InvalidStatusTransferError,
    CancelledByUser,
)


class TestExceptionHierarchy:
    """异常继承关系测试。"""

    def test_all_inherit_taskbase_error(self):
        """所有框架异常都继承 TaskbaseError。"""
        assert issubclass(AcquireTimeoutError, TaskbaseError)
        assert issubclass(LimiterUnavailableError, TaskbaseError)
        assert issubclass(TaskNotFoundError, TaskbaseError)
        assert issubclass(InvalidStatusTransferError, TaskbaseError)
        assert issubclass(CancelledByUser, TaskbaseError)

    def test_acquire_timeout_also_inherits_builtin_timeout(self):
        """AcquireTimeoutError 同时继承内置 TimeoutError（兼容性）。"""
        assert issubclass(AcquireTimeoutError, TimeoutError)

    def test_exception_messages(self):
        """异常消息可正常传递。"""
        e = AcquireTimeoutError("test timeout")
        assert str(e) == "test timeout"

        e2 = TaskNotFoundError("task_123")
        assert "task_123" in str(e2)


class TestExceptionCatching:
    """异常捕获兼容性测试。"""

    def test_catch_as_taskbase_error(self):
        """可统一捕获为 TaskbaseError。"""
        with pytest.raises(TaskbaseError):
            raise AcquireTimeoutError("timeout")

        with pytest.raises(TaskbaseError):
            raise TaskNotFoundError("not found")

    def test_catch_acquire_timeout_as_builtin_timeout(self):
        """AcquireTimeoutError 可被内置 TimeoutError 捕获（兼容旧代码）。"""
        with pytest.raises(TimeoutError):
            raise AcquireTimeoutError("timeout")