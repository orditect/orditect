"""重试策略单元测试"""
import pytest

from orditect.flow import RetryPolicy, ExponentialBackoff


class TestRetryPolicy:
    """重试策略测试"""

    async def test_retry_success_first_attempt(self):
        """第一次尝试成功"""
        policy = RetryPolicy(max_attempts=3)

        async def success_task():
            return "success"

        result = await policy.execute_with_retry(success_task)
        assert result == "success"

    async def test_retry_success_after_failures(self):
        """失败几次后成功"""
        policy = RetryPolicy(max_attempts=3, backoff=ExponentialBackoff(base=0.01))

        attempt_count = 0

        async def flaky_task():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise Exception(f"Attempt {attempt_count} failed")
            return "success"

        result = await policy.execute_with_retry(flaky_task)
        assert result == "success"
        assert attempt_count == 3

    async def test_retry_all_attempts_failed(self):
        """所有尝试都失败"""
        policy = RetryPolicy(max_attempts=3, backoff=ExponentialBackoff(base=0.01))

        async def failing_task():
            raise Exception("Always fails")

        with pytest.raises(Exception, match="Always fails"):
            await policy.execute_with_retry(failing_task)

    async def test_retry_non_retryable_exception(self):
        """不可重试的异常"""
        policy = RetryPolicy(
            max_attempts=3,
            retryable_exceptions=(ValueError,),
        )

        async def task_with_non_retryable_error():
            raise TypeError("Non-retryable")

        with pytest.raises(TypeError):
            await policy.execute_with_retry(task_with_non_retryable_error)