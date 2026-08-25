"""Retry strategy: task execution with retries."""
import asyncio
import logging
from typing import Callable, Any, Optional, Tuple, Type

from orditect.flow.retry.backoff import BackoffStrategy, ExponentialBackoff

logger = logging.getLogger(__name__)


class RetryPolicy:
    """Retry policy

        Defines retry behavior after task failure:
        - Maximum retry attempts
        - Backoff strategy (exponential, linear, etc.)
        - Retryable exception types
        - Whether to enable dead letter queue

        Usage example:
            retry_policy = RetryPolicy(
                max_attempts=3,
                backoff=ExponentialBackoff(base=1.0, multiplier=2.0),
                retryable_exceptions=(NetworkError, TimeoutError),
                dlq_enabled=True,
            )

            result = await retry_policy.execute_with_retry(
                my_task,
                arg1, arg2,
                kwarg1=value1,
            )
        """

    def __init__(
            self,
            max_attempts: int = 3,
            backoff: Optional[BackoffStrategy] = None,
            retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
            dlq_enabled: bool = False,
            dlq: Optional[Any] = None,
    ):
        """
                Args:
                    max_attempts: Maximum attempts (including the first execution)
                    backoff: Backoff strategy (default exponential)
                    retryable_exceptions: Exception types that are retryable (default all exceptions)
                    dlq_enabled: Whether to enable dead letter queue
                    dlq: Dead letter queue instance (optional)
                """
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

        self.max_attempts = max_attempts
        self.backoff = backoff or ExponentialBackoff()
        self.retryable_exceptions = retryable_exceptions
        self.dlq_enabled = dlq_enabled
        self.dlq = dlq

    async def execute_with_retry(
            self,
            func: Callable,
            *args,
            **kwargs,
    ) -> Any:
        """Execute with retry.

                Args:
                    func: The async function to execute
                    *args: Positional arguments
                    **kwargs: Keyword arguments

                Returns:
                    Function execution result

                Raises:
                    Exception: The last exception raised after all retries fail
                """
        last_exception = None

        for attempt in range(self.max_attempts):
            try:
                # execute function
                result = await func(*args, **kwargs)

                # success, return directly
                if attempt > 0:
                    logger.info(f"Retry succeeded after {attempt} attempts")
                return result

            except self.retryable_exceptions as e:
                last_exception = e

                # last attempt, no more retry
                if attempt == self.max_attempts - 1:
                    logger.error(
                        f"All {self.max_attempts} attempts failed, giving up",
                        exc_info=True,
                    )

                    # send to dead letter queue
                    if self.dlq_enabled and self.dlq:
                        await self._send_to_dlq(func, args, kwargs, e)

                    raise

                # calculate backoff time
                delay = self.backoff.calculate(attempt)

                logger.warning(
                    f"Attempt {attempt + 1}/{self.max_attempts} failed: {e}, "
                    f"retrying in {delay:.2f}s"
                )

                # wait and retry
                await asyncio.sleep(delay)

            except Exception as e:
                # non-retryable exception, re-raise directly
                logger.error(f"Non-retryable exception: {e}", exc_info=True)
                raise

        # theoretically unreachable
        if last_exception:
            raise last_exception

    async def _send_to_dlq(
            self,
            func: Callable,
            args: Tuple,
            kwargs: dict,
            error: Exception,
    ) -> None:
        """Send to dead letter queue.

                Args:
                    func: The failed function
                    args: Positional arguments
                    kwargs: Keyword arguments
                    error: Exception information
                """
        if not self.dlq:
            logger.warning("DLQ enabled but no DLQ instance provided")
            return

        try:
            await self.dlq.add(
                func=func,
                args=args,
                kwargs=kwargs,
                error=error,
            )
            logger.info(f"Sent to DLQ: {func.__name__}")
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}", exc_info=True)