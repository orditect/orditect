"""orditect-core — async task governance engine for the Orditect ecosystem.

Two-layer data plane infrastructure, zero orchestration, zero business:
  redis/   task store + state machine + quota
  limiter/ distributed semaphore + token bucket + declarative decorator

Usage example:
    from orditect.core import TaskRedisDB, AsyncLeaseSemaphore, limited

    # Task store
    task_db = TaskRedisDB("redis://localhost:6379/0")
    await task_db.connect()
    await task_db.initialize_task("task_123")

    # Distributed semaphore
    sem = AsyncLeaseSemaphore(
        client=redis_client,
        name="llm",
        limit=30,
        lease_time=30.0,
    )
    async with sem.hold():
        # business logic
        pass

    # Declarative decorator
    @limited(resource="llm", mode="wait", timeout=5.0)
    async def call_llm(prompt: str):
        return await llm_client.chat(prompt)
"""
from importlib.metadata import PackageNotFoundError, version as _pkg_version

from orditect.core.errors import (
    TaskbaseError,
    AcquireTimeoutError,
    LimiterUnavailableError,
    TaskNotFoundError,
    InvalidStatusTransferError,
    CancelledByUser,
    InvalidUsageError,
    UnavailablePolicy,
)
from orditect.core.redis.base import RedisDB
from orditect.core.redis.task_db import TaskRedisDB
from orditect.core.redis.quota_db import AdmissionQuotaRedisDB
from orditect.core.task.status import TaskStatus, can_transfer, is_terminal
from orditect.core.task.cancel import CancellationToken
from orditect.core.limiter.hooks import LimiterHooks
from orditect.core.limiter.semaphore import AsyncLeaseSemaphore, LeaseToken, SemaphoreHold
from orditect.core.limiter.bucket import AsyncTokenBucket
from orditect.core.limiter.registry import LimiterRegistry, get_registry
from orditect.core.limiter.decorators import limited
from orditect.core.redis.pool_manager import RedisPoolManager, get_pool_manager

try:
    __version__ = _pkg_version("orditect-core")
except PackageNotFoundError:  # development environment without installation
    __version__ = "0.0.0.dev0"

__all__ = [
    # version
    "__version__",
    # errors
    "TaskbaseError",
    "AcquireTimeoutError",
    "LimiterUnavailableError",
    "TaskNotFoundError",
    "InvalidStatusTransferError",
    "CancelledByUser",
    "InvalidUsageError",
    "UnavailablePolicy",
    # Redis layer
    "RedisDB",
    "TaskRedisDB",
    "AdmissionQuotaRedisDB",
    # task primitives
    "TaskStatus",
    "can_transfer",
    "is_terminal",
    "CancellationToken",
    # rate limiting
    "LimiterHooks",
    "AsyncLeaseSemaphore",
    "LeaseToken",
    "SemaphoreHold",
    "AsyncTokenBucket",
    "LimiterRegistry",
    "get_registry",
    "limited",
    "RedisPoolManager",
    "get_pool_manager",
]