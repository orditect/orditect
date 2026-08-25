"""Dead Letter Queue: stores tasks that ultimately failed."""
import json
import logging
import time
from typing import Callable, Any, Dict, List, Optional, Tuple

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class DeadLetterQueue:
    """Dead Letter Queue (stores tasks that ultimately failed)

        Responsibilities:
        - Stores failed tasks that have exhausted retry attempts
        - Supports query, retry, and delete operations
        - Persists to Redis

        Usage example:
            dlq = DeadLetterQueue(redis_client)

            # Add to dead letter queue
            await dlq.add(
                func=my_task,
                args=(arg1, arg2),
                kwargs={"key": "value"},
                error=Exception("Task failed"),
            )

            # List tasks in the dead letter queue
            failed_tasks = await dlq.list()

            # Retry a task from the dead letter queue
            await dlq.retry(task_id)
        """

    def __init__(
            self,
            redis_client: aioredis.Redis,
            key_prefix: str = "taskflow:dlq",
            ttl: int = 604800,  # 7 天
    ):
        """
                Args:
                    redis_client: Redis client
                    key_prefix: Key prefix
                    ttl: Expiration time in seconds (default 7 days)
                """
        self.redis = redis_client
        self.key_prefix = key_prefix
        self.ttl = ttl

    def _make_key(self, task_id: str) -> str:
        """Generate Redis key."""
        return f"{self.key_prefix}:task:{task_id}"

    def _make_index_key(self) -> str:
        """Generate Redis key."""
        return f"{self.key_prefix}:index"

    async def add(
            self,
            func: Callable,
            args: Tuple,
            kwargs: Dict[str, Any],
            error: Exception,
            task_id: Optional[str] = None,
    ) -> str:
        """Add to the dead letter queue.

                Args:
                    func: The failed function
                    args: Positional arguments
                    kwargs: Keyword arguments
                    error: Exception information
                    task_id: Task ID (optional, auto-generated if not provided)

                Returns:
                    Task ID
                """
        if task_id is None:
            import uuid
            task_id = f"dlq-{uuid.uuid4().hex[:12]}"

        # serialize task information
        data = {
            "task_id": task_id,
            "func_name": func.__name__,
            "func_module": func.__module__,
            "args": repr(args),  # 使用 repr 序列化（注意：反序列化时需要 eval，生产环境建议使用 pickle）
            "kwargs": repr(kwargs),
            "error": str(error),
            "error_type": type(error).__name__,
            "timestamp": time.time(),
        }

        task_key = self._make_key(task_id)
        index_key = self._make_index_key()

        # use pipeline for atomicity
        async with self.redis.pipeline(transaction=True) as pipe:
            # 1. write task record
            pipe.set(task_key, json.dumps(data, ensure_ascii=False), ex=self.ttl)

            # 2. add to index
            pipe.sadd(index_key, task_id)
            pipe.expire(index_key, self.ttl)

            await pipe.execute()

        logger.info(f"Added to DLQ: {task_id} ({func.__name__})")
        return task_id

    async def get(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get a dead letter task.

                Args:
                    task_id: Task ID

                Returns:
                    Task record (None if not found)
                """
        task_key = self._make_key(task_id)
        raw = await self.redis.get(task_key)

        if not raw:
            return None

        return json.loads(raw)

    async def list(
            self,
            limit: int = 100,
            offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List dead letter tasks.

                Args:
                    limit: Maximum number of tasks to return
                    offset: Offset for pagination

                Returns:
                    List of tasks
                """
        index_key = self._make_index_key()
        task_ids = await self.redis.smembers(index_key)

        # pagination
        task_ids = list(task_ids)[offset:offset + limit]

        # batch fetch
        if not task_ids:
            return []

        task_keys = [self._make_key(tid) for tid in task_ids]
        raws = await self.redis.mget(task_keys)

        tasks = []
        for raw in raws:
            if raw:
                tasks.append(json.loads(raw))

        return tasks

    async def retry(self, task_id: str) -> None:
        """Retry a dead letter task.

                ⚠️ R13 contract explicit: this method currently does NOT actually re-execute the task —
                the function body cannot be restored from repr serialization (production requires pickle or importable path).
                After calling, the task is removed from the DLQ but not re-executed. For actual retry,
                the business side should re-submit based on the task record.
                """
        task = await self.get(task_id)
        if not task:
            raise ValueError(f"Task not found in DLQ: {task_id}")

        logger.warning(
            f"DLQ retry is a skeleton (task NOT re-executed): {task_id} - "
            f"to actually retry, re-submit via TaskOrchestrator.submit() "
            f"(func: {task['func_module']}.{task['func_name']})"
        )

        await self.delete(task_id)

    async def delete(self, task_id: str) -> bool:
        """Delete a dead letter task.

                Args:
                    task_id: Task ID

                Returns:
                    True: deleted successfully
                    False: task not found
                """
        task_key = self._make_key(task_id)
        index_key = self._make_index_key()

        # use pipeline for atomicity
        async with self.redis.pipeline(transaction=True) as pipe:
            # 1. delete task record
            pipe.delete(task_key)

            # 2. remove from index
            pipe.srem(index_key, task_id)

            await pipe.execute()

        logger.info(f"Deleted from DLQ: {task_id}")
        return True

    async def clear(self) -> int:
        """Clear the dead letter queue.

                Returns:
                    Number of tasks deleted
                """
        index_key = self._make_index_key()
        task_ids = await self.redis.smembers(index_key)

        if not task_ids:
            return 0

        task_keys = [self._make_key(tid) for tid in task_ids]

        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.delete(*task_keys)
            pipe.delete(index_key)
            await pipe.execute()

        logger.info(f"Cleared DLQ: {len(task_ids)} tasks deleted")
        return len(task_ids)