"""F4 pinning: resume / rerun recovery primitives.

In-memory infra (FakeStorage + StubReader + SpyExecutor + factory).
Verifies the per-node decision algorithm and dispatch wiring:
- succeeded node with hot-record result -> REUSE (no reopen, no execute)
- failed / never-ran node -> RERUN (reopen + executor.execute via factory)
- rerun(scope) forces rerun inside scope, decide() outside
- one node's failure does not block the rest of the tree
"""
import asyncio
from typing import Any, Dict, Optional

import pytest

from orditect.flow.recovery import RecoveryService, ReuseDecision


class FakeStorage:
    """Duck-typed core TaskRedisDB: reopen_task + get_task."""

    def __init__(self):
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self.reopened: list[str] = []

    def seed(self, task_id: str, result: Any = None, eid: str = "exec-1"):
        rec = {"task_id": task_id, "status": "failed", "execution_id": eid}
        if result is not None:
            rec["result"] = result
            rec["status"] = "succeeded"
        self._tasks[task_id] = rec

    async def get_task(self, task_id: str):
        return dict(self._tasks.get(task_id, {}))

    async def reopen_task(self, task_id: str, **kw: Any) -> str:
        self.reopened.append(task_id)
        new_eid = f"exec-re-{len(self.reopened)}"
        self._tasks[task_id]["execution_id"] = new_eid
        self._tasks[task_id]["status"] = "pending"
        self._tasks[task_id].pop("result", None)
        return new_eid


class _FakeSnap:
    def __init__(self, task_id: str, status: str):
        self.task_id = task_id
        self.status = status


class StubReader:
    """Duck-typed protocol SnapshotReader: get + get_tree."""

    def __init__(self, statuses: Dict[str, str], tree_ids: list[str]):
        self._statuses = statuses  # task_id -> latest status
        self._tree_ids = tree_ids

    async def get(self, task_id: str, step: str = "execute"):
        st = self._statuses.get(task_id)
        return _FakeSnap(task_id, st) if st else None

    async def get_tree(self, root_task_id: str, **kw: Any):
        return [_FakeSnap(t, self._statuses.get(t, "")) for t in self._tree_ids]


class SpyExecutor:
    """Records execute dispatches."""

    def __init__(self):
        self.executed: list[str] = []

    async def execute(self, task_id: str, task: Any, **kw: Any):
        self.executed.append(task_id)
        return {"reexecuted": task_id}


class FakeTask:
    def __init__(self, task_id: str):
        self.task_id = task_id


def make_factory():
    async def factory(task_id: str):
        return FakeTask(task_id)
    return factory


async def _drain(service: RecoveryService):
    """Let background execute tasks finish."""
    for _ in range(50):
        if not service._bg_tasks:
            return
        await asyncio.sleep(0.02)


@pytest.mark.unit
class TestResume:
    async def test_succeeded_reused_failed_rerun(self):
        storage = FakeStorage()
        storage.seed("root", result={"r": 1})       # succeeded + result
        storage.seed("child_ok", result={"c": 1})   # succeeded + result
        storage.seed("child_bad")                    # failed, no result

        reader = StubReader(
            statuses={"root": "succeeded", "child_ok": "succeeded", "child_bad": "failed"},
            tree_ids=["root", "child_ok", "child_bad"],
        )
        executor = SpyExecutor()
        svc = RecoveryService(
            storage, reader, executor,
            reuse_terminal_words=frozenset({"succeeded"}),
            task_factory=make_factory(),
        )

        plan = await svc.resume("root")
        await _drain(svc)

        assert plan["root"] is ReuseDecision.REUSE
        assert plan["child_ok"] is ReuseDecision.REUSE
        assert plan["child_bad"] is ReuseDecision.RERUN

        # Only child_bad reopened + re-executed
        assert storage.reopened == ["child_bad"]
        assert executor.executed == ["child_bad"]

    async def test_succeeded_without_result_reruns(self):
        """Succeeded word but NO hot-record result -> rerun (nothing to reuse)."""
        storage = FakeStorage()
        storage.seed("root")  # no result

        reader = StubReader(statuses={"root": "succeeded"}, tree_ids=["root"])
        executor = SpyExecutor()
        svc = RecoveryService(
            storage, reader, executor,
            reuse_terminal_words=frozenset({"succeeded"}),
            task_factory=make_factory(),
        )

        plan = await svc.resume("root")
        await _drain(svc)
        assert plan["root"] is ReuseDecision.RERUN
        assert storage.reopened == ["root"]


@pytest.mark.unit
class TestRerunScope:
    async def test_scope_forces_rerun_others_decide(self):
        storage = FakeStorage()
        storage.seed("a", result={"x": 1})  # would reuse
        storage.seed("b", result={"y": 1})  # would reuse, but in scope

        reader = StubReader(
            statuses={"a": "succeeded", "b": "succeeded"},
            tree_ids=["a", "b"],
        )
        executor = SpyExecutor()
        svc = RecoveryService(
            storage, reader, executor,
            reuse_terminal_words=frozenset({"succeeded"}),
            task_factory=make_factory(),
        )

        plan = await svc.rerun("root", scope={"b"})
        await _drain(svc)

        assert plan["a"] is ReuseDecision.REUSE   # outside scope -> decide -> reuse
        assert plan["b"] is ReuseDecision.RERUN   # inside scope -> forced rerun
        assert storage.reopened == ["b"]
        assert executor.executed == ["b"]


@pytest.mark.unit
class TestRecoveryRobustness:
    async def test_one_node_failure_does_not_block_tree(self):
        storage = FakeStorage()
        storage.seed("good")
        storage.seed("bad")
        # Make 'bad' fail on reopen
        async def boom_reopen(task_id: str, **kw: Any):
            if task_id == "bad":
                raise RuntimeError("reopen blew up")
            return await FakeStorage.reopen_task(storage, task_id, **kw)
        storage.reopen_task = boom_reopen  # type: ignore[method-assign]

        reader = StubReader(statuses={"good": "failed", "bad": "failed"},
                            tree_ids=["good", "bad"])
        executor = SpyExecutor()
        svc = RecoveryService(
            storage, reader, executor,
            reuse_terminal_words=frozenset({"succeeded"}),
            task_factory=make_factory(),
        )

        plan = await svc.resume("root")
        await _drain(svc)

        # bad failed on reopen (logged), good still dispatched
        assert "good" in executor.executed

    async def test_requires_reuse_words(self):
        with pytest.raises(ValueError, match="reuse_terminal_words"):
            RecoveryService(
                FakeStorage(), StubReader({}, []), SpyExecutor(),
                reuse_terminal_words=frozenset(),  # empty -> rejected (T6)
                task_factory=make_factory(),
            )

    async def test_requires_executor(self):
        with pytest.raises(ValueError, match="executor"):
            RecoveryService(
                FakeStorage(), StubReader({}, []), None,
                reuse_terminal_words=frozenset({"succeeded"}),
                task_factory=make_factory(),
            )