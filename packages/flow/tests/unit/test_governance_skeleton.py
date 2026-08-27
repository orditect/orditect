"""Skeleton tests: DependencyGovernor construction, vocabulary, T8 degradation."""

from __future__ import annotations

import pytest

from orditect.flow import TaskOrchestrator
from orditect.flow.governance import DependencyGovernor
from orditect.flow.governance.dependency import DEFAULT_TERMINAL_WORDS
from orditect.protocol import UnsupportedCapabilityError

from fake_infra import FakeGovernanceStorage
from orditect.protocol import DependencyGraph

pytestmark = pytest.mark.unit


def test_success_words_required():
    with pytest.raises(ValueError):
        DependencyGovernor(FakeGovernanceStorage(), success_words=frozenset())


def test_vocabulary_defaults():
    gov = DependencyGovernor(
        FakeGovernanceStorage(), success_words=frozenset({"done"})
    )
    assert gov._terminal_words == DEFAULT_TERMINAL_WORDS
    assert gov._ready_status == "pending"
    assert gov._is_success("done")
    assert not gov._is_success("succeeded")


def test_vocabulary_overrides():
    gov = DependencyGovernor(
        FakeGovernanceStorage(),
        success_words=frozenset({"done"}),
        terminal_words=frozenset({"done", "broken"}),
        ready_status="created",
    )
    assert gov._is_terminal("broken")
    assert not gov._is_terminal("pending")
    assert gov._ready_status == "created"


async def test_get_dependency_graph_requires_store():
    gov = DependencyGovernor(
        FakeGovernanceStorage(), success_words=frozenset({"succeeded"})
    )
    with pytest.raises(UnsupportedCapabilityError):
        await gov.get_dependency_graph("root")


async def test_get_dependency_graph_delegates_to_store():
    class Store:
        async def read_graph(self, root_id: str) -> DependencyGraph:
            return DependencyGraph(root_task_id=root_id, task_ids=[], edges=[])

    gov = DependencyGovernor(
        FakeGovernanceStorage(),
        success_words=frozenset({"succeeded"}),
        dep_graph_store=Store(),
    )
    graph = await gov.get_dependency_graph("root")
    assert graph.root_task_id == "root"


def test_orchestrator_without_governor_stays_inert():
    orch = TaskOrchestrator(FakeGovernanceStorage())
    assert orch.dependency_governor is None