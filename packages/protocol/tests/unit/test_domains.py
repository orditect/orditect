"""B4 pinning tests: content + result domain protocol structure.

These tests pin the *shape* of the narrow protocols (runtime_checkable
structural conformance, method surface), not any storage behavior.
Behavioral conformance is the job of the B7 conformance kit.
"""

from __future__ import annotations

from typing import Any

import pytest

from orditect.protocol import (
    CapabilitySet,
    ContentReader,
    ContentWriter,
    ResultReader,
    ResultWriter,
    TaskPointer,
)


class _FullContentStore:
    """Minimal structural implementation of both content half-domains."""

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(content_sink=True, content_query=True)

    async def put(self, content: bytes, **kwargs: Any) -> TaskPointer:
        return TaskPointer(backend="mem", key="k")

    async def delete(self, pointer: TaskPointer) -> bool:
        return True

    async def get(self, pointer: TaskPointer) -> bytes:
        return b""

    async def exists(self, pointer: TaskPointer) -> bool:
        return True

    async def get_metadata(self, pointer: TaskPointer) -> dict[str, Any]:
        return {}


class _FullResultStore:
    """Minimal structural implementation of both result half-domains."""

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(result_sink=True, result_query=True)

    async def save(self, stream_id: str, manifest: dict[str, Any], **kwargs: Any) -> None:
        return None

    async def get(self, stream_id: str) -> dict[str, Any] | None:
        return None


class _NotAStore:
    pass


@pytest.mark.unit
class TestContentProtocols:
    def test_writer_runtime_checkable(self):
        assert isinstance(_FullContentStore(), ContentWriter)

    def test_reader_runtime_checkable(self):
        assert isinstance(_FullContentStore(), ContentReader)

    def test_non_conforming_rejected(self):
        assert not isinstance(_NotAStore(), ContentWriter)
        assert not isinstance(_NotAStore(), ContentReader)


@pytest.mark.unit
class TestResultProtocols:
    def test_writer_runtime_checkable(self):
        assert isinstance(_FullResultStore(), ResultWriter)

    def test_reader_runtime_checkable(self):
        assert isinstance(_FullResultStore(), ResultReader)

    def test_non_conforming_rejected(self):
        assert not isinstance(_NotAStore(), ResultWriter)
        assert not isinstance(_NotAStore(), ResultReader)


@pytest.mark.unit
class TestMethodSurface:
    """Pin the exact method surface of each protocol (guard against drift)."""

    def test_content_writer_methods(self):
        expected = {"put", "delete"}
        assert expected <= set(ContentWriter.__protocol_attrs__)  # type: ignore[attr-defined]

    def test_content_reader_methods(self):
        expected = {"get", "exists", "get_metadata"}
        assert expected <= set(ContentReader.__protocol_attrs__)  # type: ignore[attr-defined]

    def test_result_writer_methods(self):
        assert "save" in set(ResultWriter.__protocol_attrs__)  # type: ignore[attr-defined]

    def test_result_reader_methods(self):
        assert "get" in set(ResultReader.__protocol_attrs__)  # type: ignore[attr-defined]

from orditect.protocol import AuditReader, AuditWriter
from orditect.protocol.models import AuditEvent


class _FullAuditStore:
    """Minimal structural implementation of both audit half-domains."""

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(audit_sink=True, audit_query=True)

    async def append(self, event: AuditEvent) -> None:
        return None

    async def query(self, **kwargs: Any) -> list[AuditEvent]:
        return []


@pytest.mark.unit
class TestAuditProtocols:
    def test_writer_runtime_checkable(self):
        assert isinstance(_FullAuditStore(), AuditWriter)

    def test_reader_runtime_checkable(self):
        assert isinstance(_FullAuditStore(), AuditReader)

    def test_non_conforming_rejected(self):
        assert not isinstance(_NotAStore(), AuditWriter)
        assert not isinstance(_NotAStore(), AuditReader)

    def test_writer_method_surface(self):
        assert "append" in set(AuditWriter.__protocol_attrs__)  # type: ignore[attr-defined]

    def test_reader_method_surface(self):
        assert "query" in set(AuditReader.__protocol_attrs__)  # type: ignore[attr-defined]


from orditect.protocol import SnapshotReader, SnapshotWriter
from orditect.protocol.models import TaskSnapshot


class _FullSnapshotStore:
    """Minimal structural implementation of both snapshot half-domains."""

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(snapshot_sink=True, snapshot_query=True)

    async def save(self, snapshot: TaskSnapshot) -> None:
        return None

    async def save_terminal(self, snapshot: TaskSnapshot) -> None:
        return None

    async def get(self, task_id: str, step: str, **kwargs: Any) -> TaskSnapshot | None:
        return None

    async def list_versions(self, task_id: str, step: str, **kwargs: Any) -> list[TaskSnapshot]:
        return []

    async def get_tree(self, root_task_id: str, **kwargs: Any) -> list[TaskSnapshot]:
        return []

    async def list_children(self, parent_task_id: str, **kwargs: Any) -> list[TaskSnapshot]:
        return []

    async def get_ancestors(self, task_id: str, **kwargs: Any) -> list[TaskSnapshot]:
        return []

    async def query(self, **kwargs: Any) -> list[TaskSnapshot]:
        return []

    async def aggregate(self, **kwargs: Any) -> dict[str, Any]:
        return {}


@pytest.mark.unit
class TestSnapshotProtocols:
    def test_writer_runtime_checkable(self):
        assert isinstance(_FullSnapshotStore(), SnapshotWriter)

    def test_reader_runtime_checkable(self):
        assert isinstance(_FullSnapshotStore(), SnapshotReader)

    def test_non_conforming_rejected(self):
        assert not isinstance(_NotAStore(), SnapshotWriter)
        assert not isinstance(_NotAStore(), SnapshotReader)

    def test_writer_method_surface(self):
        expected = {"save", "save_terminal"}
        assert expected <= set(SnapshotWriter.__protocol_attrs__)  # type: ignore[attr-defined]

    def test_reader_method_surface(self):
        expected = {
            "get", "list_versions", "get_tree", "list_children",
            "get_ancestors", "query", "aggregate",
        }
        assert expected <= set(SnapshotReader.__protocol_attrs__)  # type: ignore[attr-defined]