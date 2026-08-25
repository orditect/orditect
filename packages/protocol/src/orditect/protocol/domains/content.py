"""Content domain: storage and addressing of content bodies (all modalities).

Scope discipline: this domain stores and retrieves content bodies only.
Business retrieval (e.g. vector similarity search) is explicitly out of
contract scope — adapters exposing such retrieval do so outside this
protocol, under their own interfaces.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from orditect.protocol.capabilities import CapabilitySet
from orditect.protocol.models import TaskPointer


@runtime_checkable
class ContentWriter(Protocol):
    """Write side of the content domain.

    Capability half-domain: content_sink.
    """

    @property
    def capabilities(self) -> CapabilitySet:
        """Declared capability set (term T8)."""
        ...

    async def put(
        self,
        content: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TaskPointer:
        """Persist content and return its pointer.

        Semantics: stores an immutable content body and returns the pointer
        that addresses it. Content is immutable once stored (term T5): this
        method never mutates previously stored content; storing again always
        yields a (possibly new) pointer. "Content before pointer" ordering is
        guaranteed by this call's atomicity: a returned pointer always
        resolves (term T5).

        Idempotency / concurrency: implementations may deduplicate identical
        content internally (e.g. content-addressed storage); two concurrent
        puts of identical content must both succeed and must never yield a
        pointer that resolves to corrupted or partial content (term T10).

        Raises:
            UnsupportedCapabilityError: content_sink not declared (T8).
            ContractError: any other failure (observation non-blocking, T9 —
                only ContractError subclasses may escape this method).
        """
        ...

    async def delete(self, pointer: TaskPointer) -> bool:
        """Delete the content addressed by a pointer (compliance / lifecycle).

        Semantics: removes the content body. Deleting a non-existent pointer
        is a silent success (idempotent delete); returns False in that case.

        Idempotency / concurrency: safe to call concurrently for the same
        pointer; at most one call observes True (term T10).

        Raises:
            UnsupportedCapabilityError: content_sink not declared (T8).
            ContractError: any other failure (T9).
        """
        ...


@runtime_checkable
class ContentReader(Protocol):
    """Read side of the content domain.

    Capability half-domain: content_query.
    """

    @property
    def capabilities(self) -> CapabilitySet:
        """Declared capability set (term T8)."""
        ...

    async def get(self, pointer: TaskPointer) -> bytes:
        """Fetch the content body addressed by a pointer.

        Semantics: returns the exact bytes stored by `put`. A recorded pointer
        always resolves (term T5); if the content is missing (e.g. deleted or
        never persisted), the failure is explicit.

        Raises:
            ContentNotFoundError: the pointer does not resolve.
            UnsupportedCapabilityError: content_query not declared (T8).
            ContractError: any other failure (T9).
        """
        ...

    async def exists(self, pointer: TaskPointer) -> bool:
        """Check whether a pointer currently resolves.

        Semantics: existence probe. Never raises for a missing pointer —
        returns False instead. Note this is a point-in-time answer with no
        atomicity guarantee against concurrent delete (term T10 does not
        apply to probes).

        Raises:
            UnsupportedCapabilityError: content_query not declared (T8).
            ContractError: any other failure (T9).
        """
        ...

    async def get_metadata(self, pointer: TaskPointer) -> dict[str, Any]:
        """Fetch the metadata associated with a pointer's content.

        Semantics: returns the metadata dict supplied at `put` time (or an
        empty dict if none was supplied). The metadata reflects the content
        as stored; it is not independently mutable through this contract.

        Raises:
            ContentNotFoundError: the pointer does not resolve.
            UnsupportedCapabilityError: content_query not declared (T8).
            ContractError: any other failure (T9).
        """
        ...