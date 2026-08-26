"""B1 pinning tests: error taxonomy."""

import pytest

from orditect.protocol.errors import (
    ContractError,
    ContentNotFoundError,
    IdempotencyConflictError,
    InvalidQueryError,
    SnapshotNotFoundError,
    TerminalStateViolationError,
    UnsupportedCapabilityError,
)


@pytest.mark.unit
class TestErrorHierarchy:
    """All contract errors inherit ContractError."""

    def test_all_inherit_contract_error(self):
        assert issubclass(UnsupportedCapabilityError, ContractError)
        assert issubclass(ContentNotFoundError, ContractError)
        assert issubclass(SnapshotNotFoundError, ContractError)
        assert issubclass(TerminalStateViolationError, ContractError)
        assert issubclass(IdempotencyConflictError, ContractError)
        assert issubclass(InvalidQueryError, ContractError)

    def test_contract_error_is_exception(self):
        assert issubclass(ContractError, Exception)

    def test_message_passthrough(self):
        e = ContentNotFoundError("ptr-123")
        assert "ptr-123" in str(e)


@pytest.mark.unit
class TestErrorCatchCompatibility:
    """Catch-as-base-class compatibility."""

    def test_catch_as_contract_error(self):
        with pytest.raises(ContractError):
            raise UnsupportedCapabilityError("snapshot_query not supported")

        with pytest.raises(ContractError):
            raise TerminalStateViolationError("cannot mutate succeeded task")