"""B1 pinning tests: CapabilitySet model."""

import pytest
from pydantic import ValidationError

from orditect.protocol.capabilities import CapabilitySet


@pytest.mark.unit
class TestCapabilitySetDefaults:
    """Default capability set: everything unsupported (explicit opt-in)."""

    def test_all_default_false(self):
        caps = CapabilitySet()
        assert caps.content_sink is False
        assert caps.content_query is False
        assert caps.audit_sink is False
        assert caps.audit_query is False
        assert caps.result_sink is False
        assert caps.result_query is False
        assert caps.snapshot_sink is False
        assert caps.snapshot_query is False

    def test_default_protocol_compat(self):
        caps = CapabilitySet()
        assert caps.protocol_compat == ">=0.1,<0.2"


@pytest.mark.unit
class TestCapabilitySetSupports:
    """supports() half-domain lookup."""

    def test_supports_declared(self):
        caps = CapabilitySet(content_sink=True, snapshot_query=True)
        assert caps.supports("content_sink") is True
        assert caps.supports("snapshot_query") is True
        assert caps.supports("content_query") is False

    def test_supports_unknown_raises(self):
        caps = CapabilitySet()
        with pytest.raises(ValueError, match="unknown half-domain"):
            caps.supports("nonexistent_domain")


@pytest.mark.unit
class TestCapabilitySetSerialization:
    """Serialization round-trip and immutability."""

    def test_round_trip(self):
        caps = CapabilitySet(audit_sink=True, result_query=True)
        data = caps.model_dump()
        restored = CapabilitySet(**data)
        assert restored == caps

    def test_json_round_trip(self):
        caps = CapabilitySet(content_query=True)
        json_str = caps.model_dump_json()
        restored = CapabilitySet.model_validate_json(json_str)
        assert restored == caps

    def test_frozen(self):
        caps = CapabilitySet()
        with pytest.raises(ValidationError):
            caps.content_sink = True  # type: ignore[misc]