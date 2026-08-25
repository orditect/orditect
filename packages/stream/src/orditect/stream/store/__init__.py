"""Result store layer."""
from orditect.stream.store.memory import MemoryResultStore
from orditect.stream.store.factory import get_default_store, get_protocol_store
from orditect.stream.store.adapter import ProtocolResultStore

__all__ = [
    "MemoryResultStore",
    "get_default_store",
    "get_protocol_store",
    "ProtocolResultStore",
]