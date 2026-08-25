"""Storage implementation layer (taskbase promoted to hard dependency, local implementations removed)."""
from orditect.flow.storage.factory import get_default_storage

__all__ = ["get_default_storage"]