"""In-memory reference implementation of orditect-protocol contracts."""

from importlib.metadata import PackageNotFoundError, version as _pkg_version

from orditect.adapter.memory.store import MemoryStore

try:
    __version__ = _pkg_version("orditect-adapter-memory")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"

__all__ = ["MemoryStore", "__version__"]