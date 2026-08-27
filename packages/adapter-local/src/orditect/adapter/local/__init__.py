"""Local-file reference implementation of orditect-protocol contracts."""

from importlib.metadata import PackageNotFoundError, version as _pkg_version

from orditect.adapter.local.store import LocalFileStore

try:
    __version__ = _pkg_version("orditect-adapter-local")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"

__all__ = ["LocalFileStore", "__version__"]