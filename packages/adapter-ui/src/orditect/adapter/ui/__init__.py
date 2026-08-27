"""UI adapter reference implementation (consumer read + action sink)."""

from importlib.metadata import PackageNotFoundError, version as _pkg_version

from orditect.adapter.ui.reader import TraceBundleReader
from orditect.adapter.ui.action import ActionSinkAdapter
from orditect.adapter.ui.queue import MemoryActionQueue
from orditect.flow.actions.models import ActionType

try:
    __version__ = _pkg_version("orditect-adapter-ui")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"

__all__ = [
    "TraceBundleReader",
    "ActionSinkAdapter",
    "ActionType",
    "MemoryActionQueue",
    "__version__",
]