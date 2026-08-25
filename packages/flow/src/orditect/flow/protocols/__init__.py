"""Abstract interface layer: define loosely coupled protocols."""
from orditect.flow.protocols.storage import TaskStorageProtocol
from orditect.flow.protocols.governor import ResourceGovernorProtocol
from orditect.flow.protocols.callback import CallbackProtocol
from orditect.flow.protocols.scheduler import SchedulerProtocol

__all__ = [
    "TaskStorageProtocol",
    "ResourceGovernorProtocol",
    "CallbackProtocol",
    "SchedulerProtocol",
]