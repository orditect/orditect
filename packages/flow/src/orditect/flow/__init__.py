"""orditect-flow — FastAPI async task orchestration and lifecycle management framework

Core features:
- FastAPI native: no extra worker processes required
- Complete state machine: pending → queued → running → succeeded/failed
- Workflow orchestration: multi-step, dependencies, rollback
- Retry mechanisms: exponential backoff, dead letter queue
- Callback mechanisms: Webhook, WebSocket, custom
- Task scheduling: priority, scheduled, delayed, dependency
- Progress tracking: real-time progress reporting
- Loosely coupled: can be used independently or integrated with orditect-core
- Dual-layer governance: task-level resource type (BaseBackEndTask.resource_type)
  + global call-point governance (GovernedClient)
- Dual cancellation modes: cancel() graceful marking / terminate() forceful termination

Usage example:
    from orditect.flow import BaseBackEndTask, TaskOrchestrator
    from orditect.flow.storage.factory import get_default_storage
    from orditect.flow.governor.factory import get_default_governor

    # Define task
    class MyTask(BaseBackEndTask):
        async def execute(self, task_id: str, **kwargs):
            result = await process_data(kwargs["data"])
            return result

    # Create orchestrator
    storage = get_default_storage(redis_client)
    governor = get_default_governor(redis_client)
    orchestrator = TaskOrchestrator(storage, governor)

    # Submit task
    task = MyTask(storage, governor)
    task_id = await orchestrator.submit(task, data={"key": "value"})

    # Query status
    status = await orchestrator.get_status(task_id)

Dual-layer governance example:
    from orditect.flow import BaseBackEndTask, GovernedClient

    class AgentTask(BaseBackEndTask):
        resource_type = "task_agent"  # Task governance type

        async def execute(self, task_id: str, **kwargs):
            governor = kwargs.get("governor")  # Injected by TaskExecutor
            llm = GovernedClient(governor, resource="default_stream_llm",
                                 handler=call_llm)
            return await llm.call(prompt)

Resource status query example:
    from orditect.flow import GovernorManager

    manager = GovernorManager(governor)
    status = await manager.get_resource_status("task_agent")
    all_status = await manager.get_all_resources()
"""
from importlib.metadata import PackageNotFoundError, version as _pkg_version

try:
    __version__ = _pkg_version("orditect-flow")
except PackageNotFoundError:
    __version__ = "0.0.0.dev0"

from orditect.flow.exceptions import (
    TaskflowError,
    TaskNotFoundError,
    InvalidStateTransitionError,
    TaskCancelledError,
    AcquireTimeoutError,
    DependencyNotSatisfiedError,
    WorkflowExecutionError,
)

from orditect.flow.protocols import (
    TaskStorageProtocol,
    ResourceGovernorProtocol,
    CallbackProtocol,
    SchedulerProtocol,
)

from orditect.flow.storage import (

    get_default_storage,
)

from orditect.flow.governor import (

    UnlimitedGovernor,
    get_default_governor,
    GovernedClient,
    GovernorManager,
    BudgetLedger,       
    BudgetExhaustedError,  
    BudgetAuditSink,     
    NullAuditSink,         
)

from orditect.flow.core import (
    BaseBackEndTask,
    TaskOrchestrator,
    TaskStateMachine,
    TaskStatus,
    TaskExecutor,
    TaskLifecycle,
    LineageInspector,
)

from orditect.flow.workflow import (
    Workflow,
    WorkflowStep,
    DAG,
    WorkflowExecutor,
    SagaPattern,
)

from orditect.flow.retry import (
    RetryPolicy,
    BackoffStrategy,
    ExponentialBackoff,
    LinearBackoff,
    ConstantBackoff,
    DeadLetterQueue,
)

from orditect.flow.callback import (
    WebhookCallback,
    WebSocketCallback,
    CompositeCallback,
)

from orditect.flow.scheduler import (
    PriorityScheduler,
    CronScheduler,
    DelayedScheduler,
    DependencyScheduler,
)

from orditect.flow.progress import (
    ProgressTracker,
    ProgressReporter,
    ProgressEstimator,
)

# Snapshot (F2 sink + F3 reuse query)
from orditect.flow.snapshot import (
    NullSnapshotQuery,
    NullSnapshotSink,
    ProtocolSnapshotQuery,
    ProtocolSnapshotSink,
    SnapshotQuery,
    SnapshotSink,
)

# Recovery (F4)
from orditect.flow.recovery import RecoveryService, ReuseDecision, TaskFactory
# Dependency governance (v0.1.1)
from orditect.flow.governance import (
    DependencyGovernor,
    rebuild_dep_counters,
    scan_dependency_cycles,
)

__all__ = [

    "__version__",

    "TaskflowError",
    "TaskNotFoundError",
    "InvalidStateTransitionError",
    "TaskCancelledError",
    "AcquireTimeoutError",
    "DependencyNotSatisfiedError",
    "WorkflowExecutionError",

    "TaskStorageProtocol",
    "ResourceGovernorProtocol",
    "CallbackProtocol",
    "SchedulerProtocol",

    "get_default_storage",

    "UnlimitedGovernor",
    "get_default_governor",
    "GovernedClient",
    "GovernorManager",
    "BudgetLedger",
    "BudgetExhaustedError",
    "BudgetAuditSink",
    "NullAuditSink",

    "BaseBackEndTask",
    "TaskOrchestrator",
    "TaskStateMachine",
    "TaskStatus",
    "TaskExecutor",
    "TaskLifecycle",
    "LineageInspector",

    "Workflow",
    "WorkflowStep",
    "DAG",
    "WorkflowExecutor",
    "SagaPattern",

    "RetryPolicy",
    "BackoffStrategy",
    "ExponentialBackoff",
    "LinearBackoff",
    "ConstantBackoff",
    "DeadLetterQueue",

    "WebhookCallback",
    "WebSocketCallback",
    "CompositeCallback",

    "PriorityScheduler",
    "CronScheduler",
    "DelayedScheduler",
    "DependencyScheduler",

    "ProgressTracker",
    "ProgressReporter",
    "ProgressEstimator",
    "NullSnapshotSink",
    "SnapshotSink",
    "ProtocolSnapshotSink",
    "NullSnapshotQuery",
    "SnapshotQuery",
    "ProtocolSnapshotQuery",
    "RecoveryService",
    "ReuseDecision",
    "TaskFactory",
    "DependencyGovernor",
    "scan_dependency_cycles",
    "rebuild_dep_counters",
]