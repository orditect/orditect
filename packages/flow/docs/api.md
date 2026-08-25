
# orditect-flow API 参考

## 核心类

### BaseBackEndTask

任务基类，所有任务必须继承此类。

```python
class BaseBackEndTask(ABC):
    def __init__(
        self,
        storage: TaskStorageProtocol,
        governor: Optional[ResourceGovernorProtocol] = None,
    ): ...
    
    @abstractmethod
    async def execute(self, task_id: str, **kwargs) -> Any: ...
    
    async def on_success(self, task_id: str, result: Any) -> None: ...
    async def on_failure(self, task_id: str, error: Exception) -> None: ...
    async def on_cancel(self, task_id: str) -> None: ...
    async def report_progress(self, task_id: str, progress: float) -> None: ...



**方法说明**：

- `execute()`：执行任务（子类必须实现）
- `on_success()`：任务成功钩子（可选实现）
- `on_failure()`：任务失败钩子（可选实现）
- `on_cancel()`：任务取消钩子（可选实现）
- `report_progress()`：上报进度（子类可调用）

### TaskOrchestrator

任务编排器，管理任务的完整生命周期。

python

class TaskOrchestrator:
    def __init__(
        self,
        storage: TaskStorageProtocol,
        governor: Optional[ResourceGovernorProtocol] = None,
        state_machine: Optional[TaskStateMachine] = None,
    ): ...
    
    async def submit(
        self,
        task: BaseBackEndTask,
        task_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        resource: str = "task_execution",
        timeout: Optional[float] = None,
        **kwargs,
    ) -> str: ...
    
    async def get_status(self, task_id: str) -> TaskStatus: ...
    async def get_task(self, task_id: str) -> Dict[str, Any]: ...
    async def cancel(self, task_id: str) -> bool: ...
    async def list_tasks(
        self,
        status: Optional[TaskStatus] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Dict[str, Any]]: ...



**方法说明**：

- `submit()`：提交任务
- `get_status()`：获取任务状态
- `get_task()`：获取任务完整信息
- `cancel()`：取消任务
- `list_tasks()`：列出任务

### Workflow

工作流定义，包含多个步骤及其依赖关系。

python

class Workflow:
    def __init__(
        self,
        name: str,
        steps: List[WorkflowStep],
        metadata: Optional[Dict[str, Any]] = None,
    ): ...
    
    def get_execution_order(self) -> List[WorkflowStep]: ...
    def get_parallel_groups(self) -> List[List[WorkflowStep]]: ...



### WorkflowStep

工作流步骤定义。

python

@dataclass
class WorkflowStep:
    name: str
    handler: Callable
    dependencies: List[str] = field(default_factory=list)
    rollback_handler: Optional[Callable] = None
    retry_policy: Optional[Any] = None
    timeout: Optional[float] = None



**字段说明**：

- `name`：步骤名称（唯一标识）
- `handler`：处理函数（async 函数）
- `dependencies`：依赖的步骤名称列表
- `rollback_handler`：回滚函数（可选）
- `retry_policy`：重试策略（可选）
- `timeout`：步骤执行超时时间（可选）

### WorkflowExecutor

工作流执行器。

python

class WorkflowExecutor:
    def __init__(self, saga: Optional[SagaPattern] = None): ...
    
    async def execute(
        self,
        workflow: Workflow,
        context: Optional[Dict[str, Any]] = None,
        parallel: bool = False,
    ) -> Dict[str, Any]: ...



### RetryPolicy

重试策略。

python

class RetryPolicy:
    def __init__(
        self,
        max_attempts: int = 3,
        backoff: Optional[BackoffStrategy] = None,
        retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
        dlq_enabled: bool = False,
        dlq: Optional[Any] = None,
    ): ...
    
    async def execute_with_retry(
        self,
        func: Callable,
        *args,
        **kwargs,
    ) -> Any: ...



## 存储接口

### TaskStorageProtocol

任务存储抽象接口。

python

class TaskStorageProtocol(Protocol):
    async def initialize_task(
        self,
        task_id: str,
        initial_status: str,
    ) -> None: ...
    
    async def update_task(
        self,
        task_id: str,
        updates: Dict[str, Any],
    ) -> None: ...
    
    async def get_task(self, task_id: str) -> Dict[str, Any]: ...
    
    async def request_cancel(self, task_id: str) -> bool: ...
    
    async def list_tasks(
        self,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Dict[str, Any]]: ...



## 资源治理接口

### ResourceGovernorProtocol

资源治理抽象接口。

python

class ResourceGovernorProtocol(Protocol):
    async def acquire(
        self,
        resource: str,
        timeout: Optional[float] = None,
    ) -> str: ...
    
    async def try_acquire(self, resource: str) -> Optional[str]: ...
    
    async def release(self, resource: str, token: str) -> None: ...
    
    async def get_usage(self, resource: str) -> int: ...



## 回调接口

### CallbackProtocol

回调抽象接口。

python

class CallbackProtocol(Protocol):
    async def on_success(self, task_id: str, result: Dict[str, Any]) -> None: ...
    async def on_failure(self, task_id: str, error: Exception) -> None: ...
    async def on_progress(self, task_id: str, progress: float) -> None: ...
    async def on_status_change(self, task_id: str, old_status: str, new_status: str) -> None: ...



## 异常

### TaskflowError

框架所有异常的基类。

### TaskNotFoundError

任务不存在。

### InvalidStateTransitionError

非法状态流转。

### TaskCancelledError

任务被取消。

### AcquireTimeoutError

获取资源超时。

### WorkflowExecutionError

工作流执行错误。

