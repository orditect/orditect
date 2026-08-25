# orditect-stream 事件协议规范（v1）

## 帧格式

标准 SSE：

```
id: {stream_id}:{seq}
event: {event_type}
data: {json}
```

- `id` = `{stream_id}:{seq}`，断点续传锚点（v1 预留）
- data 内多行 JSON 拆分多个 `data:` 行
- 心跳为注释帧 `:ping\n\n`

## 事件信封

| 字段 | 说明 |
|---|---|
| v | 协议版本，v1=1 |
| stream_id | 子流标识 |
| stage | 所属 stage（可为 null） |
| seq | 单 stream_id 内单调递增 |
| ts | 服务端时间戳 |
| data | 载荷（业务扩展只能进 data.ext） |

## 事件类型

| 事件 | 触发 | data 关键字段 |
|---|---|---|
| stream.start | 子流启动 | stages, resume_token |
| stream.delta | 内容增量 | kind(content/thinking/references), text/references |
| enrich.marker | 检测到占位符 | placeholder_id, context_text |
| enrich.placeholder | 占位符生效 | placeholder_id, loading_url |
| enrich.resolved | settle 窗口内完成 | placeholder_id, url, state |
| stage.end | 单 stage 完成 | name, result{content, thinking?} |
| stream.manifest | finalizer 后 | stages, placeholders, usage, errors, ext |
| stream.end | 流关闭 | （无业务载荷） |
| stream.error | 任意时刻 | code, message, retryable, stage |

## manifest.placeholders

| 字段 | 说明 |
|---|---|
| placeholder_id | 占位符 id |
| task_ref | `tf:task-xxx`（taskflow）/ `local:job-xxx`（本地） |
| state | pending / resolved / failed |
| url | resolved 时的真实地址（pending/failed 时省略） |
| fallback_url | failed 时的降级地址 |


## task_ref 命名空间约定（v0.3.2 冻结）

| 命名空间 | 语义 | 委托解析 |
|---|---|---|
| `tf:enrich-{placeholder_id}` | taskflow 派发（确定性 task_id） | ManifestResolver 按 task_id 轮询任务记录，取 result.url |
| `local:{placeholder_id}` | 本地协程派发（provenance 标识） | **无委托**——settle 超窗即 failed + fallback_url，客户端无需轮询 |

- 确定性 ID 约定（框架规范）：taskflow 派发的 enrich 任务
  task_id = `enrich-{placeholder_id}`，派发方与引用方零通道对齐；
  同一 placeholder 的重试/重放收敛到同一任务（幂等）。
- v0.3.2 起 manifest 中 local 模式的 placeholder 只有 resolved/failed 两态，
  不再出现 `pending` + `local:` 的引用组合。



## 客户端消费要点

- optional 字段为 None 时**键被省略**，必须用 `.get()` 读取
- `stream.end` 是唯一终态信号
- manifest 中 pending 的 placeholder 由客户端按 task_ref 轮询回填
- ## cancel 后的事件序列（v0.3.3 冻结）

`runner.cancel()` 后子流的事件序列契约：

- **停止**：stream.delta（content/thinking/references）、enrich.* 业务增量
- - `kind=references`：引用块增量（v0.3.3 起真实可用——v0.3.2 及之前
  版本该类型在管道中被吞，不会实际发出）
- **送达**：stream.cancelled（含 partial_content）→ stream.manifest → stream.end

即：cancel 后客户端仍能收到 manifest 与 end——"stream.end 是唯一终态信号"
在 cancel 路径同样成立，客户端可以 manifest 为最终对账依据。


## Pause and resume semantics (v0.2.0 预留决策)

流式输出与 flow 的暂停/恢复机制（v0.2.0 单独迭代）的衔接语义，
本版本先冻结决策，实现随 flow 暂停机制落地：

### 决策：暂停 = 流正常结束落 manifest；恢复 = 开新流

| 选项 | 评估 | 选择 |
|---|---|---|
| 流保持挂起（连接不断，等唤醒续推） | 长连接占用、代理超时风险、断连策略复杂化 | ❌ |
| 流结束落 manifest，恢复开新流 | 与 execution_id 新代对齐、连接生命周期清晰、客户端重连简单 | ✅ |

### 暂停时的客户端可见行为

- 当前流走正常终态序列：`stream.manifest` → `stream.end`
  （"stream.end 是唯一终态信号"的纪律在暂停路径同样成立）。
- `stream.cancelled`（若因暂停触发取消）的 `partial_content`
  携带中断时已生成内容（content 纯度：只含 content，不含 thinking）。
- manifest 落盘后可经 refetch 端点（结果域）查询中断点。

### 恢复时的客户端可见行为

- 恢复产生新 execution_id（flow resume 开新代），客户端发起新请求、
  开新流（新 stream_id）。
- 断点续传体验：旧流 manifest 经 refetch 查看中断点，新流从恢复点
  开始推流。两流通过 task_id 关联（业务侧负责串联展示）。

### 与取消的区别

- cancel / terminate：任务终止（落终态 cancelled），不可恢复。
- suspend（v0.2.0）：任务挂起（落非终态机制位），可唤醒；流式侧表现
  为"流结束落 manifest"，唤醒后开新流。