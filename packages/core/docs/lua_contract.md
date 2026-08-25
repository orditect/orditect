# Lua 脚本调用契约（v0.3.2 冻结）

本文档冻结 taskbase 全部 Lua 脚本的 KEYS/ARGV 规格与返回格式。
上层框架（taskflow / taskstream / 未来框架）调用时必须按本契约传参。
规格变更需版本升级并同步本文档。

- v0.3.3：`task_update.lua` 新增 ARGV[7]（ARGV[2] 负值语义扩展）；
  `quota_reserve.lua` 幂等分支增加续租写入。

## ⚠️ v0.3.2 数据模型变更（部署注意）

- **状态索引 / 谱系索引数据结构：SET → ZSET**（member=task_id, score=expire_at_ms）。
- **升级部署需 flushdb 或更换 key_prefix**（旧 SET 数据与新 ZSET 读写路径不兼容）。
- 变更脚本：`task_update.lua`（索引维护方式）、`bucket_acquire.lua`（返回值）、
  `quota_reserve.lua`（units 语义 + TTL）、`quota_release.lua`（TTL 保留）、
  `sem_acquire.lua`（EXPIRE 整数化）。
- 新增脚本：`task_init.lua`（initialize_task 全面 Lua 化，Python pipeline 路径废弃）。

## 索引租约模型（v0.3.2 设计契约）

共享索引（状态索引 / 谱系索引）采用与 sem/quota 同构的 ZSET 租约模型：

- **成员级精确语义**：member=task_id，score=expire_at_ms（服务端时钟 TIME 计算）。
- **读路径惰性清理**：读取方先 `ZREMRANGEBYSCORE key -inf now_ms` 再取成员；
  成员清空时 key 自动蒸发（聚合类型固有行为），无独立清理机制。
- **key 级 TTL 只增不减**：写入时 `TTL < expiry 或无 TTL → EXPIRE expiry`，
  仅作"永无人读"场景的防残留兜底，不承载精确语义。
- 该模型修复了"key 级同 TTL"契约的缺陷：共享集合 + 成员独立 TTL 场景下，
  单点 TTL 无法同时满足先写入者与后写入者（活跃成员连坐失踪 / 幽灵成员积累）。

## task_init.lua —— 任务初始化原子化（v0.3.2 新增）

**KEYS**:
- KEYS[1]: task_key（任务主记录键）
- KEYS[2]: status_index_key（ARGV[5]="0" 时传 KEYS[1] 占位，脚本不写）
- KEYS[3]: children_index_key（ARGV[6]="" 时传 KEYS[1] 占位，脚本不写）

**ARGV**:
- ARGV[1]: data_json（任务初始记录，JSON 对象）
- ARGV[2]: expiry_seconds（过期时间，秒）
- ARGV[3]: task_id
- ARGV[4]: if_not_exists（"1"/"0"）
- ARGV[5]: has_status（"1"/"0"，initial_status 为空字符串时传 "0"）
- ARGV[6]: parent_task_id（"" = 无谱系登记）
- ARGV[7]: execution_id（v0.1.0 新增：初始代标识，Python 侧生成
  `exec-{uuid4hex[:12]}`）。任务从创建即携带代次身份（T11 热路径投影），
  reopen_task 只推进代次；幂等跳过时不改写既有 execution_id。
**返回**:
- 1：初始化成功
- 0：幂等跳过（if_not_exists="1" 且任务已存在）

**语义**：
- 幂等检查与写入原子化（EXISTS + 条件写同脚本），封死 TOCTOU 窗口。
- 状态索引 / 谱系索引按租约模型写入（ZADD expire_at + key TTL 只增不减）。
- 占位键纪律：不写对应索引时 KEYS[2]/KEYS[3] 传 KEYS[1]，
  脚本由 ARGV[5]/ARGV[6] 门控，绝不写占位键。

## task_update.lua —— 任务记录原子 merge + 状态索引维护

**KEYS**:
- KEYS[1]: task_key（任务主记录键）

**ARGV**（v0.3.3 规格）:
- ARGV[1]: updates_json（待 merge 的字段，JSON 对象）
- ARGV[2]: expiry_seconds。**v0.3.3：<0 表示"保持剩余到期时刻"**——
  Lua 读主记录 TTL 解析（无 TTL 时兜底 ARGV[7]）；>=0 为显式推进（旧行为）
- ARGV[3]: status_index_prefix（状态索引键前缀）
- ARGV[4]: validate_transfer（"1"/"0"，保留位，Lua 不消费）
- ARGV[5]: task_id（任务 ID）
- ARGV[6]: terminal_statuses_json（终态集合）
- ARGV[7]: default_expiry_seconds（**v0.3.3 新增**：保持模式下无 TTL 时的兜底）
- 
**返回**（cjson）:
- `{"ok": true}` 成功
- `{"ok": false, "err": "NOT_FOUND"}` 任务不存在
- `{"ok": false, "err": "INVALID_TRANSFER"}` 终态被覆盖（old_status 在 ARGV[6] 声明的终态集合中）

**语义**：
- 终态保护无条件执行（白名单：只保护 ARGV[6] 声明的词）。
- v0.3.2：状态索引维护从 SREM/SADD 改为 ZREM/ZADD（租约模型）：
  - 状态变化时：ZREM 旧索引 + ZADD 新索引（score=expire_at）。
  - 状态未变时：同样 ZADD（主记录 EX 已重设，租约同步推进；ZADD 对存量成员幂等）。
  - 新索引 key TTL 只增不减。

## task_reopen.lua —— 终态任务重开新代（v0.1.0 新增）

**定位**：恢复体系的热路径原语。为断点续传 / 中间点重跑提供"受控开新代"
能力——不是状态流转，不违反终态保护（T3）；终态保护在同一 execution
代内无条件生效，reopen 只是产生新的一代。

**KEYS**:
- KEYS[1]: task_key（任务主记录键）

**ARGV**:
- ARGV[1]: task_id
- ARGV[2]: new_execution_id（Python 侧生成，格式 `exec-{uuid4hex[:12]}`）
- ARGV[3]: initial_status（重开后的初始状态词，由调用方词表决定）
- ARGV[4]: expiry_seconds（新代租约；<0 表示沿用当前剩余 TTL，与
  task_update.lua 的 B1 保持语义一致；无 TTL 时兜底 ARGV[7]）
- ARGV[5]: status_index_prefix（状态索引键前缀）
- ARGV[6]: terminal_statuses_json（终态集合，调用方声明注入）
- ARGV[7]: default_expiry_seconds（保持模式兜底）

**返回**（cjson）:
- `{"ok": true, "execution_id": "...", "previous_status": "..."}` 成功
- `{"ok": false, "err": "NOT_FOUND"}` 任务不存在
- `{"ok": false, "err": "NOT_TERMINAL", "current_status": "..."}`
  当前状态不在 ARGV[6] 声明的终态集合中——显式拒绝

**语义**（单脚本原子完成）:
1. 读取主记录，不存在 → NOT_FOUND。
2. 读取当前状态，不在终态集合 → NOT_TERMINAL（拒绝；终态判定词表完全
   由调用方注入，脚本不内置任何词——词汇表中立 T6）。
3. 原子写入：
   - `previous_execution_ids` 数组追加旧 execution_id（无旧 id 则跳过；
     数组为审计追溯留痕，长度上限 50，超出丢弃最旧——防热记录膨胀）；
   - `execution_id` 覆写为 ARGV[2]；
   - `previous_status` 记录重开前状态（供审计/观测）；
   - `status` 重置为 ARGV[3]；
   - `cancel_requested` 重置为 false（新代取消标记清零）；
   - `reopened_at` 写入服务端时刻；
   - 主记录 EX 按 ARGV[4] 语义设置（显式值 / 保持剩余 / 兜底 default）。
4. 状态索引迁移（ZSET 租约模型，与 task_init/task_update 同构）：
   - ZREM 旧状态索引成员；
   - ZADD 新初始状态索引成员（score = 服务端时钟 now_ms + expiry×1000）；
   - 新索引 key TTL 只增不减（防残留兜底）。
5. 谱系索引不动（父子关系跨代保持——子任务不因其自身 reopen 改变
   父任务登记）。

**并发语义（T4/T10）**：并发 reopen 同一终态任务，Lua 单脚本原子执行——
先至者完成状态重置（新代初始态），后至者读取到初始态（非终态）而
NOT_TERMINAL 拒绝。恰好一个胜者，无双重新代。

**与既有原语的关系**：
- `initialize_task(if_not_exists=True)` = 存在则跳过（提交侧防重）；
- `update_task` = 同代内状态流转（终态保护无条件）；
- `reopen_task` = 终态后开新代（恢复/重放）。
三者互补，覆盖任务生命周期的三个正交动作。

## sem_acquire.lua —— ZSET 租约信号量获取

**KEYS**:
- KEYS[1]: semaphore key

**ARGV**:
- ARGV[1]: limit（并发上限）
- ARGV[2]: lease_time（租约时长，秒）
- ARGV[3]: token（令牌值）

**返回**:
- token（成功）
- nil（满）

**语义**：
- 清理过期占用（score < now - lease_ms）
- ZCARD < limit 时 ZADD 并设置 key TTL = lease × 2
- v0.3.2：EXPIRE 整数化（`math.max(1, math.floor(lease × 2))`），
  修复小数 lease（如 0.7）导致 `ERR value is not an integer`。

## sem_refresh.lua —— watchdog 续约

**KEYS**:
- KEYS[1]: semaphore key

**ARGV**:
- ARGV[1]: token
- ARGV[2]: lease_time_sec（v0.3.0 新增：用于同步刷新 key TTL）

**返回**:
- 1（已续约）
- 0（token 不存在，停止续约）

**语义**：
- token 存在时 ZADD 刷新 score + EXPIRE 刷新 key TTL（lease × 2，整数化）
- v0.3.0 修复：续约同步刷 TTL，防 key 在 2×lease 后蒸发（假互斥复活）
- v0.3.2 无变更

## sem_release.lua —— 幂等释放

**KEYS**:
- KEYS[1]: semaphore key

**ARGV**:
- ARGV[1]: token

**返回**: 1（总是成功，幂等）

## bucket_acquire.lua —— 预约式令牌桶

**KEYS**:
- KEYS[1]: bucket key

**ARGV**:
- ARGV[1]: capacity（桶容量）
- ARGV[2]: refill_amount（每次补充令牌数）
- ARGV[3]: refill_frequency（补充间隔，秒）
- ARGV[4]: max_sleep_ms（最大允许等待毫秒，巨大值=无限等待）

**返回**（v0.3.2 变更：二元组 → 三元组）: `{status, slot_ms, server_now_ms}`
- status=1：预约成功，slot_ms=预约槽位时间戳（毫秒）
- status=0：拒绝（预估等待超 max_sleep），未提交状态变更
- server_now_ms：脚本执行时的服务端时钟（毫秒）。客户端用
  `wait_ms = slot_ms - server_now_ms` 计算等待时长——与脚本时钟源一致，
  拒绝客户端时钟漂移污染（v0.3.2 #15 修复，此前用客户端 time.time() 计算）。

**语义**：
- 服务端时钟（redis TIME），拒绝客户端时钟污染
- 状态 TTL 自计算（最远预约到期 + 回满容量 + 余量），不再硬编码 30s

## quota_reserve.lua —— 配额预占

**KEYS**:
- KEYS[1]: pending_key
- KEYS[2]: leases_key（ZSET）

**ARGV**:
- ARGV[1]: units
- ARGV[2]: max_units
- ARGV[3]: task_ttl_sec
- ARGV[4]: task_id

**返回**（cjson）:
- `{"ok": true, "reason": "", "current": N, "reserved": M}` 成功
- `{"ok": true, "reason": "already_reserved", ...}` 幂等
- `{"ok": false, "reason": "limit_exceeded", ...}` 超限
- `{"ok": false, "reason": "invalid_units"/"invalid_max_units", ...}` 参数错误

**语义**：
- 清理过期租约（ZSET score < now - ttl_ms）
- 原子预占：增加 pending + 记录租约 + 设置 EXPIRE
- v0.3.2 变更：
  - **units=0 合法**（建账语义：登记租约位但不消耗额度；
    taskflow BudgetLedger.open() 消费）。units<0 才返回 invalid_units。
  - **pending_key TTL 兜底**：每次写入后 bump（无 TTL 或小于 leases_ttl 时
    EXPIRE 为 leases_ttl），防 scope 废弃后残留。
  - leases_ttl 整数化（`math.max(1, math.floor(task_ttl × 2))`），
    修复小数 task_ttl 导致 EXPIRE 报错。
  - v0.3.3 追加：**幂等命中（already_reserved）刷新租约 score 与 key TTL**——
    重试即续租，防"重试后长任务的租约先过期被崩溃回收误清"。
  - 
## quota_release.lua —— 配额释放

**KEYS**:
- KEYS[1]: pending_key
- KEYS[2]: leases_key

**ARGV**:
- ARGV[1]: task_id

**返回**（cjson）:
- `{"ok": true, "reason": "", "current": N, "released": M}` 成功
- `{"ok": true, "reason": "not_reserved", ...}` 幂等（未预占）

**语义**：
- v0.3.2 变更：SET 会清除既有 TTL，释放路径先读 TTL 再恢复，
  保持 pending_key 的兜底过期语义（与 reserve 的 bump 配合）。

## json_merge.lua —— 通用原子 JSON merge

**KEYS**:
- KEYS[1]: key

**ARGV**:
- ARGV[1]: updates_json（待 merge 的字段，JSON 对象）
- ARGV[2]: expiry_seconds

**返回**（cjson）:
- `{"ok": true}` 成功
- `{"ok": false, "err": "NOT_FOUND"}` key 不存在
- `{"ok": false, "err": "NOT_A_JSON_OBJECT"}` 存量值不是 JSON 对象
- `{"ok": false, "err": "BAD_UPDATES_JSON"}` 入参不是合法 JSON 对象

**语义**：
- 原子 read-modify-write（公共原语，任务记录 / dataset 记录 / agent 状态等
  所有"读-改-写"场景复用）
- v0.3.2 无变更