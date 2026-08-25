-- task_update.lua —— 任务记录原子 merge + 状态索引维护（v0.3.3：B1 保持剩余到期）
-- KEYS[1] = task_key
-- ARGV[1] = updates_json
-- ARGV[2] = expiry_seconds（v0.3.3：<0 表示"保持剩余到期时刻"，由 Lua 读 TTL 解析）
-- ARGV[3] = status_index_prefix
-- ARGV[4] = validate_transfer ("1"/"0")（保留位：完整状态机校验在 Python 侧，Lua 不消费）
-- ARGV[5] = task_id
-- ARGV[6] = terminal_statuses_json（终态集合，如 '["succeeded","failed","cancelled"]'）
-- ARGV[7] = default_expiry_seconds（v0.3.3 新增：保持模式下无 TTL 时的兜底）
--
-- v0.3.3（B1）：expiry=None（Python 侧传 -1）不再把主记录 TTL 重置为
-- default_expire_time——修复"任意一次状态更新把短 TTL 任务续命到 7 天"的
-- 租约膨胀；索引 expire_at 与主记录同口径推进，维持成员级同到期时刻契约。
local task_key = KEYS[1]
local updates_json = ARGV[1]
local expiry = tonumber(ARGV[2])
local status_index_prefix = ARGV[3]
local validate_transfer = ARGV[4]
local task_id = ARGV[5]

local current_raw = redis.call("GET", task_key)
if not current_raw then
    return cjson.encode({ok=false, err="NOT_FOUND"})
end

-- v0.3.3（B1）：负值 = 保持剩余到期时刻（须在 SET 之前读 TTL）
if expiry < 0 then
    local ttl = redis.call('TTL', task_key)
    if ttl > 0 then
        expiry = ttl
    else
        -- 无 TTL（理论仅历史数据）：兜底 default，防永不过期残留
        expiry = tonumber(ARGV[7])
    end
end

local current = cjson.decode(current_raw)
local updates = cjson.decode(updates_json)

-- 终态集合（白名单，由调用方声明）
local terminal = cjson.decode(ARGV[6])
local is_terminal = {}
for _, s in ipairs(terminal) do
    is_terminal[s] = true
end

local old_status = current["status"] or ""
local new_status = old_status
if updates["status"] ~= nil then
    new_status = updates["status"]
end

-- 终态保护：无条件执行（词汇表来自 ARGV[6]）
if old_status ~= new_status and is_terminal[old_status] then
    return cjson.encode({ok=false, err="INVALID_TRANSFER"})
end

-- merge
for k, v in pairs(updates) do
    current[k] = v
end

redis.call("SET", task_key, cjson.encode(current), "EX", expiry)

-- 状态索引 ZSET 租约（服务端时钟 expire_at；读路径惰性清理）
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local expire_at = now_ms + expiry * 1000

if old_status ~= new_status and old_status ~= "" then
    redis.call("ZREM", status_index_prefix .. ":" .. old_status, task_id)
end
if new_status ~= "" then
    -- 状态未变时同样 ZADD：主记录 EX 已确定，租约同步推进（ZADD 对存量成员幂等）
    local new_key = status_index_prefix .. ":" .. new_status
    redis.call("ZADD", new_key, expire_at, task_id)
    -- key 级 TTL 只增不减（防残留兜底）
    local t = redis.call('TTL', new_key)
    if t == -1 or t < expiry then
        redis.call('EXPIRE', new_key, expiry)
    end
end

return cjson.encode({ok=true})