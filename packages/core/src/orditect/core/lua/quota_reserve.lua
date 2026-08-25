-- quota_reserve.lua —— 配额预占（v0.3.2：units=0 建账语义 + pending_key TTL 兜底）
-- KEYS[1] = pending_key, KEYS[2] = leases_key (ZSET)
-- ARGV[1] = units, ARGV[2] = max_units, ARGV[3] = task_ttl_sec, ARGV[4] = task_id
local pending_key = KEYS[1]
local leases_key = KEYS[2]
local units = tonumber(ARGV[1])
local max_units = tonumber(ARGV[2])
local task_ttl = tonumber(ARGV[3])
local task_id = ARGV[4]

-- v0.3.2（#6）：整数化（task_ttl=1.7 时 3.4 不再炸）
local leases_ttl = math.max(1, math.floor(task_ttl * 2))

-- v0.3.2（#21）：pending_key TTL 兜底。SET 会清除既有 TTL，每次写入后恢复/提升。
local function bump_pending_ttl()
    local t = redis.call('TTL', pending_key)
    if t == -1 or t < leases_ttl then
        redis.call('EXPIRE', pending_key, leases_ttl)
    end
end

-- v0.3.2（簇 C 预埋）：units=0 合法（BudgetLedger.open 建账语义，只占租约位不消耗），<0 拒绝
if (not units) or units < 0 then
    return cjson.encode({ok=false, reason="invalid_units", current=tonumber(redis.call("GET", pending_key) or "0"), reserved=0})
end
if (not max_units) or max_units <= 0 then
    return cjson.encode({ok=false, reason="invalid_max_units", current=tonumber(redis.call("GET", pending_key) or "0"), reserved=0})
end

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local ttl_ms = task_ttl * 1000

-- 清理过期租约（防任务崩溃导致 pending_units 虚高）
local expired = redis.call('ZRANGEBYSCORE', leases_key, '-inf', now_ms - ttl_ms)
for _, expired_task_id in ipairs(expired) do
    local expired_units_str = redis.call('HGET', leases_key .. ':units', expired_task_id)
    if expired_units_str then
        local expired_units = tonumber(expired_units_str)
        local current = tonumber(redis.call("GET", pending_key) or "0")
        local nextv = math.max(0, current - expired_units)
        redis.call("SET", pending_key, tostring(nextv))
        bump_pending_ttl()
        redis.call('HDEL', leases_key .. ':units', expired_task_id)
    end
    redis.call('ZREM', leases_key, expired_task_id)
end

-- 幂等：已预占则刷新租约 score 后返回
-- v0.3.3（B2）：重试即续租——修复"重试后长任务的租约先过期被崩溃回收误清"：
-- 原实现命中幂等直接返回，score 停留在首次预占时刻；任务若长过 task_ttl，
-- 后续任意 reserve 的清理段会把它当崩溃任务回收其 units。
local existing_score = redis.call('ZSCORE', leases_key, task_id)
if existing_score then
    redis.call('ZADD', leases_key, now_ms, task_id)
    redis.call('EXPIRE', leases_key, leases_ttl)
    redis.call('EXPIRE', leases_key .. ':units', leases_ttl)
    local existing_units_str = redis.call('HGET', leases_key .. ':units', task_id)
    local existing_units = tonumber(existing_units_str or "0")
    local cur = tonumber(redis.call("GET", pending_key) or "0")
    return cjson.encode({ok=true, reason="already_reserved", current=cur, reserved=existing_units})
end

-- 检查配额
local current = tonumber(redis.call("GET", pending_key) or "0")
local nextv = current + units
if nextv > max_units then
    return cjson.encode({ok=false, reason="limit_exceeded", current=current, reserved=0})
end

-- 预占：增加 pending + 记录租约
redis.call("SET", pending_key, tostring(nextv))
bump_pending_ttl()
redis.call('ZADD', leases_key, now_ms, task_id)
redis.call('HSET', leases_key .. ':units', task_id, tostring(units))
redis.call('EXPIRE', leases_key, leases_ttl)
redis.call('EXPIRE', leases_key .. ':units', leases_ttl)

return cjson.encode({ok=true, reason="", current=nextv, reserved=units})