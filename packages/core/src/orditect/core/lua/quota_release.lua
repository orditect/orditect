-- quota_release.lua —— 配额释放（v0.3.2：SET 后恢复 pending_key TTL）
local pending_key = KEYS[1]
local leases_key = KEYS[2]
local task_id = ARGV[1]

local score = redis.call('ZSCORE', leases_key, task_id)
if not score then
    local cur0 = tonumber(redis.call("GET", pending_key) or "0")
    return cjson.encode({ok=true, reason="not_reserved", current=cur0, released=0})
end

local units_str = redis.call('HGET', leases_key .. ':units', task_id)
local units = tonumber(units_str or "0")

-- v0.3.2（#21）：SET 会清 TTL，先读后恢复
local pttl = redis.call('TTL', pending_key)
local current = tonumber(redis.call("GET", pending_key) or "0")
local nextv = math.max(0, current - units)
redis.call("SET", pending_key, tostring(nextv))
if pttl > 0 then
    redis.call('EXPIRE', pending_key, pttl)
end
redis.call('ZREM', leases_key, task_id)
redis.call('HDEL', leases_key .. ':units', task_id)

return cjson.encode({ok=true, reason="", current=nextv, released=units})