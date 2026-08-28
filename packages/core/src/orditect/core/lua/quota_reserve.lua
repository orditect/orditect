-- quota_reserve.lua —— Quota reservation (idempotent, crash-safe)
-- KEYS[1] = pending_key, KEYS[2] = leases_key (ZSET)
-- ARGV[1] = units, ARGV[2] = max_units, ARGV[3] = task_ttl_sec, ARGV[4] = task_id
local pending_key = KEYS[1]
local leases_key = KEYS[2]
local units = tonumber(ARGV[1])
local max_units = tonumber(ARGV[2])
local task_ttl = tonumber(ARGV[3])
local task_id = ARGV[4]
local leases_ttl = math.max(1, math.floor(task_ttl * 2))
local function bump_pending_ttl()
    local t = redis.call('TTL', pending_key)
    if t == -1 or t < leases_ttl then
        redis.call('EXPIRE', pending_key, leases_ttl)
    end
end

-- lease slot without consuming quota); units<0 is rejected.
if (not units) or units < 0 then
    return cjson.encode({ok=false, reason="invalid_units", current=tonumber(redis.call("GET", pending_key) or "0"), reserved=0})
end
if (not max_units) or max_units <= 0 then
    return cjson.encode({ok=false, reason="invalid_max_units", current=tonumber(redis.call("GET", pending_key) or "0"), reserved=0})
end

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local ttl_ms = task_ttl * 1000

-- Reap expired leases (prevents pending_units inflation after task crash).
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
-- v0.1.4: the renewal must ALSO keep pending_key alive for at least as long
-- as the renewed lease. A renewal chain (>= 2 renewals, each within task_ttl
-- of the previous) can push the lease's logical lifetime (score + task_ttl)
-- beyond pending_key's fallback TTL (set at first reserve). If pending_key
-- dies while a live lease remains, the lease's units evaporate from the
-- counter and a later reserve over-admits (over-allocation).
local existing_score = redis.call('ZSCORE', leases_key, task_id)
if existing_score then
    redis.call('ZADD', leases_key, now_ms, task_id)
    redis.call('EXPIRE', leases_key, leases_ttl)
    redis.call('EXPIRE', leases_key .. ':units', leases_ttl)
    local existing_units_str = redis.call('HGET', leases_key .. ':units', task_id)
    local existing_units = tonumber(existing_units_str or "0")

    if redis.call('EXISTS', pending_key) == 1 then
        bump_pending_ttl()
    else
        -- pending_key already died: rebuild it from the surviving leases
        -- (the reaping pass above already ran, so ZRANGE yields exactly the
        -- un-expired leases — the single source of truth).
        local total = 0
        for _, member in ipairs(redis.call('ZRANGE', leases_key, 0, -1)) do
            total = total + tonumber(redis.call('HGET', leases_key .. ':units', member) or "0")
        end
        redis.call('SET', pending_key, tostring(total), 'EX', leases_ttl)
    end

    local cur = tonumber(redis.call("GET", pending_key) or "0")
    return cjson.encode({ok=true, reason="already_reserved", current=cur, reserved=existing_units})
end

-- Quota check
local current = tonumber(redis.call("GET", pending_key) or "0")
local nextv = current + units
if nextv > max_units then
    return cjson.encode({ok=false, reason="limit_exceeded", current=current, reserved=0})
end

-- Reserve: increment pending + record the lease
redis.call("SET", pending_key, tostring(nextv))
bump_pending_ttl()
redis.call('ZADD', leases_key, now_ms, task_id)
redis.call('HSET', leases_key .. ':units', task_id, tostring(units))
redis.call('EXPIRE', leases_key, leases_ttl)
redis.call('EXPIRE', leases_key .. ':units', leases_ttl)

return cjson.encode({ok=true, reason="", current=nextv, reserved=units})