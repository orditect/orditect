-- quota_release.lua —— Quota release
-- KEYS[1] = pending_key, KEYS[2] = leases_key (ZSET)
-- ARGV[1] = task_id
-- ARGV[2] = fallback_ttl_seconds (used when pending_key would otherwise
--           become eternal; the release side does not know the original
--           task_ttl, so the caller passes a safe default)
local pending_key = KEYS[1]
local leases_key = KEYS[2]
local task_id = ARGV[1]
local fallback_ttl = tonumber(ARGV[2])

local score = redis.call('ZSCORE', leases_key, task_id)
if not score then
    local cur0 = tonumber(redis.call("GET", pending_key) or "0")
    return cjson.encode({ok=true, reason="not_reserved", current=cur0, released=0})
end

local units_str = redis.call('HGET', leases_key .. ':units', task_id)
local units = tonumber(units_str or "0")

local pttl = redis.call('TTL', pending_key)
local current = tonumber(redis.call("GET", pending_key) or "0")
local nextv = math.max(0, current - units)

if nextv == 0 and pttl <= 0 then
    -- A zero counter with no TTL is deleted instead of SET — an eternal
    -- "0" key is pure residue with no semantic value.
    redis.call('DEL', pending_key)
else
    redis.call("SET", pending_key, tostring(nextv))
    if pttl > 0 then
        redis.call('EXPIRE', pending_key, pttl)
    else
        -- v0.1.5: TTL reads integer seconds, so a key in its final second
        -- reports pttl==0; SET then clears the TTL and the key would live
        -- forever, over-rejecting later admissions. Never allow that.
        redis.call('EXPIRE', pending_key, fallback_ttl)
    end
end

redis.call('ZREM', leases_key, task_id)
redis.call('HDEL', leases_key .. ':units', task_id)

return cjson.encode({ok=true, reason="", current=nextv, released=units})