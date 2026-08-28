
local key = KEYS[1]
local token = ARGV[1]
local lease_sec = tonumber(ARGV[2])

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

if redis.call('ZSCORE', key, token) then
    redis.call('ZADD', key, now_ms, token)
    redis.call('EXPIRE', key, math.max(1, math.floor(lease_sec * 2)))
    return 1
end
return 0