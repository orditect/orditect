
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local lease_ms = tonumber(ARGV[2]) * 1000
local token = ARGV[3]

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

redis.call('ZREMRANGEBYSCORE', key, '-inf', now_ms - lease_ms)

local cnt = redis.call('ZCARD', key)
if cnt < limit then
    redis.call('ZADD', key, now_ms, token)
    redis.call('EXPIRE', key, math.max(1, math.floor(tonumber(ARGV[2]) * 2)))
    return token
end
return nil