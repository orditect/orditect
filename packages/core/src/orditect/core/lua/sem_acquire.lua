-- sem_acquire.lua —— ZSET 租约信号量获取（P1 原样搬运）
-- KEYS[1] = semaphore key
-- ARGV[1] = limit, ARGV[2] = lease_time(秒), ARGV[3] = token
-- 返回: token(成功) / nil(满)
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local lease_ms = tonumber(ARGV[2]) * 1000
local token = ARGV[3]

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

-- 清理过期占用（P1 缺陷：无续租，长持有会被误清 → P2 watchdog 修复）
redis.call('ZREMRANGEBYSCORE', key, '-inf', now_ms - lease_ms)

local cnt = redis.call('ZCARD', key)
if cnt < limit then
    redis.call('ZADD', key, now_ms, token)
    -- v0.3.2（#6）：整数化——lease_time=0.7 时 1.4 不再炸 ERR not an integer
    redis.call('EXPIRE', key, math.max(1, math.floor(tonumber(ARGV[2]) * 2)))
    return token
end
return nil