-- sem_refresh.lua —— watchdog 续约（v0.3.0：续约同步刷新 key 级 TTL）
-- KEYS[1] = semaphore key
-- ARGV[1] = token
-- ARGV[2] = lease_time_sec（新增：用于同步刷新 key 级 EXPIRE）
-- 返回: 1(已续约) / 0(token 已不存在,停止续约)
--
-- v0.3.0 修复（潜伏 bug）：
--   原实现续约只 ZADD 不刷 EXPIRE。sem_acquire 设置的 key TTL = lease×2，
--   当所有槽位被长任务持有、无新 acquire 刷新 TTL 时，key 在 2×lease 后整个蒸发：
--   持有者不知（watchdog 因 ZSCORE=nil 停止）→ 新 acquire 看到空集合放行 →
--   假互斥复活（P2 watchdog 要解决的病，在持有 >2×lease 时复发）。
local key = KEYS[1]
local token = ARGV[1]
local lease_sec = tonumber(ARGV[2])

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

if redis.call('ZSCORE', key, token) then
    redis.call('ZADD', key, now_ms, token)
    -- 与 sem_acquire.lua 同策略刷新 key TTL（lease×2，下限 1s）
    redis.call('EXPIRE', key, math.max(1, math.floor(lease_sec * 2)))
    return 1
end
return 0