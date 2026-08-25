-- sem_release.lua —— 幂等释放（P1 原样）
-- KEYS[1] = semaphore key, ARGV[1] = token
local key = KEYS[1]
local token = ARGV[1]
redis.call('ZREM', key, token)
return 1
