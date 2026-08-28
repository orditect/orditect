
local key = KEYS[1]
local token = ARGV[1]
redis.call('ZREM', key, token)
return 1
