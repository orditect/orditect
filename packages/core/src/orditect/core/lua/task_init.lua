
local task_key = KEYS[1]
local status_key = KEYS[2]
local children_key = KEYS[3]
local data_json = ARGV[1]
local expiry = tonumber(ARGV[2])
local task_id = ARGV[3]

if ARGV[4] == "1" and redis.call('EXISTS', task_key) == 1 then
    return 0
end

local data = cjson.decode(data_json)
data["execution_id"] = ARGV[7]
data_json = cjson.encode(data)

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local expire_at = now_ms + expiry * 1000

local function bump_ttl(k)
    local t = redis.call('TTL', k)
    if t == -1 or t < expiry then
        redis.call('EXPIRE', k, expiry)
    end
end

redis.call('SET', task_key, data_json, 'EX', expiry)

if ARGV[5] == "1" then
    redis.call('ZADD', status_key, expire_at, task_id)
    bump_ttl(status_key)
end

if ARGV[6] ~= "" then
    redis.call('ZADD', children_key, expire_at, task_id)
    bump_ttl(children_key)
end

return 1