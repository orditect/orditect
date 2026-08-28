
local task_key = KEYS[1]
local updates_json = ARGV[1]
local expiry = tonumber(ARGV[2])
local status_index_prefix = ARGV[3]
local validate_transfer = ARGV[4]
local task_id = ARGV[5]

local current_raw = redis.call("GET", task_key)
if not current_raw then
    return cjson.encode({ok=false, err="NOT_FOUND"})
end

if expiry < 0 then
    local ttl = redis.call('TTL', task_key)
    if ttl > 0 then
        expiry = ttl
    else
        expiry = tonumber(ARGV[7])
    end
end

local current = cjson.decode(current_raw)
local updates = cjson.decode(updates_json)

local terminal = cjson.decode(ARGV[6])
local is_terminal = {}
for _, s in ipairs(terminal) do
    is_terminal[s] = true
end

local old_status = current["status"] or ""
local new_status = old_status
if updates["status"] ~= nil then
    new_status = updates["status"]
end

if old_status ~= new_status and is_terminal[old_status] then
    return cjson.encode({ok=false, err="INVALID_TRANSFER"})
end

for k, v in pairs(updates) do
    current[k] = v
end

redis.call("SET", task_key, cjson.encode(current), "EX", expiry)

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local expire_at = now_ms + expiry * 1000

if old_status ~= new_status and old_status ~= "" then
    redis.call("ZREM", status_index_prefix .. ":" .. old_status, task_id)
end
if new_status ~= "" then
    local new_key = status_index_prefix .. ":" .. new_status
    redis.call("ZADD", new_key, expire_at, task_id)
    local t = redis.call('TTL', new_key)
    if t == -1 or t < expiry then
        redis.call('EXPIRE', new_key, expiry)
    end
end

return cjson.encode({ok=true})