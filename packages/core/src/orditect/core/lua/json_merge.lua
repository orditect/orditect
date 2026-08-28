local key = KEYS[1]
local updates_json = ARGV[1]
local expiry = tonumber(ARGV[2])

local raw = redis.call('GET', key)
if not raw then
    return cjson.encode({ok=false, err="NOT_FOUND"})
end

local ok, data = pcall(cjson.decode, raw)
if not ok or type(data) ~= 'table' then
    return cjson.encode({ok=false, err="NOT_A_JSON_OBJECT"})
end

local ok2, updates = pcall(cjson.decode, updates_json)
if not ok2 or type(updates) ~= 'table' then
    return cjson.encode({ok=false, err="BAD_UPDATES_JSON"})
end

for k, v in pairs(updates) do
    data[k] = v
end

redis.call('SET', key, cjson.encode(data), 'EX', expiry)
return cjson.encode({ok=true})