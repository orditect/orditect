-- json_merge.lua —— 通用原子 JSON merge（v0.3.0 新增）
-- KEYS[1] = key
-- ARGV[1] = updates_json
-- ARGV[2] = expiry_seconds
-- 返回: cjson {ok=true}
--       / {ok=false, err="NOT_FOUND"}        key 不存在
--       / {ok=false, err="NOT_A_JSON_OBJECT"} 存量值不是 JSON 对象
--       / {ok=false, err="BAD_UPDATES_JSON"} 入参不是合法 JSON 对象
--
-- 定位：公共原子原语。替代 P2-R2 标注的非原子 read-modify-write
-- （并发下同 key 互相覆盖丢更新）。任务记录 / dataset 记录 / agent 状态等
-- 所有"读-改-写"场景复用本脚本，各域不再各写特化版本。
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