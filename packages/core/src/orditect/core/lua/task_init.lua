-- task_init.lua —— 任务初始化原子化（v0.3.2 新增，簇 A）
-- v0.1.0 (C3.5): inject initial execution_id at creation — task carries a
-- generation identity from birth; reopen only advances the generation (T11).
-- 一次性完成：幂等检查 + 主记录写入 + 状态索引 + 谱系索引（同 expire_at 租约）
--
-- KEYS[1] = task_key
-- KEYS[2] = status_index_key（ARGV[5]="0" 时传 KEYS[1] 占位，脚本不写）
-- KEYS[3] = children_index_key（ARGV[6]="" 时传 KEYS[1] 占位，脚本不写）
-- ARGV[1] = data_json
-- ARGV[2] = expiry_seconds
-- ARGV[3] = task_id
-- ARGV[4] = if_not_exists ("1"/"0")
-- ARGV[5] = has_status ("1"/"0")
-- ARGV[6] = parent_task_id ("" = 无谱系)
-- ARGV[7] = execution_id (v0.1.0 新增：初始代标识，Python 侧生成)
--
-- 返回: 1=初始化成功 / 0=幂等跳过（if_not_exists 且任务已存在）
local task_key = KEYS[1]
local status_key = KEYS[2]
local children_key = KEYS[3]
local data_json = ARGV[1]
local expiry = tonumber(ARGV[2])
local task_id = ARGV[3]

if ARGV[4] == "1" and redis.call('EXISTS', task_key) == 1 then
    return 0
end

-- v0.1.0 (C3.5): inject execution_id into the record before writing
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