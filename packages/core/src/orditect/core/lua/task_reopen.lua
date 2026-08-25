-- task_reopen.lua —— terminal task new-generation reopen (v0.1.0 new)
-- Atomic: verify terminal -> write new execution_id -> reset state ->
-- migrate status index -> leave old generation trace.
-- Single-script atomicity seals concurrent reopen double-winner window (T10).
--
-- KEYS[1] = task_key
-- ARGV[1] = task_id
-- ARGV[2] = new_execution_id (Python-generated, "exec-{uuid4hex[:12]}")
-- ARGV[3] = initial_status (post-reopen initial state word, caller vocabulary)
-- ARGV[4] = expiry_seconds (<0 = keep remaining TTL, B1 preserve semantics;
--           fallback ARGV[7] when no TTL)
-- ARGV[5] = status_index_prefix
-- ARGV[6] = terminal_statuses_json (caller-declared terminal set)
-- ARGV[7] = default_expiry_seconds (preserve-mode fallback)
--
-- Returns (cjson):
--   {ok=true, execution_id=..., previous_status=...}
--   {ok=false, err="NOT_FOUND"}
--   {ok=false, err="NOT_TERMINAL", current_status=...}
local task_key = KEYS[1]
local task_id = ARGV[1]
local new_execution_id = ARGV[2]
local initial_status = ARGV[3]
local expiry = tonumber(ARGV[4])
local status_index_prefix = ARGV[5]

local current_raw = redis.call("GET", task_key)
if not current_raw then
    return cjson.encode({ok=false, err="NOT_FOUND"})
end

-- Preserve-mode: parse remaining TTL before SET overwrites it (B1 semantics)
if expiry < 0 then
    local ttl = redis.call('TTL', task_key)
    if ttl > 0 then
        expiry = ttl
    else
        expiry = tonumber(ARGV[7])
    end
end

local current = cjson.decode(current_raw)

-- Terminal set (whitelist, caller-declared — vocabulary neutrality T6)
local terminal = cjson.decode(ARGV[6])
local is_terminal = {}
for _, s in ipairs(terminal) do
    is_terminal[s] = true
end

local old_status = current["status"] or ""
if not is_terminal[old_status] then
    return cjson.encode({ok=false, err="NOT_TERMINAL", current_status=old_status})
end

-- Leave old generation trace (audit trail, cap 50 to prevent hot record bloat)
local prev_ids = current["previous_execution_ids"]
if type(prev_ids) ~= "table" then
    prev_ids = {}
end
local old_eid = current["execution_id"]
if old_eid ~= nil and old_eid ~= "" then
    table.insert(prev_ids, old_eid)
    if #prev_ids > 50 then
        table.remove(prev_ids, 1)
    end
end

-- Reset to new generation
current["previous_execution_ids"] = prev_ids
current["previous_status"] = old_status
current["execution_id"] = new_execution_id
current["status"] = initial_status
current["cancel_requested"] = false

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
current["reopened_at"] = now_ms

redis.call("SET", task_key, cjson.encode(current), "EX", expiry)

-- Status index migration (ZSET lease model, same as task_init/task_update)
local expire_at = now_ms + expiry * 1000
if old_status ~= "" then
    redis.call("ZREM", status_index_prefix .. ":" .. old_status, task_id)
end
if initial_status ~= "" then
    local new_key = status_index_prefix .. ":" .. initial_status
    redis.call("ZADD", new_key, expire_at, task_id)
    local t = redis.call('TTL', new_key)
    if t == -1 or t < expiry then
        redis.call('EXPIRE', new_key, expiry)
    end
end

return cjson.encode({ok=true, execution_id=new_execution_id, previous_status=old_status})