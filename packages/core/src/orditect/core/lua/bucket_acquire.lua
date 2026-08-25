-- bucket_acquire.lua —— 预约式令牌桶（v0.3.0：状态 TTL 自计算）
-- 修复点(对照 redis-rate-limiters)：
--   1) 服务端时钟 redis.call('TIME')，拒绝客户端时钟污染共享状态
--   2) max_sleep 在脚本内原子判断：超限拒绝且【不提交预约】(被拒不烧未来配额)
--   3) 删除 +20ms 无依据魔法补偿
-- v0.3.0 修复（潜伏 bug）：
--   4) 状态 TTL 不再硬编码 30s。慢速桶（refill_frequency > 30s）状态提前蒸发，
--      桶被静默重置为满容量；已预约槽位在 TTL 后被重复发放。
--      新策略：TTL 覆盖"最远预约时刻 + 桶回满所需时间 + 余量"，脚本参数自计算。
-- KEYS[1] = bucket key
-- ARGV[1] = capacity, ARGV[2] = refill_amount
-- ARGV[3] = refill_frequency(秒), ARGV[4] = max_sleep_ms(巨大值=无限等待)
-- 返回: {status, slot_ms, server_now_ms}
--   status=1 预约成功 / 0 拒绝；server_now_ms 供客户端用同一时钟源计算等待时长（#15）
local capacity = tonumber(ARGV[1])
local refill_amount = tonumber(ARGV[2])
local time_between_slots = tonumber(ARGV[3]) * 1000
local max_sleep_ms = tonumber(ARGV[4])
local data_key = KEYS[1]

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local tokens = capacity
local slot = now

local data = redis.call('GET', data_key)
if data then
    local last_slot, stored_tokens = data:match('(%S+) (%S+)')
    slot = tonumber(last_slot)
    tokens = tonumber(stored_tokens)
    local slots_passed = math.floor((now - slot) / time_between_slots)
    if slots_passed > 0 then
        tokens = math.min(tokens + slots_passed * refill_amount, capacity)
        slot = now
    end
end

if tokens <= 0 then
    slot = slot + time_between_slots
    tokens = refill_amount
end

local wait_ms = slot - now
if wait_ms > max_sleep_ms then
    return {0, slot, now}   -- 拒绝：未提交任何状态变更
end

tokens = tokens - 1

-- v0.3.0：状态 TTL 自计算
-- 必须覆盖两个时间尺度的合计：
--   a) 最远预约槽位的到期时间：max(0, slot - now)
--   b) 从当前 tokens 回满 capacity 所需时间
-- 在此之前状态过期 = 限流语义失真（桶重置/预约重发）；在此之后过期无害
-- （存储状态与全新满桶状态等价）。
local refill_to_full_ms = math.ceil((capacity - tokens) / refill_amount) * time_between_slots
local state_ttl_ms = math.max(0, slot - now) + refill_to_full_ms + 5000
local state_ttl_sec = math.max(1, math.ceil(state_ttl_ms / 1000))

-- 成功分支（尾部）：
redis.call('SETEX', data_key, state_ttl_sec, string.format('%d %d', slot, tokens))
return {1, slot, now}