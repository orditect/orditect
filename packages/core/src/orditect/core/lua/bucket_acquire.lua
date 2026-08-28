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
    return {0, slot, now}
end

tokens = tokens - 1
local refill_to_full_ms = math.ceil((capacity - tokens) / refill_amount) * time_between_slots
local state_ttl_ms = math.max(0, slot - now) + refill_to_full_ms + 5000
local state_ttl_sec = math.max(1, math.ceil(state_ttl_ms / 1000))

redis.call('SETEX', data_key, state_ttl_sec, string.format('%d %d', slot, tokens))
return {1, slot, now}