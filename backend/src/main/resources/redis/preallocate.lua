local previous = redis.call('HGET', KEYS[1], 'r:' .. ARGV[1])
if previous then
    local request = cjson.decode(previous)
    if request.activityId ~= ARGV[2] or request.userId ~= ARGV[3] or request.quantity ~= tonumber(ARGV[4]) then
        return 'IDEMPOTENCY_CONFLICT'
    end
    return previous
end
if redis.call('HGET', KEYS[1], 'ready') ~= '1' then return 'INITIALIZE' end
local rules = redis.call('HGET', KEYS[1], 'a:' .. ARGV[2])
if not rules then return 'ACTIVITY_MISSING' end
rules = cjson.decode(rules)
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
if now < rules.startsAtMs or now >= rules.endsAtMs then return 'ACTIVITY_CLOSED' end
local quantity = tonumber(ARGV[4])
if quantity > rules.limitPerUser then return 'LIMIT_EXCEEDED' end
local quotaKey = 'q:' .. ARGV[2] .. ':' .. ARGV[3]
local used = tonumber(redis.call('HGET', KEYS[1], quotaKey) or '0')
local stockKey = 'stock:' .. ARGV[2]
local stock = tonumber(redis.call('HGET', KEYS[1], stockKey))
local request = {
    requestId = ARGV[1], activityId = ARGV[2], userId = ARGV[3], quantity = quantity,
    state = 'QUEUED', reason = cjson.null, orderId = cjson.null, expiresAtMs = cjson.null,
    queueDeadlineMs = math.min(rules.endsAtMs, now + rules.queueTimeoutSeconds * 1000),
    createdAtMs = now, updatedAtMs = now, funded = false, persisted = false
}
if used + quantity > rules.limitPerUser then
    request.state = 'REJECTED'
    request.reason = 'LIMIT_EXCEEDED'
elseif stock < quantity then
    request.state = 'REJECTED'
    request.reason = 'SOLD_OUT'
else
    redis.call('HINCRBY', KEYS[1], stockKey, -quantity)
    redis.call('HINCRBY', KEYS[1], quotaKey, quantity)
    request.funded = true
end
local result = cjson.encode(request)
redis.call('HSET', KEYS[1], 'r:' .. ARGV[1], result)
redis.call('ZADD', KEYS[2], now, ARGV[1])
local historyKey = 'history:' .. ARGV[3]
local history = cjson.decode(redis.call('HGET', KEYS[1], historyKey) or '[]')
table.insert(history, 1, ARGV[1])
if #history > 30 then table.remove(history) end
redis.call('HSET', KEYS[1], historyKey, cjson.encode(history))
return result
