if redis.call('HGET', KEYS[1], 'ready') ~= '1' then return 0 end
local result = cjson.decode(ARGV[1])
local raw = redis.call('HGET', KEYS[1], 'r:' .. result.requestId)
local previous = raw and cjson.decode(raw)
-- 终态同步完成后忽略旧事件，防止乱序消息重新占用已释放库存。
if previous and previous.persisted and previous.state ~= 'QUEUED' and previous.state ~= 'RESERVED' then
    return 1
end
local wasFunded = previous and previous.funded or false
local funded = result.state == 'RESERVED' or result.state == 'CONFIRMED'
local change = 0
if wasFunded and not funded then change = result.quantity end
if not wasFunded and funded then change = -result.quantity end
if change ~= 0 then
    redis.call('HINCRBY', KEYS[1], 'stock:' .. result.activityId, change)
    redis.call('HINCRBY', KEYS[1], 'q:' .. result.activityId .. ':' .. result.userId, -change)
end
result.funded = funded
result.persisted = true
redis.call('HSET', KEYS[1], 'r:' .. result.requestId, cjson.encode(result))
if result.state ~= 'QUEUED' then redis.call('ZREM', KEYS[2], result.requestId) end
return 1
