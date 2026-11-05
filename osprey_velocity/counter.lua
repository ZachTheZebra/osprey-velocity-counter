-- Caller passes the already-routed key. One key per call.
-- apply: ARGV event id, timestamp. ZADD NX so an existing member keeps its score.
-- count: ARGV now, window. Inside when now - window < ts <= now.
-- cleanup: ARGV now, retention. Remove ts <= now - retention.
local op = ARGV[1]
local key = KEYS[1]

if op == "apply" then
  return redis.call("ZADD", key, "NX", tonumber(ARGV[3]), ARGV[2])
elseif op == "count" then
  local now = tonumber(ARGV[2])
  local window = tonumber(ARGV[3])
  return redis.call("ZCOUNT", key, "(" .. tostring(now - window), now)
elseif op == "cleanup" then
  -- shortcut: an entity id that contains ":" is not this held entity, change the separator if that shows up
  local entity = string.match(key, "^([^:]+:[^:]+):")
  if entity and redis.call("EXISTS", "hold:" .. entity) == 1 then
    return 0
  end
  local now = tonumber(ARGV[2])
  local retention = tonumber(ARGV[3])
  return redis.call("ZREMRANGEBYSCORE", key, "-inf", now - retention)
else
  return redis.error_reply("unknown op")
end
