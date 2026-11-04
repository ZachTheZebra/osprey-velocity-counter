UserId: Entity[str] = EntityJson(
  type='user',
  path='$.user_id',
  coerce_type=True
)

Ip: Entity[str] = EntityJson(
  type='ip',
  path='$.ip',
  coerce_type=True
)

UserMessages = VelocityCount(
  entity=UserId,
  behavior='message_sent',
  window=10
)

IpRequests = VelocityCount(
  entity=Ip,
  behavior='request',
  window=10
)
