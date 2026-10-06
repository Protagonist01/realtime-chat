"""Atomic, expiring presence leases for individual WebSocket connections."""

import json
import time

from redis.asyncio import Redis

from app.config import settings

LEASE_SECONDS = 60
HEARTBEAT_SECONDS = 20

# Prune, mutate and snapshot atomically. Hash tags keep keys in one cluster slot.
PRESENCE_SCRIPT = """
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
for _, id in ipairs(expired) do
    redis.call('ZREM', KEYS[1], id)
    redis.call('HDEL', KEYS[2], id)
end
if ARGV[2] == 'connect' then
    redis.call('ZADD', KEYS[1], ARGV[3], ARGV[4])
    redis.call('HSET', KEYS[2], ARGV[4], ARGV[5])
elseif ARGV[2] == 'disconnect' then
    redis.call('ZREM', KEYS[1], ARGV[4])
    redis.call('HDEL', KEYS[2], ARGV[4])
end
redis.call('EXPIRE', KEYS[1], ARGV[6])
redis.call('EXPIRE', KEYS[2], ARGV[6])
return redis.call('HVALS', KEYS[2])
"""


class PresenceTracker:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def _update(
        self,
        room_id: int,
        action: str,
        connection_id: str = "",
        user_id: int = 0,
        username: str = "",
    ) -> dict[str, str]:
        prefix = f"{settings.redis_namespace}:presence:{{{room_id}}}"
        now = time.time()
        values = await self._redis.eval(
            PRESENCE_SCRIPT,
            2,
            f"{prefix}:leases",
            f"{prefix}:names",
            now,
            action,
            now + LEASE_SECONDS,
            connection_id,
            json.dumps([str(user_id), username]),
            LEASE_SECONDS * 2,
        )
        return dict(json.loads(value) for value in values)

    async def user_connected(
        self, room_id: int, user_id: int, username: str, connection_id: str
    ) -> dict[str, str]:
        return await self._update(room_id, "connect", connection_id, user_id, username)

    async def user_disconnected(self, room_id: int, connection_id: str) -> dict[str, str]:
        return await self._update(room_id, "disconnect", connection_id)

    async def get_online(self, room_id: int) -> dict[str, str]:
        return await self._update(room_id, "read")
