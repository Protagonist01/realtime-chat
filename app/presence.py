"""Cross-process presence tracking via Redis hashes.

Why this needs Redis
--------------------
The ConnectionManager knows which users have local WebSocket connections in
*this* process. But "who's online in room 42?" is a cross-process question:
user 1 might be connected on worker A, user 2 on worker B. We need a single
source of truth.

Why a counter, not a set
------------------------
A user can hold multiple sockets for the same room (multiple tabs). If we
used a Redis SET and did SADD on connect / SREM on disconnect, closing one
tab would remove the user from the set even though their other tab is still
open. So we use HINCRBY on a per-room hash keyed by user_id: +1 on connect,
-1 on disconnect. When the count reaches 0, the field is deleted. A parallel
hash stores user_id → username so we can render the online list without a
DB hit.

Two Redis keys per room
-----------------------
- ``presence:count:room:{room_id}`` — hash: {user_id(str): count(int)}
- ``presence:names:room:{room_id}`` — hash: {user_id(str): username(str)}

HGETALL on the names hash returns only users still online (fields are
deleted from both hashes when the count hits 0).
"""

from redis.asyncio import Redis

COUNT_PREFIX = "presence:count:room:"
NAMES_PREFIX = "presence:names:room:"


class PresenceTracker:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def user_connected(self, room_id: int, user_id: int, username: str) -> dict[str, str]:
        """Increment the user's connection count, store their name, return the full online list."""
        count_key = f"{COUNT_PREFIX}{room_id}"
        names_key = f"{NAMES_PREFIX}{room_id}"
        await self._redis.hincrby(count_key, str(user_id), 1)
        await self._redis.hset(names_key, str(user_id), username)
        raw = await self._redis.hgetall(names_key)
        return {k: v for k, v in raw.items()}

    async def user_disconnected(self, room_id: int, user_id: int) -> dict[str, str]:
        """Decrement the user's connection count. If 0, remove them. Return the updated online list."""
        count_key = f"{COUNT_PREFIX}{room_id}"
        names_key = f"{NAMES_PREFIX}{room_id}"
        new_count = await self._redis.hincrby(count_key, str(user_id), -1)
        if new_count <= 0:
            await self._redis.hdel(count_key, str(user_id))
            await self._redis.hdel(names_key, str(user_id))
        raw = await self._redis.hgetall(names_key)
        return {k: v for k, v in raw.items()}

    async def get_online(self, room_id: int) -> dict[str, str]:
        raw = await self._redis.hgetall(f"{NAMES_PREFIX}{room_id}")
        return {k: v for k, v in raw.items()}
