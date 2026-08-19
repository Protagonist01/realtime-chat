"""Shared Redis client.

We keep one ``redis.asyncio.Redis`` instance for the whole process. It is used
for two things that must share the same connection pool:

1. **Publishing** — when a client sends a chat message, the WebSocket handler
   persists it and calls ``bridge.publish(room_id, event)`` which does
   ``redis.publish("chat:room:{room_id}", payload)``.

2. **Subscribing** — the :class:`PubSubBridge` opens a PubSub object on this
   same client and listens for messages published by *any* worker.

The important detail: the publisher does NOT broadcast to its own local
sockets directly. It publishes to Redis, and its own subscriber picks the
event back up and broadcasts locally. That gives a single delivery code path
— every client, including the sender's other tabs, gets the message through
the same fan-out logic.
"""

from redis.asyncio import Redis

from app.config import settings

redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
