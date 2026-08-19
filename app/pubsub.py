"""Redis pub/sub bridge.

Why this exists
---------------
If you store WebSocket connections in a plain ``dict`` and run uvicorn with
``--workers 4``, a message sent from a client on worker 1 will never reach a
client on worker 3 — they have separate dicts. Redis pub/sub fixes that:

* every worker that has at least one local client in a room subscribes to the
  channel ``chat:room:{room_id}``;
* when a worker wants to deliver a message, it *publishes* to that channel
  instead of touching its own sockets directly;
* each worker's subscriber task picks the event up and calls
  ``manager.broadcast(room_id, event)`` locally.

So the publisher's own clients receive the message through the same path as
everyone else's — one delivery code path, no special-casing.

Subscribe / unsubscribe lifecycle
---------------------------------
We don't want one Redis subscriber per WebSocket (that would balloon). Instead
there is **one subscriber task per room per process**, refcounted by the
number of local sockets in that room:

* ``ensure_subscribed(room_id)`` is called on connect. If no task exists, it
  subscribes to the channel and spawns a listener task. It bumps the refcount
  either way.
* ``maybe_unsubscribe(room_id)`` is called on disconnect. It decrements the
  refcount; when it hits zero the listener task is cancelled and the PubSub
  object is closed.

The race we have to guard against: two clients joining the same room
concurrently in the same process both see ``no task yet`` and both subscribe.
We hold an ``asyncio.Lock`` per room_id around the check-and-subscribe.
"""

import asyncio
import json
import logging

from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from app.connection_manager import ConnectionManager
from app.schemas import ChatEvent

logger = logging.getLogger(__name__)

CHANNEL_PREFIX = "chat:room:"


def channel_for(room_id: int) -> str:
    return f"{CHANNEL_PREFIX}{room_id}"


class PubSubBridge:
    def __init__(self, redis: Redis, manager: ConnectionManager) -> None:
        self._redis = redis
        self._manager = manager
        self._locks: dict[int, asyncio.Lock] = {}
        self._tasks: dict[int, asyncio.Task] = {}
        self._pubsubs: dict[int, PubSub] = {}
        self._refcount: dict[int, int] = {}

    def _lock_for(self, room_id: int) -> asyncio.Lock:
        return self._locks.setdefault(room_id, asyncio.Lock())

    async def ensure_subscribed(self, room_id: int) -> None:
        async with self._lock_for(room_id):
            self._refcount[room_id] = self._refcount.get(room_id, 0) + 1
            if room_id in self._tasks:
                return  # already listening
            pubsub = self._redis.pubsub()
            await pubsub.subscribe(channel_for(room_id))
            self._pubsubs[room_id] = pubsub
            self._tasks[room_id] = asyncio.create_task(
                self._listener(room_id, pubsub), name=f"pubsub:{room_id}"
            )

    async def maybe_unsubscribe(self, room_id: int) -> None:
        async with self._lock_for(room_id):
            self._refcount[room_id] = self._refcount.get(room_id, 0) - 1
            if self._refcount[room_id] > 0:
                return
            # No more local clients — tear down.
            self._refcount.pop(room_id, None)
            task = self._tasks.pop(room_id, None)
            pubsub = self._pubsubs.pop(room_id, None)
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if pubsub is not None:
            await pubsub.unsubscribe(channel_for(room_id))
            await pubsub.aclose()

    async def publish(self, room_id: int, event: ChatEvent) -> None:
        await self._redis.publish(channel_for(room_id), event.model_dump_json())

    async def _listener(self, room_id: int, pubsub: PubSub) -> None:
        try:
            async for message in pubsub.listen():
                if message is None:
                    continue
                if message.get("type") != "message":
                    continue
                raw = message.get("data")
                if not raw:
                    continue
                try:
                    event = ChatEvent.model_validate_json(raw)
                except (json.JSONDecodeError, ValueError, TypeError) as exc:
                    logger.warning("dropping malformed pubsub payload: %r (%s)", raw, exc)
                    continue
                await self._manager.broadcast(room_id, event)
        except asyncio.CancelledError:
            # Normal teardown path from maybe_unsubscribe().
            raise
        except Exception:
            logger.exception("pubsub listener for room %s crashed", room_id)

    async def shutdown(self) -> None:
        for room_id in list(self._tasks.keys()):
            await self.maybe_unsubscribe(room_id)
