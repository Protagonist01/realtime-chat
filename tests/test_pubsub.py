import fakeredis.aioredis

from app.connection_manager import ConnectionManager
from app.pubsub import PubSubBridge


async def test_shutdown_cancels_room_with_multiple_subscribers():
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    bridge = PubSubBridge(redis, ConnectionManager())
    try:
        await bridge.ensure_subscribed(1)
        await bridge.ensure_subscribed(1)
        task = bridge._tasks[1]
        await bridge.shutdown()
        assert task.done()
        assert not bridge._tasks
        assert not bridge._pubsubs
        assert not bridge._refcount
    finally:
        await bridge.shutdown()
        await redis.aclose()
