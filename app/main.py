import logging

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.connection_manager import ConnectionManager
from app.database import init_db
from app.presence import PresenceTracker
from app.pubsub import PubSubBridge
from app.redis_client import redis_client
from app.routers import auth, chat, rooms

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = FastAPI(title="Real-time Chat", version="0.2.0")

app.include_router(auth.router)
app.include_router(rooms.router)
app.include_router(chat.router)


@app.on_event("startup")
async def on_startup() -> None:
    await init_db()
    # Clear stale presence from any previous server run. Redis persists
    # across app restarts; presence is ephemeral — any leftover entries
    # from unclean shutdowns (crash, kill -9) are garbage that would
    # show disconnected users as "online."
    stale = await redis_client.keys("presence:count:room:*")
    stale.extend(await redis_client.keys("presence:names:room:*"))
    if stale:
        await redis_client.delete(*stale)
    app.state.connection_manager = ConnectionManager()
    app.state.pubsub_bridge = PubSubBridge(redis_client, app.state.connection_manager)
    app.state.presence = PresenceTracker(redis_client)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    bridge: PubSubBridge = getattr(app.state, "pubsub_bridge", None)
    if bridge is not None:
        await bridge.shutdown()
    await redis_client.aclose()


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


# Static frontend — mounted after API routes so /api/* and /ws/* take precedence.
app.mount("/", StaticFiles(directory="static", html=True), name="static")
