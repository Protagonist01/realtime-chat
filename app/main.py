import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

from app.config import settings
from app.connection_manager import ConnectionManager
from app.database import engine, init_db
from app.presence import PresenceTracker
from app.pubsub import PubSubBridge
from app.redis_client import redis_client
from app.routers import auth, chat, reports, rooms

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.auto_create_db:
        await init_db()
    await redis_client.ping()
    app.state.connection_manager = ConnectionManager()
    app.state.redis_client = redis_client
    app.state.pubsub_bridge = PubSubBridge(redis_client, app.state.connection_manager)
    app.state.presence = PresenceTracker(redis_client)
    try:
        yield
    finally:
        await app.state.pubsub_bridge.shutdown()
        await redis_client.aclose()
        await engine.dispose()


app = FastAPI(title="Real-time Chat", version="0.3.0", lifespan=lifespan)
app.include_router(auth.router)
app.include_router(rooms.router)
app.include_router(chat.router)
app.include_router(reports.router)


@app.get("/health", tags=["operations"])
async def health() -> dict:
    """Liveness: the API process can respond."""
    return {"status": "ok"}


@app.get("/ready", tags=["operations"])
async def ready() -> dict:
    """Readiness: the database and live-chat Redis are reachable."""
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        await app.state.redis_client.ping()
    except Exception:
        logger.exception("Readiness check failed")
        raise HTTPException(status_code=503, detail="Service dependencies unavailable") from None
    return {"status": "ready"}


@app.middleware("http")
async def security_headers(request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    if request.url.path.startswith("/api/auth"):
        response.headers["Cache-Control"] = "no-store"
    return response


app.mount(
    "/",
    StaticFiles(directory=Path(__file__).resolve().parent.parent / "static", html=True),
    name="static",
)
