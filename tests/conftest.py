"""Each test gets its own database, settings and Redis; never touch chat.db."""

import os
import tempfile
from pathlib import Path

# Set before importing the application; checked-in .env is never a test input.
_TEST_HOME = tempfile.TemporaryDirectory(prefix="realtime-chat-pytest-")
os.environ["ENVIRONMENT"] = "test"
os.environ["JWT_SECRET"] = "test-secret-with-at-least-32-characters"
os.environ["DATABASE_URL"] = (
    f"sqlite+aiosqlite:///{(Path(_TEST_HOME.name) / 'bootstrap.db').as_posix()}"
)
os.environ["AUTO_CREATE_DB"] = "true"
os.environ["REDIS_NAMESPACE"] = "realtime-chat-tests"

import fakeredis.aioredis
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import app.database as database
import app.main as main
import app.routers.chat as chat
from app.config import settings


@pytest.fixture
def client(tmp_path, monkeypatch):
    url = f"sqlite+aiosqlite:///{(tmp_path / 'test.db').as_posix()}"
    engine = create_async_engine(url)

    @event.listens_for(engine.sync_engine, "connect")
    def enable_foreign_keys(connection, _):
        cursor = connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    sessions = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(settings, "database_url", url)
    monkeypatch.setattr(database, "engine", engine)
    monkeypatch.setattr(database, "AsyncSessionLocal", sessions)
    monkeypatch.setattr(chat, "AsyncSessionLocal", sessions)
    monkeypatch.setattr(main, "engine", engine)
    monkeypatch.setattr(main, "redis_client", fakeredis.aioredis.FakeRedis(decode_responses=True))
    with TestClient(main.app) as test_client:
        yield test_client


@pytest.fixture
def alice(client):
    return register(client, "alice")


def register(client, name):
    response = client.post(
        "/api/auth/register",
        json={"username": name, "email": f"{name}@example.com", "password": "hunter2hunter2"},
    )
    assert response.status_code == 201, response.text
    response = client.post("/api/auth/login", data={"username": name, "password": "hunter2hunter2"})
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


def next_event(ws, kind):
    for _ in range(20):
        event = ws.receive_json()
        if event["type"] == kind:
            return event
    raise AssertionError(f"Did not receive {kind}")
