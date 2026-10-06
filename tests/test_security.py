import asyncio
from types import SimpleNamespace

import fakeredis.aioredis
import jwt
import pytest
from pydantic import ValidationError

from app.config import Settings, settings
from app.presence import PresenceTracker
from app.schemas import UserCreate
from app.security import create_access_token, decode_access_token, hash_password, verify_password
from tests.conftest import register


def test_passwords_are_not_silently_truncated():
    with pytest.raises(ValueError):
        hash_password("a" * 73)
    with pytest.raises(ValidationError):
        UserCreate(username="alice", email="alice@example.com", password="é" * 37)
    hashed = hash_password("correct")
    assert verify_password("correct", hashed)
    assert not verify_password("wrong", hashed)
    assert not verify_password("a" * 73, hashed)


@pytest.mark.parametrize(
    "claims",
    [
        {"sub": "1"},
        {"exp": 1, "sub": "1"},
        {"exp": 9999999999, "sub": "nope"},
        {"exp": 9999999999, "sub": "0"},
        {"exp": 9999999999, "sub": "-1"},
        {"exp": 9999999999, "sub": []},
    ],
)
def test_bad_jwt_claims(claims):
    token = jwt.encode(claims, settings.jwt_secret, algorithm="HS256")
    assert decode_access_token(token) is None


def test_valid_jwt_and_wrong_signature():
    assert decode_access_token(create_access_token(42)).user_id == 42
    token = jwt.encode(
        {"sub": "1", "exp": 9999999999}, "other-secret-with-32-characters!", algorithm="HS256"
    )
    assert decode_access_token(token) is None


def test_production_configuration_fails_closed():
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None, environment="production", auto_create_db=False, jwt_secret="dev-secret"
        )
    with pytest.raises(ValidationError):
        Settings(_env_file=None, environment="production", jwt_secret="x" * 32, auto_create_db=True)
    assert Settings(
        _env_file=None,
        environment="production",
        jwt_secret="x" * 32,
        auto_create_db=False,
        database_url="postgres://user:pass@host/db",
    ).database_url.startswith("postgresql+asyncpg://")


def test_private_room_cannot_be_joined_by_guessing_id(client, alice):
    bob = register(client, "bob")
    room = client.post(
        "/api/rooms", headers=alice, json={"name": "secret", "is_private": True}
    ).json()
    room_id = room["id"]
    assert room["is_member"]
    assert client.post(f"/api/rooms/{room_id}/join", headers=bob).status_code == 403
    assert client.get(f"/api/rooms/{room_id}", headers=bob).status_code == 403
    assert client.get(f"/api/rooms/{room_id}/messages", headers=bob).status_code == 403
    assert client.get("/api/rooms", headers=bob).json() == []


def test_registration_validation_conflicts_and_email_login(client):
    alice = register(client, "alice")
    assert client.get("/api/auth/me", headers=alice).json()["username"] == "alice"
    assert (
        client.post(
            "/api/auth/login", data={"username": "alice@example.com", "password": "hunter2hunter2"}
        ).status_code
        == 200
    )
    assert (
        client.post(
            "/api/auth/register",
            json={"username": "alice", "email": "other@example.com", "password": "password"},
        ).status_code
        == 409
    )
    for username in ("   ", "bob@example.com"):
        assert (
            client.post(
                "/api/auth/register",
                json={"username": username, "email": "other@example.com", "password": "password"},
            ).status_code
            == 422
        )
    assert (
        client.post(
            "/api/auth/register",
            json={"username": "longpass", "email": "long@example.com", "password": "a" * 73},
        ).status_code
        == 422
    )
    assert client.get("/api/rooms").status_code == 401


def test_auth_rate_limit_is_enforced(client):
    for _ in range(20):
        assert (
            client.post(
                "/api/auth/login", data={"username": "missing", "password": "wrong"}
            ).status_code
            == 401
        )
    response = client.post("/api/auth/login", data={"username": "missing", "password": "wrong"})
    assert response.status_code == 429
    assert int(response.headers["Retry-After"]) > 0


async def test_presence_tracks_every_tab_and_expires_crashes(monkeypatch):
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    a, b = PresenceTracker(redis), PresenceTracker(redis)
    now = [1000.0]
    monkeypatch.setattr("app.presence.time", SimpleNamespace(time=lambda: now[0]))
    try:
        await asyncio.gather(*(a.user_connected(1, 1, "alice", f"tab-{i}") for i in range(10)))
        # A new worker sees other workers' records; startup never wipes them.
        assert await b.get_online(1) == {"1": "alice"}
        for i in range(9):
            assert await b.user_disconnected(1, f"tab-{i}") == {"1": "alice"}
        now[0] += 61
        assert await a.get_online(1) == {}
        await a.user_connected(1, 2, "bob", "bob")
        # Duplicate cleanup cannot remove another connection or create negatives.
        assert await a.user_disconnected(1, "tab-9") == {"2": "bob"}
        assert await a.user_disconnected(1, "bob") == {}
    finally:
        await redis.aclose()
