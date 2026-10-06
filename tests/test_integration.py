"""OS-process integration tests. Opt in with TEST_REDIS_URL and TEST_POSTGRES_URL."""

import asyncio
import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from uuid import uuid4

import httpx
import pytest
import websockets
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import settings
from tests.process_helpers import child_environment, terminate_tree


def unused_port():
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def process_env(tmp_path, monkeypatch):
    redis_url = os.environ.get("TEST_REDIS_URL")
    postgres_url = os.environ.get("TEST_POSTGRES_URL")
    if not redis_url or not postgres_url:
        pytest.skip("Set TEST_REDIS_URL and TEST_POSTGRES_URL for isolated integration services")
    name = "chat_test_" + uuid4().hex
    url = make_url(postgres_url).set(database=name).render_as_string(hide_password=False)

    async def database(create):
        engine = create_async_engine(postgres_url, isolation_level="AUTOCOMMIT")
        try:
            async with engine.connect() as connection:
                await connection.execute(
                    text(
                        f'CREATE DATABASE "{name}"'
                        if create
                        else f'DROP DATABASE "{name}" WITH (FORCE)'
                    )
                )
        finally:
            await engine.dispose()

    asyncio.run(database(True))
    env = child_environment(
        DATABASE_URL=url,
        REDIS_URL=redis_url,
        CELERY_BROKER_URL=redis_url,
        CELERY_RESULT_BACKEND=redis_url,
        AUTO_CREATE_DB="false",
        ENVIRONMENT="test",
        REDIS_NAMESPACE="test-" + uuid4().hex,
    )
    try:
        monkeypatch.setattr(settings, "database_url", url)
        config = Config("alembic.ini")
        command.upgrade(config, "head")
        command.check(config)
        yield env
    finally:
        asyncio.run(database(False))


@contextmanager
def process(command, env, log):
    with log.open("w", encoding="utf-8") as output:
        worker = subprocess.Popen(
            [sys.executable, "-m", *command], env=env, stdout=output, stderr=subprocess.STDOUT
        )
        try:
            yield worker
        finally:
            terminate_tree(worker)


def wait_ready(worker, port, log):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        assert worker.poll() is None, log.read_text(encoding="utf-8")
        try:
            if httpx.get(f"http://127.0.0.1:{port}/ready", timeout=1).status_code == 200:
                return
        except httpx.TransportError:
            pass
        time.sleep(0.1)
    raise AssertionError(log.read_text(encoding="utf-8"))


def register(base, name):
    response = httpx.post(
        base + "/api/auth/register",
        json={"username": name, "email": f"{name}@example.com", "password": "hunter2hunter2"},
    )
    assert response.status_code == 201, response.text
    token = httpx.post(
        base + "/api/auth/login", data={"username": name, "password": "hunter2hunter2"}
    ).json()["access_token"]
    return token, {"Authorization": f"Bearer {token}"}


async def event(ws, kind):
    for _ in range(30):
        import json

        payload = json.loads(await asyncio.wait_for(ws.recv(), timeout=8))
        if payload["type"] == kind:
            return payload
    raise AssertionError(f"No {kind} received")


@pytest.mark.integration
def test_delivery_presence_restart_and_revocation_across_processes(process_env, tmp_path):
    port_a, port_b = unused_port(), unused_port()
    log_a, log_b = tmp_path / "a.log", tmp_path / "b.log"
    base_a, base_b = f"http://127.0.0.1:{port_a}", f"http://127.0.0.1:{port_b}"
    with process(
        ["uvicorn", "app.main:app", "--port", str(port_a), "--log-level", "warning"],
        process_env,
        log_a,
    ) as a:
        wait_ready(a, port_a, log_a)
        token_a, headers_a = register(base_a, "alice")
        room_id = httpx.post(
            base_a + "/api/rooms", headers=headers_a, json={"name": "cross-process"}
        ).json()["id"]

        async def scenario():
            import json

            async with websockets.connect(
                f"ws://127.0.0.1:{port_a}/ws/chat/{room_id}?token={token_a}"
            ) as alice:
                assert len((await event(alice, "presence"))["online_users"]) == 1
                # Start B after Alice connects: startup must preserve A's lease.
                with process(
                    ["uvicorn", "app.main:app", "--port", str(port_b), "--log-level", "warning"],
                    process_env,
                    log_b,
                ) as b:
                    await asyncio.to_thread(wait_ready, b, port_b, log_b)
                    token_b, headers_b = await asyncio.to_thread(register, base_b, "bob")
                    joined = await asyncio.to_thread(
                        httpx.post, base_b + f"/api/rooms/{room_id}/join", headers=headers_b
                    )
                    assert joined.status_code == 200
                    async with websockets.connect(
                        f"ws://127.0.0.1:{port_b}/ws/chat/{room_id}"
                    ) as bob:
                        await bob.send(json.dumps({"type": "auth", "token": token_b}))
                        assert len((await event(bob, "presence"))["online_users"]) == 2
                        await alice.send(
                            json.dumps({"type": "message", "content": "cross-worker hello"})
                        )
                        original = await event(bob, "message")
                        assert original["content"] == "cross-worker hello"
                        assert (await event(alice, "message"))["message_id"] == original[
                            "message_id"
                        ]
                        await bob.send(
                            json.dumps(
                                {
                                    "type": "message",
                                    "content": "reply",
                                    "reply_to_id": original["message_id"],
                                }
                            )
                        )
                        assert (await event(alice, "message"))[
                            "reply_to_content"
                        ] == "cross-worker hello"
                        await bob.send(json.dumps({"type": "typing"}))
                        assert (await event(alice, "typing"))["username"] == "bob"
                        # Leave on A must close Bob's existing socket on B.
                        left = await asyncio.to_thread(
                            httpx.post, base_a + f"/api/rooms/{room_id}/leave", headers=headers_b
                        )
                        assert left.status_code == 204
                        with pytest.raises(websockets.exceptions.ConnectionClosed):
                            while True:
                                await asyncio.wait_for(bob.recv(), timeout=8)
                    # Alice may still have an earlier two-user snapshot queued.
                    for _ in range(5):
                        online = (await event(alice, "presence"))["online_users"]
                        if len(online) == 1:
                            break
                    assert list(online.values()) == ["alice"]

        asyncio.run(scenario())


@pytest.mark.integration
def test_real_celery_worker_reports_shared_postgres(process_env, tmp_path):
    port = unused_port()
    api_log, job_log = tmp_path / "api.log", tmp_path / "celery.log"
    base = f"http://127.0.0.1:{port}"
    with process(
        ["uvicorn", "app.main:app", "--port", str(port), "--log-level", "warning"],
        process_env,
        api_log,
    ) as api:
        wait_ready(api, port, api_log)
        token, headers = register(base, "reporter")
        room_id = httpx.post(base + "/api/rooms", headers=headers, json={"name": "reports"}).json()[
            "id"
        ]

        async def seed():
            import json

            async with websockets.connect(
                f"ws://127.0.0.1:{port}/ws/chat/{room_id}?token={token}"
            ) as ws:
                await event(ws, "presence")
                for content in ("one", "two"):
                    await ws.send(json.dumps({"type": "message", "content": content}))
                    await event(ws, "message")

        asyncio.run(seed())
        path = base + f"/api/rooms/{room_id}/statistics-jobs"
        queued = httpx.post(path, headers=headers)
        assert queued.status_code == 202, queued.text
        task_id = queued.json()["task_id"]
        # Queue before starting the worker: this proves the broker holds the job.
        with process(
            [
                "celery",
                "-A",
                "app.celery_app:celery_app",
                "worker",
                "--pool=solo",
                "--loglevel=WARNING",
            ],
            process_env,
            job_log,
        ) as worker:
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                assert worker.poll() is None, job_log.read_text(encoding="utf-8")
                fetched = httpx.get(path + "/" + task_id, headers=headers)
                assert fetched.status_code == 200, fetched.text
                result = fetched.json()
                if result["status"] in ("SUCCESS", "FAILURE"):
                    break
                time.sleep(0.2)
            assert result["status"] == "SUCCESS", job_log.read_text(encoding="utf-8")
            assert result["result"] == {
                "room_id": room_id,
                "message_count": 2,
                "contributor_count": 1,
                "latest_message_id": 2,
            }
