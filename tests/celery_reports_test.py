"""Real query and HTTP ownership checks using eager Celery (no external Redis)."""

from uuid import uuid4

from app.celery_app import celery_app
from tests.conftest import register


def test_room_statistics_job(client, alice):
    previous = {
        key: celery_app.conf[key]
        for key in ("task_always_eager", "task_store_eager_result", "result_backend")
    }
    celery_app.conf.update(
        task_always_eager=True, task_store_eager_result=True, result_backend="cache+memory://"
    )
    try:
        room_id = client.post("/api/rooms", headers=alice, json={"name": "reports"}).json()["id"]
        bob = register(client, "bob")
        assert client.post(f"/api/rooms/{room_id}/join", headers=bob).status_code == 200
        path = f"/api/rooms/{room_id}/statistics-jobs"
        created = client.post(path, headers=alice)
        assert created.status_code == 202, created.text
        task_id = created.json()["task_id"]
        fetched = client.get(f"{path}/{task_id}", headers=alice)
        assert fetched.status_code == 200, fetched.text
        body = fetched.json()
        assert body["status"] == "SUCCESS"
        assert body["result"]["message_count"] == 0
        assert body["result"]["room_id"] == room_id
        assert client.get(f"{path}/{'0' * 36}", headers=alice).status_code == 422
        assert client.get(f"{path}/{uuid4()}", headers=alice).status_code == 404
        assert client.get(f"{path}/{task_id}", headers=bob).status_code == 404
        for _ in range(4):
            assert client.post(path, headers=alice).status_code == 202
        assert client.post(path, headers=alice).status_code == 429
    finally:
        celery_app.conf.update(previous)


def test_queue_failure_returns_503_without_orphan_job(client, alice, monkeypatch):
    from app.tasks import room_statistics

    room_id = client.post("/api/rooms", headers=alice, json={"name": "failure"}).json()["id"]

    def unavailable(*args, **kwargs):
        raise ConnectionError("Broker unavailable")

    monkeypatch.setattr(room_statistics, "apply_async", unavailable)
    response = client.post(f"/api/rooms/{room_id}/statistics-jobs", headers=alice)
    assert response.status_code == 503
