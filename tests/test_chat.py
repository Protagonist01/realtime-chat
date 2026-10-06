import pytest
from starlette.websockets import WebSocketDisconnect

from tests.conftest import next_event, register


def test_chat_replies_history_and_membership_revocation(client, alice):
    bob = register(client, "bob")
    room = client.post("/api/rooms", headers=alice, json={"name": "chat-room"}).json()
    room_id = room["id"]
    assert client.post(f"/api/rooms/{room_id}/join", headers=bob).status_code == 200
    with client.websocket_connect(f"/ws/chat/{room_id}") as a:
        a.send_json({"type": "auth", "token": alice["Authorization"].split()[1]})
        assert next_event(a, "presence")["online_users"]
        with client.websocket_connect(f"/ws/chat/{room_id}") as b:
            b.send_json({"type": "auth", "token": bob["Authorization"].split()[1]})
            assert len(next_event(b, "presence")["online_users"]) == 2
            a.send_json({"type": "message", "content": "hello"})
            original = next_event(b, "message")
            assert original["content"] == "hello"
            b.send_json(
                {"type": "message", "content": "reply", "reply_to_id": original["message_id"]}
            )
            reply = next_event(a, "message")  # own original message may still be queued
            if reply["content"] == "hello":
                reply = next_event(a, "message")
            assert reply["reply_to_content"] == "hello"
            b.send_json({"type": "message", "content": "bad reply", "reply_to_id": 999999})
            assert "not found" in next_event(b, "error")["content"]
            b.send_json({"type": "message", "content": " "})
            assert next_event(b, "error")["content"]
            b.send_json({"type": "typing"})
            assert next_event(a, "typing")["username"] == "bob"
            assert client.post(f"/api/rooms/{room_id}/leave", headers=bob).status_code == 204
            with pytest.raises(WebSocketDisconnect):
                while True:
                    b.receive_json()
            # TestClient does not simulate the peer's close acknowledgement.
            # Real sockets cover that path in test_integration.py.
            b.close()
            for _ in range(5):
                online = next_event(a, "presence")["online_users"]
                if len(online) == 1:
                    break
            assert list(online.values()) == ["alice"]
    history = client.get(f"/api/rooms/{room_id}/messages", headers=alice).json()
    assert [row["content"] for row in history] == ["hello", "reply"]
    assert history[1]["reply_to_username"] == "alice"
    assert history[0]["created_at"].endswith("Z")
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect(
            f"/ws/chat/{room_id}?token={bob['Authorization'].split()[1]}"
        ):
            pass
    assert client.post(f"/api/rooms/{room_id}/join", headers=bob).status_code == 200


def test_message_pagination_has_no_overlap(client, alice):
    room_id = client.post("/api/rooms", headers=alice, json={"name": "pages"}).json()["id"]
    token = alice["Authorization"].split()[1]
    with client.websocket_connect(f"/ws/chat/{room_id}?token={token}") as ws:
        next_event(ws, "presence")
        for i in range(12):
            ws.send_json({"type": "message", "content": f"message {i}"})
            next_event(ws, "message")
    url = f"/api/rooms/{room_id}/messages"
    newest = client.get(url + "?limit=5", headers=alice).json()
    older = client.get(url + f"?limit=5&before={newest[0]['id']}", headers=alice).json()
    assert [row["content"] for row in newest] == [f"message {i}" for i in range(7, 12)]
    assert [row["content"] for row in older] == [f"message {i}" for i in range(2, 7)]
    assert not {row["id"] for row in newest} & {row["id"] for row in older}
    assert client.get(url + "?limit=201", headers=alice).status_code == 422


def test_auth_first_frame_rejects_missing_credentials(client):
    with client.websocket_connect("/ws/chat/1") as ws:
        ws.send_json({"type": "message", "content": "unauthorized"})
        with pytest.raises(WebSocketDisconnect) as closed:
            ws.receive_json()
        assert closed.value.code == 4401


def test_health_static_and_readiness(client, monkeypatch):
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/ready").status_code == 200
    assert "realtime.chat" in client.get("/").text
    assert client.get("/").headers["X-Content-Type-Options"] == "nosniff"

    async def unavailable():
        raise ConnectionError("Redis offline")

    monkeypatch.setattr(client.app.state.redis_client, "ping", unavailable)
    assert client.get("/ready").status_code == 503
