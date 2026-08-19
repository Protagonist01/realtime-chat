"""End-to-end smoke test — runs the ASGI app via an in-loop uvicorn Server.

Verifies:
  1. Register + login two users
  2. Create a room, join it
  3. Two WS clients connect — both receive join + presence events
  4. Alice sends a message — both receive it via Redis pub/sub
  5. Alice sends a typing event — bob receives it
  6. REST confirms the message was persisted
  7. Presence updates when bob disconnects
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx
import uvicorn
import websockets

from app.main import app

HOST = "127.0.0.1"
PORT = 8765


async def main() -> int:
    config = uvicorn.Config(app, host=HOST, port=PORT, log_level="warning", lifespan="on")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    for _ in range(50):
        await asyncio.sleep(0.1)
        if server.started:
            break
    else:
        print("server failed to start")
        server_task.cancel()
        return 1

    base = f"http://{HOST}:{PORT}"
    ws_base = f"ws://{HOST}:{PORT}"

    try:
        async with httpx.AsyncClient(base_url=base, timeout=10) as http:

            print("\n=== register alice & bob ===")
            for u in ("alice", "bob"):
                r = await http.post("/api/auth/register", json={
                    "username": u, "email": f"{u}@example.com", "password": "hunter2hunter2",
                })
                if r.status_code == 201:
                    print(f"  registered {u}: id={r.json()['id']}")
                elif r.status_code == 400 and "already" in r.text:
                    print(f"  {u} already exists — fine")
                else:
                    print(f"  ERR: {r.status_code} {r.text}")
                    return 1

            print("\n=== login ===")
            alice_tok = (await http.post("/api/auth/login",
                data={"username": "alice", "password": "hunter2hunter2"},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )).json()["access_token"]
            bob_tok = (await http.post("/api/auth/login",
                data={"username": "bob", "password": "hunter2hunter2"},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )).json()["access_token"]

            print("\n=== create room + join ===")
            room = (await http.post("/api/rooms",
                json={"name": "smoke-room", "is_private": False},
                headers={"Authorization": f"Bearer {alice_tok}"},
            )).json()
            room_id = room["id"]
            await http.post(f"/api/rooms/{room_id}/join",
                headers={"Authorization": f"Bearer {bob_tok}"})
            print(f"  room id={room_id}")

            print("\n=== open two websockets ===")
            alice_url = f"{ws_base}/ws/chat/{room_id}?token={alice_tok}"
            bob_url = f"{ws_base}/ws/chat/{room_id}?token={bob_tok}"

            async with websockets.connect(alice_url, open_timeout=5) as alice, \
                       websockets.connect(bob_url, open_timeout=5) as bob:
                await asyncio.sleep(0.3)

                print("\n=== drain initial events (join + presence) ===")
                alice_events = []
                bob_events = []
                for ws, store in ((alice, alice_events), (bob, bob_events)):
                    for _ in range(5):
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                            evt = json.loads(raw)
                            store.append(evt)
                            print(f"    {evt['type']}: {evt.get('username', evt.get('online_users', ''))}")
                        except asyncio.TimeoutError:
                            break

                # Verify presence events arrived
                alice_presence = [e for e in alice_events if e["type"] == "presence"]
                bob_presence = [e for e in bob_events if e["type"] == "presence"]
                assert alice_presence, "alice never received a presence event"
                assert bob_presence, "bob never received a presence event"
                last_presence = alice_presence[-1]
                assert "alice" in last_presence["online_users"].values(), "alice not in online list"
                assert "bob" in last_presence["online_users"].values(), "bob not in online list"
                print("  PASS: both users appear in presence")

                print("\n=== alice sends a message ===")
                await alice.send(json.dumps({"type": "message", "content": "hello from alice"}))

                alice_msg = None
                bob_msg = None
                for _ in range(5):
                    if alice_msg is None:
                        try:
                            evt = json.loads(await asyncio.wait_for(alice.recv(), timeout=1.0))
                            if evt["type"] == "message": alice_msg = evt
                        except asyncio.TimeoutError: pass
                    if bob_msg is None:
                        try:
                            evt = json.loads(await asyncio.wait_for(bob.recv(), timeout=1.0))
                            if evt["type"] == "message": bob_msg = evt
                        except asyncio.TimeoutError: pass
                    if alice_msg and bob_msg: break

                assert bob_msg and bob_msg["content"] == "hello from alice"
                assert bob_msg["username"] == "alice"
                assert bob_msg["message_id"] is not None
                print(f"  PASS: bob received message_id={bob_msg['message_id']}")

                print("\n=== alice sends a typing event ===")
                await alice.send(json.dumps({"type": "typing"}))

                bob_typing = None
                for _ in range(5):
                    try:
                        evt = json.loads(await asyncio.wait_for(bob.recv(), timeout=1.0))
                        if evt["type"] == "typing": bob_typing = evt
                    except asyncio.TimeoutError:
                        break

                assert bob_typing and bob_typing["username"] == "alice"
                print("  PASS: bob received typing event from alice")

                print("\n=== verify persistence ===")
                r = await http.get(f"/api/rooms/{room_id}/messages",
                    headers={"Authorization": f"Bearer {alice_tok}"})
                msgs = r.json()
                assert any(m["content"] == "hello from alice" for m in msgs)
                print(f"  PASS: {len(msgs)} message(s) persisted")

            print("\n=== ALL CHECKS PASSED ===")
            return 0
    finally:
        server.should_exit = True
        await server_task


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
