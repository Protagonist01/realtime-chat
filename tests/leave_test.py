"""Exit-room feature test — runs the ASGI app via an in-loop uvicorn Server
with an in-memory fakeredis (no real Redis needed).

Verifies the exit-room sequence the frontend performs:
  1. Register alice + bob, create a room, bob joins.
  2. Both open WebSockets — both appear in presence.
  3. Bob "exits": close his WS (fires leave + presence events to alice),
     then call POST /api/rooms/{id}/leave (removes membership).
  4. Alice receives a leave event and a presence event with bob gone.
  5. Bob's membership is gone: list_rooms shows is_member=false, and
     reopening his WebSocket is rejected with close code 4403.
  6. Bob can re-join (POST /join) and reopen the WS — proving exit was
     a clean membership revocation, not a ban.
"""

import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fakeredis
import httpx
import uvicorn
import websockets

import app.main as main_module
from app.main import app

HOST = "127.0.0.1"
PORT = 8766


async def recv_events(ws, count, timeout=1.0):
    events = []
    for _ in range(count):
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            events.append(json.loads(raw))
        except asyncio.TimeoutError:
            break
    return events


async def main() -> int:
    # Swap the shared Redis client for an in-memory fake BEFORE startup runs.
    main_module.redis_client = fakeredis.FakeAsyncRedis(decode_responses=True)

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
                    print(f"  registered {u}")
                elif r.status_code == 400 and "already" in r.text:
                    print(f"  {u} already exists")
                else:
                    print(f"  ERR: {r.status_code} {r.text}")
                    return 1

            async def tok(u):
                return (await http.post("/api/auth/login",
                    data={"username": u, "password": "hunter2hunter2"},
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )).json()["access_token"]

            alice_tok = await tok("alice")
            bob_tok = await tok("bob")
            auth_a = {"Authorization": f"Bearer {alice_tok}"}
            auth_b = {"Authorization": f"Bearer {bob_tok}"}

            print("\n=== create room + bob joins ===")
            room_name = f"leave-room-{int(time.time() * 1000) % 10_000_000}"
            room = (await http.post("/api/rooms", json={"name": room_name, "is_private": False}, headers=auth_a)).json()
            room_id = room["id"]
            await http.post(f"/api/rooms/{room_id}/join", headers=auth_b)
            print(f"  room id={room_id} ({room_name})")

            print("\n=== both open websockets ===")
            alice_url = f"{ws_base}/ws/chat/{room_id}?token={alice_tok}"
            bob_url = f"{ws_base}/ws/chat/{room_id}?token={bob_tok}"

            async with websockets.connect(alice_url, open_timeout=5) as alice, \
                       websockets.connect(bob_url, open_timeout=5) as bob:
                await asyncio.sleep(0.3)

                alice_events = await recv_events(alice, 6)
                print("  alice initial:", [e["type"] for e in alice_events])
                presence = [e for e in alice_events if e["type"] == "presence"]
                assert presence, "alice never got presence"
                assert "bob" in presence[-1]["online_users"].values(), "bob not online"
                print("  PASS: bob online in presence")

                print("\n=== bob exits: close WS then leave REST ===")
                # 1. Close the WebSocket (frontend does this first).
                await bob.close()
                # Drain: alice should get a 'leave' + 'presence' (bob gone).
                exit_events = await recv_events(alice, 4, timeout=2.0)
                types = [e["type"] for e in exit_events]
                print("  alice after bob exit:", types)
                assert "leave" in types, "alice never got bob's leave event"
                final_presence = [e for e in exit_events if e["type"] == "presence"][-1:]
                assert final_presence, "alice never got updated presence after bob left"
                assert "bob" not in final_presence[0]["online_users"].values(), "bob still online after exit"
                print("  PASS: alice saw bob leave + presence updated")

                # 2. Remove membership via REST (returns 204).
                leave_r = await http.post(f"/api/rooms/{room_id}/leave", headers=auth_b)
                print(f"  leave status={leave_r.status_code}")
                assert leave_r.status_code == 204, f"expected 204, got {leave_r.status_code}"

                print("\n=== verify membership revoked ===")
                rooms = (await http.get("/api/rooms", headers=auth_b)).json()
                left = [r for r in rooms if r["id"] == room_id][0]
                assert left["is_member"] is False, "bob still shows as member"
                print("  PASS: is_member=False after leave")

                print("\n=== bob cannot reopen WS (HTTP 403) ===")
                try:
                    async with websockets.connect(bob_url, open_timeout=5) as bob2:
                        await asyncio.sleep(0.2)
                        print(f"  ERR: bob2 unexpectedly opened, code={bob2.close_code}")
                        return 1
                except websockets.exceptions.InvalidStatus as exc:
                    code = exc.response.status_code
                    print(f"  rejected with HTTP {code}")
                    assert code == 403, f"expected 403, got {code}"
                print("  PASS: WS rejected for non-member (HTTP 403)")

                print("\n=== bob can re-join (not a ban) ===")
                rejoin = await http.post(f"/api/rooms/{room_id}/join", headers=auth_b)
                print(f"  rejoin status={rejoin.status_code}")
                assert rejoin.status_code == 200 and rejoin.json()["is_member"] is True
                async with websockets.connect(bob_url, open_timeout=5) as bob3:
                    await asyncio.sleep(0.2)
                    assert bob3.close_code is None, "bob3 closed unexpectedly"
                    print("  PASS: bob rejoined + WS reopened")

            print("\n=== ALL CHECKS PASSED ===")
            return 0
    finally:
        server.should_exit = True
        await server_task


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
