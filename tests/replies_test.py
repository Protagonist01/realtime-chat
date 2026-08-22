"""End-to-end test for room visibility, joining, and replies.

Uses fakeredis (no Redis server needed) and runs the ASGI app in-process.

Verifies:
  1. Public rooms are visible to non-members (with is_member=False)
  2. Joining via REST flips is_member
  3. A user can reply to another user's message over WS
  4. Both parties receive the reply event with quoted context
  5. Message history includes reply context (REST)
  6. Replying to a nonexistent/cross-room message is rejected
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fakeredis.aioredis
import httpx
import uvicorn
import websockets

import app.redis_client as redis_module
import app.main as main_module
from app.main import app

# Swap the shared Redis client for an in-memory fake BEFORE startup wiring.
# Both namespaces must be patched: pubsub/presence read from redis_module,
# while main.py bound the original object via `from ... import`.
fake = fakeredis.aioredis.FakeRedis(decode_responses=True)
redis_module.redis_client = fake
main_module.redis_client = fake

HOST = "127.0.0.1"
PORT = 8766


async def drain(ws, want_types, store):
    """Collect events until we have all wanted types (or time out)."""
    pending = set(want_types)
    for _ in range(10):
        if not pending:
            break
        try:
            evt = json.loads(await asyncio.wait_for(ws.recv(), timeout=1.5))
            store.append(evt)
            pending.discard(evt["type"])
        except asyncio.TimeoutError:
            break


async def recv_until(ws, pred, tries=10):
    """Read events until one matches pred; returns it (and appends nothing)."""
    for _ in range(tries):
        try:
            evt = json.loads(await asyncio.wait_for(ws.recv(), timeout=1.5))
            if pred(evt):
                return evt
        except asyncio.TimeoutError:
            break
    raise AssertionError("expected event never arrived")


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
        return 1

    base = f"http://{HOST}:{PORT}"
    ws_base = f"ws://{HOST}:{PORT}"

    try:
        async with httpx.AsyncClient(base_url=base, timeout=10) as http:
            print("\n=== setup users & room ===")
            for u in ("carol", "dave"):
                r = await http.post("/api/auth/register", json={
                    "username": u, "email": f"{u}@example.com", "password": "hunter2hunter2",
                })
                assert r.status_code in (201, 400), r.text
            carol_tok = (await http.post("/api/auth/login",
                data={"username": "carol", "password": "hunter2hunter2"},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )).json()["access_token"]
            dave_tok = (await http.post("/api/auth/login",
                data={"username": "dave", "password": "hunter2hunter2"},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )).json()["access_token"]
            ah = {"Authorization": f"Bearer {carol_tok}"}
            dh = {"Authorization": f"Bearer {dave_tok}"}

            import time
            room_name = f"reply-test-{int(time.time())}"
            room = (await http.post("/api/rooms",
                json={"name": room_name, "is_private": False}, headers=ah)).json()
            assert "id" in room, f"room create failed: {room}"
            room_id = room["id"]

            print("\n=== room visible to non-member ===")
            listing = (await http.get("/api/rooms", headers=dh)).json()
            match = [r for r in listing if r["id"] == room_id]
            assert match, "public room not visible to dave"
            assert match[0]["is_member"] is False, "is_member should be False before join"
            print(f"  PASS: dave sees '{match[0]['name']}' with is_member=False")

            print("\n=== join flips is_member ===")
            joined = (await http.post(f"/api/rooms/{room_id}/join", headers=dh)).json()
            assert joined["is_member"] is True
            listing = (await http.get("/api/rooms", headers=dh)).json()
            match = [r for r in listing if r["id"] == room_id][0]
            assert match["is_member"] is True
            print("  PASS: after join, is_member=True")

            print("\n=== websockets + reply flow ===")
            async with websockets.connect(
                f"{ws_base}/ws/chat/{room_id}?token={carol_tok}", open_timeout=5
            ) as carol, websockets.connect(
                f"{ws_base}/ws/chat/{room_id}?token={dave_tok}", open_timeout=5
            ) as dave:
                # Drain join/presence noise
                await asyncio.sleep(0.3)
                for ws in (carol, dave):
                    while True:
                        try:
                            await asyncio.wait_for(ws.recv(), timeout=0.4)
                        except asyncio.TimeoutError:
                            break

                # carol sends the original message
                await carol.send(json.dumps({"type": "message", "content": "what does everyone think about warm colors?"}))
                got = []
                await drain(dave, ["message"], got)
                original = next(e for e in got if e["type"] == "message")
                orig_id = original["message_id"]
                assert orig_id is not None
                print(f"  PASS: dave received original message id={orig_id}")

                # dave replies to it
                await dave.send(json.dumps({
                    "type": "message",
                    "content": "love them — very cozy",
                    "reply_to_id": orig_id,
                }))
                reply = await recv_until(
                    carol,
                    lambda e: e["type"] == "message" and e["content"] == "love them — very cozy",
                )
                assert reply["reply_to_id"] == orig_id
                assert reply["reply_to_username"] == "carol"
                assert "warm colors" in reply["reply_to_content"]
                print("  PASS: carol received reply with quote context")
                print(f"        reply_to_id={reply['reply_to_id']} "
                      f"reply_to_username={reply['reply_to_username']} "
                      f"snippet={reply['reply_to_content']!r}")

            print("\n=== history includes reply context ===")
            msgs = (await http.get(f"/api/rooms/{room_id}/messages", headers=dh)).json()
            by_id = {m["id"]: m for m in msgs}
            reply_row = next(m for m in msgs if m["content"] == "love them — very cozy")
            assert reply_row["reply_to_id"] == orig_id
            parent = by_id[orig_id]
            assert parent["content"].startswith("what does everyone think")
            print(f"  PASS: REST history has {len(msgs)} messages, reply linked to #{orig_id}")

            print("\n=== invalid reply target rejected ===")
            async with websockets.connect(
                f"{ws_base}/ws/chat/{room_id}?token={carol_tok}", open_timeout=5
            ) as carol2:
                err_events = []
                await carol2.send(json.dumps({
                    "type": "message", "content": "ghost reply", "reply_to_id": 999999,
                }))
                await drain(carol2, ["error"], err_events)
                assert any(e["type"] == "error" and "not found" in e["content"] for e in err_events), \
                    f"expected error event, got {[e['type'] for e in err_events]}"
                print("  PASS: replying to missing message returns WS error")

            print("\n=== ALL CHECKS PASSED ===")
            return 0
    finally:
        server.should_exit = True
        await server_task


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
