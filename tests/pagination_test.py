"""Pagination cursor test — verifies the `before` parameter works.

Creates a room, sends 15 messages via WebSocket, then paginates:
  1. GET messages (default limit=50) → all 15
  2. GET messages?limit=5 → newest 5
  3. GET messages?limit=5&before=<oldest_id_from_step_2> → next 5
  4. Verify no overlap between pages
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
PORT = 8766


async def main() -> int:
    config = uvicorn.Config(app, host=HOST, port=PORT, log_level="warning", lifespan="on")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    for _ in range(50):
        await asyncio.sleep(0.1)
        if server.started:
            break

    base = f"http://{HOST}:{PORT}"
    ws_base = f"ws://{HOST}:{PORT}"

    try:
        async with httpx.AsyncClient(base_url=base, timeout=10) as http:

            # Register + login
            await http.post("/api/auth/register", json={
                "username": "pager", "email": "pager@example.com", "password": "hunter2hunter2",
            })
            tok = (await http.post("/api/auth/login",
                data={"username": "pager", "password": "hunter2hunter2"},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )).json()["access_token"]
            headers = {"Authorization": f"Bearer {tok}"}

            # Create room
            room = (await http.post("/api/rooms",
                json={"name": "pagination-room", "is_private": False},
                headers=headers)).json()
            room_id = room["id"]

            # Send 15 messages via WebSocket
            ws_url = f"{ws_base}/ws/chat/{room_id}?token={tok}"
            async with websockets.connect(ws_url, open_timeout=5) as ws:
                await asyncio.sleep(0.3)
                # Drain initial events
                for _ in range(10):
                    try:
                        await asyncio.wait_for(ws.recv(), timeout=0.3)
                    except asyncio.TimeoutError:
                        break

                for i in range(15):
                    await ws.send(json.dumps({"type": "message", "content": f"msg-{i:02d}"}))
                    await asyncio.sleep(0.05)

                # Drain echo + broadcast
                for _ in range(30):
                    try:
                        await asyncio.wait_for(ws.recv(), timeout=0.5)
                    except asyncio.TimeoutError:
                        break

            # Step 1: all 15
            all_msgs = (await http.get(f"/api/rooms/{room_id}/messages", headers=headers)).json()
            assert len(all_msgs) == 15, f"expected 15, got {len(all_msgs)}"
            print(f"  Step 1: {len(all_msgs)} messages total")

            # Step 2: limit=5 → newest 5
            page1 = (await http.get(f"/api/rooms/{room_id}/messages?limit=5", headers=headers)).json()
            assert len(page1) == 5, f"expected 5, got {len(page1)}"
            # API returns oldest-first; page1 should be msgs 10-14
            assert page1[0]["content"] == "msg-10", f"expected msg-10, got {page1[0]['content']}"
            assert page1[-1]["content"] == "msg-14", f"expected msg-14, got {page1[-1]['content']}"
            cursor = page1[0]["id"]
            print(f"  Step 2: page 1 = {[m['content'] for m in page1]}, cursor={cursor}")

            # Step 3: before=cursor → next 5
            page2 = (await http.get(
                f"/api/rooms/{room_id}/messages?limit=5&before={cursor}", headers=headers
            )).json()
            assert len(page2) == 5, f"expected 5, got {len(page2)}"
            assert page2[0]["content"] == "msg-05"
            assert page2[-1]["content"] == "msg-09"
            print(f"  Step 3: page 2 = {[m['content'] for m in page2]}")

            # Step 4: no overlap
            page1_ids = {m["id"] for m in page1}
            page2_ids = {m["id"] for m in page2}
            assert page1_ids.isdisjoint(page2_ids), "pages overlap!"
            print("  Step 4: no overlap between pages")

            # Step 5: one more page → msgs 0-4
            cursor2 = page2[0]["id"]
            page3 = (await http.get(
                f"/api/rooms/{room_id}/messages?limit=5&before={cursor2}", headers=headers
            )).json()
            assert len(page3) == 5
            assert page3[0]["content"] == "msg-00"
            assert page3[-1]["content"] == "msg-04"
            print(f"  Step 5: page 3 = {[m['content'] for m in page3]}")

            print("\n=== PAGINATION TEST PASSED ===")
            return 0
    finally:
        server.should_exit = True
        await server_task


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
