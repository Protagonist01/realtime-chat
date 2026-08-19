"""Quick end-to-end test against a running Docker container on port 9000."""

import asyncio
import json
import sys

import httpx
import websockets

BASE = "http://127.0.0.1:9000"
WS_BASE = "ws://127.0.0.1:9000"


async def main() -> int:
    async with httpx.AsyncClient(base_url=BASE, timeout=10) as http:
        for u in ("alice", "bob"):
            await http.post("/api/auth/register", json={
                "username": u, "email": f"{u}@x.com", "password": "hunter2hunter2"
            })
        a = (await http.post("/api/auth/login",
            data={"username": "alice", "password": "hunter2hunter2"},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )).json()["access_token"]
        b = (await http.post("/api/auth/login",
            data={"username": "bob", "password": "hunter2hunter2"},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )).json()["access_token"]

        room = (await http.post("/api/rooms",
            json={"name": "docker-test", "is_private": False},
            headers={"Authorization": f"Bearer {a}"},
        )).json()
        rid = room["id"]
        await http.post(f"/api/rooms/{rid}/join", headers={"Authorization": f"Bearer {b}"})

        async with websockets.connect(f"{WS_BASE}/ws/chat/{rid}?token={a}") as alice, \
                   websockets.connect(f"{WS_BASE}/ws/chat/{rid}?token={b}") as bob:
            await asyncio.sleep(0.3)
            for ws in (alice, bob):
                for _ in range(10):
                    try:
                        await asyncio.wait_for(ws.recv(), timeout=0.3)
                    except asyncio.TimeoutError:
                        break

            await alice.send(json.dumps({"type": "message", "content": "hello from docker"}))
            bob_msg = None
            for _ in range(5):
                try:
                    evt = json.loads(await asyncio.wait_for(bob.recv(), timeout=1.0))
                    if evt["type"] == "message":
                        bob_msg = evt
                        break
                except asyncio.TimeoutError:
                    break

            assert bob_msg and bob_msg["content"] == "hello from docker"
            print("PASS: Docker container end-to-end test passed")
            print(f"  bob received: {bob_msg['content']} from {bob_msg['username']} (id={bob_msg['message_id']})")
            return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
