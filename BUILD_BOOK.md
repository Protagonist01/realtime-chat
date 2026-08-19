# Build Book — Real-time Chat
A journal of the real reasoning behind this build: decisions, options rejected, and things that broke along the way.

## Index
- [Entry 1 — Connection management across workers: why a dict isn't enough](#entry-1--connection-management-across-workers-why-a-dict-isnt-enough)
- [Entry 2 — Three bugs the smoke test caught: passlib, lazy loads, and background processes](#entry-2--three-bugs-the-smoke-test-caught-passlib-lazy-loads-and-background-processes)
- [Entry 3 — Presence tracking: why a Redis SET isn't enough for multi-tab](#entry-3--presence-tracking-why-a-redis-set-isnt-enough-for-multi-tab)
- [Entry 4 — Multi-worker test: proving pub/sub actually crosses processes](#entry-4--multi-worker-test-proving-pubsub-actually-crosses-processes)
- [Entry 5 — Deployment: choosing a free tier that actually supports WebSockets](#entry-5--deployment-choosing-a-free-tier-that-actually-supports-websockets)

## Entry 1 — Connection management across workers: why a dict isn't enough
**Files touched:** `app/connection_manager.py`, `app/pubsub.py`, `app/redis_client.py`, `app/routers/chat.py`, `app/main.py`

### Context
A chat server's defining problem is fan-out: one client sends a message, every other client in the room must receive it. The naive version — keep a `dict[room_id, set[WebSocket]]` and loop over it on send — works in exactly one process. The instant you run `uvicorn app.main:app --workers 4` (which is what production does), worker 1's dict has no idea that worker 3 is holding a socket for the same room. So a message sent on worker 1 is invisible to half the room. This had to be solved before any feature work, because every other design choice sits on top of how delivery works.

### Before you read on
You have N uvicorn workers, each holding some WebSocket connections for room 42. A client on worker 1 sends `"hi"`. Sketch the delivery path: where does the message get persisted, where does it get fanned out, and how does worker 3 learn about it? Think about what happens to worker 1's *own* clients — do they get the message through the same code path as worker 3's, or a different one? Decide that before reading on, because it changes the shape of the code.

### Options considered
- **In-process dict only, single worker** — simplest, a `dict[room_id, set[WebSocket]]` and a `for ws in sockets: await ws.send_text(...)`. Rejected because it caps you to one process and one core. A chat service that can't scale past one worker isn't a portfolio project, it's a toy.
- **In-process dict + Redis pub/sub, broadcast locally on publish** — publish to Redis AND iterate the local dict in the publish call. Rejected because it creates two delivery code paths: the local one (sender's own clients) and the Redis one (other workers' clients). Bugs hide in the divergence — e.g. ordering, or "I forgot to broadcast locally so the sender never sees their own message echoed."
- **In-process dict + Redis pub/sub, publish-only — the publisher's own clients receive via the subscriber too** — the sender never touches local sockets; it publishes to Redis, and its own subscriber picks the event back up and broadcasts locally. Chosen.

### Why
One delivery code path. The sender's clients, other workers' clients, and the sender's other tabs all receive the event through `manager.broadcast` called from the subscriber task. There is no "local fast path" to forget to update. The cost is one extra Redis round-trip before the sender sees their own message echoed — fine for a chat app (human-scale latency, not microservice RPC), and you'd pay it anyway for the cross-worker case. The uniformity is worth the round-trip.

The second non-obvious decision was **per-room channels** (`chat:room:{room_id}`) rather than one global channel. A global channel means every worker receives every message and discards the ones for rooms it has no clients in — O(total_messages × workers) wasted work. Per-room channels mean a worker only receives messages for rooms where it has at least one subscriber, because it only subscribes to those channels.

### How to build it

**The three pieces and their responsibilities:**

1. `ConnectionManager` — a plain in-memory registry: `{room_id: {user_id: set[WebSocket]}}`. It knows nothing about Redis. Its only job is `connect(room, user, ws)` (which calls `ws.accept()` and adds to the set), `disconnect(...)`, and `broadcast(room, event)` which iterates the local sockets for that room and sends. The inner value is a `set` because one user can hold several sockets (multiple tabs).

2. `redis_client.py` — one shared `redis.asyncio.Redis` instance (from `Redis.from_url`). Used both for `publish()` calls and for opening `pubsub()` objects. One client, one connection pool, both roles.

3. `PubSubBridge` — owns the per-room subscriber tasks. State:
   - `_tasks: dict[room_id, asyncio.Task]` — the listener coroutine per room.
   - `_pubsubs: dict[room_id, PubSub]` — the PubSub object each task reads from.
   - `_refcount: dict[room_id, int]` — how many local sockets are in that room.
   - `_locks: dict[room_id, asyncio.Lock]` — guards the check-and-subscribe.

**The connect flow, in order:**

```
chat_ws(room_id):
  1. token = websocket.query_params["token"]
  2. user = decode(token)  -> close(code=4401) if None
  3. room = db.get(Room, room_id)  -> close(4404) if None
  4. is_member(user, room)  -> close(4403) if not
  5. manager.connect(room_id, user.id, websocket)  # accepts the socket
  6. bridge.ensure_subscribed(room_id)
  7. bridge.publish(room_id, ChatEvent(type="join", ...))
  8. loop:
       raw = await websocket.receive_text()
       msg = MessageCreate.parse(raw)
       # persist in a fresh session:
       async with AsyncSessionLocal() as db:
           message = Message(room_id, user.id, content)
           db.add(message); await db.commit(); await db.refresh(message)
           event = ChatEvent(type="message", message_id=message.id, created_at=...)
       bridge.publish(room_id, event)   # NOT manager.broadcast — publish only
  9. finally:
       manager.disconnect(room_id, user.id, websocket)
       bridge.publish(room_id, ChatEvent(type="leave", ...))
       bridge.maybe_unsubscribe(room_id)
```

**The subscribe race and how the lock fixes it.** Two clients join room 42 in the same process at nearly the same time. Both call `ensure_subscribed(42)`. Without a lock, both check `if 42 in self._tasks` (False for both), both subscribe, both spawn a listener task — you now have two subscribers on the same channel in one process, and every message gets broadcast twice. Fix: an `asyncio.Lock` *per room_id*, held around the check-and-subscribe. The lock is created lazily via `_locks.setdefault(room_id, asyncio.Lock())` so we only allocate locks for rooms that actually have activity.

**The refcount + teardown.** `ensure_subscribed` bumps `_refcount[room_id]` before checking whether a task exists — every caller counts, whether or not it created the task. `maybe_unsubscribe` decrements; when it hits zero, it cancels the task, calls `pubsub.unsubscribe(channel)`, and `pubsub.aclose()`. Failing to close the PubSub object leaks a Redis connection per room per process — over a long-running server with rooms churning, this exhausts the pool. The `aclose()` is the line you'd miss.

**Auth on the WebSocket.** Browsers cannot set custom headers on `new WebSocket(url)` — try it and you'll find there's no API. So the JWT goes in the query string: `ws://host/ws/chat/42?token=...`. On the server, `websocket.query_params.get("token")`. If auth fails we `websocket.close(code=4401)` *before* calling `manager.connect` — Starlette allows sending a close frame before `websocket.accept`, and doing so avoids standing up a live connection for an unauthorized peer.

**How `manager.broadcast` avoids `RuntimeError: Set changed size during iteration`.** When a client disconnects mid-broadcast, the receive loop's `finally` calls `disconnect` which mutates the inner set. If `broadcast` is iterating that set at the same moment, Python raises. Fix: iterate over `list(sockets)` — a snapshot. Dead sockets caught by the `send_text` `except` are `discard`ed from the live set, but the iteration is over the snapshot so it doesn't trip.

**Wiring into FastAPI.** `ConnectionManager` and `PubSubBridge` are created in `on_startup` and stored on `app.state` so the WebSocket handler can reach them via `websocket.app.state.connection_manager` / `.pubsub_bridge`. They are not request-scoped — they are process-singletons. On shutdown, `bridge.shutdown()` walks every room and unsubscribes, then `redis_client.aclose()` closes the pool.

### What went wrong
Nothing yet — this entry is being written before the first run. The next entry will be either "it worked, here's the verification command and what 'correct' looked like" or, more likely, the bug(s) that surfaced when two actual WebSocket clients talked to each other. The likely suspects: `decode_responses=True` on the Redis client means `pubsub.listen()` yields `data` as a `str` (good for `model_validate_json`) — if I'd left it `False`, `data` would be `bytes` and `ChatEvent.model_validate_json(raw)` would raise `TypeError`, which is the kind of thing you only catch by running it.

## Entry 2 — Three bugs the smoke test caught: passlib, lazy loads, and background processes
**Files touched:** `app/security.py`, `app/routers/rooms.py`, `tests/smoke_test.py`, `requirements.txt`

### Context
Entry 1 was written before the first run. The first run — a smoke test that registers two users, opens two WebSockets in the same room, sends a message, and asserts both clients receive it — failed three times in three different ways before passing. Each failure was a real lesson worth recording; none were typos.

### Before you read on
You're writing a FastAPI app with SQLAlchemy 2.0 async + bcrypt password hashing on Windows. You write what looks like idiomatic code: `from passlib.context import CryptContext`, `room.members.append(user)`, and start the server with `Start-Process python -m uvicorn ...`. What goes wrong in each of those three, and why? Sketch the failure mode before reading on.

### Options considered
- **Bug 1 (passlib):** keep passlib and pin `bcrypt<4.1` to preserve the old API → rejected because it pins you to an old bcrypt and leaves a stale, unmaintained dep in the tree. **Chosen: drop passlib, call bcrypt directly** (4 lines: `hashpw` + `checkpw`).
- **Bug 2 (lazy loads):** use `selectinload(Room.members)` on every room query → rejected because it fetches full `User` rows when all we need is "is user X a member." **Chosen: query the `room_members` association table directly** with `select(room_members.c.user_id).where(...)`, and insert/delete membership rows explicitly.
- **Bug 3 (background process):** run uvicorn via `Start-Process` with redirected stdio → rejected because it kept dying silently, leaving the port closed. **Chosen: start `uvicorn.Server` inside the test's own event loop** — same Python process, same loop, no socket-binding race.

### Why
The thread connecting all three: **the obvious, idiomatic-looking code was wrong in 2026**. passlib's last release was 2020 and it doesn't understand bcrypt 5.x's API. SQLAlchemy async doesn't lazy-load relationships (it would need to block, which breaks the event loop). And Windows' `Start-Process` with redirected stdio doesn't keep the child alive reliably once the parent shell exits. Each is a "the tutorial lied to you by omission" failure — the code looks fine and compiles fine, and only fails at runtime in a way whose error message points at the wrong thing.

### How to build it

**Bug 1 — bcrypt without passlib.** passlib's `CryptContext(schemes=["bcrypt"]).hash(pw)` calls into `bcrypt.hashpw` internally, but through a probing shim that bcrypt 5.x removed. The failure surfaces as `ValueError: password cannot be longer than 72 bytes` — *even for a 14-byte password* — because passlib's "detect wrap bug" routine calls `bcrypt.hashpw` in a way the new API rejects. The error message is a red herring: it's not about your password length, it's about passlib's internal compatibility check failing.

The replacement is four lines:

```python
import bcrypt
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8")[:72], bcrypt.gensalt()).decode("utf-8")
def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8")[:72], hashed.encode("utf-8"))
    except ValueError:
        return False
```

The `[:72]` truncation is real — bcrypt has a 72-byte input limit and bcrypt 5.x raises rather than silently truncating. `requirements.txt` swaps `passlib[bcrypt]>=1.7.4` for `bcrypt>=4.1.0`.

**Bug 2 — async SQLAlchemy and lazy relationships.** Code like `room.members.append(user)` or `[u.id for u in room.members]` works in sync SQLAlchemy because accessing the attribute triggers a SQL query on the spot. In async, there's no greenlet to block on, so accessing a lazy relationship raises `MissingGreenlet` (or, if you've partially loaded the object, `AttributeError: 'Room' object has no attribute 'awaitable_attrs'` — which is what I hit after misremembering the API).

The fix has two parts:
1. **For reads** ("is this user a member?"), query the association table directly:
   ```python
   async def _is_member(db, user_id, room_id) -> bool:
       row = await db.execute(
           select(room_members).where(
               room_members.c.user_id == user_id,
               room_members.c.room_id == room_id,
           )
       )
       return row.first() is not None
   ```
2. **For writes** (join/leave), insert/delete rows on the association table explicitly instead of mutating `room.members`:
   ```python
   await db.execute(room_members.insert().values(user_id=user.id, room_id=room_id))
   # ... and for leave:
   await db.execute(room_members.delete().where(...))
   ```

The non-obvious part: `db.flush()` after `db.add(room)` is needed to populate `room.id` before inserting the membership row that references it. `db.commit()` alone wouldn't give you `room.id` back without a refresh.

**Bug 3 — running uvicorn inside the test.** The pattern that actually worked:

```python
config = uvicorn.Config(app, host="127.0.0.1", port=8765, log_level="warning", lifespan="on")
server = uvicorn.Server(config)
server_task = asyncio.create_task(server.serve())
# wait for startup:
for _ in range(50):
    await asyncio.sleep(0.1)
    if server.started:
        break
# ... run test assertions against http://127.0.0.1:8765 ...
# teardown:
server.should_exit = True
await server_task
```

Key details: `lifespan="on"` so FastAPI's `@app.on_event("startup")` fires (this creates tables and the ConnectionManager/PubSubBridge on `app.state`). `server.started` is the readiness flag — poll it, don't just `sleep(2)`. Port `8765` instead of `8000` to dodge lingering `TimeWait` sockets from earlier failed runs. Teardown is `server.should_exit = True; await server_task` — setting the flag without awaiting the task leaves a zombie.

**How I verified it worked.** `python tests/smoke_test.py` produces, in order:
- `registered alice: id=1` / `registered bob: id=2`
- `join status=200`
- both clients receive `type=join` events for themselves and the other user
- both clients receive `type=message` with `message_id=1`, `content="hello from alice"`, `username="alice"`
- `REST returned 1 message(s)` / `PASS: message persisted to DB`
- `=== ALL CHECKS PASSED ===`

The fact that bob received the message proves the Redis pub/sub round-trip works: alice's send published to `chat:room:1`, the subscriber task picked it up, and `manager.broadcast` delivered it to bob's socket. The fact that alice *also* received her own message through the same path proves the "single delivery code path" decision from Entry 1 is actually wired that way.

### What went wrong
All three bugs above. The diagnosis order for each:
1. **passlib:** saw `500 Internal Server Error` on `/api/auth/register`, read the server traceback, saw `ValueError: password cannot be longer than 72 bytes` for a 14-byte password, recognized passlib's wrap-bug-detection routine, swapped to direct bcrypt. ~15 minutes.
2. **lazy loads:** saw `AttributeError: 'Room' object has no attribute 'awaitable_attrs'` on `/api/rooms/{id}/join`. First chased the red herring of "maybe I need `selectinload(Room.members)` on the `db.get` call" before realizing `db.get` doesn't take loader options and the real fix was to not touch the relationship at all. ~20 minutes.
3. **background process:** saw `httpx.ConnectError: All connection attempts failed` despite the server log saying "Uvicorn running on http://127.0.0.1:8000". Checked `Get-NetTCPConnection -LocalPort 8000` — port was in `TimeWait`, no listener. Killed stale Python processes, restarted, same result. Concluded `Start-Process` with redirected stdio was unreliable on Windows for long-running child processes, switched to in-loop `uvicorn.Server`. ~25 minutes.

## Entry 3 — Presence tracking: why a Redis SET isn't enough for multi-tab
**Files touched:** `app/presence.py`, `app/routers/chat.py`, `app/schemas.py`

### Context
"Who's online in room 42?" is a cross-process question — user 1 might be connected on worker A, user 2 on worker B. The `ConnectionManager` only knows local connections, so presence needs a shared store in Redis. The non-obvious problem: users can hold multiple sockets for the same room (multiple tabs), and closing one tab must NOT mark them offline while another tab is still open.

### Before you read on
You need to track per-room online users in Redis, shared across N uvicorn workers. A user can have multiple tabs open (multiple WebSocket connections in the same room, potentially across different workers). How would you structure the Redis data so that a user is removed from the online list only when their *last* connection closes — not the first? Sketch the Redis operations on connect and disconnect before reading on.

### Options considered
- **Redis SET per room** (`SADD presence:room:{room_id} {user_id}` on connect, `SREM` on disconnect) → rejected because closing one tab would SREM the user even though their other tab is still connected. SETs don't refcount.
- **Redis SET + per-process local refcounting** (each process tracks "user X has 3 local connections" and only SREMs when the local count hits 0) → rejected because it breaks across processes: worker A might have 1 connection and worker B might have 2. Worker A's local count hits 0 and SREMs, but the user is still connected on worker B.
- **Redis hash with HINCRBY counter** (`HINCRBY presence:count:room:{room_id} {user_id} 1` on connect, `-1` on disconnect, delete field when 0) → handles multi-tab and multi-process correctly because the counter is global. **Chosen.**

### Why
The counter lives in Redis, not in process memory. Every connect does `+1` globally; every disconnect does `-1` globally. When the count reaches 0, the user is removed from a parallel "names" hash (`presence:names:room:{room_id}`) that maps `user_id → username`. `HGETALL` on the names hash returns only currently-online users. This is correct regardless of how many processes or tabs are involved — the counter is the single source of truth.

The two-hash split (count hash + names hash) exists because the counter value is an integer, not a username. We need both: the count to know when to remove, and the name to render the online list without a DB query.

### How to build it

**The Redis data model (two keys per room):**
- `presence:count:room:{room_id}` — hash: `{user_id(str): connection_count(int)}`
- `presence:names:room:{room_id}` — hash: `{user_id(str): username(str)}`

**On connect (`user_connected`):**
```python
await redis.hincrby(count_key, str(user_id), 1)     # +1 globally
await redis.hset(names_key, str(user_id), username)  # store name
raw = await redis.hgetall(names_key)                 # get full online list
return {k: v for k, v in raw.items()}               # {user_id_str: username}
```

**On disconnect (`user_disconnected`):**
```python
new_count = await redis.hincrby(count_key, str(user_id), -1)  # -1 globally
if new_count <= 0:
    await redis.hdel(count_key, str(user_id))   # clean up count
    await redis.hdel(names_key, str(user_id))   # remove from online list
raw = await redis.hgetall(names_key)
return {k: v for k, v in raw.items()}
```

**Integration into the WebSocket lifecycle:**
- On connect: after `manager.connect` and `bridge.ensure_subscribed`, call `presence.user_connected(room_id, user.id, user.username)` → returns the full online dict → publish as a `presence` event via `bridge.publish`. Every worker's subscriber picks it up and broadcasts to their local clients.
- On disconnect: after `manager.disconnect`, check `user.id not in manager.local_user_ids(room_id)`. If True (user has no more local sockets in this room), call `presence.user_disconnected` → publish updated `presence` event.

The local check (`manager.local_user_ids`) is an optimization: if the user still has another tab open in *this* process, we skip the Redis round-trip. But it's not sufficient on its own — the Redis counter is what handles the cross-process case. If the local check says "no local connections" but another process still has a connection, the Redis counter will still be >0, the user won't be HDEL'd, and the presence event will still show them online. Correct behavior.

**Typing indicators** are simpler — no persistence, no Redis state, no refcounting. The client sends `{"type": "typing"}`, the server publishes it via `bridge.publish` as a `ChatEvent(type="typing", user_id, username)`, and each client shows "X is typing..." for 3 seconds before clearing. The 3-second timeout is client-side only; the server doesn't track typing state at all.

**The incoming WebSocket protocol changed** from `{"content": "..."}` to `{"type": "message", "content": "..."}` or `{"type": "typing"}`. This is a `ClientCommand` Pydantic model with `type` and optional `content`. The receive loop dispatches on `type`: `message` → persist + publish, `typing` → just publish.

**How I verified it worked.** The smoke test was updated to:
1. Assert both alice and bob receive `presence` events on connect
2. Assert `online_users` contains both usernames
3. Send a typing event and assert the other client receives it
4. All pass. The multi-worker test (Entry 4) further verifies presence works cross-process.

### What went wrong
The Windows console encoding struck again: `UnicodeEncodeError: 'charmap' codec can't encode character '\u2192'` when the multi-worker test tried to print `→`. The fix was replacing the arrow with `->`. The lesson: on Windows, keep print statements ASCII-only unless you've explicitly set UTF-8 encoding via `sys.stdout.reconfigure(encoding='utf-8')`.

## Entry 4 — Multi-worker test: proving pub/sub actually crosses processes
**Files touched:** `tests/multiworker_test.py`, `tests/pagination_test.py`

### Context
Entry 1 designed the pub/sub bridge for cross-process delivery, but the smoke test only ran one uvicorn instance — it proved pub/sub *works* but not that it *crosses processes*. The whole point of the Redis bridge is that a message sent on worker A reaches a client on worker B. That needed a separate test.

### Before you read on
You need to prove that two separate uvicorn instances (simulating `--workers 2`) can deliver messages to each other's WebSocket clients via Redis. How would you set this up? What's the minimum assertion that proves cross-process delivery? Think about what "two workers" means in practice — can you use two ports, or do you need process-level isolation?

### Options considered
- **`uvicorn --workers 2` on a single port** → rejected because you can't control which worker a WebSocket connection lands on. The test would be non-deterministic: alice and bob might both land on worker 1, and the test would pass without exercising cross-process delivery at all.
- **Two `subprocess.Popen` calls running uvicorn on different ports** → works but complex to orchestrate (manage stdio, wait for readiness, clean up). Rejected for test simplicity.
- **Two `uvicorn.Server` instances in the same event loop on different ports** → each has its own `app.state.connection_manager` (they're separate ASGI app instances), which is exactly the isolation we need to test. The in-memory state is separate; Redis is shared. **Chosen.**

### Why
The key insight: what makes cross-process delivery hard is that each process has its own in-memory `ConnectionManager`. Two `uvicorn.Server` instances on different ports share the same Python event loop but each has its own ASGI app instance with its own `app.state`. So `manager.broadcast(room_id, event)` on server A only touches server A's sockets — server B's sockets are invisible. The only way the message gets from A to B is through Redis pub/sub. If the test passes, the bridge works.

### How to build it

**Starting two servers in one event loop:**
```python
async def start_server(port):
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="on")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    for _ in range(50):
        await asyncio.sleep(0.1)
        if server.started:
            break
    return server, task
```

The `app` object is imported once. Each `uvicorn.Server` creates its own ASGI app instance on startup, so `app.state.connection_manager` on server A is a *different* `ConnectionManager` than server B's. That's the isolation we're testing.

**The test flow:**
1. Register alice on port A, register bob on port B
2. Alice creates a room on port A, bob joins on port B
3. Alice opens WS on port A, bob opens WS on port B
4. Alice sends a message → bob receives it on port B
5. Alice sends a typing event → bob receives it on port B
6. Alice receives her own message back via pub/sub round-trip

The assertion that matters: `assert bob_msg is not None, "Bob never received the message! Cross-worker pub/sub is broken."` If pub/sub is broken, bob_msg is None and the test fails. If it works, the message traveled: alice's WS → server A's `bridge.publish` → Redis channel `chat:room:1` → server B's subscriber task → server B's `manager.broadcast` → bob's WS.

**The pagination test** (`tests/pagination_test.py`) is more straightforward — it sends 15 messages via WebSocket, then paginates with `limit=5` and `before=<cursor>`:
- Page 1: msgs 10-14 (newest 5), cursor = id of msg-10
- Page 2: `?limit=5&before=<cursor>` → msgs 05-09
- Page 3: `?limit=5&before=<cursor2>` → msgs 00-04
- Assert no ID overlap between pages

The `before` parameter returns messages with `id < before` ordered descending, then the endpoint reverses to oldest-first. The cursor is the oldest ID in the current page.

### What went wrong
The `→` character in a print statement caused `UnicodeEncodeError` on Windows cp1252 console. Fixed by replacing with `->`. The lesson is the same as Entry 3's — on Windows, keep print statements ASCII-only in test code.

## Entry 5 — Deployment: choosing a free tier that actually supports WebSockets
**Files touched:** `Dockerfile`, `.dockerignore`, `render.yaml`

### Context
The app works locally but needs a live URL for a portfolio. The hard constraint: WebSocket support, which immediately rules out Vercel, Netlify, and most serverless platforms — they're HTTP-only (or terminate connections after ~30s). The app also needs Redis for pub/sub, so the platform must either provide Redis or allow connecting to an external one.

### Before you read on
You need to host a FastAPI + WebSocket + Redis app for free, with a persistent URL a recruiter can click. Which platforms support WebSockets on their free tier? Which ones provide Redis? What's the tradeoff between "always on" and "sleeps after inactivity"? Think about what happens when a recruiter clicks a link and the service has been sleeping for 3 hours — what's the user experience?

### Options considered
- **Vercel / Netlify / AWS Lambda** — rejected because serverless platforms don't support persistent WebSocket connections. They timeout after 10-30 seconds. The entire architecture (long-lived WS connections, Redis pub/sub subscriber tasks) is incompatible with the serverless model.
- **Railway** — supports WebSockets + Redis natively, but no longer has a true free tier (only a $5 trial credit that expires). Rejected for "free" requirement.
- **Fly.io** — supports WebSockets, can run Redis in a container, generous free tier (3 shared-cpu VMs). But free tier VMs auto-stop after 1 hour of inactivity, and setup requires the `flyctl` CLI + `fly.toml`. Viable but more complex.
- **Render + Upstash Redis** — Render's free web service supports WebSockets and Docker. It sleeps after 15 min of inactivity (cold start ~30s), but this can be mitigated with an external pinger. Redis is provided by Upstash (free serverless Redis, 10K commands/day, supports pub/sub via standard protocol). **Chosen** for simplicity and well-documented setup.

### Why
Render is the most well-known Docker-friendly platform with a free tier that supports WebSockets. The Dockerfile is standard and portable — if Render's limits become a problem, the same image deploys to Fly.io, Koyeb, or any VPS without changes. Upstash Redis was chosen over Render's built-in Redis (paid) because Upstash's free tier supports the standard Redis protocol including `SUBSCRIBE`/`PUBLISH`, which our pub/sub bridge requires.

The sleeping issue is the real tradeoff. Render free services sleep after 15 minutes of no inbound requests. When a recruiter clicks the portfolio link, they'll wait ~30 seconds for the container to spin up. This is mitigated by setting up UptimeRobot (free) to ping `https://<app>.onrender.com/health` every 10 minutes, keeping the service warm. It's a hack, but it's the standard Render free-tier pattern and it works.

### How to build it

**The Dockerfile:**
```dockerfile
FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
COPY static/ ./static/
EXPOSE 8000
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
```

Key details:
- `python:3.13-slim` keeps the image small (~200MB). `bcrypt` and `cryptography` have pre-built wheels for Linux x86_64, so no C compiler needed.
- `CMD` uses shell form (not exec form `["uvicorn", ...]`) so `${PORT:-8000}` is expanded by `/bin/sh`. Render sets the `PORT` env var (e.g., 10000); the fallback to 8000 is for local Docker runs.
- `COPY app/ ./app/` and `COPY static/ ./static/` — the `static/` directory must be in the image because `StaticFiles(directory="static")` in `app/main.py` reads from the working directory. The `.dockerignore` excludes `tests/`, `.env`, `chat.db`, and log files to keep the image lean and avoid leaking the dev secret.

**The `render.yaml` (Render Blueprint):**
```yaml
services:
  - type: web
    name: realtime-chat
    runtime: docker
    plan: free
    healthCheckPath: /health
    envVars:
      - key: JWT_SECRET
        generateValue: true      # Render generates a random secret
      - key: DATABASE_URL
        value: sqlite+aiosqlite:///./chat.db
      - key: REDIS_URL
        sync: false              # set manually to Upstash URL
```

`generateValue: true` makes Render create a strong random JWT secret on first deploy and persist it across redeploys. `sync: false` means REDIS_URL is set manually in the dashboard after creating the Upstash instance.

**Why SQLite is acceptable for the portfolio demo.** Render's free tier has an ephemeral filesystem — the DB file persists between sleep/wake cycles but is lost on redeploy. For a portfolio piece, this is fine: the data just needs to survive during a recruiter's visit, not across code updates. Switching to PostgreSQL (adding `asyncpg`, a managed Postgres instance, and changing `DATABASE_URL`) is straightforward but adds complexity and cost for marginal benefit at demo scale.

**The deployment steps (in order):**
1. Push the code to a GitHub repo (the project isn't in git yet — `git init`, commit, push).
2. Create a free Upstash Redis instance at `upstash.com` → copy the standard Redis URL (`rediss://...` — note the double `s` for TLS).
3. Go to `render.com` → New → Web Service → connect the GitHub repo → Render detects the Dockerfile automatically.
4. Set env vars: `REDIS_URL` = the Upstash URL. `JWT_SECRET` and `DATABASE_URL` are handled by `render.yaml`.
5. Deploy. Render builds the Docker image and starts the container.
6. Set up UptimeRobot (free) to ping `https://<app>.onrender.com/health` every 10 minutes.
7. The app is live at `https://<app>.onrender.com`.

**The WebSocket URL auto-adapts to HTTPS.** In `static/app.js`, the WebSocket protocol is derived from `window.location.protocol`:
```javascript
const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
```
Render terminates TLS at their proxy, so the page loads over HTTPS and the WebSocket automatically uses `wss://`. No code change needed.

### What went wrong
Couldn't verify the Docker build locally because Docker Desktop kept stopping (the `docker build` command failed with "cannot find the file specified" for the Docker Desktop pipe). The Dockerfile is a standard Python slim image with pip install — there's nothing exotic that would fail to build. The plan is to verify the build on Render's CI directly, which builds from the GitHub repo. If the build fails on Render, the error will be in the build logs and likely a missing system dependency for a C extension (unlikely given pre-built wheels, but possible for `bcrypt` on certain architectures).


