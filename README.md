# Real-time Chat Service

A production-grade real-time chat backend built with FastAPI, WebSockets, and Redis pub/sub. Features JWT authentication, cross-process message delivery, presence tracking, and typing indicators.

## Architecture

```
┌──────────────┐     ┌──────────────┐
│  Uvicorn A   │     │  Uvicorn B   │     (N workers)
│              │     │              │
│  ┌────────┐  │     │  ┌────────┐  │
│  │ WS     │  │     │  │ WS     │  │   In-process ConnectionManager
│  │ clients│  │     │  │ clients│  │   {room_id: {user_id: set[WebSocket]}}
│  └───┬────┘  │     │  └───┬────┘  │
│      │       │     │      │       │
│  ┌───▼────┐  │     │  ┌───▼────┐  │
│  │PubSub  │  │     │  │PubSub  │  │   Per-room subscriber task
│  │Bridge  │  │     │  │Bridge  │  │   (refcounted, auto sub/unsub)
│  └───┬────┘  │     │  └───▲────┘  │
│      │       │     │      │       │
└──────┼───────┘     └──────┼───────┘
       │                     │
       ▼                     │
   ┌─────────────────────────────┐
   │         Redis               │
   │   channel: chat:room:{id}   │   Single source of truth
   │   hash:   presence:room:... │   for cross-process state
   └─────────────────────────────┘
```

**Key design decisions:**

- **Redis pub/sub with per-room channels** — each worker subscribes only to rooms where it has local clients. Messages published to `chat:room:{id}` are received by every worker with a client in that room.
- **Single delivery code path** — the publisher never broadcasts to its own sockets directly. It publishes to Redis, its own subscriber picks the event back up, and broadcasts locally. The sender's clients receive messages through the same path as everyone else's.
- **Refcounted subscriptions** — one subscriber task per room per process, not one per WebSocket. A counter tracks how many local sockets are in a room; when it hits zero, the Redis subscription is torn down.
- **Cross-process presence** — Redis `HINCRBY` counters per `(room_id, user_id)` handle multi-tab and multi-worker correctly. A user is removed from the online list only when their last connection closes across all processes.
- **JWT auth on WebSocket** — browsers can't set custom headers on `new WebSocket()`, so the token is passed via query string (`?token=...`). Auth happens before the socket is accepted.

## Features

- **Real-time messaging** — WebSocket-based, sub-second delivery via Redis pub/sub
- **JWT authentication** — register, login, token-protected REST + WS endpoints
- **Room management** — create rooms, join/leave, public/private rooms with membership enforcement
- **Presence** — see who's online in each room, updated in real-time across all workers
- **Typing indicators** — ephemeral events, no persistence, client-side timeout
- **Message history** — cursor-based pagination via `?before=<message_id>&limit=N`
- **Multi-worker ready** — tested with two separate uvicorn instances proving cross-process delivery

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Framework | FastAPI + Starlette WebSockets |
| ASGI server | Uvicorn |
| Database | SQLAlchemy 2.0 async + SQLite (swap to Postgres via `DATABASE_URL`) |
| Pub/sub | Redis (`redis.asyncio`) |
| Auth | python-jose (JWT) + bcrypt (password hashing) |
| Validation | Pydantic v2 + pydantic-settings |
| Frontend | Vanilla JS + CSS (served as static files from FastAPI) |
| Deploy | Dockerfile + render.yaml |

## Project Structure

```
app/
  main.py               # FastAPI app, startup/shutdown lifecycle
  config.py             # pydantic-settings (env-driven config)
  database.py           # async SQLAlchemy engine + session factory
  models.py             # User, Room, Message, room_members
  schemas.py            # Pydantic request/response models + ChatEvent
  security.py           # bcrypt hashing + JWT encode/decode
  deps.py               # get_current_user dependency
  redis_client.py       # shared redis.asyncio client
  connection_manager.py # in-process WebSocket registry
  pubsub.py             # Redis pub/sub bridge (refcounted subscribers)
  presence.py           # cross-process presence via Redis HINCRBY
  routers/
    auth.py             # /api/auth/register, /login
    rooms.py            # /api/rooms CRUD, join/leave, message history
    chat.py             # /ws/chat/{room_id} WebSocket endpoint
static/
  index.html            # chat UI
  style.css             # Tokyo Night-inspired dark theme
  app.js                # WS client, event handling, typing, presence
tests/
  smoke_test.py         # end-to-end: 2 users, WS, pub/sub, presence, typing
  pagination_test.py    # cursor-based message pagination
  multiworker_test.py   # cross-process delivery (2 uvicorn instances)
  docker_test.py        # end-to-end against Docker container
```

## Quick Start

### Prerequisites

- Python 3.13+
- Docker (for Redis)

### Run locally

```bash
# Start Redis
docker compose up -d

# Install dependencies
pip install -r requirements.txt

# Run the server
uvicorn app.main:app --reload

# Open the chat UI
# http://127.0.0.1:8000
```

### Run tests

```bash
# All tests start uvicorn in-process — no separate server needed
python tests/smoke_test.py          # end-to-end: register, WS, message, typing, presence
python tests/pagination_test.py     # cursor-based pagination
python tests/multiworker_test.py    # cross-process pub/sub delivery
```

## API Reference

### Auth

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/auth/register` | Register a new user (username, email, password) |
| POST | `/api/auth/login` | Login, returns JWT (OAuth2PasswordRequestForm) |

### Rooms

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/rooms` | Create a room (creator auto-joins) |
| GET | `/api/rooms` | List rooms you're a member of |
| GET | `/api/rooms/{id}` | Get room details |
| POST | `/api/rooms/{id}/join` | Join a room |
| POST | `/api/rooms/{id}/leave` | Leave a room |
| GET | `/api/rooms/{id}/messages` | Message history (`?limit=50&before=<id>`) |

### WebSocket

```
ws://host/ws/chat/{room_id}?token=<JWT>
```

**Incoming (client → server):**
```json
{"type": "message", "content": "hello"}
{"type": "typing"}
```

**Outgoing (server → client):**
```json
{"type": "message", "room_id": 1, "username": "alice", "content": "hello", "message_id": 42, "created_at": "..."}
{"type": "join", "room_id": 1, "username": "bob"}
{"type": "leave", "room_id": 1, "username": "bob"}
{"type": "typing", "room_id": 1, "username": "alice"}
{"type": "presence", "room_id": 1, "online_users": {"1": "alice", "2": "bob"}}
{"type": "error", "room_id": 1, "content": "Invalid message payload"}
```

## Configuration

Environment variables (see `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `JWT_SECRET` | `dev-secret` | Secret for signing JWTs — **change in production** |
| `JWT_ALGORITHM` | `HS256` | JWT algorithm |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | `60` | Token expiry |
| `DATABASE_URL` | `sqlite+aiosqlite:///./chat.db` | SQLAlchemy async URL |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |

## Deployment

The app is containerized and deploy-ready. See `Dockerfile`, `render.yaml`, and `BUILD_BOOK.md` Entry 5 for the full deployment guide (Render + Upstash Redis).

## Build Book

See [`BUILD_BOOK.md`](BUILD_BOOK.md) for the engineering journal — architecture decisions, bugs hit during development, and how they were diagnosed. Written as the work happened, not backfilled.
