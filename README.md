# Real-time Chat

A backend portfolio project showing how independent FastAPI workers share live chat events through Redis, while PostgreSQL stores messages and Celery processes room reports.

**Python 3.12+ · FastAPI · SQLAlchemy · Redis · PostgreSQL · Celery · WebSockets**

[Architecture](docs/architecture.md) · [Deployment](docs/deployment.md) · [Audit evidence](docs/audit.md) · [Build Book](BUILD_BOOK.md) · [Redis and Celery tutorial](REDIS_CELERY_TUTORIAL.md)

![Working desktop chat interface](docs/images/chat-desktop.png)

[Mobile screenshot](docs/images/chat-mobile.png)

## What this project demonstrates

- **Distributed delivery:** one Redis subscriber per active room per API process. The sender receives its message through the same Redis path as other users.
- **Access control:** JWT authentication, membership checks, private-room protection, and socket revocation when a user leaves.
- **Connection lifecycle:** multiple tabs per user, expiring presence leases, heartbeat renewal, subscription cleanup and bounded commands.
- **Persistent history:** cursor pagination, replies with quoted context, and database migrations.
- **Background work:** a separate Celery worker reads the shared database; authenticated users can queue and poll their own room statistics jobs.
- **Repeatable delivery:** locked dependencies, a non-root Docker image, a complete Compose stack and GitHub Actions checks.

This is a demonstrable engineering project with documented limits, rather than a claim of unlimited production scale. See [tradeoffs and failure behavior](docs/architecture.md#tradeoffs-and-limits).

## Run in Docker

Install Docker Desktop with Linux containers, then:

~~~bash
docker compose up -d --build --wait
~~~

Open [the chat UI](http://localhost:8000), [interactive API docs](http://localhost:8000/docs) or [readiness](http://localhost:8000/ready).

Compose starts PostgreSQL, Redis, a one-time migration service, the API and a Celery worker. The database and Redis use named volumes. Ports are bound to localhost. Compose credentials are local development defaults; hosted services must use generated secrets.

Stop services with `docker compose down`. Use `docker compose down --volumes` only when you intend to erase the local database and queue.

If a port is busy or reserved, set `WEB_PORT`, `POSTGRES_PORT` or `REDIS_PORT` before starting Compose. Defaults are 8000, 5433 and 6380.

## Try it in two minutes

1. Register Alice in one browser session. Create a public room named `engineering`.
2. Register Bob in a second browser session or private window. Select and join the room.
3. Send messages, reply to one, and watch typing and presence updates.
4. Open another Alice tab. Closing one tab should leave Alice online while the other stays connected.
5. Leave the room as Bob. His membership and active sockets are revoked; he can explicitly rejoin a public room.
6. In `/docs`, authorize with a login token, create a statistics job and poll its task ID.

Private rooms are currently owner-only: there is no invitation API yet. Public rooms and their history are visible to authenticated users.

## Run Python locally

Start only the supporting Redis service:

~~~bash
docker compose up -d redis
python -m venv .venv
~~~

Activate the environment: `.venv\Scripts\Activate.ps1` on PowerShell, or `source .venv/bin/activate` on macOS/Linux.

Copy `.env.example` to `.env` (`Copy-Item .env.example .env` on PowerShell; `cp .env.example .env` on macOS/Linux), then:

~~~bash
python -m pip install -r requirements-dev.txt
python -m uvicorn app.main:app --reload --ws-max-size 20000
~~~

This path uses a local SQLite file. For room reports, start a separate worker from the same directory:

~~~bash
# Windows: use --pool=solo
python -m celery -A app.celery_app:celery_app worker --loglevel=INFO --pool=solo
~~~

On Linux, omit `--pool=solo` for Celery's default process pool. Use PostgreSQL when API and worker run on separate machines.

## Verification

~~~bash
python -m pytest -q
python -m ruff check app tests/test_*.py tests/conftest.py tests/celery_reports_test.py tests/process_helpers.py migrations
python -m ruff format --check app tests/test_*.py tests/conftest.py tests/celery_reports_test.py tests/process_helpers.py migrations
python -m pip_audit -r requirements.txt --no-deps --disable-pip
~~~

The default suite uses temporary SQLite databases and fake Redis. The two real-process tests skip until isolated PostgreSQL and Redis URLs are supplied. CI supplies both and runs the full suite on Python 3.12 and 3.13.

For a full local run, start `docker compose up -d postgres redis`, then set:

~~~dotenv
TEST_REDIS_URL=redis://127.0.0.1:6380/0
TEST_POSTGRES_URL=postgresql+asyncpg://chat:chat@127.0.0.1:5433/chat
~~~

Set these in your shell before `python -m pytest -q`; the tests create and delete uniquely named databases. Point them at disposable development services with permission to create databases. Existing `chat.db` is never used by the pytest suite.

The older `smoke_test.py`, `pagination_test.py`, `replies_test.py`, `leave_test.py` and `docker_test.py` scripts are historical manual checks. Prefer pytest, which isolates data and is run in CI. `multiworker_test.py` now delegates to the real-process test.

## API and WebSocket contract

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/api/auth/register` | Register; passwords are 6+ characters and at most 72 UTF-8 bytes |
| POST | `/api/auth/login` | OAuth2 form login using username or email |
| GET | `/api/auth/me` | Authenticated user identity |
| POST / GET | `/api/rooms` | Create a room / discover accessible rooms |
| GET | `/api/rooms/{id}` | Room details and membership status |
| POST | `/api/rooms/{id}/join` | Join a public room; owners may rejoin their private room |
| POST | `/api/rooms/{id}/leave` | Revoke membership and close existing sockets |
| GET | `/api/rooms/{id}/messages?limit=50&before=42` | Cursor-based history, oldest first within each page |
| POST | `/api/rooms/{id}/statistics-jobs` | Queue a room report; returns 202 and a task ID |
| GET | `/api/rooms/{id}/statistics-jobs/{task_id}` | Poll a job owned by the requesting room member |
| GET | `/health` / `/ready` | Process liveness / database and Redis readiness |

Connect to `/ws/chat/{room_id}`, then send an authentication frame within five seconds:

~~~json
{"type":"auth","token":"<access_token>"}
~~~

Once the server sends presence, commands are:

~~~json
{"type":"message","content":"hello"}
{"type":"message","content":"a reply","reply_to_id":42}
{"type":"typing"}
~~~

Events include `message`, `typing`, `join`, `leave`, `presence` and `error`. Message events carry the saved ID, timestamp, author and optional reply context. Presence maps user IDs to usernames. See `app/schemas.py` for fields.

Existing API clients may still use `?token=...`. The browser uses the first-frame flow to avoid putting credentials in connection URLs. Use HTTPS/WSS for hosted traffic. Tokens expire after 60 minutes by default; clients must sign in again.

## Configuration and dependencies

`.env.example` lists local settings. Production requires `ENVIRONMENT=production`, a random `JWT_SECRET` of at least 32 characters and `AUTO_CREATE_DB=false`. Use `python -m alembic upgrade head` before startup. PostgreSQL URLs from providers are normalized to the async driver format.

`REDIS_NAMESPACE` separates room channels, presence, rate limits and Celery keys between environments. Set explicit database, Redis, broker and result URLs for each deployment. Authentication is limited to 20 attempts per client IP per minute; reports to 5 jobs per user per minute; socket commands to 30 per second per connection. Configure trusted proxy addresses before relying on client-IP limits.

`requirements.in` and `requirements-dev.in` specify allowed versions; generated `.txt` files pin the resolved dependencies. To update with [uv](https://docs.astral.sh/uv/pip/compile/):

~~~bash
uv pip compile requirements.in -o requirements.txt --python-version 3.13 --universal
uv pip compile requirements-dev.in -o requirements-dev.txt --python-version 3.13 --universal
~~~

Run the full checks after updates. Runtime images install only `requirements.txt`.

## Deployment and license

The Render Blueprint includes PostgreSQL, Redis, API and worker services and provisions **paid resources**. Review [deployment instructions](docs/deployment.md) and provider pricing before applying it. No live deployment is claimed by this repository.

[MIT License](LICENSE).
