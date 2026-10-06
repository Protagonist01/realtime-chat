# Deployment guide

## Local containers

`docker compose up -d --build --wait` starts PostgreSQL, Redis, migrations, API and Celery. The migration service must complete before the API starts; the worker waits for API readiness. Data lives in named volumes, independently of container images.

Ports default to 8000, 5433 and 6380 on localhost. On PowerShell, override a reserved port with `$env:POSTGRES_PORT='55432'` (similarly `WEB_PORT` and `REDIS_PORT`). Start Python processes with the matching URLs.

The Docker image runs as user 10001 and includes runtime dependencies, static assets and migrations. It excludes test tools, local secrets, local databases and generated portfolio assets. It handles termination through `exec uvicorn`.

## Render

The checked-in `render.yaml` replaces the old free web-only SQLite configuration. Render's default filesystem is temporary, so SQLite data in that container would disappear on redeploy. API and Celery now share managed PostgreSQL and Redis through private connection strings.

**The Blueprint provisions paid services.** Review [Render pricing](https://render.com/pricing) and the resource preview before applying. This audit does not create services, migrate hosted data or verify a live URL.

1. Publish the reviewed repository changes.
2. Create or update a Blueprint from the repository after reviewing existing service names and data. Existing service settings may be changed by Blueprint sync.
3. Keep production settings: `ENVIRONMENT=production`, `AUTO_CREATE_DB=false`, a generated JWT secret, and one shared database URL. The application converts provider `postgres://` and `postgresql://` URLs to `postgresql+asyncpg://`.
4. Run `python -m alembic upgrade head` in the API pre-deploy step. Deploy the API and Celery worker from the same revision. For the first rollout, keep job traffic off until migrations and the API are ready.
5. Verify `/ready`, registration, two-browser message delivery, a report job and data persistence across redeploy.
6. Terminate HTTPS/WSS at the provider edge. Configure Uvicorn's `FORWARDED_ALLOW_IPS` for trusted proxies, and redact query strings from logs if legacy socket clients use token URLs.
7. Enable database backups and monitor queue depth, worker failures, Redis availability and database storage. Readiness alone does not check job execution.

The configuration uses a shared environment group for plain settings/secrets. Database and Redis references are defined on each service because Render environment groups cannot reference other services. External datastore access is disabled with `ipAllowList: []`.

## Existing SQLite data and migrations

The pytest suite does not modify your existing `chat.db`. Development startup retains a narrow compatibility step that adds a missing `reply_to_id` column and index after inspecting the schema. Production uses Alembic rather than swallowing migration errors at startup.

For an existing database without an Alembic version:

1. Stop writes and take a verified backup, including SQLite WAL files or a database-native backup.
2. Check all tables, columns, types, foreign keys and indexes against `migrations/versions/0001_initial_schema.py`. Older SQLite databases may lack the reply column/index; run the development compatibility step on a copy first.
3. Only if the schema matches, use `python -m alembic stamp 0001` to mark the baseline. Stamping creates no missing schema and must not be used to hide differences.
4. Run `python -m alembic check`, then `python -m alembic upgrade head`.
5. Keep the original backup until behavior and row counts have been checked.

Moving SQLite records to PostgreSQL is a separate data migration. Do not replace an existing production URL and expect data to transfer automatically. Copy and validate records, sequences and constraints first.

Legacy passwords longer than 72 UTF-8 bytes were silently truncated. They are now rejected. Such accounts need a managed credential reset; there is no self-service password-reset endpoint. Standard existing bcrypt hashes and unexpired HS256 JWTs remain compatible.
