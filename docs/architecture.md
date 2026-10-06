# Architecture and engineering tradeoffs

~~~mermaid
flowchart LR
  A[Browser A] <-->|WebSocket| W1[API process A]
  B[Browser B] <-->|WebSocket| W2[API process B]
  W1 <-->|Room pub/sub| R[(Redis)]
  W2 <-->|Room pub/sub| R
  W1 <-->|Messages and membership| DB[(PostgreSQL)]
  W2 <-->|Messages and membership| DB
  W1 -->|Queue report| R
  R -->|Celery job| C[Celery worker]
  C -->|Read snapshot| DB
  C -->|Expiring result| R
~~~

## Message delivery

The API checks membership, saves a message, then publishes the serialized event to a room channel. Every subscribed API process broadcasts the event to its own sockets, including the originating process. One subscriber handles all local sockets in the room; a reference count controls its lifetime.

Database commit precedes publication. This makes history durable, but introduces a deliberate failure window: if Redis fails after the commit, the message may appear in history without reaching connected clients. Pub/sub is ephemeral and has no replay. Stronger guarantees would require an outbox plus durable delivery and client-side deduplication.

## Presence and cleanup

Each socket owns a UUID lease in a Redis sorted set and a matching user/name record. A Lua script prunes expired records and changes the two structures atomically. The server renews leases every 20 seconds; records expire after 60 seconds. Users with multiple tabs remain online until their last lease disappears.

Clean disconnect removes exactly one lease. A worker crash leaves records until expiry. Another worker's startup never clears live users. A connected room gets updated presence through heartbeats; abandoned room keys have TTLs.

Leaving a room removes membership in the database and publishes a control event that closes that user's sockets on every listening worker. Command and heartbeat membership checks also reject revoked users if a control event is missed.

## Authentication and jobs

Passwords use bcrypt with a strict 72-byte input limit; hashing runs in a thread pool so it does not block socket processing. JWTs require a positive user ID and expiration claim and use a fixed HS256 algorithm. Production rejects development secrets.

The browser stores tokens for its browser session and sends socket credentials in the first frame. Session storage is still readable by scripts in the page; user-provided content is rendered as text. Existing query-token clients need query-string redaction at the proxy.

Report creation checks membership, rate limits requests and stores a one-hour ownership mapping. Polling checks both current membership and ownership. Celery uses JSON, bounded retries, expiring results and a read-only task. Duplicate execution is harmless. The worker creates and disposes its own database engine.

## Tradeoffs and limits

- Private rooms have no invitations yet; only their creator can join. Public-room history is available to authenticated users.
- SQLite is convenient for one-machine development. PostgreSQL is needed for separate API and job hosts.
- Messages are persisted, but live events have no delivery acknowledgments, replay or automatic browser reconnection. Reselect the room to reconnect and load history.
- A slow socket can delay local fan-out. There are no per-client outbound queues or measured capacity claims.
- Socket commands have a per-connection cap. There are no global per-user socket limits, moderation tools, account recovery or distributed connection quotas.
- JWT expiry is checked on commands and at heartbeats; idle connections may remain open for up to one heartbeat interval after expiry.
- Presence after a crash may remain visible for up to one lease plus one heartbeat interval.
- Rate-limit keys use the client address reported by ASGI. Configure a trusted proxy list; multiple people sharing one public IP share the authentication limit.
- Redis outages close affected sockets; clients reconnect manually. Shutdown cancels all subscriptions. Full Redis outage recovery and sustained load tests are future work.
- `/ready` checks the database and chat Redis. It does not prove Celery worker availability; monitor job completion separately.
- The test suite verifies behavior, not internet scale, availability targets or resistance to every attack.

See [audit results](audit.md) for the checks actually run.
