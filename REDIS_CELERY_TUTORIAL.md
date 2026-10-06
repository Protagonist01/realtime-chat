# Redis pub/sub and Celery: build them from this chat project

A beginner's hands-on guide to live events, background jobs, and the decisions needed when a project grows.

**Audience:** You know basic Python and HTTP, but Redis, WebSockets, and Celery may be new.

**What you will build:** Follow a chat message across Redis pub/sub; queue a room statistics report in Celery; run the web app, Redis, and a worker as separate processes.

**Project map:** [README.md](README.md), [app/pubsub.py](app/pubsub.py), [app/routers/chat.py](app/routers/chat.py), [app/celery_app.py](app/celery_app.py), [app/tasks.py](app/tasks.py), [app/routers/reports.py](app/routers/reports.py).

## How to study this guide

Start with section 1. You need only basic Python and a rough idea of a browser sending a request to a server. Do not read the whole guide in one sitting. After each example, say in your own words who sends something, who receives it, and whether the information is saved. The later sections connect those ideas to code and exercises.

## 1. Why two copies of the web app need help

Start with **one running copy** of this chat app. A running program is called a **process**. The app keeps a Python dictionary of open browser connections in [app/connection_manager.py](app/connection_manager.py). If Alice and Bob are both connected to this one process, it can find both connections and send a message to both.

Now run **two copies** of the web app to handle more users. We will call them **web process A** and **web process B**. Each has its **own memory** and its **own dictionary**. This is the key point: putting a value in A's dictionary does not put it in B's dictionary. Think of two people writing in separate notebooks. Each can read their own notebook, but neither can see new writing in the other's.

Suppose Alice connects to A and Bob connects to B:

~~~text
Alice's browser --> Web process A --> A's dictionary: Alice
Bob's browser   --> Web process B --> B's dictionary: Bob
~~~

Alice sends “hello” to A. A looks in **A's dictionary**. It finds Alice. It does **not** find Bob, because Bob's connection is in B's dictionary. A cannot send directly through a connection it does not have. This is the problem Redis solves for live chat.

If room 42 contains only these two people, picture the dictionaries like this:

~~~python
# Inside web process A
connections = {42: {"alice"}}

# Inside web process B
connections = {42: {"bob"}}
~~~

The real [ConnectionManager](app/connection_manager.py) stores user IDs and WebSocket objects rather than these simple names. The important fact is that A's connections variable and B's connections variable are **two different objects in two different processes**.

### Give both web processes a shared meeting point

Redis is another running program that both A and B can contact. Both tell Redis, “Send me new events for room 42.” That request is called **subscribing** to a **channel**. A channel is a name, such as chat:room:42.

When Alice sends “hello,” A **publishes** a room-42 event to Redis. Publishing means sending one event to that channel now. Redis forwards it to **both** subscribed web processes. A sends it to Alice's local connection; B sends it to Bob's local connection.

~~~text
                         one published event
Alice --> Web process A ------------------------> Redis
             |                                    |   |
             | receives the event back            |   +--> Web process B --> Bob
             +<-----------------------------------+
             |
             +--> Alice
~~~

Notice that Redis does not hold Bob's browser connection. B still owns that connection. Redis only tells B about the event. Also, Redis sends A the event even though A published it; this project uses the same delivery path for Alice and Bob.

**Check yourself before moving on:**

1. Where is Alice's connection stored? In A's memory.
2. Where is Bob's connection stored? In B's memory.
3. Why can A not send directly to Bob? Bob's connection is absent from A's dictionary.
4. What does Redis pass between them? A room event, not the browser connection.

If those four answers make sense, you have the foundation for pub/sub. Do not worry yet about async, locks, or the exact Python lines. Section 2 shows the Redis commands and then follows the code.

**One naming warning:** A “web worker” often means one copy of the web app. A “Celery worker” is a different program that performs queued background jobs. We will meet Celery in section 3. For now, A and B are only **web processes**.

## 2. What Redis is doing in this repository

Redis is a separate server. The Python library in [app/redis_client.py](app/redis_client.py) creates an async client from the REDIS_URL setting. The default is redis://localhost:6379/0. The trailing /0 selects logical database 0 for keys. Pub/sub channel names are server-wide, so a logical database number does **not** isolate channel names. This project prefixes channels with REDIS_NAMESPACE, for example realtime-chat:chat:room:42, even when databases differ. The shorter channel names in the diagrams omit that prefix for readability.

The live chat path uses Redis in two ways (Celery adds broker and result storage later):

1. **Channels** named chat:room:{room_id} carry live JSON events.
2. **Expiring socket leases** in a sorted set and matching names hash hold online user information. The keys are `<namespace>:presence:{<room_id>}:leases` and `:names`; Lua updates them atomically.

The durable chat message itself goes into SQL in [app/models.py](app/models.py). Redis pub/sub carries a notification of that saved message. This distinction lets clients reload missed history from SQL after reconnecting. Typing events have no SQL row because they are disposable.

### See pub/sub without Python

Start Redis from the project root:

~~~powershell
docker compose up -d redis
docker compose exec redis redis-cli PING
~~~

You should see PONG. Open two terminals. In terminal A:

~~~powershell
docker compose exec redis redis-cli SUBSCRIBE tutorial:hello
~~~

In terminal B:

~~~powershell
docker compose exec redis redis-cli PUBLISH tutorial:hello "hello from Redis"
~~~

Terminal A should receive the message. Start another subscriber and publish again: both receive it. Stop all subscribers and publish again: Redis reports zero subscribers, and the message is gone. There is no replay. Do not use pub/sub as your only storage for an event that must be recovered. See [Redis pub/sub delivery semantics](https://redis.io/docs/latest/develop/interact/pubsub/).

### A few Python words before reading the code

- An **async function** can pause while waiting for Redis, a socket, or a database. Python marks it with async def.
- **await** means “pause here until this operation finishes and let the program work on something else meanwhile.” It does not create another process.
- An **event loop** coordinates many such waiting functions inside one process.
- asyncio.create_task(...) starts a function running on that process's event loop. Here it keeps the Redis subscriber listening while WebSocket handlers continue.
- An asyncio.Lock stops two functions **in the same process** from changing a room's subscription at the same time. It is not a lock shared between web processes A and B.

### Read the actual delivery path

Start at [app/routers/chat.py](app/routers/chat.py):

1. The WebSocket handler checks the JWT, room, and membership before accepting.
2. The local [ConnectionManager](app/connection_manager.py) stores the socket under its room and user.
3. PubSubBridge.ensure_subscribed(room_id) creates a room subscription if this process has none. Its per-room lock stops two simultaneous joins from opening duplicate subscribers.
4. A client sends JSON such as {"type":"message","content":"hello"}.
5. The handler commits a Message row to SQL and creates a ChatEvent with the new message ID.
6. PubSubBridge.publish serializes that event and calls Redis PUBLISH on chat:room:{id}.
7. Every process with a subscriber for that room reads the event, validates it, and calls its own ConnectionManager.broadcast.
8. Each process sends the JSON to the WebSockets it owns. The sender also hears the event through this path.

The event shape is in [app/schemas.py](app/schemas.py). The subscriber only acts on Redis messages with type "message"; Redis also emits subscription control messages. PubSubBridge uses one subscriber task **per room per web process**, not one per socket. A local reference count closes that subscription when the last local socket leaves. Calling PubSub.aclose matters because the subscription reserves a connection.

Here is the core relationship in simplified form:

~~~python
# Producer: after the SQL commit
await redis.publish("chat:room:42", event.model_dump_json())

# Each subscribed web process
async for item in pubsub.listen():
    if item["type"] == "message":
        event = ChatEvent.model_validate_json(item["data"])
        await local_connection_manager.broadcast(42, event)
~~~

A Redis PUBLISH return value is a count of Redis subscribers matched by the publication, **not** a count of browser users or a delivery acknowledgement. A WebSocket send can still fail.

### Read PubSubBridge one field at a time

Open [app/pubsub.py](app/pubsub.py) next to this section. The four dictionaries inside PubSubBridge each answer a different question:

| Field | Question it answers |
| --- | --- |
| _locks[room_id] | Is another coroutine changing this room's subscription right now? |
| _tasks[room_id] | Is a listener coroutine running for this room in this process? |
| _pubsubs[room_id] | Which Redis PubSub object owns the subscribed connection? |
| _refcount[room_id] | How many **local sockets** asked this process to keep the room subscription? |

Walk through room 42 with two local sockets:

1. First socket joins. ensure_subscribed takes the lock, changes the count from 0 to 1, subscribes to chat:room:42, and starts one listener task.
2. Second socket joins. The count changes from 1 to 2. A listener already exists, so no new Redis subscription is opened.
3. First socket leaves. maybe_unsubscribe changes the count from 2 to 1. The listener stays.
4. Second socket leaves. The count becomes 0. The task is canceled; the Redis subscription and its connection are closed.

Why hold the lock around “check whether a task exists” and “create one”? Without it, two simultaneous joins can both see no task and create two listeners. One published event could then be sent twice to every local browser. The count and the lock solve different problems: the count controls lifetime; the lock controls concurrent setup.

Read [app/connection_manager.py](app/connection_manager.py) next. Its nested dictionary maps room ID to user ID to a **set of sockets**. One person can have several tabs, so a single socket per user would lose connections. The subscriber calls broadcast, which turns one ChatEvent into sends to the local sockets. The manager has no Redis code because the bridge handles the cross-process part.

**A useful breakpoint exercise:** Put a temporary print or debugger breakpoint in PubSubBridge.publish, PubSubBridge._listener, and ConnectionManager.broadcast. Send one chat message. Record the order. With two web processes, the listener and broadcast steps run once per subscribed process, while the publish step runs only where the sender's command arrived.

### What is stored, and what is lost?

For a chat message, the SQL commit occurs before PUBLISH. If every subscriber is offline when PUBLISH runs, the live notification is lost, but the Message row remains in SQL. A reconnecting client can ask for message history. A typing event has no SQL row and is supposed to disappear. This is an intentional choice for two different data needs.

There is also a failure gap: if the process crashes after the SQL commit and before PUBLISH, clients do not get the live notification even though the row exists. Section 6 explains the transactional outbox pattern for stronger delivery. Saying “saved in SQL” does not mean “guaranteed live broadcast.”

### Prove cross-process delivery

With dependencies installed and Redis running:

~~~powershell
python tests/multiworker_test.py
~~~

The test creates two server instances with separate in-memory connection managers, places Alice on one and Bob on the other, and checks that Bob receives Alice's message. This isolates the property that a one-process smoke test cannot prove. Note that the test uses two app instances in one Python process; it verifies separate app state, not operating-system process isolation.

## 3. Add a background job with Celery

Celery needs at least a **producer** (the web app), a **broker** (Redis here), and a **consumer** (a Celery worker). This project also enables a **result backend** (another Redis logical database) so the API can report success and return the statistics.

Keep the two uses separate:

| | Live chat pub/sub | Celery report job |
| --- | --- | --- |
| What happens to one event or job? | Every web process subscribed to the room hears it | One Celery worker claims the queued job |
| What if nobody is listening now? | The live event is missed | The job can wait in the broker for a worker |
| What is it for here? | Deliver a chat update right now | Calculate a report outside the web request |

**Important:** Celery's Redis broker uses Redis data structures for task queues. Sending a normal Celery task is not the same as publishing to this application's chat pub/sub channels. [Celery's Redis guide](https://docs.celeryq.dev/en/latest/getting-started/backends-and-brokers/redis.html) explains the broker's lists, acknowledgements, and result keys.

~~~text
Browser -> POST /statistics-jobs -> FastAPI -> Redis broker /1
                                              |
                                              v
                                      Celery worker -> SQL database
                                              |
                                              v
                                      Redis results /2
Browser -> GET /statistics-jobs/{task_id} -> FastAPI -> result backend
~~~

The chat pub/sub Redis client continues to use /0. The worker is a separate process, so it must have the same DATABASE_URL as the API and access to the same database. A local SQLite file works when both processes run in this repository directory on one machine. For containers or multiple hosts, use a shared database such as PostgreSQL; separate local SQLite files do not share data.

### Installation and configuration

The project now includes celery[redis] in [requirements.txt](requirements.txt). Install all dependencies:

~~~powershell
python -m pip install -r requirements.txt
~~~

Copy [.env.example](.env.example) to .env if needed. The relevant defaults are:

~~~text
REDIS_URL=redis://localhost:6380/0
CELERY_BROKER_URL=redis://localhost:6380/1
CELERY_RESULT_BACKEND=redis://localhost:6380/2
DATABASE_URL=sqlite+aiosqlite:///./chat.db
~~~

Use rediss:// URLs when your Redis provider requires TLS. In production, configure the three URLs explicitly. Logical databases help separate keys on a standalone Redis server; managed Redis Cluster has different database support, so follow your provider's supported layout. A dedicated broker instance is often clearer at higher scale.

[app/celery_app.py](app/celery_app.py) creates one Celery app and explicitly imports app.tasks. Without task registration, a worker may receive a name it does not know. JSON is used for task arguments and results. worker_prefetch_multiplier=1 reduces how many tasks one worker reserves ahead of execution. result_expires=3600 makes the result temporary. task_track_started lets polling show STARTED.

Read the constructor as a sentence: “Create the chat Celery application, use CELERY_BROKER_URL to send and receive jobs, use CELERY_RESULT_BACKEND to save results, and load app.tasks so the worker knows its task names.” The Celery app manages its own Redis connections. It is different from the async redis_client used by chat and ownership mappings.

The three Redis URLs use logical databases /0, /1, and /2 locally to keep ordinary keys apart. You could run them on separate Redis servers later. A database number does not turn pub/sub into a queue, and it does not provide a security boundary between applications.

### Follow one task message

The API calls room_statistics.apply_async(args=(room_id,), task_id=task_id). Celery serializes the integer room ID and task name into a broker message. The worker listens on the broker, receives one queued task, finds the registered function named chat.room_statistics, runs it, and writes the return value to the result backend. The API later builds an AsyncResult from the task ID and asks the backend for its state and value.

The task ID is a tracking number, not the result itself. Sending a task does not wait for the report to finish. If you call the Python function directly, it runs in the caller; if you call apply_async, Celery sends it to the broker. In this project's tests, eager mode is deliberately enabled, so apply_async executes immediately inside the test process. That test checks the code path but does not prove that an independent worker consumed a Redis queue.

| State you may see | Plain meaning | What to do |
| --- | --- | --- |
| PENDING | No stored state yet; it may be waiting, unknown, or expired | Check the broker and worker if it stays here |
| STARTED | A worker reported that it began | Poll again; watch duration |
| RETRY | The task failed in a way configured for retry | Wait for its next attempt; inspect logs if repeated |
| SUCCESS | The worker returned and Celery saved the result | Read the result object |
| FAILURE | The task ended with an error | Inspect worker logs; the API hides internal exception text |

The worker **acknowledges** a task to tell the broker it is handled. With late acknowledgement, that happens after task execution rather than before. If a worker disappears before acknowledging, the broker may redeliver the work. An application must tolerate a task running more than once. Retry is separate: the task itself can ask Celery to schedule another attempt after a known error. Neither mechanism means “exactly once.”

### The task itself

[app/tasks.py](app/tasks.py) registers chat.room_statistics. Its only argument is an integer room ID. A Celery message must carry serializable data, so pass identifiers rather than WebSocket objects, ORM sessions, or database models. The worker opens its own SQLAlchemy engine, counts messages and distinct contributors, finds the latest message ID, closes the engine, and returns a JSON object:

~~~json
{
  "room_id": 42,
  "message_count": 12,
  "contributor_count": 3,
  "latest_message_id": 107
}
~~~

This report is a snapshot at the time its SQL query runs. New messages can arrive after it. The task uses asyncio.run only **inside the synchronous Celery worker task** to execute its async database helper. Do not call asyncio.run inside a running FastAPI event loop.

Read [app/tasks.py](app/tasks.py) from the decorator downward. The decorator gives the function a stable task name and sets retry and acknowledgement behavior. room_statistics(room_id) is synchronous because a normal Celery worker calls it. It creates an event loop with asyncio.run to execute _room_statistics(room_id). The helper builds its own engine and session because the worker is a separate process. The SQL query uses count for all messages, count(distinct user_id) for contributors, and max(id) for the latest message. The finally block disposes the engine even when the query fails.

Why not pass a Message or Room object into the task? It may contain a database session or lazy relationships that exist only in the web process. It is also harder to serialize and may be stale by the time the worker runs. Passing room_id tells the worker what to load from the shared database at execution time.

The task retries SQL OperationalError up to three times with backoff. It uses a late acknowledgement. That is reasonable here because the task only reads data: if it executes twice, there is no duplicate email, payment, or mutation. Late acknowledgement is not an exactly-once promise. A process crash, visibility timeout, or retry can lead to duplicate execution. See [Celery's task and retry guidance](https://docs.celeryq.dev/en/latest/userguide/tasks.html).

### The HTTP endpoints

[app/routers/reports.py](app/routers/reports.py) adds:

| Method | URL | Response |
| --- | --- | --- |
| POST | /api/rooms/{room_id}/statistics-jobs | 202 with task_id |
| GET | /api/rooms/{room_id}/statistics-jobs/{task_id} | Job state and, on success, result |

Both endpoints require authentication and room membership. POST creates a random task ID and writes a one-hour Redis mapping from that ID to the requester and room. GET checks this mapping before reading the Celery result; another user's task ID is not enough to see the report. A missing or expired mapping returns 404. The API calls Celery's synchronous publish and result methods in a thread pool so Redis I/O does not block FastAPI's event loop.

Trace POST in [app/routers/reports.py](app/routers/reports.py): check membership; make a random ID; store {room_id, user_id} under report:job:{task_id}; enqueue the task with that exact ID; return HTTP 202. HTTP 202 means “accepted for processing,” not “finished.” If publishing fails, delete the mapping and return 503.

Trace GET: check membership again; read the mapping; reject an unknown, expired, or differently owned task; ask Celery for its state; include the report only on SUCCESS and only if its room_id matches the URL. The extra room check prevents a mismatched result from being served as this room's report. The mapping expires after one hour, like the result, so a client should save a permanent report elsewhere if it needs one.

There is a small two-system gap between writing the ownership mapping and publishing the task. If publishing fails, the API deletes the mapping. If the process crashes in between, a mapping can remain without a queued task until it expires. For business-critical jobs, persist a job row and use an outbox or a reconciler rather than assuming a Redis mapping and broker publish are atomic.

Celery uses PENDING for a task it has not seen **and** for a task whose result is no longer in the backend. The ownership mapping plus its expiry makes an unknown ID return 404, but a lost queued job may still look PENDING until the mapping expires. Monitor queue age and worker health; do not treat PENDING alone as proof that work is progressing.

## 4. Run the complete example

Open separate terminals in the project root. First start Redis:

~~~powershell
docker compose up -d redis
~~~

Start FastAPI:

~~~powershell
python -m uvicorn app.main:app --reload
~~~

Start a Celery worker. On Windows, use the solo pool for a reliable local exercise:

~~~powershell
python -m celery -A app.celery_app:celery_app worker --loglevel=INFO --pool=solo
~~~

On Linux, the standard prefork pool can use multiple processes:

~~~bash
celery -A app.celery_app:celery_app worker --loglevel=INFO --concurrency=2
~~~

The worker log should list chat.room_statistics among registered tasks. Celery's [concurrency guide](https://docs.celeryq.dev/en/latest/userguide/concurrency/) describes the pool choices and Windows caveats.

Create an account and a room using the chat UI at http://127.0.0.1:8000. Enter the room and send a few messages. The creator is a room member and can request its report. Then obtain a JWT through the login API. A JWT is the token the API uses to recognize the signed-in user. In PowerShell, replace the credentials below with your own and use the room ID shown by the UI or returned by the room API:

~~~powershell
$login = Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/auth/login' -ContentType 'application/x-www-form-urlencoded' -Body @{ username = 'YOUR_USERNAME'; password = 'YOUR_PASSWORD' }
$headers = @{ Authorization = "Bearer $($login.access_token)" }
$rooms = Invoke-RestMethod -Method Get -Uri 'http://127.0.0.1:8000/api/rooms' -Headers $headers
$rooms | Format-Table id,name,is_member
$roomId = 1 # Replace with an id from the list where is_member is True
$job = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8000/api/rooms/$roomId/statistics-jobs" -Headers $headers
$job
$result = Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8000/api/rooms/$roomId/statistics-jobs/$($job.task_id)" -Headers $headers
$result
~~~

If the task is quick, the first poll may already show SUCCESS. If it shows PENDING, STARTED, or RETRY, poll again shortly. A successful response includes result.message_count. Stop the worker and submit another job: it stays queued until a worker starts. Restart the worker and poll again. This experiment makes the queue behavior visible.

Expected shape of a completed response:

~~~json
{
  "task_id": "a-random-uuid",
  "status": "SUCCESS",
  "result": {
    "room_id": 1,
    "message_count": 2,
    "contributor_count": 1,
    "latest_message_id": 2
  }
}
~~~

Your IDs and counts will differ. If the POST returns 403, check that the selected room has is_member=True. If it returns 503, check the broker URL and Redis connection. If GET stays PENDING after the worker starts, compare the API and worker broker URLs and look for chat.room_statistics in the worker's registered task list.

A focused automated exercise is available:

~~~powershell
python -m pytest tests/celery_reports_test.py -q
~~~

That test runs the real report query and HTTP endpoints with Celery eager mode and a fake Redis mapping. It verifies the code path, but it does **not** replace the manual three-process check above.

## 5. Choose the correct pattern in another project

Use **pub/sub** when every active consumer should hear an event immediately and a missed event can be recovered elsewhere or simply ignored: live UI updates, typing indicators, cache invalidation signals. Give channels meaningful prefixes and validate payloads. For a durable event log that consumers must replay independently, consider Redis Streams or another event system instead. [Redis Streams overview](https://redis.io/docs/latest/develop/data-types/streams/) explains the different model.

Use **Celery** when work should continue after an HTTP request returns or wait for a worker: reports, image processing, notifications, imports, scheduled maintenance. Put small identifiers and settings in the task message; let the worker load current data. Decide whether the result is needed. If nobody will poll it, you can avoid storing a result. If the result is a permanent business record, write it to your application's database; a Redis result backend with one-hour expiry is not a durable record.

For any job that changes the outside world, answer these before enabling retries or late acknowledgements:

1. What is the unique business operation ID?
2. If the worker runs twice, what stops a duplicate effect?
3. Which failures should retry, and how long?
4. How can an operator see a stuck job?
5. What should happen if the broker or database is unavailable?

For example, a payment job should enforce uniqueness using an idempotency key in the database or payment provider. Merely setting acks_late=True cannot make a non-idempotent task safe.

### A reusable pub/sub build sequence

When starting another project, make these decisions in order:

1. **Name the event and audience.** For example, order:{order_id} means every currently connected viewer of one order should hear an update. Avoid a global channel unless every subscriber truly needs every event.
2. **Decide what is permanent.** Write the important order or chat change to the database first. Use pub/sub as a live signal. Decide how a reconnecting client fetches changes it missed.
3. **Define a small event schema.** Include event type, object ID, event ID or version, and the fields the client needs. Validate incoming events and ignore unknown versions deliberately.
4. **Make one subscription owner per process or service.** Track which channels local clients need. Subscribe before promising a client live updates. Close subscriptions when no longer needed.
5. **Route a received event to local clients.** Keep the network subscription code separate from the socket registry, as PubSubBridge and ConnectionManager do here.
6. **Test two isolated consumers.** One process publishes; both active subscribers should receive it. Then disconnect one and prove it misses the event. Fetch the permanent record from the database to recover.
7. **Add failure handling.** Detect listener exit, reconnect and resubscribe, bound memory use, and measure subscriber count and delivery delay. If gaps are unacceptable, change the transport or add a durable log.

Ask yourself after step 3: “If this event is published twice, can the client identify it as the same event?” A database message ID lets a reconnecting chat client deduplicate.

### A reusable Celery build sequence

1. **Pick a bounded job.** Specify its input IDs, expected output, maximum runtime, and what counts as success.
2. **Choose a broker and decide whether results are needed.** Set broker URL, result backend if required, and expiry. Make worker and producer use the same settings.
3. **Create one Celery app and register tasks.** Start a worker and confirm the task name appears before sending work.
4. **Keep arguments serializable and small.** Pass an object ID; load the current object in the worker. Do not put secrets, sessions, or large files into the task message.
5. **Queue from the request and return a tracking ID.** Authenticate first. Keep synchronous broker calls off an async web loop. Decide how clients are authorized to poll.
6. **Make repeat execution safe.** Choose a business idempotency key for side effects. Retry only errors that might succeed on another attempt; set time limits and backoff intentionally.
7. **Test both modes.** Eager mode checks function and API wiring. A real broker and separate worker check the actual queue and process boundary. Stop the worker, enqueue, restart, and confirm the job is consumed.
8. **Operate it.** Monitor queue age, failure rate, retries, and worker health. Set capacity and separate queues when job types have different runtimes.

If a job must be permanently visible to users, create an application job table with owner, input, status, timestamps, and output location. A Celery result backend is useful for short-lived tracking, but its expiry makes it unsuitable as the only permanent record.

## 6. Scale and failure checklist

This repository is a teaching base. These are the changes to make as load and reliability requirements rise.

- **Database:** Move from local SQLite to a shared PostgreSQL service before running the API and worker on different machines. Use migrations rather than create_all for schema changes.
- **Pub/sub recovery:** A subscriber that disconnects misses messages. On reconnect, fetch message history from SQL using the last seen message ID. The current bridge logs a listener crash but does not restart it automatically.
- **SQL commit then publish:** A crash after SQL commit but before PUBLISH leaves a stored message without a live notification. For stronger delivery, use a transactional outbox: save the message and an outbox row in one SQL transaction, then a worker publishes the outbox row and marks it delivered. Clients should deduplicate by message ID.
- **Presence:** Each socket now owns an atomic Redis lease, renewed every 20 seconds and expiring after 60 seconds. Disconnect removes that socket only; worker startup never clears other workers. Crash recovery is bounded by lease expiry and the next presence broadcast. See [architecture](docs/architecture.md).
- **Subscription count:** The current bridge opens one Redis PubSub connection per active room per process. At thousands of rooms, multiplex many room subscriptions on one PubSub connection per process and route each incoming channel to the matching local room. Keep room names namespaced.
- **Subscription lifecycle:** Shutdown now cancels every subscription regardless of reference count. A failed listener closes its local room sockets; a later join can replace a finished listener. Automatic browser reconnect, backpressure queues and continuous outage recovery remain future work.
- **Large fan-out:** Standard Redis pub/sub is ephemeral. On Redis Cluster, consider sharded pub/sub when channel throughput becomes a cluster-wide cost; measure before changing the design. See [Redis sharded pub/sub](https://redis.io/docs/latest/develop/interact/pubsub/).
- **Worker capacity:** Separate queues for short and long jobs, set concurrency deliberately, monitor queue depth and oldest task age, and apply request-side rate limits. worker_prefetch_multiplier=1 is a useful starting point for longer jobs, not a universal optimum.
- **Task reliability:** Configure broker persistence and a no-eviction policy if queued work must survive Redis restart. Redis AOF narrows the persistence window but is not a substitute for a durable business database. Understand the Redis broker visibility timeout before using long-running tasks or long countdowns. See [Celery's Redis caveats](https://docs.celeryq.dev/en/latest/getting-started/backends-and-brokers/redis.html).
- **Observability:** Record task ID, room ID, retries, duration, and failures in structured logs; expose worker and broker health separately from the web /health endpoint. Never log JWTs or full Redis credentials.
- **Deployment:** The [Render Blueprint](render.yaml) and [Compose stack](docker-compose.yml) now include a separate Celery worker sharing PostgreSQL and Redis with the API. Render resources are paid; review [deployment instructions](docs/deployment.md) before applying.

## 7. Troubleshooting by symptom

| Symptom | First checks |
| --- | --- |
| WebSocket users on different workers miss messages | Is Redis reachable from both? Do both subscribe to the same namespaced room channel? Run the multiworker test. |
| Task remains PENDING | Is the Celery worker running and listening to the same broker URL? Does its startup log list chat.room_statistics? Check queue age. |
| Worker says “unregistered task” | Start it with -A app.celery_app:celery_app and confirm app.tasks is imported. Restart after code changes. |
| Worker report has zero messages | Is the worker using the same DATABASE_URL and database file/service as the API? For SQLite, compare working directories. |
| API polling returns 404 | Check the task ID, requesting user, room membership, and one-hour mapping expiry. |
| Task runs twice | Review retries, late acknowledgements, broker visibility timeout, and idempotency. Duplicate execution must be safe by design. |
| Redis works locally but fails in cloud | Check TLS scheme (rediss://), network policy, credentials, and whether the provider supports the Redis protocol required by both pub/sub and Celery. |

## 8. Practice until the pattern is yours

1. Add a “messages per contributor” field to the report. Keep the task argument as room_id and return JSON-friendly data.
2. Run two Celery workers, submit several reports, and observe that each report goes to one worker.
3. Run two chat web instances and confirm one message reaches clients on both.
4. Disconnect Bob while Alice remains connected, have Alice send a message, then reconnect Bob and recover the saved message from SQL history. Explain why Bob missed the live event.
5. Design an idempotency key and outbox row for a hypothetical “send report by email” job before implementing the email side effect.

If you can explain why exercise 2 distributes jobs while exercise 3 broadcasts events, you have the key distinction between Celery and pub/sub.

### Check your understanding

Cover the answers and predict first:

| Situation | Expected answer |
| --- | --- |
| Three web processes subscribe to room 42. One PUBLISH occurs. | All three active Redis subscribers receive the event; each sends only to its own local sockets. |
| Three Celery workers listen to the same queue. One report task is enqueued. | One worker claims that task; retries or redelivery can cause another execution later. |
| No web process subscribes when a typing event is published. | Nobody receives it; that is acceptable for a temporary indicator. |
| No Celery worker is running when a report is queued. | The job waits in the broker while Redis retains it; a later worker can take it. |
| The report task returns SUCCESS but the browser closes. | The temporary result remains in the backend until it expires; the browser can poll with the same authorized task ID before then. |
| A worker sends an email, then crashes before acknowledging. | The task may run again; use a business idempotency key to avoid sending twice. |

**Code-reading test:** Without this guide open, find the lines that (a) save a chat message, (b) publish its live event, (c) subscribe to a room, (d) broadcast to local sockets, (e) enqueue a report, and (f) return the report result. If you can explain each boundary in your own words and reproduce both the live-message and queued-job experiments, you have moved past copy-and-paste setup into working understanding.

## Official references

- [Redis pub/sub semantics](https://redis.io/docs/latest/develop/interact/pubsub/)
- [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/)
- [Celery Redis broker and result backend](https://docs.celeryq.dev/en/latest/getting-started/backends-and-brokers/redis.html)
- [Celery tasks, acknowledgements, and retries](https://docs.celeryq.dev/en/latest/userguide/tasks.html)
- [Celery worker concurrency](https://docs.celeryq.dev/en/latest/userguide/concurrency/)
