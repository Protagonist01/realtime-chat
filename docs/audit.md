# Portfolio audit evidence

**Date:** 2026-10-06

**Repository:** [Protagonist01/realtime-chat](https://github.com/Protagonist01/realtime-chat)

**Reviewed remote revision:** `f9dcadc8227ba611a5e8dd31f208a66de7f133e9`

**Scope:** application modules, browser client, tests, dependency locks, Docker/Compose, Render Blueprint, migrations, documentation and GitHub deployment records. Live URL testing was excluded at the owner's request.

## Assessment

The revised project is ready for recruiter review: its main claims have runnable examples and direct checks. At the time of this audit, the changes were local and had not been committed, pushed or deployed. Publication is recorded in Git history; a successful hosted CI run is still needed to verify the full Linux/Python matrix.

It demonstrates distributed chat and background jobs with clear limits. This audit does not certify production security, sustained capacity or a live service.

## Findings and changes

| Priority | Finding | Change and evidence |
| --- | --- | --- |
| High | Startup wiped presence belonging to other API workers; tab counters could leave incorrect online users | Per-socket expiring Redis leases with atomic updates. Multiple-tab, expiry and two-process tests passed. |
| High | The older multiworker test shared application state in one interpreter | Two independent Uvicorn processes now share isolated PostgreSQL and real Redis. Messages, replies, typing, presence and cross-worker membership revocation passed. |
| High | Hosted SQLite was temporary and inaccessible to a separate worker | Paid Render Blueprint and local Compose use shared PostgreSQL; Alembic runs before API startup. A saved message survived API container recreation. Hosted rollout and data transfer remain separate work. |
| High | Vulnerable JWT dependencies; silent bcrypt password truncation | Replaced python-jose/ecdsa with PyJWT, require expiration and valid subject, restrict HS256, reject passwords exceeding 72 UTF-8 bytes. Runtime and development dependency audits found no known vulnerabilities. |
| High | Private-room access and sockets could outlive membership | Protected private joining, publish revocation, recheck membership and expiration during socket commands and heartbeats. Regression tests passed. |
| Medium | Credentials appeared in browser socket URLs and persistent local storage | First-frame authentication and session storage. Legacy query-token clients remain supported and need proxy log redaction. |
| Medium | Queue failures, repeated requests and subscription lifecycle lacked safeguards | Authentication/report/command limits, explicit failure responses, bounded Celery configuration, subscription acknowledgment and cleanup. Separate worker execution and queue failure tests passed. |
| Medium | Browser identity, timestamp and mobile navigation defects | Canonical identity for email login/registration, UTC timestamps, friendly validation errors, chronological history and correct cursor, clean sign-out state and mobile room access before selection. Browser evidence is recorded below. |
| Medium | No repeatable release checks, production migration path or complete setup | Dependency locks, non-root image, health/readiness checks, migration tests, GitHub Actions, Dependabot and deployment instructions added. |
| Medium | Recruiter-facing explanation lacked verified scope and licensing | README, architecture/tradeoffs, deployment guide, screenshots, contribution/security guidance and owner-selected MIT license added. |

## Checks actually run

| Check | Result |
| --- | --- |
| Complete pytest run with real PostgreSQL and Redis | **23 passed** in 226.33 seconds on Python 3.12.13 / Windows. This includes two OS-process integration tests. |
| Final migration upgrade/check/downgrade and new-revision template test | **2 passed** after correcting the template. One new regression test brings the current collection to 24 tests; the earlier complete run contained 23. |
| Ruff lint and format | Passed for application, maintained tests, process helpers and migrations (30 Python files formatted). |
| Dependency resolution | `pip check` passed in the project virtual environment. |
| Vulnerability scans | Both pinned runtime and development requirements reported no known vulnerabilities at audit time. This is a known-advisory check, not proof of all security properties. |
| Docker image and Compose | Built Python 3.13 image; migrations completed; PostgreSQL, Redis and API were healthy. API runs as UID **10001**. Final images were rebuilt after the browser fixes, became ready, and the packaged frontend hash matched the workspace. |
| Running container workflow | Readiness, register/login, room creation, first-frame WebSocket authentication, message persistence and a separate Celery worker's successful statistics result passed. |
| Persistence check | Message row count stayed **1** before and after forced API container recreation; readiness recovered. |
| Render configuration | Validated against the provider's [official Blueprint JSON schema](https://render.com/schema/render.yaml.json). No resources were provisioned. |
| Browser JavaScript and documentation | Node syntax check and local documentation link checks passed. |
| CI configuration | YAML structure validated; actions pinned to verified commit SHAs from [checkout](https://github.com/actions/checkout) and [setup-python](https://github.com/actions/setup-python). Hosted execution awaits publication. |
| Git whitespace | `git diff --check` passed. Windows line-ending conversion notices are informational. |
| Git history | No tracked environment/database/private-key files or matches for the common credential patterns checked. This limited scan is not a forensic secret audit. |

The suite emitted one Starlette warning about its HTTPX test-client compatibility. The warning concerns test tooling; the checked workflows passed. A failed earlier subprocess test included an unrelated inherited AGENTROUTER_API_KEY in pytest diagnostics. Subprocess environments now allow only required operating-system and test settings. Rotate that unrelated key if it remains active; its value is deliberately absent from repository documentation.

### Browser evidence

The local app was exercised in Microsoft Edge using Playwright: registration, room creation, sending a message and loading saved history after reload. Screenshots were visually inspected at **1440 × 1000** and **390 × 844**.

- [Desktop chat](images/chat-desktop.png)
- [Mobile chat](images/chat-mobile.png)

Mobile initial-room navigation, email login and sign-out cleanup were subsequently checked against the final frontend. The final history check returned ascending message IDs [1, 2] and cursor 1; sign-out left zero visible messages and cleared room/identity state. These are viewport checks on a desktop browser, not physical-device or full accessibility certification.

## GitHub and deployment records

Read-only GitHub API inspection found a public repository, **0 Actions runs** and **10 deployment records** at the audit time. The three latest records reported success:

| Deployment ID | Revision | Recorded date | Latest state |
| --- | --- | --- | --- |
| 6047825397 | `f9dcadc8227ba611a5e8dd31f208a66de7f133e9` | 2026-08-23 | success |
| 6047613655 | `f6cbc0a0b7e6f7a23c0018c82f3954df31ac4831` | 2026-08-23 | success |
| 6047171045 | `5af6332a476411302198784bbba06cfcfad34fb5` | 2026-08-23 | success |

These records establish what GitHub reported for those revisions. They do not establish current live availability or deploy the local changes. Inspect the [repository Actions](https://github.com/Protagonist01/realtime-chat/actions) and [deployment records](https://api.github.com/repos/Protagonist01/realtime-chat/deployments) after publication.

## Remaining limits

- Run the new GitHub Actions matrix on Linux/Python 3.12 and 3.13 after publishing. Local Python 3.12 tests and Python 3.13 container checks do not replace that matrix.
- Render resources are paid. Review pricing and existing service/data settings before Blueprint sync; migrating old SQLite data requires explicit backup and transfer.
- Live Redis events have no replay or delivery acknowledgment. A database commit can succeed before a Redis publish fails. Load testing, slow-client queues, a durable outbox and automatic reconnect are future work.
- Private rooms are owner-only; invitations, moderation, password recovery, email verification and global connection quotas are absent. The six-character password minimum is a basic demo policy and should be strengthened before exposing a production service.
- Session tokens remain accessible to page scripts. HTTPS/WSS and trusted proxy configuration are required when hosted.
- Readiness does not prove a Celery worker is alive. Monitor successful job completion.
- Existing bcrypt passwords beyond the new byte limit need a managed reset.

See [architecture](architecture.md) for the exact failure behavior and [deployment](deployment.md) for rollout steps.
