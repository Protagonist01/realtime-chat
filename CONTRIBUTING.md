# Contributing

Use Python 3.12 or 3.13 and an isolated virtual environment. Install `requirements-dev.txt`, copy `.env.example` to `.env`, and follow the README.

Before submitting changes, run pytest, Ruff and the dependency audit listed in the README. Supply disposable Redis/PostgreSQL URLs for the real-process tests. Run the Compose stack when changing the Docker image or service wiring.

For schema changes, add an Alembic revision, review the generated operations and test upgrades on both SQLite and PostgreSQL. Never modify a previously deployed revision. Update API docs and the Build Book when a decision or diagnostic lesson changes.

Do not commit real credentials, personal chat databases, logs or generated output. Compose and CI use clearly documented disposable credentials. Describe the user-visible change and the checks run in pull requests.
