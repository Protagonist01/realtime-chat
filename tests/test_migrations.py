import ast
import shutil

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect

from app.config import settings


def test_sqlite_migration_upgrade_repeat_check_and_downgrade(tmp_path, monkeypatch):
    path = tmp_path / "migrations.db"
    monkeypatch.setattr(settings, "database_url", f"sqlite+aiosqlite:///{path.as_posix()}")
    config = Config("alembic.ini")
    command.upgrade(config, "head")
    command.upgrade(config, "head")
    command.check(config)
    engine = create_engine(f"sqlite:///{path.as_posix()}")
    try:
        assert set(inspect(engine).get_table_names()) == {
            "users",
            "rooms",
            "room_members",
            "messages",
            "alembic_version",
        }
        assert "reply_to_id" in {c["name"] for c in inspect(engine).get_columns("messages")}
    finally:
        engine.dispose()
    command.downgrade(config, "base")


def test_new_revision_template_generates_valid_python(tmp_path):
    script_dir = tmp_path / "migrations"
    shutil.copytree("migrations", script_dir)
    config = Config("alembic.ini")
    config.set_main_option("script_location", str(script_dir))
    revision = command.revision(config, message="template verification")
    ast.parse(open(revision.path, encoding="utf-8").read())
    assert revision.down_revision == "0001"
