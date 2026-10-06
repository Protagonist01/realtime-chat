from sqlalchemy import event, inspect, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import settings


class Base(DeclarativeBase):
    pass


engine = create_async_engine(
    settings.database_url,
    echo=False,
    future=True,
)

if engine.dialect.name == "sqlite":

    @event.listens_for(engine.sync_engine, "connect")
    def enable_foreign_keys(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db() -> AsyncSession:
    """FastAPI dependency that yields an async DB session."""
    async with AsyncSessionLocal() as session:
        yield session


async def init_db() -> None:
    """Create all tables. Called on startup. In a real app, use Alembic migrations."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Lightweight column migration: create_all() won't add new columns to
        # tables that already exist (e.g. an older chat.db on disk).
        columns = await conn.run_sync(lambda sync_conn: inspect(sync_conn).get_columns("messages"))
        if "reply_to_id" not in {column["name"] for column in columns}:
            await conn.execute(
                text("ALTER TABLE messages ADD COLUMN reply_to_id INTEGER REFERENCES messages (id)")
            )
            await conn.execute(
                text("CREATE INDEX ix_messages_reply_to_id ON messages (reply_to_id)")
            )
