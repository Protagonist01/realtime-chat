"""Background jobs executed by a separate Celery worker process."""

import asyncio

from sqlalchemy import distinct, func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.celery_app import celery_app
from app.config import settings
from app.models import Message


async def _room_statistics(room_id: int) -> dict[str, int | None]:
    # Create the engine inside the worker task: no live DB connection is
    # inherited from the web process or a Celery prefork parent.
    engine = create_async_engine(settings.database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as db:
            row = (
                await db.execute(
                    select(
                        func.count(Message.id),
                        func.count(distinct(Message.user_id)),
                        func.max(Message.id),
                    ).where(Message.room_id == room_id)
                )
            ).one()
            return {
                "room_id": room_id,
                "message_count": row[0],
                "contributor_count": row[1],
                "latest_message_id": row[2],
            }
    finally:
        await engine.dispose()


@celery_app.task(
    name="chat.room_statistics",
    autoretry_for=(OperationalError,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
)
def room_statistics(room_id: int) -> dict[str, int | None]:
    """Compute a read-only room snapshot; duplicate execution is harmless."""
    return asyncio.run(_room_statistics(room_id))
