"""Authenticated API for queuing and polling a room statistics job."""

import json
from contextlib import suppress
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from app.celery_app import celery_app
from app.config import settings
from app.database import get_db
from app.deps import get_current_user
from app.models import Room, User, room_members
from app.rate_limit import enforce_limit
from app.tasks import room_statistics

router = APIRouter(prefix="/api/rooms", tags=["reports"])
JOB_TTL_SECONDS = 3600


async def _require_member(db: AsyncSession, room_id: int, user_id: int) -> None:
    if await db.get(Room, room_id) is None:
        raise HTTPException(status_code=404, detail="Room not found")
    membership = await db.execute(
        select(room_members.c.user_id).where(
            room_members.c.room_id == room_id,
            room_members.c.user_id == user_id,
        )
    )
    if membership.first() is None:
        raise HTTPException(status_code=403, detail="Not a member of this room")


@router.post("/{room_id}/statistics-jobs", status_code=status.HTTP_202_ACCEPTED)
async def create_statistics_job(
    room_id: int,
    request: Request,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, str]:
    await _require_member(db, room_id, user.id)
    await enforce_limit(request, "reports", str(user.id), 5)
    task_id = str(uuid4())
    key = f"{settings.redis_namespace}:report:job:{task_id}"
    # The short-lived mapping prevents a caller from polling an arbitrary
    # Celery task ID and binds the job to its requester and room.
    try:
        await request.app.state.redis_client.setex(
            key, JOB_TTL_SECONDS, json.dumps({"room_id": room_id, "user_id": user.id})
        )
        # Celery's producer uses synchronous I/O; keep it off the ASGI loop.
        await run_in_threadpool(room_statistics.apply_async, args=(room_id,), task_id=task_id)
    except Exception:
        with suppress(Exception):
            await request.app.state.redis_client.delete(key)
        raise HTTPException(status_code=503, detail="Could not queue report") from None
    return {"task_id": task_id, "status": "PENDING"}


@router.get("/{room_id}/statistics-jobs/{task_id}")
async def get_statistics_job(
    room_id: int,
    task_id: UUID,
    request: Request,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    await _require_member(db, room_id, user.id)
    try:
        raw = await request.app.state.redis_client.get(
            f"{settings.redis_namespace}:report:job:{task_id}"
        )
    except Exception:
        raise HTTPException(status_code=503, detail="Report tracking unavailable") from None
    if raw is None:
        raise HTTPException(status_code=404, detail="Job not found or expired")
    owner = json.loads(raw)
    if owner != {"room_id": room_id, "user_id": user.id}:
        raise HTTPException(status_code=404, detail="Job not found")

    result = celery_app.AsyncResult(str(task_id))
    try:
        state = await run_in_threadpool(lambda: result.state)
    except Exception:
        raise HTTPException(status_code=503, detail="Report results unavailable") from None
    response: dict = {"task_id": str(task_id), "status": state}
    if state == "SUCCESS":
        report = await run_in_threadpool(lambda: result.result)
        if not isinstance(report, dict) or report.get("room_id") != room_id:
            raise HTTPException(status_code=500, detail="Invalid report result")
        response["result"] = report
    elif state == "FAILURE":
        response["detail"] = "Report failed; check worker logs"
    return response
