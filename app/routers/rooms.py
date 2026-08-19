from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_db
from app.deps import get_current_user
from app.models import Message, Room, User, room_members
from app.schemas import MessageOut, RoomCreate, RoomOut

router = APIRouter(prefix="/api/rooms", tags=["rooms"])


async def _member_ids(db: AsyncSession, room_id: int) -> set[int]:
    """Return the set of user ids that belong to ``room_id``.

    We query the association table directly instead of accessing
    ``room.members`` because the relationship is lazy-loaded: in async
    SQLAlchemy, touching a lazy relationship raises ``MissingGreenlet``.
    """
    res = await db.scalars(
        select(room_members.c.user_id).where(room_members.c.room_id == room_id)
    )
    return set(res)


async def _is_member(db: AsyncSession, user_id: int, room_id: int) -> bool:
    row = await db.execute(
        select(room_members).where(
            room_members.c.user_id == user_id, room_members.c.room_id == room_id
        )
    )
    return row.first() is not None


@router.post("", response_model=RoomOut, status_code=status.HTTP_201_CREATED)
async def create_room(
    payload: RoomCreate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> Room:
    existing = await db.scalar(select(Room).where(Room.name == payload.name))
    if existing is not None:
        raise HTTPException(status_code=400, detail="Room name already taken")
    room = Room(name=payload.name, is_private=payload.is_private, created_by=user.id)
    db.add(room)
    await db.flush()  # populate room.id
    await db.execute(room_members.insert().values(user_id=user.id, room_id=room.id))
    await db.commit()
    await db.refresh(room)
    return room


@router.get("", response_model=list[RoomOut])
async def list_my_rooms(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[Room]:
    res = await db.scalars(
        select(Room).join(room_members).where(room_members.c.user_id == user.id)
    )
    return list(res)


@router.get("/{room_id}", response_model=RoomOut)
async def get_room(
    room_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> Room:
    room = await db.get(Room, room_id)
    if room is None:
        raise HTTPException(status_code=404, detail="Room not found")
    if room.is_private and not await _is_member(db, user.id, room_id):
        raise HTTPException(status_code=403, detail="Not a member of this room")
    return room


@router.post("/{room_id}/join", response_model=RoomOut)
async def join_room(
    room_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> Room:
    room = await db.get(Room, room_id)
    if room is None:
        raise HTTPException(status_code=404, detail="Room not found")
    if await _is_member(db, user.id, room_id):
        raise HTTPException(status_code=400, detail="Already a member")
    await db.execute(room_members.insert().values(user_id=user.id, room_id=room_id))
    await db.commit()
    await db.refresh(room)
    return room


@router.post("/{room_id}/leave", status_code=status.HTTP_204_NO_CONTENT)
async def leave_room(
    room_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> None:
    room = await db.get(Room, room_id)
    if room is None:
        raise HTTPException(status_code=404, detail="Room not found")
    if not await _is_member(db, user.id, room_id):
        raise HTTPException(status_code=400, detail="Not a member")
    await db.execute(
        room_members.delete().where(
            room_members.c.user_id == user.id,
            room_members.c.room_id == room_id,
        )
    )
    await db.commit()


@router.get("/{room_id}/messages", response_model=list[MessageOut])
async def list_messages(
    room_id: int,
    limit: int = Query(50, ge=1, le=200),
    before: int | None = Query(None, description="Return messages with id < before (cursor)"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[Message]:
    room = await db.get(Room, room_id)
    if room is None:
        raise HTTPException(status_code=404, detail="Room not found")
    if room.is_private and not await _is_member(db, user.id, room_id):
        raise HTTPException(status_code=403, detail="Not a member of this room")

    stmt = (
        select(Message)
        .options(selectinload(Message.user))
        .where(Message.room_id == room_id)
        .order_by(Message.id.desc())
        .limit(limit)
    )
    if before is not None:
        stmt = stmt.where(Message.id < before)
    res = await db.scalars(stmt)
    # Newest-first from DB; clients usually want oldest-first.
    return list(reversed(list(res)))
