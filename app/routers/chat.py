"""WebSocket chat endpoint.

Auth via query string
---------------------
Browsers cannot set custom headers on a ``new WebSocket()`` connection, so the
JWT is passed as ``?token=...``. This is the standard workaround; the token
still never appears in server logs if you log request paths with redaction,
and it lives in memory only (not persisted in URL history on the server).

Lifecycle
---------
1. Pull token from ``query_params``, decode it, load the user. If any of
   that fails we close with code 4401 *before* accepting — this is valid ASGI
   and avoids spinning up a live connection for an unauthenticated peer.
2. Load the room, verify membership.
3. ``manager.connect`` accepts the socket and registers it.
4. ``bridge.ensure_subscribed`` makes sure this process is listening on the
   room's Redis channel.
5. ``presence.user_connected`` adds the user to the Redis presence hash,
   returns the full online list, which we publish as a ``presence`` event.
6. Publish a ``join`` event.
7. Receive loop: parse ``ClientCommand``, dispatch on ``type``:
   - ``message``: persist, publish ``message`` event.
   - ``typing``: publish ``typing`` event (no persistence — ephemeral).
8. On disconnect: deregister, publish ``leave``. If the user's last local
   connection closed, update presence and publish ``presence`` event.
   Drop the Redis pub/sub subscription if no local clients remain.
"""

import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import AsyncSessionLocal
from app.models import Message, Room, User, room_members
from app.schemas import ChatEvent, ClientCommand
from app.security import decode_access_token

logger = logging.getLogger(__name__)
router = APIRouter(tags=["chat"])

CLOSE_AUTH_FAILURE = 4401
CLOSE_ROOM_NOT_FOUND = 4404
CLOSE_FORBIDDEN = 4403


async def _load_user(db: AsyncSession, token: str | None) -> User | None:
    if not token:
        return None
    data = decode_access_token(token)
    if data is None or data.user_id is None:
        return None
    return await db.get(User, data.user_id)


async def _is_member(db: AsyncSession, user_id: int, room_id: int) -> bool:
    row = await db.execute(
        select(room_members).where(
            room_members.c.user_id == user_id, room_members.c.room_id == room_id
        )
    )
    return row.first() is not None


@router.websocket("/ws/chat/{room_id}")
async def chat_ws(websocket: WebSocket, room_id: int) -> None:
    manager = websocket.app.state.connection_manager
    bridge = websocket.app.state.pubsub_bridge
    presence = websocket.app.state.presence

    # --- Auth & membership (before accepting) ---
    async with AsyncSessionLocal() as db:
        user = await _load_user(db, websocket.query_params.get("token"))
        if user is None:
            await websocket.close(code=CLOSE_AUTH_FAILURE)
            return
        room = await db.get(Room, room_id)
        if room is None:
            await websocket.close(code=CLOSE_ROOM_NOT_FOUND)
            return
        member = await _is_member(db, user.id, room_id)
        if not member:
            await websocket.close(code=CLOSE_FORBIDDEN)
            return

    # --- Live connection starts here ---
    await manager.connect(room_id, user.id, websocket)
    await bridge.ensure_subscribed(room_id)

    # Presence: add user to Redis, publish the full online list.
    online = await presence.user_connected(room_id, user.id, user.username)
    await bridge.publish(
        room_id, ChatEvent(type="presence", room_id=room_id, online_users=online)
    )
    await bridge.publish(
        room_id,
        ChatEvent(type="join", room_id=room_id, user_id=user.id, username=user.username),
    )

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                cmd = ClientCommand.model_validate_json(raw)
            except Exception:
                await _send_error(websocket, room_id, "Invalid command payload")
                continue

            if cmd.type == "message":
                if not cmd.content or len(cmd.content) > 4000:
                    await _send_error(websocket, room_id, "Message content required (1-4000 chars)")
                    continue

                # Resolve the replied-to message (must exist in the same room).
                # Eager-load the author so username is readable after the
                # session closes.
                replied = None
                if cmd.reply_to_id is not None:
                    async with AsyncSessionLocal() as db:
                        res = await db.execute(
                            select(Message)
                            .options(selectinload(Message.user))
                            .where(Message.id == cmd.reply_to_id)
                        )
                        replied = res.scalar_one_or_none()
                    if replied is None or replied.room_id != room_id:
                        await _send_error(websocket, room_id, "Replied-to message not found in this room")
                        continue

                async with AsyncSessionLocal() as db:
                    message = Message(
                        room_id=room_id,
                        user_id=user.id,
                        content=cmd.content,
                        reply_to_id=replied.id if replied else None,
                    )
                    db.add(message)
                    await db.commit()
                    await db.refresh(message)
                    event = ChatEvent(
                        type="message",
                        room_id=room_id,
                        user_id=user.id,
                        username=user.username,
                        content=message.content,
                        message_id=message.id,
                        created_at=message.created_at,
                        reply_to_id=message.reply_to_id,
                        # Reply context for rendering the quote client-side
                        reply_to_username=replied.username if replied else None,
                        reply_to_content=replied.content[:119] + "…" if replied and len(replied.content) > 120 else (replied.content if replied else None),
                    )
                await bridge.publish(room_id, event)

            elif cmd.type == "typing":
                await bridge.publish(
                    room_id,
                    ChatEvent(type="typing", room_id=room_id, user_id=user.id, username=user.username),
                )

            else:
                await _send_error(websocket, room_id, f"Unknown command type: {cmd.type}")

    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("unexpected error on room %s for user %s", room_id, user.id)
    finally:
        manager.disconnect(room_id, user.id, websocket)
        await bridge.publish(
            room_id,
            ChatEvent(type="leave", room_id=room_id, user_id=user.id, username=user.username),
        )
        # Only update presence when the user's last local connection closes
        # (multi-tab: user still has another socket open in this process).
        if user.id not in manager.local_user_ids(room_id):
            online = await presence.user_disconnected(room_id, user.id)
            await bridge.publish(
                room_id, ChatEvent(type="presence", room_id=room_id, online_users=online)
            )
        await bridge.maybe_unsubscribe(room_id)


async def _send_error(ws: WebSocket, room_id: int, detail: str) -> None:
    event = ChatEvent(type="error", room_id=room_id, content=detail)
    try:
        await ws.send_text(event.model_dump_json())
    except Exception:
        pass
