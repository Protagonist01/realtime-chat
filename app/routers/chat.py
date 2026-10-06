"""Authenticated WebSocket chat, persisted messages and Redis fan-out.

Browser clients send {"type":"auth","token":"..."} as their first frame.
Query-string tokens remain supported for existing API clients; avoid them in
browsers and redact query strings in reverse-proxy logs.
"""

import asyncio
import logging
import time
from contextlib import suppress
from uuid import uuid4

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import AsyncSessionLocal
from app.models import Message, Room, User, room_members
from app.presence import HEARTBEAT_SECONDS
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
    return (
        await db.scalar(
            select(room_members.c.user_id).where(
                room_members.c.user_id == user_id, room_members.c.room_id == room_id
            )
        )
        is not None
    )


async def _send_error(ws: WebSocket, room_id: int, detail: str) -> None:
    with suppress(Exception):
        await ws.send_text(
            ChatEvent(type="error", room_id=room_id, content=detail).model_dump_json()
        )


async def _heartbeat(ws, room_id, user, token, connection_id, online):
    bridge = ws.app.state.pubsub_bridge
    presence = ws.app.state.presence
    try:
        while True:
            await asyncio.sleep(HEARTBEAT_SECONDS)
            if decode_access_token(token) is None:
                await ws.close(code=CLOSE_AUTH_FAILURE)
                return
            async with AsyncSessionLocal() as db:
                if not await _is_member(db, user.id, room_id):
                    await ws.close(code=CLOSE_FORBIDDEN)
                    return
            updated = await presence.user_connected(room_id, user.id, user.username, connection_id)
            if updated != online:
                await bridge.publish(
                    room_id, ChatEvent(type="presence", room_id=room_id, online_users=updated)
                )
            online = updated
    except Exception:
        logger.exception("Presence heartbeat failed in room %s", room_id)
        with suppress(Exception):
            await ws.close(code=1011)


@router.websocket("/ws/chat/{room_id}")
async def chat_ws(websocket: WebSocket, room_id: int) -> None:
    manager = websocket.app.state.connection_manager
    bridge = websocket.app.state.pubsub_bridge
    presence = websocket.app.state.presence
    token = websocket.query_params.get("token")
    if token is None:
        await websocket.accept()
        try:
            auth = await asyncio.wait_for(websocket.receive_json(), timeout=5)
            token = (
                auth.get("token") if isinstance(auth, dict) and auth.get("type") == "auth" else None
            )
            if not isinstance(token, str):
                token = None
        except (TimeoutError, ValueError, WebSocketDisconnect):
            with suppress(Exception):
                await websocket.close(code=CLOSE_AUTH_FAILURE)
            return

    async with AsyncSessionLocal() as db:
        user = await _load_user(db, token)
        if user is None:
            await websocket.close(code=CLOSE_AUTH_FAILURE)
            return
        if await db.get(Room, room_id) is None:
            await websocket.close(code=CLOSE_ROOM_NOT_FOUND)
            return
        if not await _is_member(db, user.id, room_id):
            await websocket.close(code=CLOSE_FORBIDDEN)
            return

    connection_id = str(uuid4())
    subscribed = False
    connected = False
    heartbeat = None
    try:
        await bridge.ensure_subscribed(room_id)
        subscribed = True
        await manager.connect(room_id, user.id, websocket)
        previous_online = await presence.get_online(room_id)
        online = await presence.user_connected(room_id, user.id, user.username, connection_id)
        connected = True
        await bridge.publish(
            room_id, ChatEvent(type="presence", room_id=room_id, online_users=online)
        )
        # Join/leave describe users, not tabs.
        if str(user.id) not in previous_online:
            await bridge.publish(
                room_id,
                ChatEvent(type="join", room_id=room_id, user_id=user.id, username=user.username),
            )
        heartbeat = asyncio.create_task(
            _heartbeat(websocket, room_id, user, token, connection_id, online)
        )
        window_start, commands = time.monotonic(), 0
        while True:
            raw = await websocket.receive_text()
            now = time.monotonic()
            if now - window_start >= 1:
                window_start, commands = now, 0
            commands += 1
            if commands > 30:
                await websocket.close(code=4429)
                break
            if len(raw.encode("utf-8")) > 20000:
                await websocket.close(code=1009)
                break
            if decode_access_token(token) is None:
                await websocket.close(code=CLOSE_AUTH_FAILURE)
                break
            async with AsyncSessionLocal() as db:
                if not await _is_member(db, user.id, room_id):
                    await websocket.close(code=CLOSE_FORBIDDEN)
                    break
            try:
                cmd = ClientCommand.model_validate_json(raw)
            except ValueError:
                await _send_error(websocket, room_id, "Invalid command payload")
                continue
            if cmd.type == "message":
                if not cmd.content or not cmd.content.strip():
                    await _send_error(websocket, room_id, "Message content required (1-4000 chars)")
                    continue
                async with AsyncSessionLocal() as db:
                    replied = None
                    if cmd.reply_to_id is not None:
                        replied = await db.scalar(
                            select(Message)
                            .options(selectinload(Message.user))
                            .where(Message.id == cmd.reply_to_id, Message.room_id == room_id)
                        )
                        if replied is None:
                            await _send_error(
                                websocket, room_id, "Replied-to message not found in this room"
                            )
                            continue
                    message = Message(
                        room_id=room_id,
                        user_id=user.id,
                        content=cmd.content,
                        reply_to_id=replied.id if replied else None,
                    )
                    db.add(message)
                    await db.commit()
                    await db.refresh(message)
                    snippet = None if replied is None else replied.content
                    if snippet and len(snippet) > 120:
                        snippet = snippet[:119] + "…"
                    event = ChatEvent(
                        type="message",
                        room_id=room_id,
                        user_id=user.id,
                        username=user.username,
                        content=message.content,
                        message_id=message.id,
                        created_at=message.created_at,
                        reply_to_id=message.reply_to_id,
                        reply_to_username=replied.username if replied else None,
                        reply_to_content=snippet,
                    )
                await bridge.publish(room_id, event)
            elif cmd.type == "typing":
                await bridge.publish(
                    room_id,
                    ChatEvent(
                        type="typing", room_id=room_id, user_id=user.id, username=user.username
                    ),
                )
            else:
                await _send_error(websocket, room_id, f"Unknown command type: {cmd.type}")
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("Unexpected error in room %s for user %s", room_id, user.id)
        with suppress(Exception):
            await websocket.close(code=1011)
    finally:
        if heartbeat is not None:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
        manager.disconnect(room_id, user.id, websocket)
        if connected:
            try:
                online = await presence.user_disconnected(room_id, connection_id)
                if str(user.id) not in online:
                    await bridge.publish(
                        room_id,
                        ChatEvent(
                            type="leave", room_id=room_id, user_id=user.id, username=user.username
                        ),
                    )
                await bridge.publish(
                    room_id, ChatEvent(type="presence", room_id=room_id, online_users=online)
                )
            except Exception:
                logger.warning("Presence cleanup failed in room %s; lease will expire", room_id)
        if subscribed:
            with suppress(Exception):
                await bridge.maybe_unsubscribe(room_id)
