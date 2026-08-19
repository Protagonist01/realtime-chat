"""In-process WebSocket registry.

This object lives in one uvicorn worker's memory. It only knows about
sockets connected to *this* process. Cross-process delivery is handled by the
Redis pub/sub bridge, which calls :meth:`broadcast` whenever a message lands
on a subscribed channel.

Shape::

    {room_id: {user_id: set[WebSocket]}}

A user can hold several sockets for the same room (multiple tabs), so the
inner value is a set, not a single socket. Iterating over ``list(sockets)``
while sending is important — a socket can be removed mid-broadcast if a
client disconnects while we're faning out, and mutating a set during
iteration raises ``RuntimeError: Set changed size during iteration``.
"""

from fastapi import WebSocket

from app.schemas import ChatEvent


class ConnectionManager:
    def __init__(self) -> None:
        self._rooms: dict[int, dict[int, set[WebSocket]]] = {}

    async def connect(self, room_id: int, user_id: int, ws: WebSocket) -> None:
        await ws.accept()
        self._rooms.setdefault(room_id, {}).setdefault(user_id, set()).add(ws)

    def disconnect(self, room_id: int, user_id: int, ws: WebSocket) -> None:
        users = self._rooms.get(room_id)
        if users is None:
            return
        sockets = users.get(user_id)
        if sockets is None:
            return
        sockets.discard(ws)
        if not sockets:
            users.pop(user_id, None)
        if not users:
            self._rooms.pop(room_id, None)

    async def broadcast(self, room_id: int, event: ChatEvent) -> None:
        users = self._rooms.get(room_id)
        if not users:
            return
        payload = event.model_dump_json()
        # Snapshot to avoid "set changed size during iteration" if a socket
        # dies while we're mid-broadcast.
        for sockets in list(users.values()):
            for ws in list(sockets):
                try:
                    await ws.send_text(payload)
                except Exception:
                    # Client gone — drop silently; the receive loop's finally
                    # block will also call disconnect().
                    sockets.discard(ws)

    def has_local_clients(self, room_id: int) -> bool:
        users = self._rooms.get(room_id)
        return bool(users) and any(len(s) > 0 for s in users.values())

    def local_user_ids(self, room_id: int) -> set[int]:
        users = self._rooms.get(room_id, {})
        return set(users.keys())
