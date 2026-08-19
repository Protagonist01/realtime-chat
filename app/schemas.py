from datetime import datetime

from pydantic import BaseModel, ConfigDict, EmailStr, Field


# ---------- Auth ----------
class UserCreate(BaseModel):
    username: str = Field(min_length=3, max_length=50)
    email: EmailStr
    password: str = Field(min_length=6, max_length=128)


class UserLogin(BaseModel):
    username: str
    password: str


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    username: str
    email: EmailStr
    created_at: datetime


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    user_id: int | None = None


# ---------- Rooms ----------
class RoomCreate(BaseModel):
    name: str = Field(min_length=3, max_length=100)
    is_private: bool = False


class RoomOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    is_private: bool
    created_by: int
    created_at: datetime


class RoomMembership(BaseModel):
    room_id: int


# ---------- Chat ----------
class MessageOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    room_id: int
    user_id: int
    username: str
    content: str
    created_at: datetime


class MessageCreate(BaseModel):
    content: str = Field(min_length=1, max_length=4000)


# Wire payload used on the WebSocket and across Redis pub/sub.
# type can be: "message" | "join" | "leave" | "typing" | "presence" | "error"
class ChatEvent(BaseModel):
    type: str
    room_id: int
    user_id: int | None = None
    username: str | None = None
    content: str | None = None
    message_id: int | None = None
    created_at: datetime | None = None
    # For "presence" events: {user_id_as_string: username} of everyone online
    # in this room. Dict keys are strings because JSON object keys are always
    # strings; the frontend iterates via Object.entries().
    online_users: dict[str, str] | None = None


# Incoming WebSocket message from the client.
class ClientCommand(BaseModel):
    type: str  # "message" | "typing"
    content: str | None = None
