from datetime import datetime, timezone
from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict, EmailStr, Field, field_validator


def _utc_datetime(value: datetime) -> datetime:
    # SQLite CURRENT_TIMESTAMP is UTC but returns a naive Python datetime.
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


UTCDateTime = Annotated[datetime, AfterValidator(_utc_datetime)]


# ---------- Auth ----------
class UserCreate(BaseModel):
    username: str = Field(min_length=3, max_length=50)
    email: EmailStr
    password: str = Field(min_length=6, max_length=128)

    @field_validator("password")
    @classmethod
    def password_byte_limit(cls, value: str) -> str:
        if len(value.encode("utf-8")) > 72:
            raise ValueError("Password must be at most 72 UTF-8 bytes")
        return value

    @field_validator("username")
    @classmethod
    def normalize_username(cls, value: str) -> str:
        value = value.strip()
        if len(value) < 3 or "@" in value:
            raise ValueError("Username needs at least 3 characters and cannot contain @")
        return value


class UserLogin(BaseModel):
    username: str
    password: str


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    username: str
    email: EmailStr
    created_at: UTCDateTime


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    user_id: int | None = None


# ---------- Rooms ----------
class RoomCreate(BaseModel):
    name: str = Field(min_length=3, max_length=100)
    is_private: bool = False

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        value = value.strip()
        if len(value) < 3:
            raise ValueError("Room name needs at least 3 non-space characters")
        return value


class RoomOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    is_private: bool
    created_by: int
    created_at: UTCDateTime
    is_member: bool = False


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
    created_at: UTCDateTime
    reply_to_id: int | None = None
    # Denormalized context of the replied-to message for rendering the quote
    reply_to_username: str | None = None
    reply_to_content: str | None = None


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
    created_at: UTCDateTime | None = None
    reply_to_id: int | None = None
    reply_to_username: str | None = None
    reply_to_content: str | None = None
    # For "presence" events: {user_id_as_string: username} of everyone online
    # in this room. Dict keys are strings because JSON object keys are always
    # strings; the frontend iterates via Object.entries().
    online_users: dict[str, str] | None = None


# Incoming WebSocket message from the client.
class ClientCommand(BaseModel):
    type: str  # "message" | "typing"
    content: str | None = Field(None, max_length=4000)
    reply_to_id: int | None = Field(None, gt=0)
