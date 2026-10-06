"""Password hashing using bcrypt directly.

Why not passlib
---------------
passlib 1.7.4 (the last release, from 2020) is incompatible with
bcrypt >= 4.1 because bcrypt 5.0 removed its legacy C-extension shim API
that passlib probed at import time. The failure surfaces as a misleading
``ValueError: password cannot be longer than 72 bytes`` even for short
passwords — passlib's internal "detect wrap bug" routine calls
``bcrypt.hashpw`` in a way that the new API rejects.

Switching to bcrypt directly (4 lines: hash + verify) sidesteps the whole
compatibility problem and removes a stale, unmaintained dependency.
"""

from datetime import datetime, timedelta, timezone

import bcrypt
import jwt

from app.config import settings
from app.schemas import TokenData

# bcrypt has a 72-byte hard limit. Reject excess bytes instead of silently
# treating different passwords with the same prefix as the same password.
_MAX_BYTES = 72


def _password_bytes(password: str) -> bytes:
    encoded = password.encode("utf-8")
    if len(encoded) > _MAX_BYTES:
        raise ValueError("Password must be at most 72 UTF-8 bytes")
    return encoded


def hash_password(password: str) -> str:
    return bcrypt.hashpw(_password_bytes(password), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(_password_bytes(plain), hashed.encode("utf-8"))
    except ValueError:
        return False


# ---------- JWT ----------
def create_access_token(user_id: int) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=settings.access_token_expire_minutes)
    payload = {"sub": str(user_id), "exp": expire}
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def decode_access_token(token: str) -> TokenData | None:
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm],
            options={"require": ["exp", "sub"]},
        )
        sub = payload.get("sub")
        if not isinstance(sub, str) or not sub.isdecimal() or int(sub) < 1:
            return None
        return TokenData(user_id=int(sub))
    except (jwt.InvalidTokenError, ValueError, TypeError):
        return None
