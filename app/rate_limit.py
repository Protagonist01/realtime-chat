"""Redis-backed limits shared by API workers."""

from fastapi import HTTPException, Request

from app.config import settings

LIMIT_SCRIPT = """
local count = redis.call('INCR', KEYS[1])
if count == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
return {count, redis.call('TTL', KEYS[1])}
"""


async def enforce_limit(
    request: Request, scope: str, identity: str, limit: int, seconds: int = 60
) -> None:
    key = f"{settings.redis_namespace}:limit:{scope}:{identity}"
    try:
        count, ttl = await request.app.state.redis_client.eval(LIMIT_SCRIPT, 1, key, seconds)
    except Exception:
        raise HTTPException(status_code=503, detail="Rate-limit service unavailable") from None
    if count > limit:
        raise HTTPException(
            status_code=429, detail="Too many requests", headers={"Retry-After": str(max(ttl, 1))}
        )


async def auth_limit(request: Request) -> None:
    identity = request.client.host if request.client else "unknown"
    await enforce_limit(request, "auth", identity, 20)
