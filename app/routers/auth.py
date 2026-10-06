from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from app.database import get_db
from app.deps import get_current_user
from app.models import User
from app.rate_limit import auth_limit
from app.schemas import Token, UserCreate, UserOut
from app.security import create_access_token, hash_password, verify_password

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.post(
    "/register",
    response_model=UserOut,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(auth_limit)],
)
async def register(payload: UserCreate, db: AsyncSession = Depends(get_db)) -> User:
    existing = await db.scalar(
        select(User).where((User.username == payload.username) | (User.email == payload.email))
    )
    if existing is not None:
        raise HTTPException(status_code=409, detail="Username or email already taken")
    user = User(
        username=payload.username,
        email=payload.email,
        hashed_password=await run_in_threadpool(hash_password, payload.password),
    )
    db.add(user)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail="Username or email already taken") from None
    await db.refresh(user)
    return user


@router.post("/login", response_model=Token, dependencies=[Depends(auth_limit)])
async def login(
    form: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db),
) -> Token:
    user = await db.scalar(
        select(User).where((User.username == form.username) | (User.email == form.username))
    )
    if user is None or not await run_in_threadpool(
        verify_password, form.password, user.hashed_password
    ):
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    return Token(access_token=create_access_token(user.id))


@router.get("/me", response_model=UserOut)
async def me(user: User = Depends(get_current_user)) -> User:
    return user
