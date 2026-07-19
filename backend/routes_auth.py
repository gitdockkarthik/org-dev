from datetime import datetime, timezone
import secrets

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, EmailStr
from sqlalchemy import select

from core.config import settings as _settings

from core.auth import (
    COOKIE_NAME,
    TOKEN_EXPIRE_HOURS,
    create_access_token,
    get_current_user,
    hash_password,
    require_admin,
    require_developer,
    verify_password,
)
from core.database import AsyncSessionLocal
from models.user import User

router = APIRouter()

# ── Schemas ────────────────────────────────────────────────────────────────


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class UserOut(BaseModel):
    id: int
    name: str
    email: str
    roles: str
    created_at: datetime | None = None

    model_config = {"from_attributes": True}


class UserCreated(UserOut):
    generated_password: str


class CreateUserRequest(BaseModel):
    name: str
    email: EmailStr
    roles: str = "user"


class UpdateUserRequest(BaseModel):
    name: str | None = None
    roles: str | None = None
    is_active: bool | None = None


# ── Auth mode (public) ────────────────────────────────────────────────────


@router.get("/api/auth/mode")
async def auth_mode():
    return {"mode": _settings.auth_mode}


# ── Auth endpoints ─────────────────────────────────────────────────────────


@router.post("/api/auth/login")
async def login(body: LoginRequest, response: Response):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(User).where(User.email == body.email, User.is_active.is_(True))
        )
        user = result.scalar_one_or_none()

    if user is None or not verify_password(body.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    token = create_access_token({"sub": str(user.id)})

    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="lax",
        secure=False,  # set True behind HTTPS in production
        max_age=TOKEN_EXPIRE_HOURS * 3600,
    )

    # Update last_login
    async with AsyncSessionLocal() as session:
        db_user = await session.get(User, user.id)
        if db_user:
            db_user.last_login = datetime.now(timezone.utc)
            await session.commit()

    return {"access_token": token, "token_type": "bearer"}


@router.post("/api/auth/logout")
async def logout(response: Response):
    response.delete_cookie(key=COOKIE_NAME, httponly=True, samesite="lax")
    return {"ok": True}


@router.get("/api/auth/me", response_model=UserOut)
async def me(current_user: User = Depends(get_current_user)):
    return current_user


# ── Admin user management ──────────────────────────────────────────────────


@router.post("/api/admin/users", response_model=UserCreated, status_code=status.HTTP_201_CREATED)
async def create_user(
    body: CreateUserRequest,
    current_user: User = Depends(get_current_user),
):
    require_admin(current_user)

    valid_roles = {"admin", "developer", "user"}
    if not all(r.strip() in valid_roles for r in body.roles.split(",")):
        raise HTTPException(status_code=400, detail="roles must be comma-separated list of: admin, developer, user")

    generated_password = secrets.token_urlsafe(12)
    async with AsyncSessionLocal() as session:
        existing = await session.execute(select(User).where(User.email == body.email))
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="Email already registered")

        new_user = User(
            name=body.name,
            email=body.email,
            password_hash=hash_password(generated_password),
            roles=body.roles,
            must_change_password=True,
        )
        session.add(new_user)
        await session.commit()
        await session.refresh(new_user)

    return UserCreated(
        id=new_user.id,
        name=new_user.name,
        email=new_user.email,
        roles=new_user.roles,
        created_at=new_user.created_at,
        generated_password=generated_password,
    )


@router.get("/api/admin/users", response_model=list[UserOut])
async def list_users(current_user: User = Depends(get_current_user)):
    require_admin(current_user)

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(User).order_by(User.id))
        users = result.scalars().all()

    return [
        UserOut(
            id=u.id,
            name=u.name,
            email=u.email,
            roles=u.roles,
            created_at=u.created_at,
        )
        for u in users
    ]


@router.put("/api/admin/users/{user_id}", response_model=UserOut)
async def update_user(
    user_id: int,
    body: UpdateUserRequest,
    current_user: User = Depends(get_current_user),
):
    require_admin(current_user)

    if body.roles is not None:
        valid_roles = {"admin", "developer", "user"}
        if not all(r.strip() in valid_roles for r in body.roles.split(",")):
            raise HTTPException(status_code=400, detail="roles must be comma-separated list of: admin, developer, user")

    async with AsyncSessionLocal() as session:
        user = await session.get(User, user_id)
        if user is None:
            raise HTTPException(status_code=404, detail="User not found")
        if body.name is not None:
            user.name = body.name
        if body.roles is not None:
            user.roles = body.roles
        if body.is_active is not None:
            if user_id == current_user.id and body.is_active is False:
                raise HTTPException(status_code=400, detail="Cannot deactivate your own account")
            user.is_active = body.is_active
        await session.commit()
        await session.refresh(user)

    return UserOut(
        id=user.id,
        name=user.name,
        email=user.email,
        roles=user.roles,
        created_at=user.created_at,
    )


@router.delete("/api/admin/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(user_id: int, current_user: User = Depends(get_current_user)):
    require_admin(current_user)

    if user_id == current_user.id:
        raise HTTPException(status_code=400, detail="Cannot delete your own account")

    async with AsyncSessionLocal() as session:
        user = await session.get(User, user_id)
        if user is None:
            raise HTTPException(status_code=404, detail="User not found")
        await session.delete(user)
        await session.commit()


class PasswordRevealed(BaseModel):
    generated_password: str


@router.put("/api/admin/users/{user_id}/password", response_model=PasswordRevealed)
async def reset_user_password(
    user_id: int,
    current_user: User = Depends(get_current_user),
):
    require_admin(current_user)

    generated_password = secrets.token_urlsafe(12)
    async with AsyncSessionLocal() as session:
        user = await session.get(User, user_id)
        if user is None:
            raise HTTPException(status_code=404, detail="User not found")
        user.password_hash = hash_password(generated_password)
        user.must_change_password = True
        await session.commit()

    return PasswordRevealed(generated_password=generated_password)


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str
    confirm_password: str


@router.put("/api/auth/me/password")
async def change_own_password(
    body: ChangePasswordRequest,
    current_user: User = Depends(get_current_user),
):
    if body.new_password != body.confirm_password:
        raise HTTPException(status_code=400, detail="Passwords do not match")
    if len(body.new_password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
    if not verify_password(body.current_password, current_user.password_hash):
        raise HTTPException(status_code=400, detail="Current password is incorrect")

    async with AsyncSessionLocal() as session:
        user = await session.get(User, current_user.id)
        user.password_hash = hash_password(body.new_password)
        user.must_change_password = False
        await session.commit()

    return {"ok": True}
