import hashlib
import secrets
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.auth import get_current_user, require_admin, require_developer
from core.database import get_db
from models.developer_key import DeveloperKey
from models.agent_owner import AgentOwner
from models.user import User
from audit import log_audit_event

router = APIRouter()


# ── Schemas ────────────────────────────────────────────────────────────────

class KeyOut(BaseModel):
    id: int
    agent_slug: str
    key_prefix: str
    label: str | None
    created_by: str
    is_active: bool
    created_at: datetime
    last_used_at: datetime | None

    model_config = {"from_attributes": True}


class KeyCreated(KeyOut):
    raw_key: str  # only returned once at creation


class CreateKeyRequest(BaseModel):
    agent_slug: str
    label: str | None = None


class UpdateKeyRequest(BaseModel):
    is_active: bool


# ── Helpers ────────────────────────────────────────────────────────────────

def _generate_key() -> tuple[str, str, str]:
    """Returns (raw_key, prefix, sha256_hash)."""
    raw = "opk_" + secrets.token_urlsafe(32)
    prefix = raw[:12]
    hashed = hashlib.sha256(raw.encode()).hexdigest()
    return raw, prefix, hashed


# ── Endpoints ──────────────────────────────────────────────────────────────

@router.post("/api/developer/keys", response_model=KeyCreated, status_code=status.HTTP_201_CREATED)
async def create_key(
    body: CreateKeyRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    require_developer(current_user)
    raw, prefix, hashed = _generate_key()
    key = DeveloperKey(
        agent_slug=body.agent_slug,
        key_prefix=prefix,
        key_hash=hashed,
        label=body.label,
        created_by=current_user.email,
        is_active=True,
    )
    db.add(key)
    await db.commit()
    await db.refresh(key)
    await log_audit_event("apikey.created", user_email=getattr(current_user, "email", None),
        user_role=getattr(current_user, "role", None), resource_type="api_key",
        resource_id=str(key.id), agent_slug=key.agent_slug, action="create",
        details={"key_name": key.label, "agent_slug": key.agent_slug})
    return KeyCreated(
        id=key.id,
        agent_slug=key.agent_slug,
        key_prefix=key.key_prefix,
        label=key.label,
        created_by=key.created_by,
        is_active=key.is_active,
        created_at=key.created_at,
        last_used_at=key.last_used_at,
        raw_key=raw,
    )


@router.get("/api/developer/keys", response_model=list[KeyOut])
async def list_keys(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    require_developer(current_user)
    roles = (current_user.roles or "").split(",")
    # Admin sees all keys; developer sees only keys for agents they own
    if "admin" in roles:
        result = await db.execute(select(DeveloperKey).order_by(DeveloperKey.created_at.desc()))
    else:
        owned = await db.execute(
            select(AgentOwner.agent_slug).where(AgentOwner.user_email == current_user.email)
        )
        owned_slugs = [r[0] for r in owned.fetchall()]
        result = await db.execute(
            select(DeveloperKey)
            .where(DeveloperKey.agent_slug.in_(owned_slugs))
            .order_by(DeveloperKey.created_at.desc())
        )
    return result.scalars().all()


@router.delete("/api/developer/keys/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_key(
    key_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    require_developer(current_user)
    key = await db.get(DeveloperKey, key_id)
    if key is None:
        raise HTTPException(status_code=404, detail="Key not found")
    await db.delete(key)
    await db.commit()
    await log_audit_event("apikey.deleted", user_email=getattr(current_user, "email", None),
        user_role=getattr(current_user, "role", None), resource_type="api_key",
        resource_id=str(key_id), action="delete")


@router.patch("/api/developer/keys/{key_id}", response_model=KeyOut)
async def update_key(
    key_id: int,
    body: UpdateKeyRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    require_developer(current_user)
    key = await db.get(DeveloperKey, key_id)
    if key is None:
        raise HTTPException(status_code=404, detail="Key not found")
    key.is_active = body.is_active
    await db.commit()
    await db.refresh(key)
    return key


@router.post("/api/developer/keys/{key_id}/rotate", response_model=KeyCreated)
async def rotate_key(
    key_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    require_developer(current_user)
    key = await db.get(DeveloperKey, key_id)
    if key is None:
        raise HTTPException(status_code=404, detail="Key not found")
    raw, prefix, hashed = _generate_key()
    key.key_prefix = prefix
    key.key_hash = hashed
    key.is_active = True
    await db.commit()
    await db.refresh(key)
    await log_audit_event("apikey.rotated", user_email=getattr(current_user, "email", None),
        user_role=getattr(current_user, "role", None), resource_type="api_key",
        resource_id=str(key.id), agent_slug=key.agent_slug, action="rotate",
        details={"agent_slug": key.agent_slug})
    return KeyCreated(
        id=key.id,
        agent_slug=key.agent_slug,
        key_prefix=key.key_prefix,
        label=key.label,
        created_by=key.created_by,
        is_active=key.is_active,
        created_at=key.created_at,
        last_used_at=key.last_used_at,
        raw_key=raw,
    )
