import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.security import require_api_key
from core.auth import get_current_user, require_admin, require_admin_or_developer
from models.agent import Agent, AgentStatus
from models.agent_version import AgentVersion
from models.agent_access import AgentAccess
from models.agent_owner import AgentOwner
from models.user import User
from pydantic import BaseModel
from registry.schemas import AgentCreate, AgentResponse, AgentUpdate, AgentVersionResponse, AccessAssign, UserAgentAccessUpdate

router = APIRouter(prefix="/api/registry", tags=["registry"])


def _snapshot(agent: Agent) -> dict[str, Any]:
    return {
        "name": agent.name,
        "slug": agent.slug,
        "description": agent.description,
        "version": agent.version,
        "status": agent.status,
        "invoke_url": agent.invoke_url,
        "landing_page_url": agent.landing_page_url,
        "uses_uap_llm": agent.uses_uap_llm,
        "system_prompt": agent.system_prompt,
        "model": agent.model,
        "temperature": agent.temperature,
        "tools": agent.tools,
    }


async def _get_or_404(db: AsyncSession, agent_id: uuid.UUID) -> Agent:
    result = await db.execute(select(Agent).where(Agent.id == agent_id))
    agent = result.scalar_one_or_none()
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent


@router.post(
    "/agents",
    response_model=AgentResponse,
    status_code=201,
)
async def create_agent(body: AgentCreate, db: AsyncSession = Depends(get_db), request: Request = None):
    from core.platform_cache import get_backend_api_key
    from core.database import AsyncSessionLocal
    from core.auth import decode_token

    # Identify caller — API key or JWT
    api_key = request.headers.get("X-API-Key", "") if request else ""
    backend_key = get_backend_api_key()
    caller_user = None
    if api_key and api_key == backend_key:
        pass  # machine registration — no ownership
    else:
        try:
            token = request.cookies.get("operative_token") if request else None
            if token:
                payload = decode_token(token)
                user_id = payload.get("sub")
                if user_id:
                    async with AsyncSessionLocal() as s:
                        caller_user = await s.get(User, int(user_id))
        except Exception:
            pass
        # Fall back to API key auth if no JWT
        if caller_user is None and api_key != backend_key:
            from core.security import require_api_key as _rak
            raise HTTPException(status_code=403, detail="Invalid API key")

    existing = await db.execute(select(Agent).where(Agent.slug == body.slug))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail=f"Slug '{body.slug}' already registered")
    agent = Agent(**body.model_dump())
    db.add(agent)
    await db.flush()
    db.add(AgentVersion(
        agent_id=agent.id,
        version=agent.version,
        config_snapshot=_snapshot(agent),
    ))
    # Auto-assign developer as first owner
    if caller_user and "developer" in (caller_user.roles or "").split(",") and "admin" not in (caller_user.roles or "").split(","):
        db.add(AgentOwner(
            agent_slug=body.slug,
            user_email=caller_user.email,
            assigned_by=caller_user.email,
        ))
    await db.commit()
    await db.refresh(agent)
    return agent


@router.get(
    "/agents",
    response_model=list[AgentResponse],
)
async def list_agents(
    status: AgentStatus | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
    request: Request = None,
):
    from fastapi import Request as FastAPIRequest
    from core.platform_cache import get_backend_api_key
    from core.database import AsyncSessionLocal

    # Determine caller identity — JWT always tried first for ownership filtering
    api_key = request.headers.get("X-API-Key", "") if request else ""
    current_user = None
    try:
        from core.auth import decode_token
        from core.database import AsyncSessionLocal
        token = request.cookies.get("operative_token") if request else None
        if token:
            payload = decode_token(token)
            user_id = payload.get("sub")
            if user_id:
                async with AsyncSessionLocal() as s:
                    current_user = await s.get(User, int(user_id))
    except Exception:
        pass
    # If no JWT, validate API key
    if current_user is None and api_key != get_backend_api_key():
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail="Invalid API key")

    q = select(Agent).order_by(Agent.name)
    if status is not None:
        q = q.where(Agent.status == status)

    # Ownership filtering: developer sees only owned agents
    if current_user and "admin" not in (current_user.roles or "").split(",") and "developer" in (current_user.roles or "").split(","):
        owned = await db.execute(
            select(AgentOwner.agent_slug).where(AgentOwner.user_email == current_user.email)
        )
        owned_slugs = [r[0] for r in owned.fetchall()]
        q = q.where(Agent.slug.in_(owned_slugs))

    # Catalogue visibility filtering: plain user sees only agents they have access to
    elif current_user and "admin" not in (current_user.roles or "").split(",") and "developer" not in (current_user.roles or "").split(","):
        access = await db.execute(
            select(AgentAccess.agent_slug).where(AgentAccess.user_email == current_user.email)
        )
        allowed_slugs = [r[0] for r in access.fetchall()]
        q = q.where(Agent.slug.in_(allowed_slugs))

    result = await db.execute(q)
    return list(result.scalars().all())


@router.get(
    "/agents/{agent_id}",
    response_model=AgentResponse,
    dependencies=[Depends(require_api_key)],
)
async def get_agent(agent_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    return await _get_or_404(db, agent_id)


@router.put(
    "/agents/{agent_id}",
    response_model=AgentResponse,
    dependencies=[Depends(require_api_key)],
)
async def update_agent(
    agent_id: uuid.UUID,
    body: AgentUpdate,
    db: AsyncSession = Depends(get_db),
):
    agent = await _get_or_404(db, agent_id)

    # snapshot current config before changes
    db.add(AgentVersion(
        agent_id=agent.id,
        version=agent.version,
        config_snapshot=_snapshot(agent),
    ))

    for field, value in body.model_dump(exclude_none=True).items():
        setattr(agent, field, value)

    await db.commit()
    await db.refresh(agent)
    return agent


@router.post(
    "/agents/{agent_id}/publish",
    response_model=AgentResponse,
    dependencies=[Depends(require_api_key)],
)
async def publish_agent(agent_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    agent = await _get_or_404(db, agent_id)
    if agent.status == AgentStatus.deprecated:
        raise HTTPException(status_code=409, detail="Cannot publish a deprecated agent")
    agent.status = AgentStatus.published
    await db.commit()
    await db.refresh(agent)
    return agent


@router.post(
    "/agents/{agent_id}/deprecate",
    response_model=AgentResponse,
    dependencies=[Depends(require_api_key)],
)
async def deprecate_agent(agent_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    agent = await _get_or_404(db, agent_id)
    agent.status = AgentStatus.deprecated
    await db.commit()
    await db.refresh(agent)
    return agent


@router.delete(
    "/agents/{agent_id}",
    status_code=204,
    dependencies=[Depends(require_api_key)],
)
async def delete_agent(agent_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    agent = await _get_or_404(db, agent_id)
    await db.delete(agent)
    await db.commit()


@router.get(
    "/agents/{agent_id}/versions",
    response_model=list[AgentVersionResponse],
    dependencies=[Depends(require_api_key)],
)
async def list_versions(agent_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    await _get_or_404(db, agent_id)
    result = await db.execute(
        select(AgentVersion)
        .where(AgentVersion.agent_id == agent_id)
        .order_by(AgentVersion.created_at.desc())
    )
    return list(result.scalars().all())


# ── Agent Ownership ────────────────────────────────────────────────────────

class OwnerAssign(BaseModel):
    user_email: str

class OwnerOut(BaseModel):
    agent_slug: str
    user_email: str
    assigned_by: str
    assigned_at: str

    model_config = {"from_attributes": True}


@router.get("/agents/{slug}/owners")
async def list_owners(
    slug: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[dict]:
    require_admin_or_developer(current_user)
    result = await db.execute(
        select(AgentOwner).where(AgentOwner.agent_slug == slug)
    )
    owners = result.scalars().all()
    return [
        {
            "agent_slug": o.agent_slug,
            "user_email": o.user_email,
            "assigned_by": o.assigned_by,
            "assigned_at": str(o.assigned_at),
        }
        for o in owners
    ]


@router.post("/agents/{slug}/owners", status_code=201)
async def add_owner(
    slug: str,
    body: OwnerAssign,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> dict:
    require_admin_or_developer(current_user)
    roles = (current_user.roles or "").split(",")
    # Developer can only add owners to agents they own
    if "admin" not in roles:
        existing_self = await db.get(AgentOwner, (slug, current_user.email))
        if existing_self is None:
            raise HTTPException(status_code=403, detail="You are not an owner of this agent")
    # Check user is not admin (admins never added as owners)
    result = await db.execute(select(User).where(User.email == body.user_email))
    target_user = result.scalar_one_or_none()
    if target_user and "admin" in (target_user.roles or "").split(","):
        raise HTTPException(status_code=400, detail="Admins have implicit full access and cannot be added as owners")
    existing = await db.get(AgentOwner, (slug, body.user_email))
    if existing:
        raise HTTPException(status_code=409, detail="User is already an owner")
    owner = AgentOwner(
        agent_slug=slug,
        user_email=body.user_email,
        assigned_by=current_user.email,
    )
    db.add(owner)
    await db.commit()
    return {"agent_slug": slug, "user_email": body.user_email, "assigned_by": current_user.email}


@router.delete("/agents/{slug}/owners/{user_email}", status_code=204)
async def remove_owner(
    slug: str,
    user_email: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> None:
    require_admin_or_developer(current_user)
    roles = (current_user.roles or "").split(",")
    owner = await db.get(AgentOwner, (slug, user_email))
    if owner is None:
        raise HTTPException(status_code=404, detail="Owner not found")
    # Developer cannot remove themselves
    if "admin" not in roles and user_email == current_user.email:
        raise HTTPException(status_code=403, detail="Ask an admin or co-owner to remove you")
    # Developer cannot remove last owner
    if "admin" not in roles:
        result = await db.execute(select(AgentOwner).where(AgentOwner.agent_slug == slug))
        all_owners = result.scalars().all()
        if len(all_owners) <= 1:
            raise HTTPException(status_code=409, detail="Cannot remove last owner")
    await db.delete(owner)
    await db.commit()


# ── Agent Access Control ────────────────────────────────────────────────────

@router.get("/agents/{slug}/access")
async def list_access(
    slug: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[dict]:
    require_admin(current_user)
    result = await db.execute(
        select(AgentAccess).where(AgentAccess.agent_slug == slug)
    )
    rows = result.scalars().all()
    return [
        {
            "agent_slug": r.agent_slug,
            "user_email": r.user_email,
            "assigned_by": r.assigned_by,
            "assigned_at": str(r.assigned_at),
        }
        for r in rows
    ]


@router.post("/agents/{slug}/access", status_code=201)
async def add_access(
    slug: str,
    body: AccessAssign,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> dict:
    require_admin(current_user)
    existing = await db.get(AgentAccess, (slug, body.user_email))
    if existing:
        raise HTTPException(status_code=409, detail="User already has access")
    row = AgentAccess(
        agent_slug=slug,
        user_email=body.user_email,
        assigned_by=current_user.email,
    )
    db.add(row)
    await db.commit()
    return {"agent_slug": slug, "user_email": body.user_email, "assigned_by": current_user.email}


@router.delete("/agents/{slug}/access/{user_email}", status_code=204)
async def remove_access(
    slug: str,
    user_email: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> None:
    require_admin(current_user)
    row = await db.get(AgentAccess, (slug, user_email))
    if row is None:
        raise HTTPException(status_code=404, detail="Access not found")
    await db.delete(row)
    await db.commit()


@router.get("/users/{user_email}/agent-access")
async def get_user_agent_access(
    user_email: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> dict:
    require_admin(current_user)
    result = await db.execute(
        select(AgentAccess.agent_slug).where(AgentAccess.user_email == user_email)
    )
    return {"user_email": user_email, "agent_slugs": [r[0] for r in result.fetchall()]}


@router.put("/users/{user_email}/agent-access")
async def set_user_agent_access(
    user_email: str,
    body: UserAgentAccessUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> dict:
    require_admin(current_user)
    await db.execute(delete(AgentAccess).where(AgentAccess.user_email == user_email))
    for slug in body.agent_slugs:
        db.add(AgentAccess(agent_slug=slug, user_email=user_email, assigned_by=current_user.email))
    await db.commit()
    return {"user_email": user_email, "agent_slugs": body.agent_slugs}


@router.get("/assignable-users")
async def list_assignable_users(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[dict]:
    """Return developer-role users only — used to populate Add Owner dropdown."""
    result = await db.execute(select(User).where(User.is_active.is_(True)))
    all_users = result.scalars().all()
    return [
        {"email": u.email, "name": u.name}
        for u in all_users
        if "developer" in (u.roles or "").split(",")
        and "admin" not in (u.roles or "").split(",")
    ]
