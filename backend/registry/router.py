import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.security import require_api_key
from models.agent import Agent, AgentStatus
from models.agent_version import AgentVersion
from registry.schemas import AgentCreate, AgentResponse, AgentUpdate, AgentVersionResponse

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
    dependencies=[Depends(require_api_key)],
)
async def create_agent(body: AgentCreate, db: AsyncSession = Depends(get_db)):
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
    await db.commit()
    await db.refresh(agent)
    return agent


@router.get(
    "/agents",
    response_model=list[AgentResponse],
    dependencies=[Depends(require_api_key)],
)
async def list_agents(
    status: AgentStatus | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    q = select(Agent).order_by(Agent.name)
    if status is not None:
        q = q.where(Agent.status == status)
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
