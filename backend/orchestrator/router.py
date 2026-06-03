import json
import uuid
from typing import Any

import anthropic
import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.platform_cache import get_anthropic_key
from core.security import require_api_key
from models.agent import Agent, AgentStatus
from models.chat_message import ChatMessage
from models.chat_session import ChatSession
from orchestrator.schemas import (
    ChatMessageResponse,
    InvokeRequest,
    InvokeResponse,
    SessionHistoryResponse,
)

router = APIRouter(prefix="/api", tags=["orchestrator"])

_HISTORY_LIMIT = 40
_INVOKE_TIMEOUT = 60.0


# ── helpers ───────────────────────────────────────────────────────────────────

async def _require_published(db: AsyncSession, slug: str) -> Agent:
    result = await db.execute(select(Agent).where(Agent.slug == slug))
    agent = result.scalar_one_or_none()
    if not agent:
        raise HTTPException(status_code=404, detail=f"Agent '{slug}' not found")
    if agent.status != AgentStatus.published:
        raise HTTPException(
            status_code=409,
            detail=f"Agent '{slug}' is not published (current status: {agent.status})",
        )
    return agent


async def _get_or_create_session(
    db: AsyncSession, session_uuid: uuid.UUID, agent_id: uuid.UUID
) -> ChatSession:
    result = await db.execute(
        select(ChatSession).where(ChatSession.session_id == session_uuid)
    )
    session = result.scalar_one_or_none()
    if not session:
        session = ChatSession(session_id=session_uuid, agent_id=agent_id)
        db.add(session)
        await db.flush()
    return session


async def _load_db_history(
    db: AsyncSession, session_uuid: uuid.UUID
) -> list[ChatMessage]:
    result = await db.execute(
        select(ChatMessage)
        .where(ChatMessage.session_id == session_uuid)
        .order_by(ChatMessage.created_at.asc())
        .limit(_HISTORY_LIMIT)
    )
    return list(result.scalars().all())


def _build_messages(
    history: list[ChatMessage], user_message: str
) -> list[dict[str, str]]:
    msgs = [{"role": m.role, "content": m.content} for m in history]
    msgs.append({"role": "user", "content": user_message})
    return msgs


def _build_system(agent: Agent, context: dict[str, Any]) -> str:
    system = agent.system_prompt or "You are a helpful assistant."
    if context:
        system += f"\n\n## Request context\n{json.dumps(context, indent=2)}"
    return system


async def _call_anthropic(
    agent: Agent, messages: list[dict], context: dict[str, Any]
) -> tuple[str, int]:
    kwargs: dict[str, Any] = {
        "model": agent.model,
        "max_tokens": 4096,
        "system": _build_system(agent, context),
        "messages": messages,
        "temperature": agent.temperature,
    }
    if agent.tools:
        kwargs["tools"] = agent.tools

    resp = await anthropic.AsyncAnthropic(api_key=get_anthropic_key()).messages.create(**kwargs)
    text = resp.content[0].text if resp.content else ""
    tokens = resp.usage.input_tokens + resp.usage.output_tokens
    return text, tokens


async def _call_remote_agent(
    agent: Agent, request: InvokeRequest, history: list[ChatMessage]
) -> tuple[str, int]:
    forward = InvokeRequest(
        session_id=request.session_id,
        user_message=request.user_message,
        context=request.context,
        history=[{"role": m.role, "content": m.content} for m in history],
    )
    url = f"{agent.invoke_url.rstrip('/')}/invoke"
    async with httpx.AsyncClient(timeout=_INVOKE_TIMEOUT) as client:
        try:
            resp = await client.post(
                url,
                json=forward.model_dump(),
                headers={"X-Anthropic-Key": get_anthropic_key()},
            )
            resp.raise_for_status()
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail=f"Agent '{agent.slug}' timed out")
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Agent '{agent.slug}' returned HTTP {exc.response.status_code}",
            )
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=502, detail=f"Could not reach agent '{agent.slug}': {exc}"
            )
    data = resp.json()
    tokens = data.get("metadata", {}).get("output_tokens", 0)
    return data["response"], tokens


async def _persist(
    db: AsyncSession,
    session_uuid: uuid.UUID,
    user_message: str,
    assistant_text: str,
    tokens: int,
) -> None:
    db.add(ChatMessage(session_id=session_uuid, role="user", content=user_message))
    db.add(ChatMessage(
        session_id=session_uuid,
        role="assistant",
        content=assistant_text,
        tokens_used=tokens or None,
    ))
    await db.commit()


# ── endpoints ─────────────────────────────────────────────────────────────────

@router.post(
    "/invoke/{agent_slug}",
    response_model=InvokeResponse,
    dependencies=[Depends(require_api_key)],
)
async def invoke_agent(
    agent_slug: str,
    body: InvokeRequest,
    db: AsyncSession = Depends(get_db),
) -> InvokeResponse:
    agent = await _require_published(db, agent_slug)

    try:
        session_uuid = uuid.UUID(body.session_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="session_id must be a valid UUID v4")

    await _get_or_create_session(db, session_uuid, agent.id)
    history = await _load_db_history(db, session_uuid)

    if agent.invoke_url:
        response_text, tokens = await _call_remote_agent(agent, body, history)
    else:
        messages = _build_messages(history, body.user_message)
        response_text, tokens = await _call_anthropic(agent, messages, body.context)

    await _persist(db, session_uuid, body.user_message, response_text, tokens)

    return InvokeResponse(
        session_id=body.session_id,
        response=response_text,
        metadata={"tokens_used": tokens},
    )


@router.get("/agents", response_model=list[dict])
async def list_published_agents(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Agent)
        .where(Agent.status == AgentStatus.published)
        .order_by(Agent.name)
    )
    return [
        {
            "id": str(a.id),
            "name": a.name,
            "slug": a.slug,
            "description": a.description,
            "version": a.version,
            "model": a.model,
            "capabilities": [],
        }
        for a in result.scalars().all()
    ]


@router.get("/agents/{slug}", response_model=dict)
async def get_published_agent(slug: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Agent).where(Agent.slug == slug))
    agent = result.scalar_one_or_none()
    if not agent or agent.status != AgentStatus.published:
        raise HTTPException(status_code=404, detail=f"Agent '{slug}' not found")
    return {
        "id": str(agent.id),
        "name": agent.name,
        "slug": agent.slug,
        "description": agent.description,
        "version": agent.version,
        "model": agent.model,
    }


@router.get("/session/{session_id}", response_model=SessionHistoryResponse)
async def get_session_history(session_id: str, db: AsyncSession = Depends(get_db)):
    try:
        session_uuid = uuid.UUID(session_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="session_id must be a valid UUID")

    result = await db.execute(
        select(ChatSession).where(ChatSession.session_id == session_uuid)
    )
    session = result.scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    agent_slug: str | None = None
    if session.agent_id:
        agent_result = await db.execute(
            select(Agent).where(Agent.id == session.agent_id)
        )
        agent = agent_result.scalar_one_or_none()
        if agent:
            agent_slug = agent.slug

    msgs_result = await db.execute(
        select(ChatMessage)
        .where(ChatMessage.session_id == session_uuid)
        .order_by(ChatMessage.created_at.asc())
    )
    messages = list(msgs_result.scalars().all())

    return SessionHistoryResponse(
        session_id=session_uuid,
        agent_slug=agent_slug,
        messages=[ChatMessageResponse.model_validate(m) for m in messages],
        created_at=session.created_at,
    )


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.api_route(
    "/agents/{slug}/proxy/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE"],
    dependencies=[Depends(require_api_key)],
)
async def proxy_agent_endpoint(
    slug: str,
    path: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> Response:
    """Forward GET/POST requests to an agent's non-invoke endpoints (dashboard, reports, etc.)."""
    result = await db.execute(select(Agent).where(Agent.slug == slug))
    agent = result.scalar_one_or_none()
    if not agent or not agent.invoke_url:
        raise HTTPException(status_code=404, detail=f"Agent '{slug}' not found or has no invoke URL")

    url = f"{agent.invoke_url.rstrip('/')}/{path}"
    body = await request.body()
    fwd_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in ("host", "content-length", "transfer-encoding", "x-api-key")
    }

    # Inject Anthropic key for agents that call Claude directly
    anthropic_key = get_anthropic_key()
    if anthropic_key:
        fwd_headers["x-anthropic-key"] = anthropic_key

    async with httpx.AsyncClient(timeout=120.0) as client:
        try:
            resp = await client.request(
                method=request.method,
                url=url,
                content=body or None,
                headers=fwd_headers,
                params=dict(request.query_params),
            )
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"Could not reach agent '{slug}': {exc}")

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        media_type=resp.headers.get("content-type", "application/json"),
    )
