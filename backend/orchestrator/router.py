import hashlib
import hmac
import json
import os
import secrets
import time
import uuid
from typing import Any

import anthropic
import httpx
from shared.llm import create_message as llm_create_message
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from orchestrator.mcp_client import route_via_mcp
from core.platform_cache import get_anthropic_key
from core.security import require_api_key
from models.agent import Agent, AgentStatus
from models.agent_access import AgentAccess
from models.chat_message import ChatMessage
from models.chat_session import ChatSession
from models.developer_key import DeveloperKey
from models.user import User
from orchestrator.schemas import (
    ChatMessageResponse,
    InvokeRequest,
    InvokeResponse,
    SessionHistoryResponse,
)

router = APIRouter(prefix="/api", tags=["orchestrator"])

_HISTORY_LIMIT = 40
_INVOKE_TIMEOUT = 120.0


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

    resp = await llm_create_message(
        model=kwargs.pop("model"),
        max_tokens=kwargs.pop("max_tokens"),
        system=kwargs.pop("system"),
        messages=kwargs.pop("messages"),
        tools=kwargs.pop("tools", None),
        api_key=get_anthropic_key(),
    )
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
    meta = data.get("metadata", {})
    tokens = meta.get("tokens_used") or meta.get("output_tokens") or 0
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


async def _check_developer_key(request: Request, agent_slug: str, db: AsyncSession) -> bool:
    """
    Check if request carries a valid developer key for this agent_slug.
    Returns True if authorised via developer key.
    Raises 403 if key exists but is wrong slug or inactive.
    Raises nothing if no developer key present (falls through to require_api_key).
    """
    from core.platform_cache import get_backend_api_key
    api_key = request.headers.get("X-API-Key", "")
    if not api_key or api_key == get_backend_api_key():
        return False  # not a developer key — handled by require_api_key
    hashed = hashlib.sha256(api_key.encode()).hexdigest()
    result = await db.execute(
        select(DeveloperKey).where(DeveloperKey.key_hash == hashed)
    )
    dev_key = result.scalar_one_or_none()
    if dev_key is None:
        raise HTTPException(status_code=403, detail="Invalid API key")
    if not dev_key.is_active:
        raise HTTPException(status_code=403, detail="API key is inactive")
    if dev_key.agent_slug != agent_slug:
        import logging
        logging.getLogger(__name__).warning(
            "Developer key slug mismatch: key scoped to '%s', invoked for '%s'",
            dev_key.agent_slug, agent_slug,
        )
        raise HTTPException(
            status_code=403,
            detail=f"API key is not authorised for agent '{agent_slug}'",
        )
    return True


# ── endpoints ─────────────────────────────────────────────────────────────────

@router.post(
    "/invoke/{agent_slug}",
    response_model=InvokeResponse,
)
async def invoke_agent(
    agent_slug: str,
    body: InvokeRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> InvokeResponse:
    from core.platform_cache import get_backend_api_key
    api_key = request.headers.get("X-API-Key", "")
    if api_key != get_backend_api_key():
        await _check_developer_key(request, agent_slug, db)
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

    # Audit log — capture user context from JWT if available
    try:
        from audit import log_audit_event
        user_email = None
        auth_header = request.cookies.get("operative_token")
        if auth_header:
            from core.auth import get_current_user
            try:
                user = await get_current_user(request)
                user_email = getattr(user, "email", None)
            except Exception:
                pass
        await log_audit_event(
            "llm.invoke",
            user_email=user_email,
            agent_slug=agent_slug,
            resource_type="chat_session",
            resource_id=body.session_id,
            action="invoke",
            outcome="success",
            details={"tokens_used": tokens, "model": agent.model},
            ip_address=request.client.host if request.client else None,
        )
    except Exception:
        pass

    return InvokeResponse(
        session_id=body.session_id,
        response=response_text,
        metadata={"tokens_used": tokens},
    )


@router.get("/agents", response_model=list[dict])
async def list_published_agents(db: AsyncSession = Depends(get_db), request: Request = None):
    # Resolve current_user from operative_token cookie
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

    # Require authentication
    if current_user is None:
        raise HTTPException(status_code=403, detail="Not authenticated")

    result = await db.execute(
        select(Agent)
        .where(Agent.status == AgentStatus.published)
        .order_by(Agent.name)
    )

    # Filter for plain users based on agent_access
    agents = result.scalars().all()
    if current_user and "admin" not in (current_user.roles or "").split(",") and "developer" not in (current_user.roles or "").split(","):
        access = await db.execute(
            select(AgentAccess.agent_slug).where(AgentAccess.user_email == current_user.email)
        )
        allowed_slugs = [r[0] for r in access.fetchall()]
        agents = [a for a in agents if a.slug in allowed_slugs]

    return [
        {
            "id": str(a.id),
            "name": a.name,
            "slug": a.slug,
            "description": a.description,
            "version": a.version,
            "model": a.model,
            "landing_page_url": a.landing_page_url,
            "capabilities": [],
        }
        for a in agents
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


@router.post("/llm/token")
async def vend_llm_token(
    body: dict,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    from core.platform_cache import get_anthropic_key
    from core.config import settings as _settings
    from models.agent import Agent, AgentStatus

    slug = body.get("agent_slug", "")
    if not slug:
        raise HTTPException(status_code=400, detail="agent_slug is required")

    from core.platform_cache import get_backend_api_key
    dev_key_ok = await _check_developer_key(request, slug, db)
    if not dev_key_ok:
        api_key = request.headers.get("X-API-Key", "")
        if api_key != get_backend_api_key():
            raise HTTPException(status_code=403, detail="Invalid API key")

    result = await db.execute(select(Agent).where(Agent.slug == slug))
    agent = result.scalar_one_or_none()
    if not agent or agent.status != AgentStatus.published:
        raise HTTPException(status_code=403, detail=f"Agent '{slug}' is not registered or not published")

    if not get_anthropic_key():
        raise HTTPException(status_code=503, detail="LLM credentials not configured on this platform")

    issued_at = int(time.time())
    expires_at = issued_at + 300
    payload = f"{slug}:{issued_at}:{expires_at}"
    signature = hmac.new(
        _settings.secret_key.encode(),
        payload.encode(),
        hashlib.sha256,
    ).hexdigest()
    token = f"{payload}:{signature}"

    return {
        "token": token,
        "provider": os.environ.get("LLM_PROVIDER", "anthropic"),
        "model": os.environ.get("LLM_MODEL", "claude-sonnet-4-6"),
        "expires_in": 300,
        "issued_at": issued_at,
    }


@router.post("/llm/invoke")
async def llm_invoke(
    body: dict,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    from core.platform_cache import get_anthropic_key
    from core.config import settings as _settings

    token = body.get("token", "")
    try:
        slug, issued_at_str, expires_at_str, signature = token.rsplit(":", 3)
        issued_at = int(issued_at_str)
        expires_at = int(expires_at_str)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token format")

    if int(time.time()) > expires_at:
        raise HTTPException(status_code=401, detail="Token expired")

    payload = f"{slug}:{issued_at_str}:{expires_at_str}"
    expected = hmac.new(
        _settings.secret_key.encode(),
        payload.encode(),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=401, detail="Invalid token signature")

    from core.platform_cache import get_backend_api_key
    dev_key_ok = await _check_developer_key(request, slug, db)
    if not dev_key_ok:
        api_key = request.headers.get("X-API-Key", "")
        if api_key != get_backend_api_key():
            raise HTTPException(status_code=403, detail="Invalid API key")

    messages = body.get("messages", [])
    system = body.get("system", "")
    model = body.get("model") or os.environ.get("LLM_MODEL", "claude-sonnet-4-6")
    max_tokens = body.get("max_tokens", 4096)

    api_key = get_anthropic_key()
    from models.agent import Agent
    agent_result = await db.execute(select(Agent).where(Agent.slug == slug))
    agent_obj = agent_result.scalar_one_or_none()
    app_label = f"Application: {agent_obj.name}" if agent_obj else f"Application: {slug}"

    response = await llm_create_message(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=messages,
        api_key=api_key,
        session_id=slug,
        agent_slug_override=slug,
        user_id=app_label,
    )

    text = next((b.text for b in response.content if hasattr(b, "text")), "")
    return {
        "response": text,
        "model": model,
        "usage": {
            "input_tokens": response.usage.input_tokens,
            "output_tokens": response.usage.output_tokens,
        }
    }


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

    # Audit log for invoke paths
    if "invoke" in path.lower():
        try:
            from audit import log_audit_event
            from core.auth import get_current_user
            user_email = None
            try:
                user = await get_current_user(request)
                user_email = getattr(user, "email", None)
            except Exception:
                pass
            body_json = {}
            try:
                body_json = json.loads(body) if body else {}
            except Exception:
                pass
            await log_audit_event(
                "llm.invoke",
                user_email=user_email,
                agent_slug=slug,
                resource_type="chat_session",
                resource_id=body_json.get("session_id"),
                action="invoke_stream" if "stream" in path.lower() else "invoke",
                outcome="success",
                details={"path": path},
                ip_address=request.client.host if request.client else None,
            )
        except Exception:
            pass

    # Check if this is a streaming/SSE endpoint
    is_stream = "stream" in path.lower()

    # Direct proxy to agent — MCP is for LLM tool calls only, not REST API proxying

    if is_stream:
        # Streaming response — use httpx streaming context
        async def stream_generator():
            async with httpx.AsyncClient(timeout=180.0) as client:
                async with client.stream(
                    method=request.method,
                    url=url,
                    content=body or None,
                    headers=fwd_headers,
                    params=dict(request.query_params),
                ) as resp:
                    async for chunk in resp.aiter_bytes():
                        yield chunk

        return StreamingResponse(
            stream_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )
    else:
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
                raise HTTPException(
                    status_code=502,
                    detail=f"Could not reach agent '{slug}': {exc}")
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            media_type=resp.headers.get(
                "content-type", "application/json"),
        )
