from datetime import datetime, timezone

import anthropic
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.encryption import decrypt, encrypt, is_secret_key
from core.platform_cache import (
    get_anthropic_key,
    get_backend_api_key,
    set_anthropic_key,
    set_backend_api_key,
)
from core.security import require_api_key
from models.agent import Agent, AgentStatus
from models.platform_config import PlatformConfig

router = APIRouter(prefix="/api/platform", tags=["platform"])

_VERSION = "0.2.0"
_DEFAULT_MODEL = "claude-sonnet-4-6"


@router.get("/bootstrap")
async def bootstrap() -> dict:
    """Public endpoint — no auth required. Returns the backend API key for portal self-configuration."""
    return {"api_key": get_backend_api_key() or ""}


@router.get("/agent-token")
async def agent_token() -> dict:
    """Public endpoint — no auth required. Returns the registration token for agent self-registration."""
    return {"registration_token": get_backend_api_key() or ""}


@router.get("/status")
async def platform_status(db: AsyncSession = Depends(get_db)) -> dict:
    """Public endpoint — no auth required (used by setup redirect logic)."""
    from sqlalchemy import func

    result = await db.execute(
        select(func.count()).select_from(Agent).where(Agent.status == AgentStatus.published)
    )
    agent_count = result.scalar() or 0

    return {
        "anthropic_connected": bool(get_anthropic_key()),
        "agent_count": agent_count,
        "model": _DEFAULT_MODEL,
        "version": _VERSION,
    }


@router.get("/config", dependencies=[Depends(require_api_key)])
async def get_platform_config(db: AsyncSession = Depends(get_db)) -> dict:
    """Return which config keys are set — values are never returned for secrets."""
    rows = (await db.execute(select(PlatformConfig))).scalars().all()
    return {r.key: True for r in rows}


class ConfigPayload(BaseModel):
    anthropic_api_key: str | None = None
    backend_api_key: str | None = None


@router.post("/config")
async def save_platform_config(
    body: ConfigPayload,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Save platform credentials. No auth required — protected by first-run logic only."""
    updates: dict[str, str] = {}
    if body.anthropic_api_key is not None:
        updates["anthropic_api_key"] = body.anthropic_api_key
    if body.backend_api_key is not None:
        updates["backend_api_key"] = body.backend_api_key

    if not updates:
        return {"ok": True, "updated": []}

    now = datetime.now(timezone.utc)
    for key, value in updates.items():
        stored = encrypt(value) if is_secret_key(key) else value
        stmt = (
            pg_insert(PlatformConfig)
            .values(key=key, value=stored, updated_at=now)
            .on_conflict_do_update(
                index_elements=["key"],
                set_={"value": stored, "updated_at": now},
            )
        )
        await db.execute(stmt)
    await db.commit()

    if "anthropic_api_key" in updates:
        set_anthropic_key(updates["anthropic_api_key"])
    if "backend_api_key" in updates:
        set_backend_api_key(updates["backend_api_key"])

    return {"ok": True, "updated": list(updates.keys())}


class TestAnthropicRequest(BaseModel):
    api_key: str


@router.post("/test-anthropic")
async def test_anthropic(body: TestAnthropicRequest) -> dict:
    """Test an Anthropic API key with a minimal call. No auth required (used by setup wizard)."""
    try:
        client = anthropic.AsyncAnthropic(api_key=body.api_key)
        await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=5,
            messages=[{"role": "user", "content": "Hi"}],
        )
        return {"success": True, "model": "claude-haiku-4-5-20251001"}
    except anthropic.AuthenticationError:
        return {"success": False, "error": "Invalid API key — check it and try again"}
    except Exception as exc:
        return {"success": False, "error": str(exc)}
