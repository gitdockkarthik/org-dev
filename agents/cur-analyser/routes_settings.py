import json
import logging

from datetime import datetime, timezone

from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import settings
from encryption import decrypt, encrypt, is_secret_key

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/settings", tags=["settings"])

_DEFAULTS: dict = {
    "source_type": "file",
    "last_synced": None,
    "record_count": None,
    "total_cost": None,
    "sync_interval_minutes": 0,
    "cost_window": "30",
    "display_currency": "USD",
    "anomaly_threshold_pct": 20,
    "top_drivers_count": 5,
    "min_cost_threshold": 1.00,
}

# Write-through in-memory cache; populated from DB on startup.
_config: dict = dict(_DEFAULTS)


async def _upsert(key: str, value) -> None:
    from database import SessionLocal
    from models import AgentConfig

    if SessionLocal is None:
        return
    now = datetime.now(timezone.utc)
    raw = json.dumps(value)
    stored = encrypt(raw) if is_secret_key(key) else raw
    async with SessionLocal() as session:
        stmt = (
            pg_insert(AgentConfig)
            .values(agent_slug=settings.agent_slug, key=key, value=stored, updated_at=now)
            .on_conflict_do_update(
                index_elements=["agent_slug", "key"],
                set_={"value": stored, "updated_at": now},
            )
        )
        await session.execute(stmt)
        await session.commit()


async def load_config_from_db() -> dict:
    """Load all config rows from DB into _config. Returns the raw DB dict (empty if no DB)."""
    from database import SessionLocal
    from models import AgentConfig

    if SessionLocal is None:
        logger.warning("load_config_from_db: DATABASE_URL not set — no DB session available")
        return {}
    try:
        async with SessionLocal() as session:
            rows = (
                await session.execute(
                    select(AgentConfig).where(AgentConfig.agent_slug == settings.agent_slug)
                )
            ).scalars().all()

        logger.info(
            "load_config_from_db: found %d row(s) in agent_config — keys: %s",
            len(rows),
            [r.key for r in rows],
        )

        if not rows:
            return {}

        db_cfg: dict = {}
        for r in rows:
            secret = is_secret_key(r.key)
            try:
                raw = decrypt(r.value) if secret else r.value
                db_cfg[r.key] = json.loads(raw)
                logger.debug("load_config_from_db: loaded key=%r (secret=%s)", r.key, secret)
            except Exception as exc:
                logger.error(
                    "load_config_from_db: failed to decode key=%r (secret=%s, "
                    "stored_prefix=%r): %s",
                    r.key,
                    secret,
                    r.value[:20] if r.value else "",
                    exc,
                )

        _config.update(db_cfg)
        logger.info("load_config_from_db: successfully loaded keys: %s", list(db_cfg))
        return db_cfg
    except Exception:
        logger.exception("load_config_from_db: DB query failed")
        return {}


class SettingsPayload(BaseModel):
    source_type: str = "file"
    sync_interval_minutes: int = 0
    cost_window: str = "30"
    display_currency: str = "USD"
    anomaly_threshold_pct: int = 20
    top_drivers_count: int = 5
    min_cost_threshold: float = 1.00
    api_key: str = ""


@router.get("")
async def get_settings() -> dict:
    await load_config_from_db()
    cfg = dict(_config)
    api_key = cfg.get("api_key", "")
    cfg["api_key_configured"] = bool(api_key)
    cfg["api_key_last4"] = (api_key[-4:] if api_key else "")
    if "api_key" in cfg:
        del cfg["api_key"]
    return cfg


@router.post("")
async def save_settings(payload: SettingsPayload) -> dict:
    data = payload.model_dump()
    _config.update(data)
    for k, v in data.items():
        await _upsert(k, v)
    return {"ok": True}
