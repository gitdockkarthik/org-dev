import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import settings
from encryption import decrypt, encrypt, is_secret_key
import kafka_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/settings", tags=["settings"])

_DEFAULTS: dict = {
    "source_type": "synthetic",
    "bootstrap_servers": "",
    "sasl_username": "",
    "sasl_password": "",
    "tls_enabled": False,
    "collection_interval_secs": 0,
    "last_synced": None,
    "broker_count": None,
    "consumer_group_count": None,
    "topic_count": None,
    "connector_count": None,
    "lag_threshold": 10000,
    "heap_threshold_pct": 80,
    "urp_threshold": 0,
    "connector_alert_enabled": True,
    "retention_threshold_pct": 80,
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
            "load_config_from_db: found %d row(s) — keys: %s",
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
                    "load_config_from_db: failed to decode key=%r (secret=%s): %s",
                    r.key, secret, exc,
                )

        _config.update(db_cfg)
        logger.info("load_config_from_db: loaded keys: %s", list(db_cfg))
        return db_cfg
    except Exception:
        logger.exception("load_config_from_db: DB query failed")
        return {}


class SettingsPayload(BaseModel):
    source_type: str = "synthetic"
    bootstrap_servers: str = ""
    sasl_username: str = ""
    sasl_password: str = ""
    tls_enabled: bool = False
    collection_interval_secs: int = 0
    lag_threshold: int = 10000
    heap_threshold_pct: int = 80
    urp_threshold: int = 0
    connector_alert_enabled: bool = True
    retention_threshold_pct: int = 80
    api_key: str = ""


@router.get("")
async def get_settings() -> dict:
    await load_config_from_db()
    cfg = dict(_config)
    api_key = cfg.get("api_key", "")
    cfg["api_key_configured"] = bool(api_key)
    cfg["api_key_last4"] = api_key[-4:] if api_key else ""
    cfg.pop("api_key", None)
    cfg.pop("sasl_password", None)
    return cfg


@router.post("")
async def save_settings(payload: SettingsPayload) -> dict:
    data = payload.model_dump()
    _config.update(data)
    for k, v in data.items():
        await _upsert(k, v)
    return {"ok": True}


@router.post("/sync")
async def sync_metrics() -> dict:
    source = _config.get("source_type", "synthetic")

    if source == "redpanda":
        raise HTTPException(
            status_code=400,
            detail="Redpanda Cloud connectivity is available in Phase 2. Use Synthetic Data for now.",
        )
    if source in ("kafka", "msk"):
        raise HTTPException(
            status_code=400,
            detail=f"Source type '{source}' is available in Phase 3.",
        )

    from tools.synthetic import SyntheticCollector
    collector = SyntheticCollector()
    data = await collector.collect()
    kafka_store.set_cluster_data(data, source_type=source)

    meta = kafka_store.get_sync_meta()
    _config["last_synced"] = meta["last_synced"]
    _config["broker_count"] = meta["broker_count"]
    _config["consumer_group_count"] = meta["consumer_group_count"]
    _config["topic_count"] = meta["topic_count"]
    _config["connector_count"] = meta["connector_count"]

    await _upsert("last_synced", _config["last_synced"])
    await _upsert("broker_count", _config["broker_count"])
    await _upsert("consumer_group_count", _config["consumer_group_count"])
    await _upsert("topic_count", _config["topic_count"])
    await _upsert("connector_count", _config["connector_count"])

    return {
        "ok": True,
        "broker_count": meta["broker_count"],
        "consumer_group_count": meta["consumer_group_count"],
        "topic_count": meta["topic_count"],
        "connector_count": meta["connector_count"],
        "last_synced": meta["last_synced"],
    }
