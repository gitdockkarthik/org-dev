import asyncio
import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import settings
from encryption import decrypt, encrypt, is_secret_key
from report_store import add_report
from tools.noise_detector import classify_alerts
from tools.source import AlertSource, JSMSource, OpsgenieAPISource, StandaloneOpsgenieSource

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/settings", tags=["settings"])

_sync_lock = False  # Prevent concurrent syncs

_DEFAULTS: dict = {
    "source_type": "file",
    "cloud_id": "",
    "email": "",
    "api_token": "",
    "last_synced": None,
    "alert_count": None,
    "sync_interval_minutes": 0,
    "noise_threshold_repeat": 3,
    "noise_threshold_window_mins": 60,
    "noise_threshold_close_secs": 300,
    "sync_window_days": 7,
    "priority_weights": {"P1": -3, "P2": -2, "P3": 0, "P4": 1, "P5": 2},
    "noise_classification_threshold": 0,
    "noise_suspect_threshold": -2,
    "opsgenie_base_url": "",
}

# Write-through in-memory cache; populated from DB on startup.
_config: dict = dict(_DEFAULTS)

# Fired whenever settings are saved so the background sync loop re-evaluates immediately.
_sync_changed = asyncio.Event()


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

        # Re-encrypt any secrets stored with a previous key (or stored plaintext).
        # Idempotent: if already encrypted with the current key this is a no-op in terms of data.
        secret_keys = [k for k in db_cfg if is_secret_key(k) and db_cfg[k]]
        if secret_keys:
            for key in secret_keys:
                await _upsert(key, db_cfg[key])
            logger.info("load_config_from_db: re-encrypted %d secret key(s)", len(secret_keys))

        return db_cfg
    except Exception:
        logger.exception("load_config_from_db: DB query failed")
        return {}


async def _run_opsgenie_sync(full_sync: bool = False) -> dict:
    """Core sync logic callable from HTTP handler or lifespan startup."""
    global _sync_lock
    if _sync_lock:
        raise HTTPException(status_code=429, detail="Sync already in progress")
    _sync_lock = True
    try:
        source_type = _config.get("source_type", "opsgenie")
        if source_type == "standalone":
            source: AlertSource = StandaloneOpsgenieSource(
                api_key=_config["api_token"],
                base_url=_config.get("opsgenie_base_url") or "https://api.opsgenie.com",
            )
        else:
            source = JSMSource(
                cloud_id=_config["cloud_id"],
                email=_config["email"],
                api_token=_config["api_token"],
            )
        last_synced = _config.get("last_synced")
        if last_synced and not full_sync:
            alerts = await source.load_alerts(created_after=last_synced)
        else:
            sync_window_days = _config.get("sync_window_days", 7)
            alerts = await source.load_alerts(sync_window_days=sync_window_days)
        from datetime import datetime, timezone
        filename = f"opsgenie-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}.json"
        from report_store import get_latest_classified
        existing = get_latest_classified()
        if existing and last_synced:
            all_alerts_raw = [a for a in (existing or [])]
            existing_ids = {a.get("id") for a in all_alerts_raw}
            new_alerts = [a for a in alerts if a.get("id") not in existing_ids]
            combined_alerts = all_alerts_raw + new_alerts
            classified = classify_alerts(combined_alerts)
        else:
            classified = classify_alerts(alerts)
            combined_alerts = alerts
        report = add_report(filename, combined_alerts, classified)
        _config["last_synced"] = datetime.now(timezone.utc).isoformat()
        _config["alert_count"] = len(combined_alerts)
        await _upsert("last_synced", _config["last_synced"])
        await _upsert("alert_count", _config["alert_count"])
        return {
            "ok": True,
            "alert_count": len(combined_alerts),
            "last_synced": _config["last_synced"],
            "report": report,
        }
    finally:
        _sync_lock = False

class SettingsPayload(BaseModel):
    source_type: str = "file"
    cloud_id: str = ""
    email: str = ""
    api_token: str = ""
    sync_interval_minutes: int = 0
    noise_threshold_repeat: int = 3
    noise_threshold_window_mins: int = 60
    noise_threshold_close_secs: int = 300
    sync_window_days: int = 7
    priority_weights: dict = {"P1": -3, "P2": -2, "P3": 0, "P4": 1, "P5": 2}
    noise_classification_threshold: int = 0
    noise_suspect_threshold: int = -2
    opsgenie_base_url: str = ""


@router.get("")
async def get_settings() -> dict:
    await load_config_from_db()
    result = {k: v for k, v in _config.items() if k != "api_token"}
    result["api_key_configured"] = bool(_config.get("api_key", ""))
    result["api_key_last4"] = (
        _config.get("api_key", "")[-4:]
        if _config.get("api_key") else ""
    )
    result["sync_window_days"] = _config.get("sync_window_days", 7)
    result["priority_weights"] = _config.get(
        "priority_weights", {"P1": -3, "P2": -2, "P3": 0, "P4": 1, "P5": 2}
    )
    result["noise_classification_threshold"] = _config.get(
        "noise_classification_threshold", 0
    )
    result["api_token_configured"] = bool(_config.get("api_token", ""))
    result["genie_key_configured"] = bool(_config.get("api_token", "")) and _config.get("source_type") == "standalone" and _config.get("opsgenie_type") == "standalone"
    return result


@router.post("")
async def save_settings(payload: SettingsPayload) -> dict:
    data = payload.model_dump()
    _config.update(data)
    for k, v in data.items():
        await _upsert(k, v)
    _sync_changed.set()  # wake the background loop to re-evaluate interval immediately
    return {"ok": True}


@router.post("/sync")
async def sync_alerts() -> dict:
    source_type = _config.get("source_type")
    if source_type == "standalone":
        if not _config.get("api_token"):
            raise HTTPException(status_code=400, detail="Missing required field: api_token (GenieKey)")
    elif source_type == "opsgenie":
        for field in ("cloud_id", "email", "api_token"):
            if not _config.get(field):
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
    else:
        raise HTTPException(
            status_code=400,
            detail="Source type must be 'opsgenie' (JSM) or 'standalone' to sync",
        )
    return await _run_opsgenie_sync(full_sync=True)
