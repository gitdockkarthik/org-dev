"""CRUD + trigger-history routes for kafka_alert_configs / kafka_alert_triggers,
backing the Teams tab's "Alert Configuration and Reporting" sub-tab. Firing,
resolution and the digest live in teams_alerts.py; these routes only manage
rules and read their history."""
import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select

from config import settings
from models import KafkaAlertConfig, KafkaAlertTrigger
from storage import get_backend

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/alerts", tags=["alerts"])

_SEVERITIES = ("critical", "warning", "info")

# Fields that map to NOT NULL columns -- an explicit null for these in a PUT
# body is rejected rather than written.
_NON_NULLABLE_FIELDS = ("name", "alert_type", "severity", "threshold", "cooldown_minutes", "enabled")


class AlertConfigPayload(BaseModel):
    name: str = Field(min_length=1)
    alert_type: str = Field(min_length=1)
    cluster_id: int | None = None
    severity: str = "warning"
    threshold: int = Field(default=1, ge=1)
    webhook_url: str | None = None
    cooldown_minutes: int = Field(default=30, ge=0)
    enabled: bool = True

    @field_validator("severity")
    @classmethod
    def _check_severity(cls, v: str | None) -> str | None:
        if v is not None and v not in _SEVERITIES:
            raise ValueError(f"severity must be one of {', '.join(_SEVERITIES)}")
        return v


class AlertConfigUpdatePayload(BaseModel):
    name: str | None = Field(default=None, min_length=1)
    alert_type: str | None = Field(default=None, min_length=1)
    cluster_id: int | None = None
    severity: str | None = None
    threshold: int | None = Field(default=None, ge=1)
    webhook_url: str | None = None
    cooldown_minutes: int | None = Field(default=None, ge=0)
    enabled: bool | None = None

    @field_validator("severity")
    @classmethod
    def _check_severity(cls, v: str | None) -> str | None:
        if v is not None and v not in _SEVERITIES:
            raise ValueError(f"severity must be one of {', '.join(_SEVERITIES)}")
        return v


def _get_session_factory(dashboard: bool = False):
    # UI reads use the dedicated dashboard pool (see database.py) so they can
    # never be starved by background collector jobs; writes use the main one.
    if dashboard:
        from database import DashboardSessionLocal as factory
    else:
        from database import SessionLocal as factory
    if factory is None:
        raise HTTPException(status_code=500, detail="Database not available")
    return factory


async def _get_cluster_names() -> dict[int, str]:
    clusters = await get_backend().get_clusters(settings.agent_slug)
    return {int(c["id"]): c.get("name", str(c["id"])) for c in clusters if c.get("id") is not None}


def _cluster_label(cluster_id: int | None, cluster_names: dict[int, str]) -> str:
    if cluster_id is None:
        return "All clusters"
    return cluster_names.get(cluster_id, str(cluster_id))


async def _check_cluster_exists(cluster_id: int | None, cluster_names: dict[int, str]) -> None:
    if cluster_id is not None and cluster_id not in cluster_names:
        raise HTTPException(status_code=422, detail=f"Unknown cluster_id {cluster_id}")


def _iso(ts: datetime | None) -> str | None:
    return ts.isoformat() if ts else None


def _config_out(cfg: KafkaAlertConfig, cluster_names: dict[int, str], open_trigger_count: int) -> dict:
    return {
        "id": cfg.id,
        "name": cfg.name,
        "alert_type": cfg.alert_type,
        "cluster_id": cfg.cluster_id,
        "cluster_name": _cluster_label(cfg.cluster_id, cluster_names),
        "severity": cfg.severity,
        "threshold": int((cfg.config or {}).get("threshold", 1)),
        "config": cfg.config or {},
        "webhook_url": cfg.webhook_url,
        "cooldown_minutes": cfg.cooldown_minutes,
        "enabled": cfg.enabled,
        "open_trigger_count": open_trigger_count,
        "created_at": _iso(cfg.created_at),
        "updated_at": _iso(cfg.updated_at),
    }


async def _open_trigger_count(session, config_id: int) -> int:
    return (await session.execute(
        select(func.count(KafkaAlertTrigger.id)).where(
            KafkaAlertTrigger.alert_config_id == config_id,
            KafkaAlertTrigger.resolved_at.is_(None),
        )
    )).scalar_one()


@router.get("/configs")
async def list_alert_configs() -> dict:
    try:
        cluster_names = await _get_cluster_names()
        async with _get_session_factory(dashboard=True)() as session:
            configs = (await session.execute(
                select(KafkaAlertConfig).order_by(KafkaAlertConfig.created_at.desc(), KafkaAlertConfig.id.desc())
            )).scalars().all()
            open_counts = dict((await session.execute(
                select(KafkaAlertTrigger.alert_config_id, func.count(KafkaAlertTrigger.id))
                .where(KafkaAlertTrigger.resolved_at.is_(None))
                .group_by(KafkaAlertTrigger.alert_config_id)
            )).all())
        return {"configs": [_config_out(c, cluster_names, open_counts.get(c.id, 0)) for c in configs]}
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("list_alert_configs failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Failed to list alert configs: {exc}")


@router.post("/configs")
async def create_alert_config(payload: AlertConfigPayload) -> dict:
    try:
        cluster_names = await _get_cluster_names()
        await _check_cluster_exists(payload.cluster_id, cluster_names)
        now = datetime.now(timezone.utc)
        cfg = KafkaAlertConfig(
            name=payload.name,
            alert_type=payload.alert_type,
            cluster_id=payload.cluster_id,
            severity=payload.severity,
            config={"threshold": payload.threshold},
            # Empty string stored as NULL so it falls back to the agent-level webhook.
            webhook_url=payload.webhook_url or None,
            cooldown_minutes=payload.cooldown_minutes,
            enabled=payload.enabled,
            created_at=now,
            updated_at=now,
        )
        async with _get_session_factory()() as session:
            session.add(cfg)
            await session.commit()
        return {"config": _config_out(cfg, cluster_names, 0)}
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("create_alert_config failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Failed to create alert config: {exc}")


@router.put("/configs/{config_id}")
async def update_alert_config(config_id: int, payload: AlertConfigUpdatePayload) -> dict:
    # Only fields actually present in the request body are applied, so e.g.
    # {"enabled": false} alone toggles a rule without touching anything else.
    updates = payload.model_dump(exclude_unset=True)
    for field in _NON_NULLABLE_FIELDS:
        if field in updates and updates[field] is None:
            raise HTTPException(status_code=422, detail=f"{field} cannot be null")
    try:
        cluster_names = await _get_cluster_names()
        if "cluster_id" in updates:
            await _check_cluster_exists(updates["cluster_id"], cluster_names)
        async with _get_session_factory()() as session:
            cfg = await session.get(KafkaAlertConfig, config_id)
            if cfg is None:
                raise HTTPException(status_code=404, detail="Alert config not found")
            if "threshold" in updates:
                # Reassign (not mutate in place) so SQLAlchemy detects the JSONB change.
                cfg.config = {**(cfg.config or {}), "threshold": updates.pop("threshold")}
            if "webhook_url" in updates:
                updates["webhook_url"] = updates["webhook_url"] or None
            for field, value in updates.items():
                setattr(cfg, field, value)
            cfg.updated_at = datetime.now(timezone.utc)
            await session.commit()
            open_count = await _open_trigger_count(session, config_id)
        return {"config": _config_out(cfg, cluster_names, open_count)}
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("update_alert_config failed for id=%s: %s", config_id, exc)
        raise HTTPException(status_code=500, detail=f"Failed to update alert config: {exc}")


@router.delete("/configs/{config_id}")
async def delete_alert_config(config_id: int) -> dict:
    try:
        async with _get_session_factory()() as session:
            cfg = await session.get(KafkaAlertConfig, config_id)
            if cfg is None:
                raise HTTPException(status_code=404, detail="Alert config not found")
            # Its kafka_alert_triggers rows are removed by the DB's
            # ON DELETE CASCADE (migration 0050).
            await session.delete(cfg)
            await session.commit()
        return {"ok": True, "deleted_id": config_id}
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("delete_alert_config failed for id=%s: %s", config_id, exc)
        raise HTTPException(status_code=500, detail=f"Failed to delete alert config: {exc}")


@router.get("/triggers")
async def list_alert_triggers(
    config_id: int | None = None,
    limit: int = Query(default=10, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    since_days: int = Query(default=3, ge=1),
) -> dict:
    try:
        cluster_names = await _get_cluster_names()
        since = datetime.now(timezone.utc) - timedelta(days=since_days)
        filters = [KafkaAlertTrigger.triggered_at >= since]
        if config_id is not None:
            filters.append(KafkaAlertTrigger.alert_config_id == config_id)
        async with _get_session_factory(dashboard=True)() as session:
            total_count = (await session.execute(
                select(func.count(KafkaAlertTrigger.id)).where(*filters)
            )).scalar_one()
            rows = (await session.execute(
                select(KafkaAlertTrigger, KafkaAlertConfig.name)
                .join(KafkaAlertConfig, KafkaAlertTrigger.alert_config_id == KafkaAlertConfig.id)
                .where(*filters)
                .order_by(KafkaAlertTrigger.triggered_at.desc(), KafkaAlertTrigger.id.desc())
                .limit(limit)
                .offset(offset)
            )).all()
        triggers = [
            {
                "id": t.id,
                "alert_config_id": t.alert_config_id,
                "config_name": config_name,
                "cluster_id": t.cluster_id,
                "cluster_name": _cluster_label(t.cluster_id, cluster_names),
                "triggered_at": _iso(t.triggered_at),
                "resolved_at": _iso(t.resolved_at),
                "is_recurrence": t.is_recurrence,
                "metric_value": t.metric_value,
                "teams_post_success": t.teams_post_success,
                "error_detail": t.error_detail,
            }
            for t, config_name in rows
        ]
        return {
            "triggers": triggers,
            "total_count": total_count,
            "limit": limit,
            "offset": offset,
            "since_days": since_days,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("list_alert_triggers failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Failed to list alert triggers: {exc}")
