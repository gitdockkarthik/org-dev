"""CRUD + trigger-history routes for kafka_alert_configs / kafka_alert_triggers,
backing the Teams tab's "Alert Configuration and Reporting" sub-tab. Firing
and resolution (including resolve cards) live in teams_alerts.py; these
routes only manage rules and read their history."""
import copy
import logging
import math
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy import func, select

from config import settings
from models import KafkaAlertConfig, KafkaAlertTrigger
from storage import get_backend

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/alerts", tags=["alerts"])

_SEVERITIES = ("critical", "warning", "info")

# Threshold-based rules (broker.* evaluated by _evaluate_broker_thresholds in
# teams_alerts.py; cluster.* not evaluated yet): config holds
# {"mode": "simple", <tier>: <threshold>, ...} instead of the single
# "threshold" count, and the tier that fired decides the severity -- the
# rule's own `severity` is ignored for these types.
_THRESHOLD_ALERT_TYPES = (
    "broker.cpu_pct", "broker.heap_pct", "cluster.urp_total", "cluster.rf_below_min", "cluster.leader_skew",
    "cluster.msg_rate_in", "cluster.data_gb_max", "cluster.data_spread_pct", "cluster.zk_ensemble",
    "cluster.connect_failed_connectors", "cluster.connect_failed_tasks", "cluster.connect_workers_down",
    "cluster.sr_nodes_down", "cluster.sr_soft_deleted",
)
# Threshold types that fire when the value drops BELOW a tier: tiers are
# >= 0 (zero allowed) and descending (info > warning > critical), and the
# stored config carries "direction": "below".
_BELOW_ALERT_TYPES = ("cluster.msg_rate_in",)
_TIERS = ("info", "warning", "critical")  # ascending
# Threshold types whose tiers are counts: whole numbers > 0 only.
_WHOLE_TIER_ALERT_TYPES = (
    "cluster.connect_failed_connectors", "cluster.connect_failed_tasks", "cluster.connect_workers_down",
    "cluster.sr_nodes_down", "cluster.sr_soft_deleted",
)

# The one type whose config also holds "min_rf" (integer >= 2), stored next to
# mode and tiers and never stored for any other type.
_MIN_RF_ALERT_TYPE = "cluster.rf_below_min"
_MIN_RF_ERROR = "min_rf must be a whole number of at least 2"

# Cluster lag rules (level thresholds): one trigger per consumer group or
# connector, evaluated by teams_alerts._evaluate_cluster_lag. Config holds the
# generic tiers flat like the threshold types ({"mode": "simple", <tier>: n}),
# plus "custom" ([{"name", "tiers"}], exact names with their own tiers),
# "blacklist" ([name], exact names never evaluated unless also custom -- which
# is rejected) and "reminder_hours". All tiers are whole numbers > 0.
_LAG_ALERT_TYPES = ("cluster.consumer_lag", "cluster.connector_lag")
_LAG_CUSTOM_MAX = 200
_LAG_BLACKLIST_MAX = 500
_LAG_NAME_MAX = 256
_LAG_REMINDER_DEFAULT_HOURS = 6
_LAG_REMINDER_MAX_HOURS = 168
_LAG_REMINDER_ERROR = f"reminder_hours must be a whole number from 1 to {_LAG_REMINDER_MAX_HOURS}"

# General rules apply to all clusters; every other type is a cluster rule
# (one per cluster per type, cluster required).
_GENERAL_ALERT_TYPES = ("close_wait_spike", "broker_unreachable")

# Labels used in copied rule names ("<label> - <cluster name>"); must match the
# ALERT_TYPES labels in teams.html. Types not listed fall back to alert_type.
_TYPE_LABELS = {
    "broker.cpu_pct": "Broker CPU %",
    "broker.heap_pct": "Broker heap %",
    "cluster.urp_total": "Under-replicated partitions",
    "cluster.rf_below_min": "Topics below minimum RF",
    "cluster.leader_skew": "Leader partition skew",
    "cluster.msg_rate_in": "Message rate in",
    "cluster.data_gb_max": "Data on disk (max broker)",
    "cluster.data_spread_pct": "Data spread between brokers",
    "cluster.zk_ensemble": "ZooKeeper ensemble problems",
    "cluster.connect_failed_connectors": "Connect failed connectors",
    "cluster.connect_failed_tasks": "Connect failed tasks",
    "cluster.connect_workers_down": "Connect workers down",
    "cluster.sr_nodes_down": "Schema Registry nodes down",
    "cluster.sr_soft_deleted": "Schema Registry soft-deleted subjects",
    "cluster.consumer_lag": "Consumer group lag",
    "cluster.connector_lag": "Connector lag",
}

# Fields that map to NOT NULL columns -- an explicit null for these in a PUT
# body is rejected rather than written.
_NON_NULLABLE_FIELDS = ("name", "alert_type", "severity", "threshold", "cooldown_minutes", "enabled", "email_enabled")


class AlertConfigPayload(BaseModel):
    name: str = Field(min_length=1)
    alert_type: str = Field(min_length=1)
    cluster_id: int | None = None
    severity: str = "warning"
    threshold: int = Field(default=1, ge=1)
    webhook_url: str | None = None
    cooldown_minutes: int = Field(default=30, ge=0)
    enabled: bool = True
    email_enabled: bool = False
    tiers: dict | None = None
    send_resolve_card: bool | None = None
    min_rf: int | None = None
    # Lag rule parts (_LAG_ALERT_TYPES only; validated by _lag_config).
    custom: list | None = None
    blacklist: list | None = None
    reminder_hours: int | None = None

    @field_validator("severity")
    @classmethod
    def _check_severity(cls, v: str | None) -> str | None:
        if v is not None and v not in _SEVERITIES:
            raise ValueError(f"severity must be one of {', '.join(_SEVERITIES)}")
        return v

    @field_validator("min_rf", mode="before")
    @classmethod
    def _check_min_rf_type(cls, v):
        # Before int coercion, so true, 2.5 and "3" are rejected rather than
        # turned into 1, an error and 3. The >= 2 check is per type (see _min_rf).
        if v is not None and (isinstance(v, bool) or not isinstance(v, int)):
            raise ValueError(_MIN_RF_ERROR)
        return v

    @field_validator("reminder_hours", mode="before")
    @classmethod
    def _check_reminder_hours_type(cls, v):
        # Same as min_rf: no coercion of true, 2.5 or "3"; range in _lag_config.
        if v is not None and (isinstance(v, bool) or not isinstance(v, int)):
            raise ValueError(_LAG_REMINDER_ERROR)
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
    email_enabled: bool | None = None
    tiers: dict | None = None
    send_resolve_card: bool | None = None
    min_rf: int | None = None
    # Lag rule parts (_LAG_ALERT_TYPES only; validated by _lag_config).
    custom: list | None = None
    blacklist: list | None = None
    reminder_hours: int | None = None

    @field_validator("severity")
    @classmethod
    def _check_severity(cls, v: str | None) -> str | None:
        if v is not None and v not in _SEVERITIES:
            raise ValueError(f"severity must be one of {', '.join(_SEVERITIES)}")
        return v

    @field_validator("min_rf", mode="before")
    @classmethod
    def _check_min_rf_type(cls, v):
        # Before int coercion, so true, 2.5 and "3" are rejected rather than
        # turned into 1, an error and 3. The >= 2 check is per type (see _min_rf).
        if v is not None and (isinstance(v, bool) or not isinstance(v, int)):
            raise ValueError(_MIN_RF_ERROR)
        return v

    @field_validator("reminder_hours", mode="before")
    @classmethod
    def _check_reminder_hours_type(cls, v):
        # Same as min_rf: no coercion of true, 2.5 or "3"; range in _lag_config.
        if v is not None and (isinstance(v, bool) or not isinstance(v, int)):
            raise ValueError(_LAG_REMINDER_ERROR)
        return v


class AlertCopyPayload(BaseModel):
    target_cluster_ids: list[int] = Field(min_length=1)
    source_cluster_id: int | None = None
    config_id: int | None = None
    preview: bool = False

    @model_validator(mode="after")
    def _check_source(self) -> "AlertCopyPayload":
        if (self.source_cluster_id is None) == (self.config_id is None):
            raise ValueError("give exactly one of source_cluster_id or config_id")
        return self


def _threshold_config(tiers: dict | None, below: bool = False, whole: bool = False) -> dict:
    """Validate a threshold rule's tiers and return its stored config. 422
    unless: keys only from _TIERS, values numeric and > 0, at least one tier,
    and info < warning < critical among the tiers given. With below=True
    (_BELOW_ALERT_TYPES): values >= 0 and info > warning > critical, stored
    with "direction": "below". With whole=True (_WHOLE_TIER_ALERT_TYPES):
    values must also be whole numbers."""
    if not tiers:
        raise HTTPException(status_code=422, detail=f"tiers must set at least one of {', '.join(_TIERS)}")
    unknown = [k for k in tiers if k not in _TIERS]
    if unknown:
        raise HTTPException(status_code=422, detail=f"Unknown tier(s) {', '.join(map(str, unknown))}; allowed: {', '.join(_TIERS)}")
    for tier, value in tiers.items():
        # bool is an int subclass -- reject it explicitly.
        bad = isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
        if below:
            if bad or value < 0:
                raise HTTPException(status_code=422, detail=f"tiers.{tier} must be a number of 0 or more")
        elif whole and (bad or not isinstance(value, int) or value <= 0):
            raise HTTPException(status_code=422, detail=f"tiers.{tier} must be a whole number greater than 0")
        elif bad or value <= 0:
            raise HTTPException(status_code=422, detail=f"tiers.{tier} must be a number greater than 0")
    given = [(t, tiers[t]) for t in _TIERS if t in tiers]
    if below:
        for (lower, lower_value), (higher, higher_value) in zip(given, given[1:]):
            if lower_value <= higher_value:
                raise HTTPException(status_code=422, detail=f"tiers.{lower} must be greater than tiers.{higher}")
        return {"mode": "simple", "direction": "below", **dict(given)}
    for (lower, lower_value), (higher, higher_value) in zip(given, given[1:]):
        if lower_value >= higher_value:
            raise HTTPException(status_code=422, detail=f"tiers.{lower} must be less than tiers.{higher}")
    return {"mode": "simple", **dict(given)}


def _lag_names(value, field: str, limit: int) -> list[str]:
    """Trimmed, unique, non-empty names of at most _LAG_NAME_MAX characters."""
    if not isinstance(value, list):
        raise HTTPException(status_code=422, detail=f"{field} must be a list")
    if len(value) > limit:
        raise HTTPException(status_code=422, detail=f"{field} allows at most {limit} entries")
    names: list[str] = []
    for raw in value:
        if not isinstance(raw, str) or not raw.strip():
            raise HTTPException(status_code=422, detail=f"{field} names must be non-empty strings")
        name = raw.strip()
        if len(name) > _LAG_NAME_MAX:
            raise HTTPException(status_code=422, detail=f"{field} name longer than {_LAG_NAME_MAX} characters: {name[:40]}...")
        if name in names:
            raise HTTPException(status_code=422, detail=f"{field} lists {name!r} more than once")
        names.append(name)
    return names


def _lag_config(previous: dict | None, tiers=None, custom=None, blacklist=None, reminder_hours=None) -> dict:
    """Validate a lag rule's parts and return its stored config. Each part
    left as None is carried over from previous (an existing lag rule's
    config), so a partial update keeps the rest. 422 unless: generic tiers
    valid like the whole-number threshold types; custom rows {"name",
    "tiers"} with unique names and valid tiers, at most _LAG_CUSTOM_MAX;
    blacklist unique, at most _LAG_BLACKLIST_MAX; no name in both lists;
    reminder_hours a whole number from 1 to _LAG_REMINDER_MAX_HOURS."""
    previous = previous or {}
    if tiers is None:
        tiers = {t: previous[t] for t in _TIERS if t in previous} or None
    config = _threshold_config(tiers, whole=True)

    if custom is None:
        custom = previous.get("custom", [])
    if not isinstance(custom, list):
        raise HTTPException(status_code=422, detail="custom must be a list")
    for row in custom:
        if not isinstance(row, dict) or set(row) - {"name", "tiers"}:
            raise HTTPException(status_code=422, detail='custom rows must be {"name": ..., "tiers": {...}}')
    custom_names = _lag_names([row.get("name") for row in custom], "custom", _LAG_CUSTOM_MAX)
    custom_rows = []
    for name, row in zip(custom_names, custom):
        try:
            row_tiers = _threshold_config(row.get("tiers"), whole=True)
        except HTTPException as exc:
            raise HTTPException(status_code=422, detail=f"custom {name!r}: {exc.detail}")
        custom_rows.append({"name": name, "tiers": {t: row_tiers[t] for t in _TIERS if t in row_tiers}})

    blacklist_names = _lag_names(previous.get("blacklist", []) if blacklist is None else blacklist, "blacklist", _LAG_BLACKLIST_MAX)
    both = next((n for n in custom_names if n in set(blacklist_names)), None)
    if both is not None:
        raise HTTPException(status_code=422, detail=f"{both!r} is in both custom and blacklist")

    if reminder_hours is None:
        reminder_hours = previous.get("reminder_hours", _LAG_REMINDER_DEFAULT_HOURS)
    if (isinstance(reminder_hours, bool) or not isinstance(reminder_hours, int)
            or not 1 <= reminder_hours <= _LAG_REMINDER_MAX_HOURS):
        raise HTTPException(status_code=422, detail=_LAG_REMINDER_ERROR)

    config.update({"custom": custom_rows, "blacklist": blacklist_names, "reminder_hours": reminder_hours})
    return config


def _min_rf(value) -> int:
    """Validate a cluster.rf_below_min rule's min_rf: 422 unless an integer >= 2."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 2:
        raise HTTPException(status_code=422, detail=_MIN_RF_ERROR)
    return value


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


def _check_cluster_required(alert_type: str, cluster_id: int | None) -> None:
    if alert_type not in _GENERAL_ALERT_TYPES and cluster_id is None:
        raise HTTPException(status_code=422, detail="cluster_id is required for this alert type")


async def _check_no_duplicate(session, alert_type: str, cluster_id: int, exclude_id: int | None = None) -> None:
    """409 if another rule already has this type on this cluster (cluster rules only)."""
    query = select(KafkaAlertConfig.id).where(
        KafkaAlertConfig.alert_type == alert_type,
        KafkaAlertConfig.cluster_id == cluster_id,
    )
    if exclude_id is not None:
        query = query.where(KafkaAlertConfig.id != exclude_id)
    existing_id = (await session.execute(query.limit(1))).scalar_one_or_none()
    if existing_id is not None:
        raise HTTPException(status_code=409, detail=f"A rule of this type already exists for this cluster (id {existing_id})")


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
        "send_resolve_card": bool((cfg.config or {}).get("send_resolve_card", True)),
        "min_rf": (cfg.config or {}).get("min_rf"),
        "webhook_url": cfg.webhook_url,
        "cooldown_minutes": cfg.cooldown_minutes,
        "enabled": cfg.enabled,
        "email_enabled": cfg.email_enabled,
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
        _check_cluster_required(payload.alert_type, payload.cluster_id)
        cluster_names = await _get_cluster_names()
        await _check_cluster_exists(payload.cluster_id, cluster_names)
        if payload.alert_type in _LAG_ALERT_TYPES:
            config = _lag_config(None, payload.tiers, payload.custom, payload.blacklist, payload.reminder_hours)
        elif payload.alert_type in _THRESHOLD_ALERT_TYPES:
            config = _threshold_config(
                payload.tiers, payload.alert_type in _BELOW_ALERT_TYPES, payload.alert_type in _WHOLE_TIER_ALERT_TYPES,
            )
        else:
            config = {"threshold": payload.threshold}
        if payload.alert_type == _MIN_RF_ALERT_TYPE:
            config["min_rf"] = _min_rf(payload.min_rf)
        # Stored only when given; a missing key means true (see teams_alerts._send_resolve_cards).
        if payload.send_resolve_card is not None:
            config["send_resolve_card"] = payload.send_resolve_card
        now = datetime.now(timezone.utc)
        cfg = KafkaAlertConfig(
            name=payload.name,
            alert_type=payload.alert_type,
            cluster_id=payload.cluster_id,
            severity=payload.severity,
            config=config,
            # Empty string stored as NULL so it falls back to the agent-level webhook.
            webhook_url=payload.webhook_url or None,
            cooldown_minutes=payload.cooldown_minutes,
            enabled=payload.enabled,
            email_enabled=payload.email_enabled,
            created_at=now,
            updated_at=now,
        )
        async with _get_session_factory()() as session:
            if payload.alert_type not in _GENERAL_ALERT_TYPES:
                await _check_no_duplicate(session, payload.alert_type, payload.cluster_id)
            session.add(cfg)
            await session.commit()
        return {"config": _config_out(cfg, cluster_names, 0)}
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("create_alert_config failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Failed to create alert config: {exc}")


@router.post("/configs/copy")
async def copy_alert_configs(payload: AlertCopyPayload) -> dict:
    """Copy cluster rules to other clusters. A target that already has the
    type is skipped, never modified; copies are created disabled; general
    rules are never copied; history is not copied."""
    try:
        cluster_names = await _get_cluster_names()
        target_ids = list(dict.fromkeys(payload.target_cluster_ids))
        for cluster_id in target_ids:
            await _check_cluster_exists(cluster_id, cluster_names)
        await _check_cluster_exists(payload.source_cluster_id, cluster_names)
        created_cfgs: list[KafkaAlertConfig] = []
        skipped: list[dict] = []
        async with _get_session_factory()() as session:
            if payload.config_id is not None:
                source = await session.get(KafkaAlertConfig, payload.config_id)
                if source is None:
                    raise HTTPException(status_code=404, detail="Alert config not found")
                if source.alert_type in _GENERAL_ALERT_TYPES or source.cluster_id is None:
                    raise HTTPException(status_code=422, detail="Only cluster rules can be copied")
                sources = [source]
            else:
                sources = (await session.execute(
                    select(KafkaAlertConfig)
                    .where(
                        KafkaAlertConfig.cluster_id == payload.source_cluster_id,
                        KafkaAlertConfig.alert_type.not_in(_GENERAL_ALERT_TYPES),
                    )
                    .order_by(KafkaAlertConfig.id)
                )).scalars().all()
            existing = {
                (alert_type, cluster_id): config_id
                for config_id, alert_type, cluster_id in (await session.execute(
                    select(KafkaAlertConfig.id, KafkaAlertConfig.alert_type, KafkaAlertConfig.cluster_id)
                    .where(KafkaAlertConfig.cluster_id.in_(target_ids))
                )).all()
            }
            now = datetime.now(timezone.utc)
            for source in sources:
                for cluster_id in target_ids:
                    if cluster_id == source.cluster_id:
                        reason = "same cluster"
                    elif (source.alert_type, cluster_id) in existing:
                        existing_id = existing[(source.alert_type, cluster_id)]
                        reason = f"exists (id {existing_id})" if existing_id is not None else "exists (copied in this request)"
                    else:
                        reason = None
                    if reason:
                        skipped.append({
                            "alert_type": source.alert_type,
                            "cluster_id": cluster_id,
                            "cluster_name": _cluster_label(cluster_id, cluster_names),
                            "reason": reason,
                        })
                        continue
                    created_cfgs.append(KafkaAlertConfig(
                        name=f"{_TYPE_LABELS.get(source.alert_type, source.alert_type)} - {_cluster_label(cluster_id, cluster_names)}",
                        alert_type=source.alert_type,
                        cluster_id=cluster_id,
                        severity=source.severity,
                        config=copy.deepcopy(source.config or {}),
                        webhook_url=source.webhook_url,
                        cooldown_minutes=source.cooldown_minutes,
                        enabled=False,
                        email_enabled=False,
                        created_at=now,
                        updated_at=now,
                    ))
                    # Guards against two source rules of one type landing on the same target.
                    existing[(source.alert_type, cluster_id)] = None
            if not payload.preview and created_cfgs:
                session.add_all(created_cfgs)
                await session.commit()
        created = [
            {
                "id": None if payload.preview else cfg.id,
                "alert_type": cfg.alert_type,
                "cluster_id": cfg.cluster_id,
                "cluster_name": _cluster_label(cfg.cluster_id, cluster_names),
                "name": cfg.name,
            }
            for cfg in created_cfgs
        ]
        return {"preview": payload.preview, "created": created, "skipped": skipped}
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("copy_alert_configs failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Failed to copy alert configs: {exc}")


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
            # Cluster-rule checks run only when the type or cluster is in the
            # body, so other edits to existing rules behave as before.
            if "alert_type" in updates or "cluster_id" in updates:
                new_type = updates.get("alert_type", cfg.alert_type)
                new_cluster_id = updates.get("cluster_id", cfg.cluster_id)
                if new_type not in _GENERAL_ALERT_TYPES:
                    _check_cluster_required(new_type, new_cluster_id)
                    if new_type != cfg.alert_type or new_cluster_id != cfg.cluster_id:
                        await _check_no_duplicate(session, new_type, new_cluster_id, exclude_id=cfg.id)
            tiers_given = "tiers" in updates
            tiers = updates.pop("tiers", None)
            send_resolve_card = updates.pop("send_resolve_card", None)
            min_rf = updates.pop("min_rf", None)
            lag_parts = {k: updates.pop(k, None) for k in ("custom", "blacklist", "reminder_hours")}
            previous_config = cfg.config or {}
            # Validated before any config is replaced: an rf_below_min rule
            # must end up with min_rf, either given here or carried over.
            if updates.get("alert_type", cfg.alert_type) == _MIN_RF_ALERT_TYPE:
                min_rf = _min_rf(min_rf if min_rf is not None else previous_config.get("min_rf"))
            if updates.get("alert_type", cfg.alert_type) in _LAG_ALERT_TYPES:
                # Parts not in the body are kept; a rule switched to a lag
                # type starts empty and must bring its tiers.
                updates.pop("threshold", None)
                same_type = updates.get("alert_type", cfg.alert_type) == cfg.alert_type
                cfg.config = _lag_config(previous_config if same_type else None, tiers, **lag_parts)
            elif updates.get("alert_type", cfg.alert_type) in _THRESHOLD_ALERT_TYPES:
                # Tiers replace the whole config; a rule switched to a
                # threshold type must bring its tiers with it.
                updates.pop("threshold", None)
                if tiers_given or updates.get("alert_type", cfg.alert_type) != cfg.alert_type:
                    rule_type = updates.get("alert_type", cfg.alert_type)
                    cfg.config = _threshold_config(
                        tiers, rule_type in _BELOW_ALERT_TYPES, rule_type in _WHOLE_TIER_ALERT_TYPES,
                    )
            elif "threshold" in updates:
                # Reassign (not mutate in place) so SQLAlchemy detects the JSONB change.
                cfg.config = {**(cfg.config or {}), "threshold": updates.pop("threshold")}
            if cfg.alert_type in _LAG_ALERT_TYPES and updates.get("alert_type", cfg.alert_type) not in _LAG_ALERT_TYPES:
                # Leaving a lag type: its lag-only parts no longer apply.
                cfg.config = {k: v for k, v in (cfg.config or {}).items() if k not in ("custom", "blacklist", "reminder_hours")}
            # send_resolve_card lives in config: write it when given, otherwise
            # carry the existing value over a replaced (tiers) config.
            if send_resolve_card is not None:
                cfg.config = {**(cfg.config or {}), "send_resolve_card": send_resolve_card}
            elif "send_resolve_card" in previous_config and "send_resolve_card" not in (cfg.config or {}):
                cfg.config = {**(cfg.config or {}), "send_resolve_card": previous_config["send_resolve_card"]}
            # min_rf likewise (validated above); dropped if the rule left the type.
            if updates.get("alert_type", cfg.alert_type) == _MIN_RF_ALERT_TYPE:
                if (cfg.config or {}).get("min_rf") != min_rf:
                    cfg.config = {**(cfg.config or {}), "min_rf": min_rf}
            elif "min_rf" in (cfg.config or {}):
                cfg.config = {k: v for k, v in cfg.config.items() if k != "min_rf"}
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
