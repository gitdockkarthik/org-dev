"""CSV export of the alert rules (kafka_alert_configs), read-only: one row per
rule, all rules ordered by id, for the Teams tab's "Export CSV" button. The
file is meant to be read back by a later import, so EXPORT_COLUMNS and the
cell formats below are a contract: add columns at the end, never rename or
reorder them.

Cells: booleans are true / false; numbers are plain decimals; timestamps are
ISO 8601 in UTC; a column that does not apply to the rule's type, or a tier
that is not set, is an empty cell. `enabled` is the rule's Teams toggle (the
model has no separate teams_enabled) and `email_enabled` its Email toggle;
each controls only its own channel, and a rule with both false is not
evaluated. Tier types take their severity from the
tier, so their severity cell is empty; their direction is "above" unless the
stored config says "below". threshold (the last column) is the config's
"threshold" of the two general types (close_wait_spike, broker_unreachable),
empty for every other type.

Never written: webhook URLs (only has_custom_webhook), any other secret,
email addresses, and the names in a lag rule's custom and blacklist lists
(only their counts).

Formula injection: a text cell that, after any leading single quotes, starts
with = + - @ (also after spaces or tabs), a tab or a carriage return gets one
extra single quote in front. An import undoes it exactly with
`v[1:] if v.startswith("'") and _FORMULA.match(v[1:]) else v`. Numeric cells
are never prefixed, so a negative number stays -5.

Runs on the dashboard pool with a 5 s statement timeout; a timeout is a 503.
"""
import csv
import io
import logging
import math
import re
from datetime import datetime, timezone
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Response
from sqlalchemy import select, text

from models import KafkaAlertConfig
from routes_alerts import (
    _BELOW_ALERT_TYPES, _GENERAL_ALERT_TYPES, _LAG_ALERT_TYPES, _MIN_RF_ALERT_TYPE, _THRESHOLD_ALERT_TYPES,
    _TIERS, _TYPE_LABELS, _cluster_label, _get_cluster_names, _get_session_factory,
)
from routes_notifications import _is_timeout

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/alerts", tags=["alerts"])

EXPORT_COLUMNS = (
    "id", "name", "alert_type", "alert_type_label", "cluster_id", "cluster_name",
    "enabled", "severity", "direction", "info", "warning", "critical", "min_rf",
    "cooldown_minutes", "send_resolve_card", "email_enabled", "has_custom_webhook",
    "reminder_hours", "custom_count", "blacklist_count", "created_at", "updated_at",
    "threshold",
)
NUMERIC_COLUMNS = frozenset((
    "id", "cluster_id", "info", "warning", "critical", "min_rf", "cooldown_minutes",
    "reminder_hours", "custom_count", "blacklist_count", "threshold",
))

# The two general types are missing from _TYPE_LABELS (it only names copied
# rules); these match their ALERT_TYPES labels in teams.html.
_GENERAL_TYPE_LABELS = {
    "close_wait_spike": "CLOSE_WAIT spike",
    "broker_unreachable": "Broker reachability (live check)",
}
_TIERED_ALERT_TYPES = frozenset(_THRESHOLD_ALERT_TYPES + _LAG_ALERT_TYPES)
_STATEMENT_TIMEOUT = "5s"
_FORMULA = re.compile(r"'*(?:[ \t]*[=+\-@]|[\t\r])")


def _text(value) -> str:
    s = "" if value is None else str(value)
    return "'" + s if _FORMULA.match(s) else s


def _num(value) -> str:
    """Plain decimal (no exponent, no separators); a non-number found where a
    number belongs is written as text."""
    if value is None:
        return ""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return _text(value)
    if isinstance(value, int):
        return str(value)
    s = repr(value)
    return format(Decimal(s), "f") if "e" in s or "E" in s else s


def _bool(value: bool) -> str:
    return "true" if value else "false"


def _ts(value: datetime | None) -> str:
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _count(value) -> int | None:
    return len(value) if isinstance(value, list) else None


def export_row(cfg: KafkaAlertConfig, cluster_names: dict[int, str]) -> dict:
    """One rule as {column: cell}, every cell already a string."""
    config = cfg.config if isinstance(cfg.config, dict) else {}
    alert_type = cfg.alert_type
    tiered = alert_type in _TIERED_ALERT_TYPES
    lag = alert_type in _LAG_ALERT_TYPES
    direction = None
    if tiered:
        direction = "below" if alert_type in _BELOW_ALERT_TYPES or config.get("direction") == "below" else "above"
    row = {
        "id": _num(cfg.id),
        "name": _text(cfg.name),
        "alert_type": _text(alert_type),
        "alert_type_label": _text(_TYPE_LABELS.get(alert_type) or _GENERAL_TYPE_LABELS.get(alert_type, alert_type)),
        "cluster_id": _num(cfg.cluster_id),
        "cluster_name": _text(_cluster_label(cfg.cluster_id, cluster_names)),
        "enabled": _bool(cfg.enabled),
        "severity": "" if tiered else _text(cfg.severity),
        "direction": _text(direction),
        **{tier: _num(config.get(tier)) if tiered else "" for tier in _TIERS},
        "min_rf": _num(config.get("min_rf")) if alert_type == _MIN_RF_ALERT_TYPE else "",
        "cooldown_minutes": _num(cfg.cooldown_minutes),
        # A missing key means true (teams_alerts._send_resolve_cards).
        "send_resolve_card": _bool(config.get("send_resolve_card", True) is not False),
        "email_enabled": _bool(cfg.email_enabled),
        "has_custom_webhook": _bool(bool(cfg.webhook_url and cfg.webhook_url.strip())),
        "reminder_hours": _num(config.get("reminder_hours")) if lag else "",
        "custom_count": _num(_count(config.get("custom", []))) if lag else "",
        "blacklist_count": _num(_count(config.get("blacklist", []))) if lag else "",
        "created_at": _ts(cfg.created_at),
        "updated_at": _ts(cfg.updated_at),
        "threshold": _num(config.get("threshold")) if alert_type in _GENERAL_ALERT_TYPES else "",
    }
    return {c: row[c] for c in EXPORT_COLUMNS}


def build_csv(configs, cluster_names: dict[int, str]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\r\n")
    writer.writerow(EXPORT_COLUMNS)
    for cfg in configs:
        writer.writerow(export_row(cfg, cluster_names).values())
    return buf.getvalue()


def export_filename(now: datetime) -> str:
    return f"kafka-alert-rules-{now.astimezone(timezone.utc):%Y%m%d-%H%M}.csv"


@router.get("/configs/export")
async def export_alert_configs() -> Response:
    """All rules as CSV, ordered by id (the page's filters do not apply)."""
    now = datetime.now(timezone.utc)
    try:
        async with _get_session_factory(dashboard=True)() as session:
            # SET LOCAL ends with the transaction, so the pooled connection keeps its default.
            await session.execute(text(f"SET LOCAL statement_timeout = '{_STATEMENT_TIMEOUT}'"))
            configs = (await session.execute(
                select(KafkaAlertConfig).order_by(KafkaAlertConfig.id)
            )).scalars().all()
        cluster_names = await _get_cluster_names()
        body = build_csv(configs, cluster_names)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        if _is_timeout(exc):
            logger.warning("export_alert_configs: query cancelled after %s", _STATEMENT_TIMEOUT)
            raise HTTPException(status_code=503, detail=f"Export: the query took longer than {_STATEMENT_TIMEOUT}; try again")
        logger.warning("export_alert_configs failed: %s", type(exc).__name__)
        raise HTTPException(status_code=500, detail="Export failed")
    return Response(
        content=body.encode("utf-8"),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{export_filename(now)}"',
            "Cache-Control": "no-store",
        },
    )
