"""Notification delivery log (read-only) over kafka_notification_outbox and
kafka_notification_attempts (Alembic 0058): every owner notice and rule
notification, its channels and its status, for the Kafka team.

Only the fields listed in _ITEM_SQL and _ATTEMPT_SQL are read. Payload bodies
(email HTML, Teams cards), recipient addresses, webhook URLs or references,
message ids and attempt details are never selected, so they cannot reach a
response. Errors are stored secret-free (outbox.clean_error) and cut to
_TEXT_MAX here. Queries run on the dashboard pool with a 5 s statement
timeout, so a slow query can neither starve the agent's main pool nor hang.
"""
import logging
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, HTTPException, Path, Query
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from outbox_worker import JOB_ID as _DELIVERY_JOB_ID
from routes_alerts import _cluster_label, _get_cluster_names, _get_session_factory, _iso

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/alerts/notifications", tags=["notifications"])

# Allowed values, as in the CHECK constraints of models.KafkaNotificationOutbox.
Kind = Literal["owner_summary", "owner_watchdog", "rule_fire", "rule_escalate",
               "rule_reminder", "rule_resolve", "heartbeat", "deadman"]
State = Literal["pending", "delivered", "degraded", "failed", "suppressed"]
ChannelStatus = Literal["pending", "standby", "sending", "sent", "failed", "skipped", "abandoned", "superseded"]
Channel = Literal["email", "teams"]
_STATES = State.__args__
_STATUSES = ChannelStatus.__args__
_CHANNELS = Channel.__args__

_MAX_HOURS = 168
_TEXT_MAX = 200
_STATEMENT_TIMEOUT = "5s"
_DELETED_RULE = "(deleted rule)"
# The summary looks for the delivery job's last success among this many of
# the newest kafka_job_runs rows (about 2.7 days at ~18,700 runs a day), a
# primary-key range scan: the table has no index on job_id.
_JOB_RUNS_SEARCHED = 50_000
# The last heartbeat row is looked for within this window (created_at index).
_HEARTBEAT_HOURS = 168

_ITEM_SQL = f"""
    o.id, o.created_at, o.kind, o.source, o.origin, o.routing, o.alert_config_id, o.cluster_id,
    o.severity, o.state, o.delivered_at,
    o.email_status, o.email_attempts, left(o.email_last_error, {_TEXT_MAX}) AS email_last_error, o.email_sent_at,
    o.teams_status, o.teams_attempts, left(o.teams_last_error, {_TEXT_MAX}) AS teams_last_error, o.teams_sent_at,
    left(o.payload ->> 'title', {_TEXT_MAX}) AS title,
    c.name AS config_name, o.payload ->> 'rule_name' AS payload_rule_name
"""
_FROM_SQL = "FROM kafka_notification_outbox o LEFT JOIN kafka_alert_configs c ON c.id = o.alert_config_id"
_ATTEMPT_SQL = f"""
    SELECT channel, attempt_no, started_at, duration_ms, phase_reached, outcome, left(error, {_TEXT_MAX}) AS error
    FROM kafka_notification_attempts WHERE outbox_id = :id ORDER BY channel, attempt_no, id
"""


def _session():
    return _get_session_factory(dashboard=True)()


async def _begin(session) -> None:
    # SET LOCAL ends with the transaction, so the pooled connection keeps its default.
    await session.execute(text(f"SET LOCAL statement_timeout = '{_STATEMENT_TIMEOUT}'"))


def _is_timeout(exc: Exception) -> bool:
    orig = getattr(exc, "orig", None)
    code = getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)
    return code == "57014" or "statement timeout" in str(exc)


def _db_error(exc: Exception, what: str) -> HTTPException:
    if isinstance(exc, DBAPIError) and _is_timeout(exc):
        logger.warning("%s: query cancelled after %s", what, _STATEMENT_TIMEOUT)
        return HTTPException(status_code=503, detail=f"{what}: the query took longer than {_STATEMENT_TIMEOUT}; narrow the filter")
    logger.warning("%s failed: %s", what, type(exc).__name__)
    return HTTPException(status_code=500, detail=f"{what} failed")


async def _cluster_names() -> dict[int, str]:
    try:
        return await _get_cluster_names()
    except Exception as exc:  # noqa: BLE001 -- names are cosmetic; ids are still returned
        logger.warning("notifications: cluster names unavailable: %s", type(exc).__name__)
        return {}


def _rule_name(r) -> str | None:
    if r.alert_config_id is None:
        return None
    return r.config_name or r.payload_rule_name or _DELETED_RULE


def _item(r, cluster_names: dict[int, str]) -> dict:
    return {
        "id": r.id,
        "created_at": _iso(r.created_at),
        "kind": r.kind,
        "source": r.source,
        "origin": r.origin,
        "routing": r.routing,
        "alert_config_id": r.alert_config_id,
        "rule_name": _rule_name(r),
        "cluster_id": r.cluster_id,
        "cluster_name": _cluster_label(r.cluster_id, cluster_names) if r.cluster_id is not None else None,
        "severity": r.severity,
        "title": r.title,
        "state": r.state,
        "delivered_at": _iso(r.delivered_at),
        "email_status": r.email_status,
        "email_attempts": r.email_attempts,
        "email_last_error": r.email_last_error,
        "email_sent_at": _iso(r.email_sent_at),
        "teams_status": r.teams_status,
        "teams_attempts": r.teams_attempts,
        "teams_last_error": r.teams_last_error,
        "teams_sent_at": _iso(r.teams_sent_at),
    }


@router.get("")
async def list_notifications(
    since_hours: int = Query(default=24, ge=1, le=_MAX_HOURS),
    kind: Kind | None = None,
    state: State | None = None,
    channel: Channel | None = None,
    channel_status: ChannelStatus | None = None,
    cluster_id: int | None = Query(default=None, ge=1),
    alert_config_id: int | None = Query(default=None, ge=1),
    limit: int = Query(default=25, ge=1, le=200),
    offset: int = Query(default=0, ge=0, le=100_000),
) -> dict:
    """Newest first. channel alone: rows where that channel was wanted (not
    skipped); channel_status alone: either channel has that status; both:
    that channel has that status."""
    where = ["o.created_at >= now() - make_interval(hours => :since_hours)"]
    params: dict = {"since_hours": since_hours}
    for column, value in (("kind", kind), ("state", state), ("cluster_id", cluster_id),
                          ("alert_config_id", alert_config_id)):
        if value is not None:
            where.append(f"o.{column} = :{column}")
            params[column] = value
    if channel_status is not None:
        params["channel_status"] = channel_status
        where.append(f"o.{channel}_status = :channel_status" if channel
                     else "(o.email_status = :channel_status OR o.teams_status = :channel_status)")
    elif channel is not None:
        where.append(f"o.{channel}_status <> 'skipped'")
    where_sql = " AND ".join(where)
    try:
        async with _session() as session:
            await _begin(session)
            total_count = (await session.execute(
                text(f"SELECT count(*) FROM kafka_notification_outbox o WHERE {where_sql}"), params)).scalar_one()
            rows = (await session.execute(text(
                f"SELECT {_ITEM_SQL} {_FROM_SQL} WHERE {where_sql} "
                "ORDER BY o.created_at DESC, o.id DESC LIMIT :limit OFFSET :offset"),
                {**params, "limit": limit, "offset": offset})).all()
    except Exception as exc:  # noqa: BLE001
        raise _db_error(exc, "list notifications")
    cluster_names = await _cluster_names() if any(r.cluster_id is not None for r in rows) else {}
    return {
        "notifications": [_item(r, cluster_names) for r in rows],
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "since_hours": since_hours,
    }


@router.get("/summary")
async def notifications_summary() -> dict:
    """Last 24 hours: counts by state and by channel status; the oldest
    unfinished row; the delivery job's last success; the last heartbeat."""
    day = "o.created_at >= now() - interval '24 hours'"
    try:
        async with _session() as session:
            await _begin(session)
            by_state = dict((await session.execute(text(
                f"SELECT o.state, count(*) FROM kafka_notification_outbox o WHERE {day} GROUP BY o.state"))).all())
            by_channel = (await session.execute(text(
                f"SELECT 'email', o.email_status, count(*) FROM kafka_notification_outbox o WHERE {day} GROUP BY 2 "
                f"UNION ALL SELECT 'teams', o.teams_status, count(*) FROM kafka_notification_outbox o WHERE {day} GROUP BY 2"
            ))).all()
            oldest = (await session.execute(text(
                "SELECT id, kind, created_at, extract(epoch FROM now() - created_at) AS age "
                "FROM kafka_notification_outbox WHERE finished_at IS NULL ORDER BY created_at, id LIMIT 1"))).first()
            job = (await session.execute(text(
                "SELECT started_at, ended_at, duration_seconds FROM kafka_job_runs "
                "WHERE id > (SELECT max(id) FROM kafka_job_runs) - :searched "
                "AND job_id = :job_id AND status = 'success' ORDER BY id DESC LIMIT 1"),
                {"searched": _JOB_RUNS_SEARCHED, "job_id": _DELIVERY_JOB_ID})).first()
            heartbeat = (await session.execute(text(
                f"SELECT id, created_at, state, email_status, teams_status FROM kafka_notification_outbox "
                f"WHERE kind = 'heartbeat' AND created_at >= now() - interval '{_HEARTBEAT_HOURS} hours' "
                "ORDER BY created_at DESC, id DESC LIMIT 1"))).first()
    except Exception as exc:  # noqa: BLE001
        raise _db_error(exc, "notifications summary")
    channels = {c: {s: 0 for s in _STATUSES} for c in _CHANNELS}
    for channel, status, count in by_channel:
        channels[channel][status] = count
    return {
        "window_hours": 24,
        "generated_at": _iso(datetime.now(timezone.utc)),
        "total": sum(by_state.values()),
        "by_state": {s: by_state.get(s, 0) for s in _STATES},
        "by_channel_status": channels,
        "oldest_unfinished": None if oldest is None else {
            "id": oldest.id, "kind": oldest.kind, "created_at": _iso(oldest.created_at),
            "age_seconds": int(oldest.age),
        },
        "delivery_job": {
            "job_id": _DELIVERY_JOB_ID,
            "last_success_started_at": _iso(job.started_at) if job else None,
            "last_success_ended_at": _iso(job.ended_at) if job else None,
            "last_success_duration_seconds": job.duration_seconds if job else None,
            "searched_latest_job_runs": _JOB_RUNS_SEARCHED,
        },
        "last_heartbeat": None if heartbeat is None else {
            "id": heartbeat.id, "created_at": _iso(heartbeat.created_at), "state": heartbeat.state,
            "email_status": heartbeat.email_status, "teams_status": heartbeat.teams_status,
        },
        "heartbeat_window_hours": _HEARTBEAT_HOURS,
    }


@router.get("/{notification_id}")
async def get_notification(notification_id: int = Path(ge=1, le=2**63 - 1)) -> dict:
    """One notification with its attempt rows (attempt_no 0 = channel not attempted)."""
    try:
        async with _session() as session:
            await _begin(session)
            row = (await session.execute(
                text(f"SELECT {_ITEM_SQL} {_FROM_SQL} WHERE o.id = :id"), {"id": notification_id})).first()
            attempts = (await session.execute(text(_ATTEMPT_SQL), {"id": notification_id})).all() if row else []
    except Exception as exc:  # noqa: BLE001
        raise _db_error(exc, "get notification")
    if row is None:
        raise HTTPException(status_code=404, detail=f"Notification {notification_id} not found")
    item = _item(row, await _cluster_names() if row.cluster_id is not None else {})
    item["attempts"] = [{
        "channel": a.channel,
        "attempt_no": a.attempt_no,
        "started_at": _iso(a.started_at),
        "duration_ms": a.duration_ms,
        "phase_reached": a.phase_reached,
        "outcome": a.outcome,
        "error": a.error,
    } for a in attempts]
    return item
