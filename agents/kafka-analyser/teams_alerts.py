"""Teams alert firing/resolution/digest logic for kafka_alert_configs /
kafka_alert_triggers (see models.py for the agreed notification model,
2026-09-28). Currently driven only by check_and_recycle_close_wait in
collectors.py (alert_type 'close_wait_spike').

Every public function here is best-effort notification logic riding along
on a real operational job: each one logs a warning and swallows on failure,
never raises, so it can never break the caller's own check/recycle logic.
"""
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from database import SessionLocal
from models import KafkaAlertConfig, KafkaAlertTrigger
from shared.escalation.notifier import build_adaptive_card, send_to_teams

logger = logging.getLogger(__name__)

CLOSE_WAIT_ALERT_TYPE = "close_wait_spike"

# A new trigger counts as a recurrence (flapping signal) if the same
# alert+cluster's previous trigger resolved within this window. Fixed for
# now -- tune later.
RECURRENCE_LOOKBACK_HOURS = 24

_DEFAULT_DIGEST_INTERVAL_MINUTES = 5

# In-memory, resets on restart -- same as other in-memory state in this
# codebase (e.g. the escalation notifier's cooldown cache).
_last_digest_sent_at: datetime | None = None


async def _get_cluster_names() -> dict[int, str]:
    from storage import get_backend
    from config import settings
    clusters = await get_backend().get_clusters(settings.agent_slug)
    return {int(c["id"]): c.get("name", str(c["id"])) for c in clusters if c.get("id") is not None}


async def resolve_cleared_close_wait_triggers(close_wait_by_cluster_id: dict[int, int]) -> None:
    """Mark open close_wait_spike triggers resolved once their OWN config's
    threshold is no longer met for their cluster -- not merely once the
    cluster's count reaches zero. A threshold=2 config's trigger must
    resolve as soon as count drops back below 2, not only at count==0,
    otherwise it could stay open indefinitely at a residual count that no
    longer satisfies the condition that fired it. Silent -- no Teams post;
    resolution is only surfaced via the periodic digest."""
    if SessionLocal is None:
        return
    try:
        async with SessionLocal() as session:
            open_rows = (await session.execute(
                select(KafkaAlertTrigger, KafkaAlertConfig)
                .join(KafkaAlertConfig, KafkaAlertTrigger.alert_config_id == KafkaAlertConfig.id)
                .where(
                    KafkaAlertConfig.alert_type == CLOSE_WAIT_ALERT_TYPE,
                    KafkaAlertTrigger.resolved_at.is_(None),
                )
            )).all()
            now = datetime.now(timezone.utc)
            changed = False
            for trigger, config in open_rows:
                threshold = int((config.config or {}).get("threshold", 1))
                current_count = close_wait_by_cluster_id.get(trigger.cluster_id, 0)
                if current_count < threshold:
                    trigger.resolved_at = now
                    changed = True
            if changed:
                await session.commit()
    except Exception as exc:
        logger.warning("resolve_cleared_close_wait_triggers failed: %s", exc)


async def fire_close_wait_alerts(close_wait_by_cluster_id: dict[int, int]) -> None:
    """Fire close_wait_spike alerts for confirmed CLOSE_WAIT clusters: one
    immediate Teams card per (alert config, cluster) only when no trigger
    for that pair is already open and the pair is out of cooldown."""
    if not close_wait_by_cluster_id or SessionLocal is None:
        return
    try:
        from routes_settings import _config
        if not _config.get("teams_enabled"):
            return
        async with SessionLocal() as session:
            configs = (await session.execute(
                select(KafkaAlertConfig).where(
                    KafkaAlertConfig.alert_type == CLOSE_WAIT_ALERT_TYPE,
                    KafkaAlertConfig.enabled == True,
                )
            )).scalars().all()
        if not configs:
            return
        cluster_names = await _get_cluster_names()
    except Exception as exc:
        logger.warning("fire_close_wait_alerts: setup failed: %s", exc)
        return

    for config in configs:
        for cluster_id, count in close_wait_by_cluster_id.items():
            if config.cluster_id is not None and config.cluster_id != cluster_id:
                continue
            # Isolated per (config, cluster): one pair's failure never rolls
            # back or blocks another's already-successful send+log.
            try:
                await _fire_one(config, cluster_id, count, cluster_names, _config)
            except Exception as exc:
                logger.warning(
                    "fire_close_wait_alerts: alert_config_id=%s cluster_id=%s failed: %s",
                    config.id, cluster_id, exc,
                )


async def _fire_one(
    config: KafkaAlertConfig,
    cluster_id: int,
    count: int,
    cluster_names: dict[int, str],
    agent_config: dict,
) -> None:
    threshold = int((config.config or {}).get("threshold", 1))
    if count < threshold:
        return

    async with SessionLocal() as session:
        already_open = (await session.execute(
            select(KafkaAlertTrigger.id).where(
                KafkaAlertTrigger.alert_config_id == config.id,
                KafkaAlertTrigger.cluster_id == cluster_id,
                KafkaAlertTrigger.resolved_at.is_(None),
            ).limit(1)
        )).scalar_one_or_none()
        if already_open is not None:
            return
        last_resolved = (await session.execute(
            select(KafkaAlertTrigger).where(
                KafkaAlertTrigger.alert_config_id == config.id,
                KafkaAlertTrigger.cluster_id == cluster_id,
                KafkaAlertTrigger.resolved_at.is_not(None),
            ).order_by(KafkaAlertTrigger.resolved_at.desc()).limit(1)
        )).scalar_one_or_none()

    now = datetime.now(timezone.utc)
    since_resolved = (now - last_resolved.resolved_at) if last_resolved is not None else None
    if since_resolved is not None and since_resolved < timedelta(minutes=config.cooldown_minutes):
        return
    is_recurrence = since_resolved is not None and since_resolved <= timedelta(hours=RECURRENCE_LOOKBACK_HOURS)

    description = f"{count} CLOSE_WAIT connection(s) detected" + (
        " -- RECURRING: this same issue resolved recently and has now fired again" if is_recurrence else ""
    )
    card = build_adaptive_card(
        agent_name="Kafka Analyser",
        cluster_name=cluster_names.get(cluster_id, str(cluster_id)),
        anomaly={
            "severity": config.severity,
            "category": CLOSE_WAIT_ALERT_TYPE,
            "description": description,
            "recommended_action": "Use the Breaker Status tab's Clear Stale Connections button, "
                                  "or wait roughly 5 minutes for this to clear automatically.",
        },
    )

    webhook_url = config.webhook_url or agent_config.get("teams_webhook_url", "")
    if webhook_url:
        success = await send_to_teams(webhook_url=webhook_url, card=card)
        error_detail = None
    else:
        success = False
        error_detail = "No webhook URL configured (per-alert or agent-level)"

    # Separate session from the reads above so no DB transaction is held
    # open across the Teams HTTP post.
    async with SessionLocal() as session:
        session.add(KafkaAlertTrigger(
            alert_config_id=config.id,
            cluster_id=cluster_id,
            triggered_at=now,
            metric_value=str(count),
            message_sent=description,
            teams_post_success=success,
            error_detail=error_detail,
            resolved_at=None,
            is_recurrence=is_recurrence,
        ))
        await session.commit()


async def maybe_send_digest() -> None:
    """Post the periodic Resolved/Pending/Recurrence summary card at most
    once per teams_digest_interval_minutes. No card when all three sections
    are empty (no all-clear card, per agreed design)."""
    global _last_digest_sent_at
    if SessionLocal is None:
        return
    try:
        from routes_settings import _config
        try:
            interval_minutes = int(_config.get("teams_digest_interval_minutes", _DEFAULT_DIGEST_INTERVAL_MINUTES))
        except (TypeError, ValueError):
            interval_minutes = _DEFAULT_DIGEST_INTERVAL_MINUTES
        # Defensive -- the settings UI doesn't validate this server-side.
        interval = timedelta(minutes=max(1, interval_minutes))

        now = datetime.now(timezone.utc)
        if _last_digest_sent_at is not None and now - _last_digest_sent_at < interval:
            return

        webhook_url = _config.get("teams_webhook_url", "")
        if not _config.get("teams_enabled") or not webhook_url:
            return

        # Window covers everything since the last digest, not just the last
        # `interval` -- this runs on the close-wait job's own cadence, so the
        # actual gap between digests is usually longer than `interval`, and
        # a fixed `now - interval` window would silently drop events in the
        # difference.
        window_start = _last_digest_sent_at if _last_digest_sent_at is not None else now - interval

        async with SessionLocal() as session:
            base = select(KafkaAlertTrigger, KafkaAlertConfig).join(
                KafkaAlertConfig, KafkaAlertTrigger.alert_config_id == KafkaAlertConfig.id
            )
            pending = (await session.execute(
                base.where(KafkaAlertTrigger.resolved_at.is_(None))
                .order_by(KafkaAlertTrigger.triggered_at.desc())
            )).all()
            resolved = (await session.execute(
                base.where(
                    KafkaAlertTrigger.resolved_at.is_not(None),
                    KafkaAlertTrigger.resolved_at > window_start,
                ).order_by(KafkaAlertTrigger.resolved_at.desc())
            )).all()
            recurrence = (await session.execute(
                base.where(
                    KafkaAlertTrigger.is_recurrence == True,
                    KafkaAlertTrigger.triggered_at > window_start,
                ).order_by(KafkaAlertTrigger.triggered_at.desc())
            )).all()

        if not (pending or resolved or recurrence):
            _last_digest_sent_at = now
            return

        cluster_names = await _get_cluster_names()
        card = _build_digest_card(pending, resolved, recurrence, cluster_names)
        # Set before sending, regardless of outcome -- never retry-storm a
        # failing webhook.
        _last_digest_sent_at = now
        await send_to_teams(webhook_url=webhook_url, card=card)
    except Exception as exc:
        logger.warning("maybe_send_digest failed: %s", exc)


def _build_digest_card(pending: list, resolved: list, recurrence: list, cluster_names: dict[int, str]) -> dict:
    def _line(trigger: KafkaAlertTrigger, config: KafkaAlertConfig, ts: datetime) -> str:
        cluster = cluster_names.get(trigger.cluster_id, str(trigger.cluster_id)) \
            if trigger.cluster_id is not None else "all clusters"
        return f"{config.name} — {cluster} — {ts.strftime('%Y-%m-%d %H:%M UTC')}"

    body = [{
        "type": "TextBlock",
        "text": "Kafka Alert Digest",
        "weight": "Bolder",
        "size": "Medium",
    }]
    sections = [
        ("Pending", [_line(t, c, t.triggered_at) for t, c in pending]),
        ("Resolved", [_line(t, c, t.resolved_at) for t, c in resolved]),
        ("Recurrence", [_line(t, c, t.triggered_at) for t, c in recurrence]),
    ]
    for title, lines in sections:
        if lines:
            body.append({
                "type": "TextBlock",
                "text": f"**{title}**\n\n" + "\n\n".join(lines),
                "wrap": True,
                "spacing": "Medium",
            })

    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.4",
                    "body": body,
                    "actions": [],
                },
            }
        ],
    }
