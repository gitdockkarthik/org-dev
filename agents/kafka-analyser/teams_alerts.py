"""Teams alert firing/resolution/digest logic for kafka_alert_configs /
kafka_alert_triggers (see models.py for the agreed notification model,
2026-09-28). Currently driven only by check_and_recycle_close_wait in
collectors.py (alert_type 'close_wait_spike').

Every public function here is best-effort notification logic riding along
on a real operational job: each one logs a warning and swallows on failure,
never raises, so it can never break the caller's own check/recycle logic.
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

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
    for that pair is already open and the pair is out of cooldown.

    Reworked 2026-09-28 to use three DB sessions total regardless of how
    many clusters fire at once -- two SessionLocal sessions (one for all
    reads, one for all writes) plus one opened internally by
    _get_cluster_names() via the storage backend -- never more than one
    open at a time. The previous per-cluster-session design could open ~11
    sessions in a single call with all 4 clusters firing simultaneously,
    identified as a likely contributor to a severe production incident
    (DB connection pool pressure indirectly affecting the pool-recycle
    drain task). No DB session is held open during the Teams HTTP calls,
    which can take up to ~10s each."""
    if not close_wait_by_cluster_id or SessionLocal is None:
        return
    try:
        from routes_settings import _config
        if not _config.get("teams_enabled"):
            return

        # ---- Single read session: configs + every relevant trigger ----
        async with SessionLocal() as session:
            configs = (await session.execute(
                select(KafkaAlertConfig).where(
                    KafkaAlertConfig.alert_type == CLOSE_WAIT_ALERT_TYPE,
                    KafkaAlertConfig.enabled == True,
                )
            )).scalars().all()
            if not configs:
                return
            config_ids = [c.id for c in configs]

            open_rows = (await session.execute(
                select(KafkaAlertTrigger.alert_config_id, KafkaAlertTrigger.cluster_id).where(
                    KafkaAlertTrigger.alert_config_id.in_(config_ids),
                    KafkaAlertTrigger.resolved_at.is_(None),
                )
            )).all()
            open_pairs = {(r.alert_config_id, r.cluster_id) for r in open_rows}

            # Most recent resolved_at per (config, cluster), computed by the DB.
            resolved_rows = (await session.execute(
                select(
                    KafkaAlertTrigger.alert_config_id,
                    KafkaAlertTrigger.cluster_id,
                    func.max(KafkaAlertTrigger.resolved_at).label("last_resolved_at"),
                ).where(
                    KafkaAlertTrigger.alert_config_id.in_(config_ids),
                    KafkaAlertTrigger.resolved_at.is_not(None),
                ).group_by(KafkaAlertTrigger.alert_config_id, KafkaAlertTrigger.cluster_id)
            )).all()
        last_resolved: dict[tuple[int, int], datetime] = {
            (r.alert_config_id, r.cluster_id): r.last_resolved_at for r in resolved_rows
        }

        cluster_names = await _get_cluster_names()
    except Exception as exc:
        logger.warning("fire_close_wait_alerts: setup failed: %s", exc)
        return

    # ---- Decide who fires, entirely in memory, no DB session open ----
    now = datetime.now(timezone.utc)
    to_insert: list[KafkaAlertTrigger] = []

    for config in configs:
        for cluster_id, count in close_wait_by_cluster_id.items():
            if config.cluster_id is not None and config.cluster_id != cluster_id:
                continue
            try:
                threshold = int((config.config or {}).get("threshold", 1))
                if count < threshold:
                    continue
                if (config.id, cluster_id) in open_pairs:
                    continue  # already firing, avoid duplicate spam

                last_resolved_at = last_resolved.get((config.id, cluster_id))
                since_resolved = (now - last_resolved_at) if last_resolved_at is not None else None
                if since_resolved is not None and since_resolved < timedelta(minutes=config.cooldown_minutes):
                    continue  # still in cooldown
                is_recurrence = (
                    since_resolved is not None
                    and since_resolved <= timedelta(hours=RECURRENCE_LOOKBACK_HOURS)
                )

                description = f"{count} CLOSE_WAIT connection(s) detected" + (
                    " -- RECURRING: this same issue resolved recently and has now fired again"
                    if is_recurrence else ""
                )
                card = build_adaptive_card(
                    agent_name="Kafka Analyser",
                    cluster_name=cluster_names.get(cluster_id, str(cluster_id)),
                    anomaly={
                        "severity": config.severity,
                        "category": CLOSE_WAIT_ALERT_TYPE,
                        "description": description,
                        "recommended_action": "Use the Breaker Status tab's Clear Stale "
                                              "Connections button, or wait roughly 5 minutes "
                                              "for this to clear automatically.",
                    },
                )

                webhook_url = config.webhook_url or _config.get("teams_webhook_url", "")
                if webhook_url:
                    # Isolated per (config, cluster): one pair's slow/failed
                    # post never blocks or is rolled back by another's.
                    success = await send_to_teams(webhook_url=webhook_url, card=card)
                    error_detail = None
                else:
                    success = False
                    error_detail = "No webhook URL configured (per-alert or agent-level)"

                to_insert.append(KafkaAlertTrigger(
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
            except Exception as exc:
                logger.warning(
                    "fire_close_wait_alerts: alert_config_id=%s cluster_id=%s failed: %s",
                    config.id, cluster_id, exc,
                )

    if not to_insert:
        return

    # ---- Single write session: each trigger row in its own savepoint ----
    try:
        async with SessionLocal() as session:
            for trigger in to_insert:
                try:
                    async with session.begin_nested():
                        session.add(trigger)
                    await session.commit()
                except Exception as exc:
                    await session.rollback()
                    logger.warning(
                        "fire_close_wait_alerts: failed to record trigger for "
                        "alert_config_id=%s cluster_id=%s (card was already sent -- "
                        "this cluster will likely be re-alerted next cycle): %s",
                        trigger.alert_config_id, trigger.cluster_id, exc,
                    )
    except Exception as exc:
        logger.warning("fire_close_wait_alerts: write session failed entirely: %s", exc)


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


BROKER_UNREACHABLE_ALERT_TYPE = "broker_unreachable"
_broker_fail_counts: dict[tuple[int, str], int] = {}  # in-memory, resets on restart
_BROKER_CONFIRM_CHECKS = 2  # consecutive failures required before firing


async def evaluate_alerts() -> None:
    """Global alert evaluator job entry point. Calls each check in turn;
    future alert types plug in here. Never raises."""
    try:
        await _evaluate_broker_reachability()
    except Exception as exc:
        logger.warning("evaluate_alerts: broker reachability check failed: %s", exc)


async def _evaluate_broker_reachability() -> None:
    """Fire broker_unreachable alerts: one immediate Teams card per (alert
    config, cluster, broker) once a broker has failed a fresh TCP connect on
    _BROKER_CONFIRM_CHECKS consecutive runs, only when no trigger for that
    triple is already open and it is out of cooldown. A reachable broker
    silently resolves its open trigger (surfaced via the periodic digest).

    Same four-stage structure as fire_close_wait_alerts: one read session,
    decide in memory, Teams posts with no DB session open, one write session
    with each row in its own savepoint. A cluster whose reachability check
    errored or returned no brokers is skipped entirely -- no counter change,
    no resolve, no fire -- so a config/lookup problem never looks like an
    outage or a recovery."""
    if SessionLocal is None:
        return
    try:
        from routes_settings import _config
        if not _config.get("teams_enabled"):
            return

        # ---- Single read session: configs + every relevant trigger ----
        async with SessionLocal() as session:
            configs = (await session.execute(
                select(KafkaAlertConfig).where(
                    KafkaAlertConfig.alert_type == BROKER_UNREACHABLE_ALERT_TYPE,
                    KafkaAlertConfig.enabled == True,
                )
            )).scalars().all()
            if not configs:
                return
            config_ids = [c.id for c in configs]

            open_rows = (await session.execute(
                select(
                    KafkaAlertTrigger.id,
                    KafkaAlertTrigger.alert_config_id,
                    KafkaAlertTrigger.cluster_id,
                    KafkaAlertTrigger.subject,
                ).where(
                    KafkaAlertTrigger.alert_config_id.in_(config_ids),
                    KafkaAlertTrigger.resolved_at.is_(None),
                )
            )).all()
            open_triggers: dict[tuple[int, int, str], int] = {
                (r.alert_config_id, r.cluster_id, r.subject): r.id for r in open_rows
            }

            # Most recent resolved_at per (config, cluster, subject), computed by the DB.
            resolved_rows = (await session.execute(
                select(
                    KafkaAlertTrigger.alert_config_id,
                    KafkaAlertTrigger.cluster_id,
                    KafkaAlertTrigger.subject,
                    func.max(KafkaAlertTrigger.resolved_at).label("last_resolved_at"),
                ).where(
                    KafkaAlertTrigger.alert_config_id.in_(config_ids),
                    KafkaAlertTrigger.resolved_at.is_not(None),
                ).group_by(
                    KafkaAlertTrigger.alert_config_id,
                    KafkaAlertTrigger.cluster_id,
                    KafkaAlertTrigger.subject,
                )
            )).all()
        last_resolved: dict[tuple[int, int, str], datetime] = {
            (r.alert_config_id, r.cluster_id, r.subject): r.last_resolved_at for r in resolved_rows
        }

        # Paused clusters are deliberately included: _get_enabled_clusters
        # filters on kafka_clusters.enabled only, not breaker pause state,
        # and an unreachable broker is exactly what a paused cluster may have.
        import collectors
        enabled_cluster_ids = [int(collectors._cid(c)) for c in await collectors._get_enabled_clusters()]
        cluster_ids = [
            cid for cid in enabled_cluster_ids
            if any(cfg.cluster_id is None or cfg.cluster_id == cid for cfg in configs)
        ]
        if not cluster_ids:
            return
    except Exception as exc:
        logger.warning("_evaluate_broker_reachability: setup failed: %s", exc)
        return

    # ---- Fresh TCP reachability per cluster, concurrently, no DB session open ----
    # Imported here, not at module level, to avoid a circular import.
    from routes_dashboard import get_broker_reachability
    results = await asyncio.gather(
        *[get_broker_reachability(str(cid)) for cid in cluster_ids],
        return_exceptions=True,
    )

    # ---- Decide who fires/resolves, entirely in memory, no DB session open ----
    now = datetime.now(timezone.utc)
    to_insert: list[KafkaAlertTrigger] = []
    to_resolve: list[int] = []

    for cluster_id, result in zip(cluster_ids, results):
        if isinstance(result, BaseException) or result.get("error") or not result.get("total"):
            logger.warning(
                "_evaluate_broker_reachability: cluster_id=%s skipped this cycle (check did not complete): %s",
                cluster_id, result if isinstance(result, BaseException) else result.get("error") or "no brokers",
            )
            continue
        cluster_name = result.get("cluster_name") or str(cluster_id)
        reachable_count = result.get("reachable_count", 0)
        total = result["total"]
        cluster_configs = [cfg for cfg in configs if cfg.cluster_id is None or cfg.cluster_id == cluster_id]

        for broker in result.get("brokers", []):
            subject = f"{broker['host']}:{broker['port']}"
            key = (cluster_id, subject)

            if broker.get("reachable"):
                _broker_fail_counts[key] = 0
                for config in cluster_configs:
                    trigger_id = open_triggers.get((config.id, cluster_id, subject))
                    if trigger_id is not None:
                        to_resolve.append(trigger_id)
                continue

            _broker_fail_counts[key] = _broker_fail_counts.get(key, 0) + 1
            if _broker_fail_counts[key] < _BROKER_CONFIRM_CHECKS:
                continue

            for config in cluster_configs:
                try:
                    if (config.id, cluster_id, subject) in open_triggers:
                        continue  # already firing, avoid duplicate spam

                    last_resolved_at = last_resolved.get((config.id, cluster_id, subject))
                    since_resolved = (now - last_resolved_at) if last_resolved_at is not None else None
                    if since_resolved is not None and since_resolved < timedelta(minutes=config.cooldown_minutes):
                        continue  # still in cooldown
                    is_recurrence = (
                        since_resolved is not None
                        and since_resolved <= timedelta(hours=RECURRENCE_LOOKBACK_HOURS)
                    )

                    description = (
                        f"Broker {subject} is unreachable (TCP connect failed on "
                        f"{_BROKER_CONFIRM_CHECKS} consecutive checks; "
                        f"{reachable_count}/{total} brokers in this cluster reachable)"
                    ) + (
                        " -- RECURRING: this same issue resolved recently and has now fired again"
                        if is_recurrence else ""
                    )
                    card = build_adaptive_card(
                        agent_name="Kafka Analyser",
                        cluster_name=cluster_name,
                        anomaly={
                            "severity": config.severity,
                            "category": BROKER_UNREACHABLE_ALERT_TYPE,
                            "description": description,
                            "recommended_action": "Check the broker process/host and the network path to it.",
                        },
                    )

                    webhook_url = config.webhook_url or _config.get("teams_webhook_url", "")
                    if webhook_url:
                        # Isolated per (config, cluster, broker): one slow/failed
                        # post never blocks or is rolled back by another's.
                        success = await send_to_teams(webhook_url=webhook_url, card=card)
                        error_detail = None
                    else:
                        success = False
                        error_detail = "No webhook URL configured (per-alert or agent-level)"

                    to_insert.append(KafkaAlertTrigger(
                        alert_config_id=config.id,
                        cluster_id=cluster_id,
                        subject=subject,
                        triggered_at=now,
                        metric_value="unreachable",
                        message_sent=description,
                        teams_post_success=success,
                        error_detail=error_detail,
                        resolved_at=None,
                        is_recurrence=is_recurrence,
                    ))
                except Exception as exc:
                    logger.warning(
                        "_evaluate_broker_reachability: alert_config_id=%s cluster_id=%s subject=%s failed: %s",
                        config.id, cluster_id, subject, exc,
                    )

    if not to_insert and not to_resolve:
        return

    # ---- Single write session: each row in its own savepoint ----
    try:
        async with SessionLocal() as session:
            for trigger in to_insert:
                try:
                    async with session.begin_nested():
                        session.add(trigger)
                    await session.commit()
                except Exception as exc:
                    await session.rollback()
                    logger.warning(
                        "_evaluate_broker_reachability: failed to record trigger for "
                        "alert_config_id=%s cluster_id=%s subject=%s (card was already sent -- "
                        "this broker will likely be re-alerted next cycle): %s",
                        trigger.alert_config_id, trigger.cluster_id, trigger.subject, exc,
                    )
            for trigger_id in to_resolve:
                try:
                    async with session.begin_nested():
                        trigger = await session.get(KafkaAlertTrigger, trigger_id)
                        if trigger is not None and trigger.resolved_at is None:
                            trigger.resolved_at = now
                    await session.commit()
                except Exception as exc:
                    await session.rollback()
                    logger.warning(
                        "_evaluate_broker_reachability: failed to resolve trigger id=%s: %s",
                        trigger_id, exc,
                    )
    except Exception as exc:
        logger.warning("_evaluate_broker_reachability: write session failed entirely: %s", exc)
