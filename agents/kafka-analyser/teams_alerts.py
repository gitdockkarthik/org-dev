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
from models import KafkaAlertConfig, KafkaAlertTrigger, KafkaBrokerMetrics
from shared.escalation.notifier import build_adaptive_card, build_resolve_card, send_to_teams

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


_RESOLVE_CARDS_TIMEOUT_SECS = 10


async def _send_resolve_cards(items: list[dict]) -> None:
    """Post one green "resolved" card per item ({"config": KafkaAlertConfig,
    "trigger": KafkaAlertTrigger, "cluster_name": str}), only for triggers
    whose opening card actually reached Teams (teams_post_success) and whose
    rule allows it (config.config["send_resolve_card"], default True).
    Best-effort: capped at 10 s in total, every error logged as a warning,
    never raises, never touches the database."""
    async def _send_all() -> None:
        from routes_settings import _config
        if not _config.get("teams_enabled"):
            return
        for item in items:
            try:
                config = item["config"]
                trigger = item["trigger"]
                if trigger.teams_post_success is not True:
                    continue
                if not (config.config or {}).get("send_resolve_card", True):
                    continue
                webhook_url = config.webhook_url or _config.get("teams_webhook_url", "")
                if not webhook_url:
                    continue
                open_minutes = None
                if trigger.triggered_at is not None and trigger.resolved_at is not None:
                    open_minutes = (trigger.resolved_at - trigger.triggered_at).total_seconds() / 60
                card = build_resolve_card(
                    agent_name="Kafka Analyser",
                    cluster_name=item.get("cluster_name", ""),
                    rule_name=config.name,
                    subject=trigger.subject,
                    severity=trigger.severity or config.severity,
                    open_minutes=open_minutes,
                )
                await send_to_teams(webhook_url=webhook_url, card=card)
            except Exception as exc:
                logger.warning("_send_resolve_cards: item failed: %s", exc)

    try:
        await asyncio.wait_for(_send_all(), timeout=_RESOLVE_CARDS_TIMEOUT_SECS)
    except asyncio.TimeoutError:
        logger.warning("_send_resolve_cards: stopped after %s s cap", _RESOLVE_CARDS_TIMEOUT_SECS)
    except Exception as exc:
        logger.warning("_send_resolve_cards failed: %s", exc)


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
    try:
        await _evaluate_broker_thresholds()
    except Exception as exc:
        logger.warning("evaluate_alerts: broker threshold check failed: %s", exc)


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
        logger.info("evaluate_alerts: broker_unreachable skipped: database not available")
        return
    try:
        from routes_settings import _config
        if not _config.get("teams_enabled"):
            logger.info("evaluate_alerts: broker_unreachable skipped: teams_enabled is off")
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
                logger.info("evaluate_alerts: broker_unreachable skipped: no enabled broker_unreachable rules")
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
            logger.info("evaluate_alerts: broker_unreachable skipped: no enabled clusters match a rule")
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
    resolve_context: dict[int, tuple[KafkaAlertConfig, str]] = {}
    brokers_checked = 0
    brokers_unreachable = 0
    skipped_clusters = 0

    for cluster_id, result in zip(cluster_ids, results):
        if isinstance(result, BaseException) or result.get("error") or not result.get("total"):
            skipped_clusters += 1
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
            brokers_checked += 1

            if broker.get("reachable"):
                _broker_fail_counts[key] = 0
                for config in cluster_configs:
                    trigger_id = open_triggers.get((config.id, cluster_id, subject))
                    if trigger_id is not None:
                        to_resolve.append(trigger_id)
                        resolve_context[trigger_id] = (config, cluster_name)
                continue

            brokers_unreachable += 1
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

    logger.info(
        "evaluate_alerts: broker_unreachable checked clusters=%d "
        "brokers=%d unreachable=%d skipped_clusters=%d fired=%d resolved=%d",
        len(cluster_ids) - skipped_clusters, brokers_checked,
        brokers_unreachable, skipped_clusters, len(to_insert), len(to_resolve),
    )

    if not to_insert and not to_resolve:
        return

    # ---- Single write session: each row in its own savepoint ----
    # Resolve cards are sent only after this session has closed. Each entry
    # holds a plain snapshot of the trigger (taken before its commit), since
    # a later rollback in this session expires every ORM object in it.
    from types import SimpleNamespace
    resolved_items: list[dict] = []
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
                    snapshot = None
                    async with session.begin_nested():
                        trigger = await session.get(KafkaAlertTrigger, trigger_id)
                        if trigger is not None and trigger.resolved_at is None:
                            trigger.resolved_at = now
                            snapshot = SimpleNamespace(
                                subject=trigger.subject,
                                severity=trigger.severity,
                                triggered_at=trigger.triggered_at,
                                resolved_at=trigger.resolved_at,
                                teams_post_success=trigger.teams_post_success,
                            )
                    await session.commit()
                    if snapshot is not None and trigger_id in resolve_context:
                        config, cluster_name = resolve_context[trigger_id]
                        resolved_items.append({"config": config, "trigger": snapshot, "cluster_name": cluster_name})
                except Exception as exc:
                    await session.rollback()
                    logger.warning(
                        "_evaluate_broker_reachability: failed to resolve trigger id=%s: %s",
                        trigger_id, exc,
                    )
    except Exception as exc:
        logger.warning("_evaluate_broker_reachability: write session failed entirely: %s", exc)

    if resolved_items:
        try:
            await _send_resolve_cards(resolved_items)
        except Exception as exc:
            logger.warning("_evaluate_broker_reachability: resolve cards failed: %s", exc)


# Threshold rules (Simple mode): alert_type -> (kafka_broker_metrics column,
# label used in the card). config is {"mode": "simple", <tier>: <threshold>}
# as validated by routes_alerts._threshold_config.
BROKER_THRESHOLD_METRICS = {
    "broker.cpu_pct": ("cpu_pct", "CPU"),
    "broker.heap_pct": ("heap_pct", "heap"),
}
_THRESHOLD_TIERS = ("info", "warning", "critical")  # ascending
_TIER_RANK = {tier: rank for rank, tier in enumerate(_THRESHOLD_TIERS)}
_threshold_breach_counts: dict[tuple[int, int, str], int] = {}  # in-memory, resets on restart
_THRESHOLD_CONFIRM_CHECKS = 2  # consecutive breaching evaluations required before firing
# Same 6-minute freshness window routes_dashboard.py uses for per-broker status.
_BROKER_METRICS_MAX_AGE = timedelta(minutes=6)


def _threshold_tier(tier_config: dict, value: float) -> str | None:
    """Highest configured tier whose threshold is <= value, else None."""
    for tier in reversed(_THRESHOLD_TIERS):
        threshold = tier_config.get(tier)
        if threshold is not None and value >= float(threshold):
            return tier
    return None


async def _evaluate_broker_thresholds() -> None:
    """Fire broker.cpu_pct / broker.heap_pct alerts from the latest
    kafka_broker_metrics row per broker: one Teams card per (alert config,
    cluster, broker) once the value sits in a tier on
    _THRESHOLD_CONFIRM_CHECKS consecutive runs, only when no trigger for that
    triple is already open and it is out of cooldown. An open trigger whose
    tier rises is escalated in place with a new card; one whose tier falls is
    updated silently; one whose value drops below every tier is resolved
    silently (surfaced via the periodic digest).

    Same four-stage structure as _evaluate_broker_reachability. A metric row
    older than _BROKER_METRICS_MAX_AGE is ignored entirely -- no counter
    change, no resolve, no fire -- so a stalled collector never looks healthy
    or critical."""
    if SessionLocal is None:
        logger.info("evaluate_alerts: broker thresholds skipped: database not available")
        return
    try:
        from routes_settings import _config
        if not _config.get("teams_enabled"):
            logger.info("evaluate_alerts: broker thresholds skipped: teams_enabled is off")
            return

        # ---- Single read session: configs, relevant triggers, broker metrics ----
        async with SessionLocal() as session:
            configs = (await session.execute(
                select(KafkaAlertConfig).where(
                    KafkaAlertConfig.alert_type.in_(list(BROKER_THRESHOLD_METRICS)),
                    KafkaAlertConfig.enabled == True,
                )
            )).scalars().all()
            if not configs:
                logger.info("evaluate_alerts: broker thresholds skipped: no enabled broker threshold rules")
                return
            config_ids = [c.id for c in configs]

            open_rows = (await session.execute(
                select(
                    KafkaAlertTrigger.id,
                    KafkaAlertTrigger.alert_config_id,
                    KafkaAlertTrigger.cluster_id,
                    KafkaAlertTrigger.subject,
                    KafkaAlertTrigger.severity,
                ).where(
                    KafkaAlertTrigger.alert_config_id.in_(config_ids),
                    KafkaAlertTrigger.resolved_at.is_(None),
                )
            )).all()
            open_triggers: dict[tuple[int, int, str], tuple[int, str | None]] = {
                (r.alert_config_id, r.cluster_id, r.subject): (r.id, r.severity) for r in open_rows
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

            # One row per (cluster, broker) -- the collector upserts the latest
            # values (uq_broker_metrics_cluster_broker, migration 0012).
            metrics_query = select(
                KafkaBrokerMetrics.cluster_id,
                KafkaBrokerMetrics.broker_id,
                KafkaBrokerMetrics.cpu_pct,
                KafkaBrokerMetrics.heap_pct,
                KafkaBrokerMetrics.time,
            )
            if all(cfg.cluster_id is not None for cfg in configs):
                metrics_query = metrics_query.where(
                    KafkaBrokerMetrics.cluster_id.in_({cfg.cluster_id for cfg in configs})
                )
            metric_rows = (await session.execute(metrics_query)).all()
        last_resolved: dict[tuple[int, int, str], datetime] = {
            (r.alert_config_id, r.cluster_id, r.subject): r.last_resolved_at for r in resolved_rows
        }
    except Exception as exc:
        logger.warning("_evaluate_broker_thresholds: setup failed: %s", exc)
        return

    # ---- Decide who fires/escalates/resolves, entirely in memory, no DB session open ----
    now = datetime.now(timezone.utc)
    to_insert: list[KafkaAlertTrigger] = []
    to_update: list[tuple[int, str | None, str]] = []  # (trigger id, new severity or None, metric_value)
    to_resolve: list[int] = []
    resolve_context: dict[int, tuple[KafkaAlertConfig, int]] = {}
    # (config, cluster_id, subject, tier, description, new trigger or None for an escalation)
    to_post: list[tuple[KafkaAlertConfig, int, str, str, str, KafkaAlertTrigger | None]] = []
    cluster_ids_evaluated: set[int] = set()
    brokers_evaluated = 0
    stale_rows = 0
    no_data_rows = 0
    escalated = 0

    for row in metric_rows:
        if row.time is None or now - row.time > _BROKER_METRICS_MAX_AGE:
            stale_rows += 1
            continue
        if (row.cpu_pct or 0.0) == 0.0 and (row.heap_pct or 0.0) == 0.0:
            no_data_rows += 1
            continue
        cluster_id = row.cluster_id
        subject = str(row.broker_id)
        cluster_ids_evaluated.add(cluster_id)
        brokers_evaluated += 1

        for config in configs:
            if config.cluster_id is not None and config.cluster_id != cluster_id:
                continue
            try:
                column, label = BROKER_THRESHOLD_METRICS[config.alert_type]
                value = getattr(row, column)
                if value is None:
                    continue
                tier_config = config.config or {}
                tier = _threshold_tier(tier_config, value)
                key = (config.id, cluster_id, subject)
                open_trigger = open_triggers.get(key)

                if tier is None:
                    _threshold_breach_counts[key] = 0
                    if open_trigger is not None:
                        to_resolve.append(open_trigger[0])
                        resolve_context[open_trigger[0]] = (config, cluster_id)
                    continue

                metric_value = str(value)
                description = (
                    f"Broker {subject} {label} is {value:.1f}% "
                    f"({tier}, threshold {float(tier_config[tier]):g})"
                )

                if open_trigger is not None:
                    trigger_id, open_severity = open_trigger
                    # NULL severity = the rule's own severity (see models.py).
                    old_tier = open_severity or config.severity
                    old_rank = _TIER_RANK.get(old_tier, -1)
                    if _TIER_RANK[tier] > old_rank:
                        escalated += 1
                        to_update.append((trigger_id, tier, metric_value))
                        to_post.append((
                            config, cluster_id, subject, tier,
                            f"{description} -- ESCALATED from {old_tier}", None,
                        ))
                    elif _TIER_RANK[tier] < old_rank:
                        to_update.append((trigger_id, tier, metric_value))
                    else:
                        to_update.append((trigger_id, None, metric_value))
                    continue

                _threshold_breach_counts[key] = _threshold_breach_counts.get(key, 0) + 1
                if _threshold_breach_counts[key] < _THRESHOLD_CONFIRM_CHECKS:
                    continue

                last_resolved_at = last_resolved.get(key)
                since_resolved = (now - last_resolved_at) if last_resolved_at is not None else None
                if since_resolved is not None and since_resolved < timedelta(minutes=config.cooldown_minutes):
                    continue  # still in cooldown
                is_recurrence = (
                    since_resolved is not None
                    and since_resolved <= timedelta(hours=RECURRENCE_LOOKBACK_HOURS)
                )
                description += (
                    " -- RECURRING: this same issue resolved recently and has now fired again"
                    if is_recurrence else ""
                )
                trigger = KafkaAlertTrigger(
                    alert_config_id=config.id,
                    cluster_id=cluster_id,
                    subject=subject,
                    severity=tier,
                    triggered_at=now,
                    metric_value=metric_value,
                    message_sent=description,
                    teams_post_success=False,
                    error_detail=None,
                    resolved_at=None,
                    is_recurrence=is_recurrence,
                )
                to_insert.append(trigger)
                to_post.append((config, cluster_id, subject, tier, description, trigger))
            except Exception as exc:
                logger.warning(
                    "_evaluate_broker_thresholds: alert_config_id=%s cluster_id=%s subject=%s failed: %s",
                    config.id, cluster_id, subject, exc,
                )

    logger.info(
        "evaluate_alerts: broker thresholds checked clusters=%d "
        "brokers=%d stale_skipped=%d no_data_skipped=%d fired=%d escalated=%d resolved=%d",
        len(cluster_ids_evaluated), brokers_evaluated, stale_rows, no_data_rows,
        len(to_insert), escalated, len(to_resolve),
    )

    # ---- Teams posts, no DB session open ----
    if to_post or to_resolve:
        try:
            cluster_names = await _get_cluster_names()
        except Exception as exc:
            logger.warning("_evaluate_broker_thresholds: cluster name lookup failed: %s", exc)
            cluster_names = {}
    for config, cluster_id, subject, tier, description, trigger in to_post:
        try:
            card = build_adaptive_card(
                agent_name="Kafka Analyser",
                cluster_name=cluster_names.get(cluster_id, str(cluster_id)),
                anomaly={
                    "severity": tier,
                    "category": config.alert_type,
                    "description": description,
                    "recommended_action": "Check load on the broker and recent traffic changes.",
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
            if trigger is not None:
                trigger.teams_post_success = success
                trigger.error_detail = error_detail
            elif not success:
                logger.warning(
                    "_evaluate_broker_thresholds: escalation card not sent for "
                    "alert_config_id=%s cluster_id=%s subject=%s: %s",
                    config.id, cluster_id, subject, error_detail or "Teams post failed",
                )
        except Exception as exc:
            logger.warning(
                "_evaluate_broker_thresholds: Teams post for alert_config_id=%s cluster_id=%s subject=%s failed: %s",
                config.id, cluster_id, subject, exc,
            )

    if not to_insert and not to_update and not to_resolve:
        return

    # ---- Single write session: each row in its own savepoint ----
    # Resolve cards are sent only after this session has closed. Each entry
    # holds a plain snapshot of the trigger (taken before its commit), since
    # a later rollback in this session expires every ORM object in it.
    from types import SimpleNamespace
    resolved_items: list[dict] = []
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
                        "_evaluate_broker_thresholds: failed to record trigger for "
                        "alert_config_id=%s cluster_id=%s subject=%s (card was already sent -- "
                        "this broker will likely be re-alerted next cycle): %s",
                        trigger.alert_config_id, trigger.cluster_id, trigger.subject, exc,
                    )
            for trigger_id, severity, metric_value in to_update:
                try:
                    async with session.begin_nested():
                        trigger = await session.get(KafkaAlertTrigger, trigger_id)
                        if trigger is not None and trigger.resolved_at is None:
                            if severity is not None:
                                trigger.severity = severity
                            trigger.metric_value = metric_value
                    await session.commit()
                except Exception as exc:
                    await session.rollback()
                    logger.warning(
                        "_evaluate_broker_thresholds: failed to update trigger id=%s: %s",
                        trigger_id, exc,
                    )
            for trigger_id in to_resolve:
                try:
                    snapshot = None
                    async with session.begin_nested():
                        trigger = await session.get(KafkaAlertTrigger, trigger_id)
                        if trigger is not None and trigger.resolved_at is None:
                            trigger.resolved_at = now
                            snapshot = SimpleNamespace(
                                subject=trigger.subject,
                                severity=trigger.severity,
                                triggered_at=trigger.triggered_at,
                                resolved_at=trigger.resolved_at,
                                teams_post_success=trigger.teams_post_success,
                            )
                    await session.commit()
                    if snapshot is not None and trigger_id in resolve_context:
                        config, cluster_id = resolve_context[trigger_id]
                        resolved_items.append({
                            "config": config,
                            "trigger": snapshot,
                            "cluster_name": cluster_names.get(cluster_id, str(cluster_id)),
                        })
                except Exception as exc:
                    await session.rollback()
                    logger.warning(
                        "_evaluate_broker_thresholds: failed to resolve trigger id=%s: %s",
                        trigger_id, exc,
                    )
    except Exception as exc:
        logger.warning("_evaluate_broker_thresholds: write session failed entirely: %s", exc)

    if resolved_items:
        try:
            await _send_resolve_cards(resolved_items)
        except Exception as exc:
            logger.warning("_evaluate_broker_thresholds: resolve cards failed: %s", exc)
