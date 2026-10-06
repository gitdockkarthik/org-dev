"""Teams alert firing/resolution logic for kafka_alert_configs /
kafka_alert_triggers (see models.py for the agreed notification model,
2026-09-28): an immediate card when a rule fires and a resolve card
(_send_resolve_cards) when it clears. Driven by check_and_recycle_close_wait
in collectors.py (alert_type 'close_wait_spike') and by evaluate_alerts
(broker reachability, broker CPU/heap thresholds and cluster URP/RF metrics).

Every public function here is best-effort notification logic riding along
on a real operational job: each one logs a warning and swallows on failure,
never raises, so it can never break the caller's own check/recycle logic.
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import bindparam, func, select, text

from database import SessionLocal
from models import KafkaAlertConfig, KafkaAlertTrigger, KafkaBrokerMetrics
from shared.escalation.notifier import build_adaptive_card, build_resolve_card, send_to_teams

logger = logging.getLogger(__name__)

CLOSE_WAIT_ALERT_TYPE = "close_wait_spike"

# A new trigger counts as a recurrence (flapping signal) if the same
# alert+cluster's previous trigger resolved within this window. Fixed for
# now -- tune later.
RECURRENCE_LOOKBACK_HOURS = 24


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
    longer satisfies the condition that fired it. Once the resolves are
    saved and the session has closed, sends a resolve card per trigger
    through _send_resolve_cards (only when the original card reached Teams
    and the rule allows it)."""
    if SessionLocal is None:
        return
    from types import SimpleNamespace
    # (config, cluster_id, plain trigger snapshot) for resolves actually
    # committed; snapshots are taken before the commit, as in
    # _evaluate_broker_reachability.
    saved: list[tuple] = []
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
            pending: list[tuple] = []
            for trigger, config in open_rows:
                threshold = int((config.config or {}).get("threshold", 1))
                current_count = close_wait_by_cluster_id.get(trigger.cluster_id, 0)
                if current_count < threshold:
                    trigger.resolved_at = now
                    changed = True
                    pending.append((config, trigger.cluster_id, SimpleNamespace(
                        subject=trigger.subject,
                        severity=trigger.severity,
                        triggered_at=trigger.triggered_at,
                        resolved_at=trigger.resolved_at,
                        teams_post_success=trigger.teams_post_success,
                    )))
            if changed:
                await session.commit()
                saved = pending
    except Exception as exc:
        logger.warning("resolve_cleared_close_wait_triggers failed: %s", exc)

    if saved:
        try:
            cluster_names = await _get_cluster_names()
        except Exception as exc:
            logger.warning("resolve_cleared_close_wait_triggers: cluster name lookup failed: %s", exc)
            cluster_names = {}
        items = [
            {"config": config, "trigger": snapshot, "cluster_name": cluster_names.get(cluster_id, str(cluster_id))}
            for config, cluster_id, snapshot in saved
        ]
        try:
            await _send_resolve_cards(items)
        except Exception as exc:
            logger.warning("resolve_cleared_close_wait_triggers: resolve cards failed: %s", exc)


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
    try:
        await _evaluate_cluster_metrics()
    except Exception as exc:
        logger.warning("evaluate_alerts: cluster metric check failed: %s", exc)


async def _evaluate_broker_reachability() -> None:
    """Fire broker_unreachable alerts: one immediate Teams card per (alert
    config, cluster, broker) once a broker has failed a fresh TCP connect on
    _BROKER_CONFIRM_CHECKS consecutive runs, only when no trigger for that
    triple is already open and it is out of cooldown. A reachable broker
    resolves its open trigger and gets a resolve card (_send_resolve_cards).

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


def _threshold_tier_below(tier_config: dict, value: float) -> str | None:
    """For "direction": "below" rules (descending tiers): the most severe
    configured tier whose threshold the value is at or below, else None."""
    for tier in reversed(_THRESHOLD_TIERS):
        threshold = tier_config.get(tier)
        if threshold is not None and value <= float(threshold):
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
    and gets a resolve card (_send_resolve_cards).

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


# Cluster threshold rules (Simple mode, same config as the broker thresholds):
# one value per rule from kafka_topic_metrics, which collect_topic_structure
# refreshes (one row per topic). Subject is always "cluster".
CLUSTER_METRIC_TYPES = (
    "cluster.urp_total", "cluster.rf_below_min", "cluster.leader_skew", "cluster.msg_rate_in",
    "cluster.data_gb_max", "cluster.data_spread_pct",
)
_CLUSTER_METRIC_SUBJECT = "cluster"
# A cluster is evaluated only if its topic-structure job succeeded this recently.
_CLUSTER_STRUCTURE_MAX_AGE = timedelta(minutes=15)
_CLUSTER_STRUCTURE_JOB_PREFIX = "kafka-topic-structure-"
_CLUSTER_STRUCTURE_FRESH_SQL = text(
    "SELECT DISTINCT job_id FROM kafka_job_runs "
    "WHERE status = 'success' AND job_id IN :job_ids AND ended_at >= :since"
).bindparams(bindparam("job_ids", expanding=True))
_URP_TOTAL_SQL = text(
    "SELECT cluster_id, COALESCE(SUM(urp_count), 0) AS value FROM kafka_topic_metrics "
    "WHERE cluster_id IN :cids AND partition_count > 0 GROUP BY cluster_id"
).bindparams(bindparam("cids", expanding=True))
# System topics (__consumer_offsets, __transaction_state, ...) and _schemas are excluded.
_RF_BELOW_MIN_SQL = text(
    "SELECT COUNT(*) FROM kafka_topic_metrics "
    "WHERE cluster_id = :cid AND partition_count > 0 AND replication_factor < :min_rf "
    r"AND topic NOT LIKE '\_\_%' AND topic <> '_schemas'"
)
# cluster.leader_skew: busiest broker's leader count / the cluster's average,
# from kafka_broker_distribution (one row per broker, written by
# collect_topic_structure). Its own freshness guard: the newest updated_at
# must be within _LEADER_SKEW_MAX_AGE (the structure-job guard is not used).
_LEADER_SKEW_ALERT_TYPE = "cluster.leader_skew"
_LEADER_SKEW_MAX_AGE = timedelta(minutes=15)
_LEADER_SKEW_SQL = text(
    "SELECT cluster_id, COUNT(*) AS brokers, MAX(leader_partition_count) AS max_leaders, "
    "AVG(leader_partition_count) AS avg_leaders, MAX(updated_at) AS last_updated "
    "FROM kafka_broker_distribution WHERE cluster_id IN :cids GROUP BY cluster_id"
).bindparams(bindparam("cids", expanding=True))
# cluster.msg_rate_in ("direction": "below"): msgs/sec of the latest inflow
# run per cluster from kafka_topic_message_rate_snapshots, written by
# collect_topic_message_inflow (~every 10 min; all topics of a run share one
# collected_at and interval_seconds). consumer-lag writes outflow-only rows
# to the same table, hence inflow IS NOT NULL everywhere. Rate = SUM(inflow)
# / interval_seconds of that run. Own guard: the run must be within
# _MSG_RATE_MAX_AGE and its interval in (0, _MSG_RATE_MAX_INTERVAL_SECS].
_MSG_RATE_IN_ALERT_TYPE = "cluster.msg_rate_in"
_MSG_RATE_MAX_AGE = timedelta(minutes=15)
_MSG_RATE_MAX_INTERVAL_SECS = 1200
_MSG_RATE_LATEST_SQL = text(
    "SELECT cluster_id, MAX(collected_at) AS collected_at FROM kafka_topic_message_rate_snapshots "
    "WHERE cluster_id IN :cids AND inflow IS NOT NULL AND collected_at >= :since GROUP BY cluster_id"
).bindparams(bindparam("cids", expanding=True))
# Exactly the latest runs: cluster_id IN and collected_at IN, then the
# (cluster_id, collected_at) pairs are matched in Python.
_MSG_RATE_RUN_SQL = text(
    "SELECT cluster_id, collected_at, SUM(inflow) AS msgs, MAX(interval_seconds) AS interval_seconds "
    "FROM kafka_topic_message_rate_snapshots "
    "WHERE cluster_id IN :cids AND collected_at IN :runs AND inflow IS NOT NULL "
    "GROUP BY cluster_id, collected_at"
).bindparams(bindparam("cids", expanding=True), bindparam("runs", expanding=True))
# Last inflow run seen per (config, cluster, subject): a msg_rate_in breach
# counts only on a new run (in-memory, resets on restart).
_msg_rate_last_run: dict[tuple[int, int, str], datetime] = {}
# cluster.data_gb_max (largest broker's data, GB) and cluster.data_spread_pct
# ((max - min) / max * 100), from kafka_broker_metrics.data_gb_true (one
# upserted row per broker, collect_broker_health). Own guard: a cluster is
# ignored unless it has >= 2 broker rows, all with data_gb_true set and time
# within _DATA_GB_MAX_AGE (the 6 minutes the dashboard uses for broker status).
_DATA_GB_MAX_ALERT_TYPE = "cluster.data_gb_max"
_DATA_SPREAD_ALERT_TYPE = "cluster.data_spread_pct"
_DATA_GB_ALERT_TYPES = (_DATA_GB_MAX_ALERT_TYPE, _DATA_SPREAD_ALERT_TYPE)
_DATA_GB_MAX_AGE = timedelta(minutes=6)
_DATA_GB_SQL = text(
    "SELECT cluster_id, broker_id, data_gb_true, time FROM kafka_broker_metrics WHERE cluster_id IN :cids"
).bindparams(bindparam("cids", expanding=True))
_CLUSTER_METRIC_ACTIONS = {
    "cluster.urp_total": "Check broker health and replica sync.",
    "cluster.rf_below_min": "Check the topic replication factors with the Kafka team.",
    "cluster.leader_skew": "Review leader distribution with the Kafka team; a preferred leader election may rebalance it.",
    "cluster.msg_rate_in": "Check producers and connectors writing to this cluster.",
    "cluster.data_gb_max": "Check disk capacity and retention with the Kafka team.",
    "cluster.data_spread_pct": "Check whether the low broker is replicating correctly (rebuilt, lost disks or not catching up).",
}


async def _evaluate_cluster_metrics() -> None:
    """Fire cluster.urp_total / cluster.rf_below_min / cluster.leader_skew alerts: one Teams card
    per (alert config, cluster) once the value sits in a tier on
    _THRESHOLD_CONFIRM_CHECKS consecutive runs, only when no trigger is
    already open and it is out of cooldown. Escalation, silent
    de-escalation and resolve (with a resolve card) work exactly as in
    _evaluate_broker_thresholds, whose four-stage structure this copies.

    A cluster whose kafka-topic-structure job has not succeeded within
    _CLUSTER_STRUCTURE_MAX_AGE is ignored entirely -- no counter change, no
    resolve, no fire -- so stale topic data never looks healthy or broken.
    cluster.leader_skew uses its own guard instead (_LEADER_SKEW_MAX_AGE on
    kafka_broker_distribution.updated_at) and skips clusters with fewer than
    two brokers or no leaders. cluster.msg_rate_in fires when the value is
    at or BELOW a tier (descending tiers), uses the latest inflow run with
    its own guard, and counts a breach only once per new run.
    cluster.data_gb_max / cluster.data_spread_pct use their own guard (every
    broker's data_gb_true set and fresh, at least two brokers)."""
    if SessionLocal is None:
        logger.info("evaluate_alerts: cluster metrics skipped: database not available")
        return
    try:
        from routes_settings import _config
        if not _config.get("teams_enabled"):
            logger.info("evaluate_alerts: cluster metrics skipped: teams_enabled is off")
            return

        # ---- Single read session: configs, triggers, freshness, metric values ----
        async with SessionLocal() as session:
            configs = (await session.execute(
                select(KafkaAlertConfig).where(
                    KafkaAlertConfig.alert_type.in_(list(CLUSTER_METRIC_TYPES)),
                    KafkaAlertConfig.enabled == True,
                )
            )).scalars().all()
            if not configs:
                logger.info("evaluate_alerts: cluster metrics skipped: no enabled cluster metric rules")
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

            # The structure-job guard covers URP/RF rules; leader skew has its own below.
            rule_cluster_ids = {
                cfg.cluster_id for cfg in configs
                if cfg.cluster_id is not None
                and cfg.alert_type not in (_LEADER_SKEW_ALERT_TYPE, _MSG_RATE_IN_ALERT_TYPE, *_DATA_GB_ALERT_TYPES)
            }
            fresh_cluster_ids: set[int] = set()
            if rule_cluster_ids:
                fresh_job_ids = set((await session.execute(
                    _CLUSTER_STRUCTURE_FRESH_SQL,
                    {
                        "job_ids": [f"{_CLUSTER_STRUCTURE_JOB_PREFIX}{cid}" for cid in rule_cluster_ids],
                        "since": datetime.now(timezone.utc) - _CLUSTER_STRUCTURE_MAX_AGE,
                    },
                )).scalars().all())
                fresh_cluster_ids = {
                    cid for cid in rule_cluster_ids if f"{_CLUSTER_STRUCTURE_JOB_PREFIX}{cid}" in fresh_job_ids
                }

            # Values keyed by config id; a rule missing here is skipped this run.
            values: dict[int, int | float] = {}
            urp_cluster_ids = {
                cfg.cluster_id for cfg in configs
                if cfg.alert_type == "cluster.urp_total" and cfg.cluster_id in fresh_cluster_ids
            }
            if urp_cluster_ids:
                urp_by_cluster = {
                    r.cluster_id: int(r.value)
                    for r in (await session.execute(_URP_TOTAL_SQL, {"cids": sorted(urp_cluster_ids)})).all()
                }
                for cfg in configs:
                    if cfg.alert_type == "cluster.urp_total" and cfg.cluster_id in urp_cluster_ids:
                        # A cluster with no topic rows has no URPs.
                        values[cfg.id] = urp_by_cluster.get(cfg.cluster_id, 0)
            for cfg in configs:
                if cfg.alert_type != "cluster.rf_below_min" or cfg.cluster_id not in fresh_cluster_ids:
                    continue
                min_rf = (cfg.config or {}).get("min_rf")
                if isinstance(min_rf, bool) or not isinstance(min_rf, int) or min_rf < 2:
                    logger.warning(
                        "_evaluate_cluster_metrics: alert_config_id=%s has no valid min_rf (%r); skipped",
                        cfg.id, min_rf,
                    )
                    continue
                values[cfg.id] = int((await session.execute(
                    _RF_BELOW_MIN_SQL, {"cid": cfg.cluster_id, "min_rf": min_rf}
                )).scalar_one())

            skew_cluster_ids = {
                cfg.cluster_id for cfg in configs
                if cfg.alert_type == _LEADER_SKEW_ALERT_TYPE and cfg.cluster_id is not None
            }
            skew_fresh_cluster_ids: set[int] = set()
            skew_skipped_cluster_ids: set[int] = set()  # fresh, but < 2 brokers or no leaders
            if skew_cluster_ids:
                skew_by_cluster: dict[int, float] = {}
                skew_now = datetime.now(timezone.utc)
                for r in (await session.execute(_LEADER_SKEW_SQL, {"cids": sorted(skew_cluster_ids)})).all():
                    if r.last_updated is None or skew_now - r.last_updated > _LEADER_SKEW_MAX_AGE:
                        continue  # stale: ignored like a cluster without a recent structure run
                    skew_fresh_cluster_ids.add(r.cluster_id)
                    avg_leaders = float(r.avg_leaders or 0)
                    if r.brokers < 2 or avg_leaders <= 0:
                        skew_skipped_cluster_ids.add(r.cluster_id)
                        continue
                    skew_by_cluster[r.cluster_id] = round(float(r.max_leaders) / avg_leaders, 2)
                for cfg in configs:
                    if cfg.alert_type == _LEADER_SKEW_ALERT_TYPE and cfg.cluster_id in skew_by_cluster:
                        values[cfg.id] = skew_by_cluster[cfg.cluster_id]

            rate_cluster_ids = {
                cfg.cluster_id for cfg in configs
                if cfg.alert_type == _MSG_RATE_IN_ALERT_TYPE and cfg.cluster_id is not None
            }
            # cluster_id -> (collected_at, interval_seconds) of a valid latest run
            rate_runs: dict[int, tuple[datetime, float]] = {}
            if rate_cluster_ids:
                latest = {
                    r.cluster_id: r.collected_at
                    for r in (await session.execute(_MSG_RATE_LATEST_SQL, {
                        "cids": sorted(rate_cluster_ids),
                        "since": datetime.now(timezone.utc) - _MSG_RATE_MAX_AGE,
                    })).all()
                    if r.collected_at is not None
                }
                rate_by_cluster: dict[int, float] = {}
                if latest:
                    for r in (await session.execute(_MSG_RATE_RUN_SQL, {
                        "cids": sorted(latest), "runs": sorted(set(latest.values())),
                    })).all():
                        if latest.get(r.cluster_id) != r.collected_at:
                            continue  # another cluster's run time
                        interval = float(r.interval_seconds) if r.interval_seconds is not None else None
                        if interval is None or interval <= 0 or interval > _MSG_RATE_MAX_INTERVAL_SECS:
                            continue  # ignored like a stale run
                        rate_runs[r.cluster_id] = (r.collected_at, interval)
                        rate_by_cluster[r.cluster_id] = round(float(r.msgs or 0) / interval, 2)
                for cfg in configs:
                    if cfg.alert_type == _MSG_RATE_IN_ALERT_TYPE and cfg.cluster_id in rate_by_cluster:
                        values[cfg.id] = rate_by_cluster[cfg.cluster_id]

            data_cluster_ids = {
                cfg.cluster_id for cfg in configs
                if cfg.alert_type in _DATA_GB_ALERT_TYPES and cfg.cluster_id is not None
            }
            # cluster_id -> (largest broker, its GB, smallest broker, its GB) for clusters passing the guard
            data_by_cluster: dict[int, tuple[str, float, str, float]] = {}
            data_ignored: dict[int, str] = {}  # cluster_id -> reason, naming the broker
            if data_cluster_ids:
                brokers_by_cluster: dict[int, list] = {}
                for r in (await session.execute(_DATA_GB_SQL, {"cids": sorted(data_cluster_ids)})).all():
                    brokers_by_cluster.setdefault(r.cluster_id, []).append(r)
                data_now = datetime.now(timezone.utc)
                for cid in sorted(data_cluster_ids):
                    rows = sorted(brokers_by_cluster.get(cid, []), key=lambda r: str(r.broker_id))
                    if len(rows) < 2:
                        data_ignored[cid] = (
                            f"only broker {rows[0].broker_id} has a metrics row" if rows else "no broker metrics rows"
                        )
                        continue
                    missing = next((r for r in rows if r.data_gb_true is None), None)
                    if missing is not None:
                        data_ignored[cid] = f"broker {missing.broker_id} has no data_gb_true"
                        continue
                    stale = next((r for r in rows if r.time is None or data_now - r.time > _DATA_GB_MAX_AGE), None)
                    if stale is not None:
                        data_ignored[cid] = (
                            f"broker {stale.broker_id} has no metrics in the last "
                            f"{int(_DATA_GB_MAX_AGE.total_seconds() // 60)} min"
                            + (f" (last {int((data_now - stale.time).total_seconds() // 60)} min ago)" if stale.time else "")
                        )
                        continue
                    hi = max(rows, key=lambda r: r.data_gb_true)
                    lo = min(rows, key=lambda r: r.data_gb_true)
                    data_by_cluster[cid] = (str(hi.broker_id), float(hi.data_gb_true), str(lo.broker_id), float(lo.data_gb_true))
                for cfg in configs:
                    if cfg.alert_type not in _DATA_GB_ALERT_TYPES or cfg.cluster_id not in data_by_cluster:
                        continue
                    hi_broker, hi_gb, lo_broker, lo_gb = data_by_cluster[cfg.cluster_id]
                    if cfg.alert_type == _DATA_GB_MAX_ALERT_TYPE:
                        values[cfg.id] = round(hi_gb, 1)
                    elif hi_gb > 0:
                        values[cfg.id] = round((hi_gb - lo_gb) / hi_gb * 100, 1)
                    else:
                        data_ignored.setdefault(cfg.cluster_id, f"spread skipped: largest broker {hi_broker} holds {hi_gb:g} GB")
        last_resolved: dict[tuple[int, int, str], datetime] = {
            (r.alert_config_id, r.cluster_id, r.subject): r.last_resolved_at for r in resolved_rows
        }
    except Exception as exc:
        logger.warning("_evaluate_cluster_metrics: setup failed: %s", exc)
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
    stale_cluster_ids: set[int] = set()
    stale_skew_cluster_ids: set[int] = set()
    stale_rate_cluster_ids: set[int] = set()
    rules_evaluated = 0
    escalated = 0
    subject = _CLUSTER_METRIC_SUBJECT

    for config in configs:
        cluster_id = config.cluster_id
        if cluster_id is None:
            logger.warning("_evaluate_cluster_metrics: alert_config_id=%s has no cluster_id; skipped", config.id)
            continue
        if config.alert_type == _LEADER_SKEW_ALERT_TYPE:
            if cluster_id not in skew_fresh_cluster_ids:
                stale_skew_cluster_ids.add(cluster_id)
                continue
        elif config.alert_type == _MSG_RATE_IN_ALERT_TYPE:
            if cluster_id not in rate_runs:
                stale_rate_cluster_ids.add(cluster_id)
                continue
        elif config.alert_type in _DATA_GB_ALERT_TYPES:
            if cluster_id not in data_by_cluster:
                continue  # reason logged from data_ignored
        elif cluster_id not in fresh_cluster_ids:
            stale_cluster_ids.add(cluster_id)
            continue
        if config.id not in values:
            continue
        try:
            value = values[config.id]
            cluster_ids_evaluated.add(cluster_id)
            rules_evaluated += 1
            tier_config = config.config or {}
            key = (config.id, cluster_id, subject)
            # msg_rate_in: below-direction tiers, and a breach counts only on a new run.
            new_run = True
            if config.alert_type == _MSG_RATE_IN_ALERT_TYPE:
                tier = _threshold_tier_below(tier_config, value)
                new_run = _msg_rate_last_run.get(key) != rate_runs[cluster_id][0]
                _msg_rate_last_run[key] = rate_runs[cluster_id][0]
            else:
                tier = _threshold_tier(tier_config, value)
            open_trigger = open_triggers.get(key)

            if tier is None:
                _threshold_breach_counts[key] = 0
                if open_trigger is not None:
                    to_resolve.append(open_trigger[0])
                    resolve_context[open_trigger[0]] = (config, cluster_id)
                continue

            metric_value = str(value)
            if config.alert_type == "cluster.urp_total":
                description = f"{value} under-replicated partitions"
            elif config.alert_type == _LEADER_SKEW_ALERT_TYPE:
                description = f"Leader partitions are skewed: busiest broker has {value:g} times the cluster average"
            elif config.alert_type == _DATA_GB_MAX_ALERT_TYPE:
                description = f"Broker {data_by_cluster[cluster_id][0]} holds {value} GB"
            elif config.alert_type == _DATA_SPREAD_ALERT_TYPE:
                hi_broker, hi_gb, lo_broker, lo_gb = data_by_cluster[cluster_id]
                description = (
                    f"Broker {lo_broker} holds {lo_gb:.1f} GB vs broker {hi_broker} at {hi_gb:.1f} GB: "
                    f"spread {value}%"
                )
            elif config.alert_type == _MSG_RATE_IN_ALERT_TYPE:
                description = (
                    f"Cluster inflow is {value} msgs/sec over the last "
                    f"{rate_runs[cluster_id][1] / 60:.1f} minutes"
                )
            else:
                description = (
                    f"{value} topics have replication factor below {tier_config['min_rf']} "
                    "(system topics excluded; min.insync.replicas is not checked)"
                )
            description += f" ({tier}, threshold {float(tier_config[tier]):g})"

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

            if new_run:
                _threshold_breach_counts[key] = _threshold_breach_counts.get(key, 0) + 1
            if _threshold_breach_counts.get(key, 0) < _THRESHOLD_CONFIRM_CHECKS:
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
                "_evaluate_cluster_metrics: alert_config_id=%s cluster_id=%s failed: %s",
                config.id, cluster_id, exc,
            )

    if stale_cluster_ids:
        logger.info(
            "evaluate_alerts: cluster metrics ignored clusters %s: no successful topic-structure run in %d min",
            sorted(stale_cluster_ids), int(_CLUSTER_STRUCTURE_MAX_AGE.total_seconds() // 60),
        )
    if stale_skew_cluster_ids:
        logger.info(
            "evaluate_alerts: leader skew ignored clusters %s: no broker distribution update in %d min",
            sorted(stale_skew_cluster_ids), int(_LEADER_SKEW_MAX_AGE.total_seconds() // 60),
        )
    if stale_rate_cluster_ids:
        logger.info(
            "evaluate_alerts: message rate ignored clusters %s: no valid inflow run in %d min",
            sorted(stale_rate_cluster_ids), int(_MSG_RATE_MAX_AGE.total_seconds() // 60),
        )
    if data_ignored:
        logger.info(
            "evaluate_alerts: data on disk ignored clusters: %s",
            "; ".join(f"{cid}: {reason}" for cid, reason in sorted(data_ignored.items())),
        )
    if skew_skipped_cluster_ids:
        logger.info(
            "evaluate_alerts: leader skew skipped clusters %s: fewer than 2 brokers or no leaders",
            sorted(skew_skipped_cluster_ids),
        )
    logger.info(
        "evaluate_alerts: cluster metrics checked clusters=%d rules=%d stale_skipped=%d "
        "fired=%d escalated=%d resolved=%d",
        len(cluster_ids_evaluated), rules_evaluated, len(stale_cluster_ids),
        len(to_insert), escalated, len(to_resolve),
    )

    # ---- Teams posts, no DB session open ----
    if to_post or to_resolve:
        try:
            cluster_names = await _get_cluster_names()
        except Exception as exc:
            logger.warning("_evaluate_cluster_metrics: cluster name lookup failed: %s", exc)
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
                    "recommended_action": _CLUSTER_METRIC_ACTIONS[config.alert_type],
                },
            )
            webhook_url = config.webhook_url or _config.get("teams_webhook_url", "")
            if webhook_url:
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
                    "_evaluate_cluster_metrics: escalation card not sent for "
                    "alert_config_id=%s cluster_id=%s: %s",
                    config.id, cluster_id, error_detail or "Teams post failed",
                )
        except Exception as exc:
            logger.warning(
                "_evaluate_cluster_metrics: Teams post for alert_config_id=%s cluster_id=%s failed: %s",
                config.id, cluster_id, exc,
            )

    if not to_insert and not to_update and not to_resolve:
        return

    # ---- Single write session: each row in its own savepoint ----
    # Resolve cards are sent only after this session has closed, from plain
    # snapshots taken before each commit (a later rollback expires ORM objects).
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
                        "_evaluate_cluster_metrics: failed to record trigger for "
                        "alert_config_id=%s cluster_id=%s (card was already sent -- "
                        "this cluster will likely be re-alerted next cycle): %s",
                        trigger.alert_config_id, trigger.cluster_id, exc,
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
                        "_evaluate_cluster_metrics: failed to update trigger id=%s: %s",
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
                        "_evaluate_cluster_metrics: failed to resolve trigger id=%s: %s",
                        trigger_id, exc,
                    )
    except Exception as exc:
        logger.warning("_evaluate_cluster_metrics: write session failed entirely: %s", exc)

    if resolved_items:
        try:
            await _send_resolve_cards(resolved_items)
        except Exception as exc:
            logger.warning("_evaluate_cluster_metrics: resolve cards failed: %s", exc)
