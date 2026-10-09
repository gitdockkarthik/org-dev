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
import json
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import httpx
from sqlalchemy import bindparam, func, select, text

from database import SessionLocal
from models import KafkaAlertConfig, KafkaAlertTrigger, KafkaBrokerMetrics
from shared.escalation.notifier import build_adaptive_card, build_resolve_card, send_to_teams
from tools.zookeeper import _parse_mntr, _zk_command

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


# Alert rule emails ride along with the Teams cards (step 3a, 2026-10-09).
# Each evaluator run collects them and hands them to one background task
# once its Teams posts are done, outside any DB session, so a slow or failing
# mail server can never delay a Teams card, a later evaluator or a DB write.
# Only rules with email_enabled are queued, so with no such rule nothing here
# runs. Strong references keep the tasks alive until they finish.
_rule_email_tasks: set = set()


def _rule_email(config, cluster_name, kind, severity, description="", recommended_action="", **extra) -> dict | None:
    """One queued rule email, or None when the rule has email off. Takes a
    plain snapshot of the rule so no ORM object outlives its session. Never
    raises."""
    try:
        if not getattr(config, "email_enabled", False):
            return None
        from types import SimpleNamespace
        return {
            "config_row": SimpleNamespace(
                id=config.id, name=config.name, alert_type=config.alert_type, email_enabled=True,
            ),
            "cluster_name": cluster_name,
            "kind": kind,
            "severity": severity,
            "description": description,
            "recommended_action": recommended_action,
            "extra": {k: v for k, v in extra.items() if v is not None},
        }
    except Exception as exc:
        logger.warning("_rule_email: could not queue %s email: %s", kind, exc)
        return None


def _resolve_emails(items: list[dict]) -> list[dict | None]:
    """Resolve emails for the resolved_items passed to _send_resolve_cards:
    one per trigger actually resolved, whether or not its Teams card was
    sent (no teams_post_success or send_resolve_card gating)."""
    emails = []
    for item in items:
        try:
            config, trigger = item["config"], item["trigger"]
            open_minutes = None
            if trigger.triggered_at is not None and trigger.resolved_at is not None:
                open_minutes = (trigger.resolved_at - trigger.triggered_at).total_seconds() / 60
            emails.append(_rule_email(
                config, item.get("cluster_name", ""), "resolve", trigger.severity or config.severity,
                subject=trigger.subject, open_minutes=open_minutes,
            ))
        except Exception as exc:
            logger.warning("_resolve_emails: item failed: %s", exc)
    return emails


async def _send_rule_emails(emails: list[dict]) -> None:
    from routes_settings import _config
    from shared.escalation.email_notifier import notify_rule_email
    for email in emails:
        try:
            await notify_rule_email(**email, settings=_config)
        except Exception as exc:
            logger.warning("_send_rule_emails: item failed: %s", exc)


def _queue_rule_emails(emails: list[dict | None]) -> None:
    """Start one background task sending these rule emails in order. Never
    raises and never waits."""
    try:
        emails = [e for e in emails if e]
        if not emails:
            return
        task = asyncio.create_task(_send_rule_emails(emails))
        _rule_email_tasks.add(task)
        task.add_done_callback(_rule_email_tasks.discard)
    except Exception as exc:
        logger.warning("_queue_rule_emails failed: %s", exc)


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
        _queue_rule_emails(_resolve_emails(items))


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
    emails: list[dict | None] = []

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
                recommended_action = ("Use the Breaker Status tab's Clear Stale "
                                      "Connections button, or wait roughly 5 minutes "
                                      "for this to clear automatically.")
                card = build_adaptive_card(
                    agent_name="Kafka Analyser",
                    cluster_name=cluster_names.get(cluster_id, str(cluster_id)),
                    anomaly={
                        "severity": config.severity,
                        "category": CLOSE_WAIT_ALERT_TYPE,
                        "description": description,
                        "recommended_action": recommended_action,
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
                emails.append(_rule_email(
                    config, cluster_names.get(cluster_id, str(cluster_id)), "fire",
                    config.severity, description, recommended_action,
                ))
            except Exception as exc:
                logger.warning(
                    "fire_close_wait_alerts: alert_config_id=%s cluster_id=%s failed: %s",
                    config.id, cluster_id, exc,
                )

    _queue_rule_emails(emails)

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
    try:
        await _evaluate_cluster_lag()
    except Exception as exc:
        logger.warning("evaluate_alerts: cluster lag check failed: %s", exc)


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
    emails: list[dict | None] = []
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
                    recommended_action = "Check the broker process/host and the network path to it."
                    card = build_adaptive_card(
                        agent_name="Kafka Analyser",
                        cluster_name=cluster_name,
                        anomaly={
                            "severity": config.severity,
                            "category": BROKER_UNREACHABLE_ALERT_TYPE,
                            "description": description,
                            "recommended_action": recommended_action,
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
                    emails.append(_rule_email(
                        config, cluster_name, "fire", config.severity, description, recommended_action,
                        subject=subject,
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

    _queue_rule_emails(emails)

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
        _queue_rule_emails(_resolve_emails(resolved_items))


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
    emails: list[dict | None] = []
    for config, cluster_id, subject, tier, description, trigger in to_post:
        try:
            recommended_action = "Check load on the broker and recent traffic changes."
            card = build_adaptive_card(
                agent_name="Kafka Analyser",
                cluster_name=cluster_names.get(cluster_id, str(cluster_id)),
                anomaly={
                    "severity": tier,
                    "category": config.alert_type,
                    "description": description,
                    "recommended_action": recommended_action,
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
            emails.append(_rule_email(
                config, cluster_names.get(cluster_id, str(cluster_id)),
                "fire" if trigger is not None else "escalate", tier, description, recommended_action,
                subject=subject,
            ))
        except Exception as exc:
            logger.warning(
                "_evaluate_broker_thresholds: Teams post for alert_config_id=%s cluster_id=%s subject=%s failed: %s",
                config.id, cluster_id, subject, exc,
            )
    _queue_rule_emails(emails)

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
        _queue_rule_emails(_resolve_emails(resolved_items))


# Cluster threshold rules (Simple mode, same config as the broker thresholds):
# one value per rule from kafka_topic_metrics, which collect_topic_structure
# refreshes (one row per topic). Subject is always "cluster".
CLUSTER_METRIC_TYPES = (
    "cluster.urp_total", "cluster.rf_below_min", "cluster.leader_skew", "cluster.msg_rate_in",
    "cluster.data_gb_max", "cluster.data_spread_pct", "cluster.zk_ensemble",
    "cluster.connect_failed_connectors", "cluster.connect_failed_tasks", "cluster.connect_workers_down",
    "cluster.sr_nodes_down", "cluster.sr_soft_deleted",
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
# cluster.zk_ensemble: number of ZooKeeper problems, probed live after the read
# session has closed: one `mntr` per node of kafka_clusters.zookeeper_url
# (comma-separated host:port), all nodes in parallel, each bounded by
# _ZK_PROBE_TIMEOUT_SECS and never retried in the same run. A node is down
# unless it answers with a zk_server_state. Value = down nodes, + 1 if the
# nodes that answered do not hold exactly one leader; if none answered, the
# number of configured nodes. Own guard: a cluster with an empty URL or fewer
# than 2 nodes is ignored.
_ZK_ENSEMBLE_ALERT_TYPE = "cluster.zk_ensemble"
_ZK_PROBE_TIMEOUT_SECS = 3
_ZK_DEFAULT_PORT = 2181
_ZK_URL_SQL = text(
    "SELECT id, zookeeper_url FROM kafka_clusters WHERE id IN :cids"
).bindparams(bindparam("cids", expanding=True))
# cluster.connect_failed_connectors (rows with state FAILED) and
# cluster.connect_failed_tasks (SUM(failed_tasks)), from the latest run of
# kafka_connector_snapshots per cluster (collect_connector_snapshots, every
# 2 min; all rows of a run share one collected_at). Own guard: a cluster is
# ignored if kafka_connect_url is empty, it has no snapshot rows, its latest
# run is older than _CONNECT_SNAPSHOT_MAX_AGE, or that run has fewer than half
# the rows of the previous one (the collector silently skips a worker whose
# collect fails, so a run can be partial).
_CONNECT_FAILED_CONNECTORS_ALERT_TYPE = "cluster.connect_failed_connectors"
_CONNECT_FAILED_TASKS_ALERT_TYPE = "cluster.connect_failed_tasks"
_CONNECT_SNAPSHOT_ALERT_TYPES = (_CONNECT_FAILED_CONNECTORS_ALERT_TYPE, _CONNECT_FAILED_TASKS_ALERT_TYPE)
_CONNECT_SNAPSHOT_MAX_AGE = timedelta(minutes=6)
# Runs looked at to find the latest and the previous one (~7 runs at 2 min).
_CONNECT_SNAPSHOT_WINDOW = timedelta(minutes=15)
_CONNECT_NAMES_MAX = 5
_CONNECT_URL_SQL = text(
    "SELECT id, kafka_connect_url FROM kafka_clusters WHERE id IN :cids"
).bindparams(bindparam("cids", expanding=True))
# The latest run's rows per cluster, each carrying the latest and previous
# run's row counts; the collected_at range uses ix_connector_snapshots_cluster_time.
_CONNECT_SNAPSHOT_SQL = text(
    "WITH runs AS ("
    " SELECT cluster_id, collected_at, COUNT(*) AS run_rows,"
    " ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY collected_at DESC) AS rn"
    " FROM kafka_connector_snapshots WHERE cluster_id IN :cids AND collected_at >= :since"
    " GROUP BY cluster_id, collected_at"
    ") "
    "SELECT s.cluster_id, s.collected_at, s.connector_name, s.state, s.failed_tasks,"
    " latest.run_rows AS run_rows, prev.run_rows AS previous_rows "
    "FROM runs latest "
    "JOIN kafka_connector_snapshots s ON s.cluster_id = latest.cluster_id AND s.collected_at = latest.collected_at "
    "LEFT JOIN runs prev ON prev.cluster_id = latest.cluster_id AND prev.rn = 2 "
    "WHERE latest.rn = 1"
).bindparams(bindparam("cids", expanding=True))
# cluster.connect_workers_down: number of Connect workers of
# kafka_clusters.kafka_connect_url (comma-separated URLs with scheme) that do
# not answer GET {worker}/ with HTTP 200 within _CONNECT_PROBE_TIMEOUT_SECS,
# probed live after the read session has closed, once per distinct worker,
# at most _CONNECT_PROBE_CONCURRENCY at a time, never retried in the same
# run. Own guard: a cluster with an empty URL is ignored.
_CONNECT_WORKERS_ALERT_TYPE = "cluster.connect_workers_down"
_CONNECT_PROBE_TIMEOUT_SECS = 3
_CONNECT_PROBE_CONCURRENCY = 10
_CONNECT_WORKER_NAMES_MAX = 10
# cluster.sr_nodes_down: number of Schema Registry nodes of
# kafka_clusters.schema_registry_url (comma-separated; http:// added when no
# scheme) that do not answer GET {node}/ with a status below 500 within
# _SR_PROBE_TIMEOUT_SECS (401/403/422 = up: the node answers), probed live
# after the read session has closed, once per distinct node, at most
# _SR_PROBE_CONCURRENCY at a time, never retried in the same run.
# cluster.sr_soft_deleted: len(GET /subjects?deleted=true) - len(GET /subjects)
# (as sets, floor 0) on the first node of the cluster that answered the probe,
# each request bounded by _SR_SUBJECTS_TIMEOUT_SECS and _SR_SUBJECTS_MAX_BYTES.
# Credentials in the URL are dropped: no request sends any. Own guard: a
# cluster with an empty URL is ignored; for soft-deleted also one with no
# node answering, or where either request is not HTTP 200 with a JSON list.
_SR_NODES_ALERT_TYPE = "cluster.sr_nodes_down"
_SR_SOFT_DELETED_ALERT_TYPE = "cluster.sr_soft_deleted"
_SR_ALERT_TYPES = (_SR_NODES_ALERT_TYPE, _SR_SOFT_DELETED_ALERT_TYPE)
_SR_PROBE_TIMEOUT_SECS = 3
_SR_PROBE_CONCURRENCY = 10
_SR_NODE_NAMES_MAX = 10
_SR_SUBJECTS_TIMEOUT_SECS = 10
_SR_SUBJECTS_MAX_BYTES = 20 * 1024 * 1024
_SR_SUBJECT_NAMES_MAX = 5
_SR_URL_SQL = text(
    "SELECT id, schema_registry_url FROM kafka_clusters WHERE id IN :cids"
).bindparams(bindparam("cids", expanding=True))
_CLUSTER_METRIC_ACTIONS = {
    "cluster.urp_total": "Check broker health and replica sync.",
    "cluster.rf_below_min": "Check the topic replication factors with the Kafka team.",
    "cluster.leader_skew": "Review leader distribution with the Kafka team; a preferred leader election may rebalance it.",
    "cluster.msg_rate_in": "Check producers and connectors writing to this cluster.",
    "cluster.data_gb_max": "Check disk capacity and retention with the Kafka team.",
    "cluster.data_spread_pct": "Check whether the low broker is replicating correctly (rebuilt, lost disks or not catching up).",
    "cluster.zk_ensemble": "Check the ZooKeeper nodes and ensemble quorum with the Kafka team.",
    "cluster.connect_failed_connectors": "Check the failed connectors and their logs with the Kafka team.",
    "cluster.connect_failed_tasks": "Check the failed tasks (restart or fix the connector) with the Kafka team.",
    "cluster.connect_workers_down": "Check the Connect worker nodes with the Kafka team.",
    "cluster.sr_nodes_down": "Check the Schema Registry nodes.",
    "cluster.sr_soft_deleted": "Check the soft-deleted subjects in Schema Registry.",
}


def _zk_nodes(zookeeper_url: str | None) -> list[str]:
    """The distinct host:port nodes of a zookeeper_url (a chroot suffix such as
    "/kafka" is dropped; a node without a port gets _ZK_DEFAULT_PORT)."""
    entries = [e.strip() for e in (zookeeper_url or "").split("/", 1)[0].split(",") if e.strip()]
    return list(dict.fromkeys(e if ":" in e else f"{e}:{_ZK_DEFAULT_PORT}" for e in entries))


async def _zk_probe(node: str) -> tuple[bool, str | None]:
    """One `mntr` to a host:port node within _ZK_PROBE_TIMEOUT_SECS, no retry:
    (answered, zk_server_state or None). Never raises."""
    try:
        host, _, port = node.rpartition(":")
        output = await asyncio.wait_for(
            _zk_command(host, int(port), "mntr", timeout=_ZK_PROBE_TIMEOUT_SECS),
            timeout=_ZK_PROBE_TIMEOUT_SECS,
        )
    except Exception:
        return False, None
    return bool(output.strip()), _parse_mntr(output).get("zk_server_state") or None


def _zk_ensemble_value(nodes: list[str], answers: dict[str, tuple[bool, str | None]]) -> tuple[int, str]:
    """(problem count, card description without the tier suffix) for one cluster."""
    down = [n for n in nodes if answers[n][1] is None]
    if len(down) == len(nodes):
        if any(answers[n][0] for n in nodes):
            detail = "no node returned zk_server_state (is mntr in 4lw.commands.whitelist?)"
        else:
            detail = "no node reachable from the agent: check the network path first"
        return len(nodes), f"ZooKeeper: {len(nodes)} problem(s): down {', '.join(down)}; {detail}"
    leaders = [n for n in nodes if answers[n][1] == "leader"]
    value = len(down) + (1 if len(leaders) != 1 else 0)
    parts = [f"down {', '.join(down)}"] if down else []
    if len(leaders) == 1:
        parts.append(f"leader {leaders[0]}")
    elif leaders:
        parts.append(f"{len(leaders)} leaders {', '.join(leaders)}")
    else:
        parts.append("no leader")
    return value, f"ZooKeeper: {value} problem(s): " + "; ".join(parts)


def _name_list(names: list[str], limit: int) -> str:
    """The first `limit` names, comma-separated, then "and N more"."""
    shown = ", ".join(names[:limit])
    return f"{shown} and {len(names) - limit} more" if len(names) > limit else shown


def _connect_workers(connect_url: str | None) -> list[str]:
    """The distinct worker URLs of a kafka_connect_url (spaces and a trailing
    slash stripped)."""
    entries = [e.strip().rstrip("/") for e in (connect_url or "").split(",")]
    return list(dict.fromkeys(e for e in entries if e))


def _connect_worker_label(worker: str) -> str:
    """host:port of a worker URL for the card (never any credentials in it)."""
    try:
        parts = urlsplit(worker)
        if parts.hostname:
            return f"{parts.hostname}:{parts.port}" if parts.port else parts.hostname
    except ValueError:
        pass
    return worker


async def _connect_probe(client: httpx.AsyncClient, worker: str, limit: asyncio.Semaphore) -> bool:
    """One GET {worker}/ within _CONNECT_PROBE_TIMEOUT_SECS (waiting for the
    semaphore not counted), no retry: True only on HTTP 200. Never raises."""
    async with limit:
        try:
            resp = await asyncio.wait_for(client.get(f"{worker}/"), timeout=_CONNECT_PROBE_TIMEOUT_SECS)
        except Exception:
            return False
    return resp.status_code == 200


def _connect_workers_value(workers: list[str], up: dict[str, bool]) -> tuple[int, str]:
    """(down worker count, card description without the tier suffix) for one cluster."""
    down = [_connect_worker_label(w) for w in workers if not up[w]]
    description = (
        f"{len(down)} of {len(workers)} Connect worker(s) down: "
        f"{_name_list(down, _CONNECT_WORKER_NAMES_MAX)}"
    )
    if down and len(down) == len(workers):
        description += "; no worker reachable from the agent: check the network path first"
    return len(down), description


def _sr_nodes(sr_url: str | None) -> list[str]:
    """The distinct node URLs of a schema_registry_url: spaces and a trailing
    slash stripped, http:// added when no scheme, credentials dropped."""
    nodes = []
    for entry in (sr_url or "").split(","):
        entry = entry.strip().rstrip("/")
        if not entry:
            continue
        if "://" not in entry:
            entry = f"http://{entry}"
        try:
            parts = urlsplit(entry)
            netloc = parts.netloc.rpartition("@")[2]
            entry = parts._replace(netloc=netloc).geturl() if netloc else entry
        except ValueError:
            pass
        nodes.append(entry)
    return list(dict.fromkeys(nodes))


async def _sr_probe(client: httpx.AsyncClient, node: str, limit: asyncio.Semaphore) -> bool:
    """One GET {node}/ within _SR_PROBE_TIMEOUT_SECS (waiting for the
    semaphore not counted), no retry: True on any status below 500. Never raises."""
    async with limit:
        try:
            resp = await asyncio.wait_for(client.get(f"{node}/"), timeout=_SR_PROBE_TIMEOUT_SECS)
        except Exception:
            return False
    return resp.status_code < 500


def _sr_nodes_value(nodes: list[str], up: dict[str, bool]) -> tuple[int, str]:
    """(down node count, card description without the tier suffix) for one cluster."""
    down = [_connect_worker_label(n) for n in nodes if not up[n]]
    description = (
        f"{len(down)} of {len(nodes)} Schema Registry node(s) down: "
        f"{_name_list(down, _SR_NODE_NAMES_MAX)}"
    )
    if down and len(down) == len(nodes):
        description += "; no Schema Registry node reachable from the agent: check the network path first"
    return len(down), description


async def _sr_subject_set(client: httpx.AsyncClient, url: str) -> tuple[set[str] | None, str | None]:
    """GET url within _SR_SUBJECTS_TIMEOUT_SECS, reading at most
    _SR_SUBJECTS_MAX_BYTES: (subject names, None) on HTTP 200 with a JSON
    list of strings, else (None, reason). Never raises."""
    max_mb = _SR_SUBJECTS_MAX_BYTES // (1024 * 1024)

    async def _get() -> tuple[set[str] | None, str | None]:
        async with client.stream("GET", url) as resp:
            if resp.status_code != 200:
                return None, f"returned HTTP {resp.status_code}"
            length = resp.headers.get("content-length")
            if length and length.isdigit() and int(length) > _SR_SUBJECTS_MAX_BYTES:
                return None, f"response larger than {max_mb} MB"
            body = bytearray()
            async for chunk in resp.aiter_bytes():
                body += chunk
                if len(body) > _SR_SUBJECTS_MAX_BYTES:
                    return None, f"response larger than {max_mb} MB"
        try:
            data = json.loads(body)
        except ValueError:
            return None, "did not return JSON"
        if not isinstance(data, list) or not all(isinstance(s, str) for s in data):
            return None, "did not return a JSON list of subjects"
        return set(data), None

    try:
        return await asyncio.wait_for(_get(), timeout=_SR_SUBJECTS_TIMEOUT_SECS)
    except asyncio.TimeoutError:
        return None, f"no answer within {_SR_SUBJECTS_TIMEOUT_SECS} s"
    except Exception as exc:
        return None, f"failed ({type(exc).__name__})"


async def _sr_soft_deleted_value(client: httpx.AsyncClient, node: str) -> tuple[int | None, str]:
    """(soft-deleted count, card description without the tier suffix), or
    (None, reason the cluster is ignored)."""
    label = _connect_worker_label(node)
    live, reason = await _sr_subject_set(client, f"{node}/subjects")
    if live is None:
        return None, f"GET /subjects on {label} {reason}"
    with_deleted, reason = await _sr_subject_set(client, f"{node}/subjects?deleted=true")
    if with_deleted is None:
        return None, f"GET /subjects?deleted=true on {label} {reason}"
    value = max(len(with_deleted) - len(live), 0)
    names = sorted(with_deleted - live)
    return value, (
        f"{value} soft-deleted subject(s) in Schema Registry: {_name_list(names, _SR_SUBJECT_NAMES_MAX)}"
    )


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
    broker's data_gb_true set and fresh, at least two brokers).
    cluster.zk_ensemble probes ZooKeeper live, after the read session has
    closed, and ignores clusters with an empty URL or fewer than two nodes.
    cluster.connect_failed_connectors / cluster.connect_failed_tasks read the
    latest connector snapshot with their own guard (fresh, not empty, not
    partial, Connect URL set); cluster.connect_workers_down probes the Connect
    workers live, after the read session has closed, and ignores clusters
    with an empty Connect URL. cluster.sr_nodes_down / cluster.sr_soft_deleted
    probe Schema Registry live, after the read session has closed, and
    ignore clusters with an empty URL; soft-deleted also ignores a cluster
    whose subject lists cannot be read."""
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
                and cfg.alert_type not in (
                    _LEADER_SKEW_ALERT_TYPE, _MSG_RATE_IN_ALERT_TYPE, *_DATA_GB_ALERT_TYPES, _ZK_ENSEMBLE_ALERT_TYPE,
                    *_CONNECT_SNAPSHOT_ALERT_TYPES, _CONNECT_WORKERS_ALERT_TYPE, *_SR_ALERT_TYPES,
                )
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

            zk_cluster_ids = {
                cfg.cluster_id for cfg in configs
                if cfg.alert_type == _ZK_ENSEMBLE_ALERT_TYPE and cfg.cluster_id is not None
            }
            zk_urls: dict[int, str] = {}
            if zk_cluster_ids:
                zk_urls = {
                    r.id: r.zookeeper_url or ""
                    for r in (await session.execute(_ZK_URL_SQL, {"cids": sorted(zk_cluster_ids)})).all()
                }

            connect_snapshot_cluster_ids = {
                cfg.cluster_id for cfg in configs
                if cfg.alert_type in _CONNECT_SNAPSHOT_ALERT_TYPES and cfg.cluster_id is not None
            }
            connect_worker_cluster_ids = {
                cfg.cluster_id for cfg in configs
                if cfg.alert_type == _CONNECT_WORKERS_ALERT_TYPE and cfg.cluster_id is not None
            }
            connect_urls: dict[int, str] = {}
            if connect_snapshot_cluster_ids or connect_worker_cluster_ids:
                connect_urls = {
                    r.id: r.kafka_connect_url or ""
                    for r in (await session.execute(_CONNECT_URL_SQL, {
                        "cids": sorted(connect_snapshot_cluster_ids | connect_worker_cluster_ids),
                    })).all()
                }
            # cluster_id -> {failed_connectors, failed_tasks, failed_names, task_names} for clusters passing the guard
            connect_snapshot_by_cluster: dict[int, dict] = {}
            connect_snapshot_ignored: dict[int, str] = {}  # cluster_id -> reason
            snapshot_url_cids = set()
            for cid in sorted(connect_snapshot_cluster_ids):
                if cid not in connect_urls:
                    connect_snapshot_ignored[cid] = "cluster not found in kafka_clusters"
                elif not _connect_workers(connect_urls[cid]):
                    connect_snapshot_ignored[cid] = "kafka_connect_url is empty"
                else:
                    snapshot_url_cids.add(cid)
            if snapshot_url_cids:
                snapshot_now = datetime.now(timezone.utc)
                snapshot_rows: dict[int, list] = {}
                for r in (await session.execute(_CONNECT_SNAPSHOT_SQL, {
                    "cids": sorted(snapshot_url_cids), "since": snapshot_now - _CONNECT_SNAPSHOT_WINDOW,
                })).all():
                    snapshot_rows.setdefault(r.cluster_id, []).append(r)
                for cid in sorted(snapshot_url_cids):
                    rows = snapshot_rows.get(cid, [])
                    if not rows:
                        connect_snapshot_ignored[cid] = (
                            f"no connector snapshot rows in the last "
                            f"{int(_CONNECT_SNAPSHOT_WINDOW.total_seconds() // 60)} min"
                        )
                        continue
                    age = snapshot_now - rows[0].collected_at
                    if age > _CONNECT_SNAPSHOT_MAX_AGE:
                        connect_snapshot_ignored[cid] = (
                            f"latest connector snapshot is {int(age.total_seconds() // 60)} min old "
                            f"(max {int(_CONNECT_SNAPSHOT_MAX_AGE.total_seconds() // 60)})"
                        )
                        continue
                    previous_rows = rows[0].previous_rows
                    if previous_rows is not None and len(rows) * 2 < previous_rows:
                        connect_snapshot_ignored[cid] = (
                            f"latest connector snapshot looks partial: {len(rows)} rows "
                            f"vs {previous_rows} in the previous one"
                        )
                        continue
                    failed = sorted(r.connector_name for r in rows if r.state == "FAILED")
                    with_failed_tasks = sorted(
                        (r for r in rows if (r.failed_tasks or 0) > 0),
                        key=lambda r: (-r.failed_tasks, r.connector_name),
                    )
                    connect_snapshot_by_cluster[cid] = {
                        "failed_connectors": len(failed),
                        "failed_tasks": sum(r.failed_tasks for r in with_failed_tasks),
                        "failed_names": failed,
                        "task_names": [f"{r.connector_name} ({r.failed_tasks})" for r in with_failed_tasks],
                    }
                for cfg in configs:
                    if cfg.alert_type in _CONNECT_SNAPSHOT_ALERT_TYPES and cfg.cluster_id in connect_snapshot_by_cluster:
                        snap = connect_snapshot_by_cluster[cfg.cluster_id]
                        values[cfg.id] = (
                            snap["failed_connectors"] if cfg.alert_type == _CONNECT_FAILED_CONNECTORS_ALERT_TYPE
                            else snap["failed_tasks"]
                        )

            sr_nodes_cluster_ids = {
                cfg.cluster_id for cfg in configs
                if cfg.alert_type == _SR_NODES_ALERT_TYPE and cfg.cluster_id is not None
            }
            sr_soft_cluster_ids = {
                cfg.cluster_id for cfg in configs
                if cfg.alert_type == _SR_SOFT_DELETED_ALERT_TYPE and cfg.cluster_id is not None
            }
            sr_urls: dict[int, str] = {}
            if sr_nodes_cluster_ids or sr_soft_cluster_ids:
                sr_urls = {
                    r.id: r.schema_registry_url or ""
                    for r in (await session.execute(_SR_URL_SQL, {
                        "cids": sorted(sr_nodes_cluster_ids | sr_soft_cluster_ids),
                    })).all()
                }
        last_resolved: dict[tuple[int, int, str], datetime] = {
            (r.alert_config_id, r.cluster_id, r.subject): r.last_resolved_at for r in resolved_rows
        }

        # ZooKeeper probes: no DB session open, one connection per distinct node.
        zk_by_cluster: dict[int, tuple[int, str]] = {}  # cluster_id -> (problems, description)
        zk_ignored: dict[int, str] = {}  # cluster_id -> reason
        if zk_cluster_ids:
            zk_nodes_by_cluster: dict[int, list[str]] = {}
            for cid in sorted(zk_cluster_ids):
                nodes = _zk_nodes(zk_urls.get(cid))
                if cid not in zk_urls:
                    zk_ignored[cid] = "cluster not found in kafka_clusters"
                elif not nodes:
                    zk_ignored[cid] = "zookeeper_url is empty"
                elif len(nodes) < 2:
                    zk_ignored[cid] = f"zookeeper_url lists only {nodes[0]}"
                else:
                    zk_nodes_by_cluster[cid] = nodes
            probe_nodes = list(dict.fromkeys(n for nodes in zk_nodes_by_cluster.values() for n in nodes))
            answers = dict(zip(probe_nodes, await asyncio.gather(*(_zk_probe(n) for n in probe_nodes))))
            for cid, nodes in zk_nodes_by_cluster.items():
                zk_by_cluster[cid] = _zk_ensemble_value(nodes, answers)
            for cfg in configs:
                if cfg.alert_type == _ZK_ENSEMBLE_ALERT_TYPE and cfg.cluster_id in zk_by_cluster:
                    values[cfg.id] = zk_by_cluster[cfg.cluster_id][0]

        # Connect worker probes: no DB session open, one GET per distinct worker.
        connect_workers_by_cluster: dict[int, tuple[int, str]] = {}  # cluster_id -> (down, description)
        connect_workers_ignored: dict[int, str] = {}  # cluster_id -> reason
        if connect_worker_cluster_ids:
            workers_by_cluster: dict[int, list[str]] = {}
            for cid in sorted(connect_worker_cluster_ids):
                workers = _connect_workers(connect_urls.get(cid))
                if cid not in connect_urls:
                    connect_workers_ignored[cid] = "cluster not found in kafka_clusters"
                elif not workers:
                    connect_workers_ignored[cid] = "kafka_connect_url is empty"
                else:
                    workers_by_cluster[cid] = workers
            probe_workers = list(dict.fromkeys(w for workers in workers_by_cluster.values() for w in workers))
            if probe_workers:
                probe_limit = asyncio.Semaphore(_CONNECT_PROBE_CONCURRENCY)
                async with httpx.AsyncClient(timeout=_CONNECT_PROBE_TIMEOUT_SECS) as client:
                    worker_up = dict(zip(probe_workers, await asyncio.gather(
                        *(_connect_probe(client, w, probe_limit) for w in probe_workers)
                    )))
                for cid, workers in workers_by_cluster.items():
                    connect_workers_by_cluster[cid] = _connect_workers_value(workers, worker_up)
            for cfg in configs:
                if cfg.alert_type == _CONNECT_WORKERS_ALERT_TYPE and cfg.cluster_id in connect_workers_by_cluster:
                    values[cfg.id] = connect_workers_by_cluster[cfg.cluster_id][0]

        # Schema Registry probes: no DB session open, one GET per distinct node
        # of the clusters with either rule; then the subject lists of the first
        # answering node of each soft-deleted cluster.
        sr_nodes_by_cluster: dict[int, tuple[int, str]] = {}  # cluster_id -> (down, description)
        sr_soft_by_cluster: dict[int, tuple[int, str]] = {}  # cluster_id -> (soft-deleted, description)
        sr_nodes_ignored: dict[int, str] = {}  # cluster_id -> reason
        sr_soft_ignored: dict[int, str] = {}  # cluster_id -> reason
        if sr_nodes_cluster_ids or sr_soft_cluster_ids:
            sr_node_lists: dict[int, list[str]] = {}
            for cid in sorted(sr_nodes_cluster_ids | sr_soft_cluster_ids):
                nodes = _sr_nodes(sr_urls.get(cid))
                reason = None
                if cid not in sr_urls:
                    reason = "cluster not found in kafka_clusters"
                elif not nodes:
                    reason = "schema_registry_url is empty"
                else:
                    sr_node_lists[cid] = nodes
                if reason is not None:
                    if cid in sr_nodes_cluster_ids:
                        sr_nodes_ignored[cid] = reason
                    if cid in sr_soft_cluster_ids:
                        sr_soft_ignored[cid] = reason
            probe_nodes = list(dict.fromkeys(n for nodes in sr_node_lists.values() for n in nodes))
            if probe_nodes:
                probe_limit = asyncio.Semaphore(_SR_PROBE_CONCURRENCY)
                async with httpx.AsyncClient(timeout=_SR_PROBE_TIMEOUT_SECS) as client:
                    node_up = dict(zip(probe_nodes, await asyncio.gather(
                        *(_sr_probe(client, n, probe_limit) for n in probe_nodes)
                    )))
                for cid, nodes in sr_node_lists.items():
                    if cid in sr_nodes_cluster_ids:
                        sr_nodes_by_cluster[cid] = _sr_nodes_value(nodes, node_up)
                soft_nodes: dict[int, str] = {}
                for cid, nodes in sr_node_lists.items():
                    if cid not in sr_soft_cluster_ids:
                        continue
                    first_up = next((n for n in nodes if node_up[n]), None)
                    if first_up is None:
                        sr_soft_ignored[cid] = "no Schema Registry node answered"
                    else:
                        soft_nodes[cid] = first_up
                if soft_nodes:
                    async with httpx.AsyncClient(timeout=_SR_SUBJECTS_TIMEOUT_SECS) as client:
                        soft_results = await asyncio.gather(
                            *(_sr_soft_deleted_value(client, n) for n in soft_nodes.values())
                        )
                    for cid, (value, detail) in zip(soft_nodes, soft_results):
                        if value is None:
                            sr_soft_ignored[cid] = detail
                        else:
                            sr_soft_by_cluster[cid] = (value, detail)
            for cfg in configs:
                if cfg.alert_type == _SR_NODES_ALERT_TYPE and cfg.cluster_id in sr_nodes_by_cluster:
                    values[cfg.id] = sr_nodes_by_cluster[cfg.cluster_id][0]
                elif cfg.alert_type == _SR_SOFT_DELETED_ALERT_TYPE and cfg.cluster_id in sr_soft_by_cluster:
                    values[cfg.id] = sr_soft_by_cluster[cfg.cluster_id][0]
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
        elif config.alert_type == _ZK_ENSEMBLE_ALERT_TYPE:
            if cluster_id not in zk_by_cluster:
                continue  # reason logged from zk_ignored
        elif config.alert_type in _CONNECT_SNAPSHOT_ALERT_TYPES:
            if cluster_id not in connect_snapshot_by_cluster:
                continue  # reason logged from connect_snapshot_ignored
        elif config.alert_type == _CONNECT_WORKERS_ALERT_TYPE:
            if cluster_id not in connect_workers_by_cluster:
                continue  # reason logged from connect_workers_ignored
        elif config.alert_type == _SR_NODES_ALERT_TYPE:
            if cluster_id not in sr_nodes_by_cluster:
                continue  # reason logged from sr_nodes_ignored
        elif config.alert_type == _SR_SOFT_DELETED_ALERT_TYPE:
            if cluster_id not in sr_soft_by_cluster:
                continue  # reason logged from sr_soft_ignored
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
            elif config.alert_type == _ZK_ENSEMBLE_ALERT_TYPE:
                description = zk_by_cluster[cluster_id][1]
            elif config.alert_type == _CONNECT_FAILED_CONNECTORS_ALERT_TYPE:
                description = (
                    f"{value} Connect connector(s) FAILED: "
                    f"{_name_list(connect_snapshot_by_cluster[cluster_id]['failed_names'], _CONNECT_NAMES_MAX)}"
                )
            elif config.alert_type == _CONNECT_FAILED_TASKS_ALERT_TYPE:
                task_names = connect_snapshot_by_cluster[cluster_id]["task_names"]
                description = (
                    f"{value} failed Connect task(s) across {len(task_names)} connector(s): "
                    f"{_name_list(task_names, _CONNECT_NAMES_MAX)}"
                )
            elif config.alert_type == _CONNECT_WORKERS_ALERT_TYPE:
                description = connect_workers_by_cluster[cluster_id][1]
            elif config.alert_type == _SR_NODES_ALERT_TYPE:
                description = sr_nodes_by_cluster[cluster_id][1]
            elif config.alert_type == _SR_SOFT_DELETED_ALERT_TYPE:
                description = sr_soft_by_cluster[cluster_id][1]
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
    if zk_ignored:
        logger.info(
            "evaluate_alerts: ZooKeeper ensemble ignored clusters: %s",
            "; ".join(f"{cid}: {reason}" for cid, reason in sorted(zk_ignored.items())),
        )
    if connect_snapshot_ignored:
        logger.info(
            "evaluate_alerts: Connect connector snapshot ignored clusters: %s",
            "; ".join(f"{cid}: {reason}" for cid, reason in sorted(connect_snapshot_ignored.items())),
        )
    if connect_workers_ignored:
        logger.info(
            "evaluate_alerts: Connect workers ignored clusters: %s",
            "; ".join(f"{cid}: {reason}" for cid, reason in sorted(connect_workers_ignored.items())),
        )
    if sr_nodes_ignored:
        logger.info(
            "evaluate_alerts: Schema Registry nodes ignored clusters: %s",
            "; ".join(f"{cid}: {reason}" for cid, reason in sorted(sr_nodes_ignored.items())),
        )
    if sr_soft_ignored:
        logger.info(
            "evaluate_alerts: Schema Registry soft-deleted subjects ignored clusters: %s",
            "; ".join(f"{cid}: {reason}" for cid, reason in sorted(sr_soft_ignored.items())),
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
    emails: list[dict | None] = []
    for config, cluster_id, subject, tier, description, trigger in to_post:
        try:
            recommended_action = _CLUSTER_METRIC_ACTIONS[config.alert_type]
            card = build_adaptive_card(
                agent_name="Kafka Analyser",
                cluster_name=cluster_names.get(cluster_id, str(cluster_id)),
                anomaly={
                    "severity": tier,
                    "category": config.alert_type,
                    "description": description,
                    "recommended_action": recommended_action,
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
            emails.append(_rule_email(
                config, cluster_names.get(cluster_id, str(cluster_id)),
                "fire" if trigger is not None else "escalate", tier, description, recommended_action,
                subject=subject,
            ))
        except Exception as exc:
            logger.warning(
                "_evaluate_cluster_metrics: Teams post for alert_config_id=%s cluster_id=%s failed: %s",
                config.id, cluster_id, exc,
            )
    _queue_rule_emails(emails)

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
        _queue_rule_emails(_resolve_emails(resolved_items))


# ── Cluster lag rules: cluster.consumer_lag / cluster.connector_lag ─────────
# Level thresholds only (growth is a later phase). One trigger per (rule,
# cluster, name), subject = the consumer group id or the connector name,
# severity = its tier. Per name, in this order: a "custom" row (its own
# tiers) -> "blacklist" (skipped: never fires or resolves) -> the generic
# tiers. Exact names only. Unlike the one-value cluster metrics, each rule
# sends ONE grouped card (and email) per run, when a trigger is created or
# its tier rises, or as a reminder every reminder_hours while anything is
# still above its tier; and one grouped resolve card/email per run. Not in
# CLUSTER_METRIC_TYPES, so the structure-job guard never applies; its own
# guard ignores a cluster with no fresh rows.
_CONSUMER_LAG_ALERT_TYPE = "cluster.consumer_lag"
_CONNECTOR_LAG_ALERT_TYPE = "cluster.connector_lag"
CLUSTER_LAG_TYPES = (_CONSUMER_LAG_ALERT_TYPE, _CONNECTOR_LAG_ALERT_TYPE)
# A kafka_consumer_group_lag row counts only if updated this recently (the
# same 20 minutes the dashboard uses); the latest connector snapshot too.
_LAG_MAX_AGE = timedelta(minutes=20)
_LAG_CARD_MAX_ROWS = 10
_LAG_REMINDER_DEFAULT_HOURS = 6
_LAG_ACTION = "Check the consumers or connectors with the Kafka team."
_LAG_NOUNS = {_CONSUMER_LAG_ALERT_TYPE: "consumer group", _CONNECTOR_LAG_ALERT_TYPE: "connector"}
_LAG_FRESH_SQL = text(
    "SELECT cluster_id, group_id, total_lag FROM kafka_consumer_group_lag "
    "WHERE cluster_id IN :cids AND updated_at >= :since"
).bindparams(bindparam("cids", expanding=True))
# Time of the last grouped card per rule id (in memory: after a restart the
# first run with anything still above its tier sends one reminder).
_lag_last_card_at: dict[int, datetime] = {}


def _lag_rule_parts(config: KafkaAlertConfig) -> tuple[dict, dict[str, dict], set[str]]:
    """(generic tiers, custom name -> tiers, blacklist) from a lag rule's config."""
    cfg = config.config or {}
    generic = {t: cfg[t] for t in _THRESHOLD_TIERS if t in cfg}
    custom = {
        row["name"]: row.get("tiers") or {}
        for row in cfg.get("custom") or [] if isinstance(row, dict) and row.get("name")
    }
    return generic, custom, set(cfg.get("blacklist") or [])


def _plural(n: int, noun: str) -> str:
    return f"{n} {noun}{'' if n == 1 else 's'}"


async def _evaluate_cluster_lag() -> None:
    """Fire, escalate, remind and resolve cluster.consumer_lag /
    cluster.connector_lag rules (see the comment above). Same four-stage
    structure as _evaluate_cluster_metrics: one read session, decide in
    memory, Teams posts with no DB session open, one write session with
    each row in its own savepoint, then grouped resolve cards. Never raises.

    Names: consumer lag uses every kafka_consumer_group_lag row updated in
    the last _LAG_MAX_AGE; connector lag maps each connector of the latest
    snapshot (within the same window, not partial) to its group
    "connect-<name>" (the snapshots do not store consumer.override.group.id)
    and skips a connector with no fresh group row. A cluster with no fresh
    names is ignored (no fire, no resolve). An open trigger whose name is
    blacklisted, has no fresh row or no tiers any more is resolved silently."""
    if SessionLocal is None:
        logger.info("evaluate_alerts: cluster lag skipped: database not available")
        return
    try:
        from routes_settings import _config
        if not _config.get("teams_enabled"):
            logger.info("evaluate_alerts: cluster lag skipped: teams_enabled is off")
            return

        # ---- Single read session: configs, triggers, fresh lag rows, connector snapshots ----
        async with SessionLocal() as session:
            configs = (await session.execute(
                select(KafkaAlertConfig).where(
                    KafkaAlertConfig.alert_type.in_(list(CLUSTER_LAG_TYPES)),
                    KafkaAlertConfig.enabled == True,
                )
            )).scalars().all()
            if not configs:
                logger.info("evaluate_alerts: cluster lag skipped: no enabled lag rules")
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

            since = datetime.now(timezone.utc) - _LAG_MAX_AGE
            cluster_ids = sorted({c.cluster_id for c in configs if c.cluster_id is not None})
            group_lag: dict[int, dict[str, int]] = {}
            if cluster_ids:
                for r in (await session.execute(_LAG_FRESH_SQL, {"cids": cluster_ids, "since": since})).all():
                    group_lag.setdefault(r.cluster_id, {})[r.group_id] = int(r.total_lag or 0)
            connector_cluster_ids = sorted({
                c.cluster_id for c in configs
                if c.alert_type == _CONNECTOR_LAG_ALERT_TYPE and c.cluster_id is not None
            })
            snapshot_rows: dict[int, list] = {}
            if connector_cluster_ids:
                for r in (await session.execute(
                    _CONNECT_SNAPSHOT_SQL, {"cids": connector_cluster_ids, "since": since},
                )).all():
                    snapshot_rows.setdefault(r.cluster_id, []).append(r)
        open_triggers: dict[tuple[int, int, str], tuple[int, str | None]] = {
            (r.alert_config_id, r.cluster_id, r.subject): (r.id, r.severity) for r in open_rows
        }
        last_resolved: dict[tuple[int, int, str], datetime] = {
            (r.alert_config_id, r.cluster_id, r.subject): r.last_resolved_at for r in resolved_rows
        }
    except Exception as exc:
        logger.warning("_evaluate_cluster_lag: setup failed: %s", exc)
        return

    # ---- Names and their lag per (type, cluster); a cluster without any is ignored ----
    window_min = int(_LAG_MAX_AGE.total_seconds() // 60)
    lag_names: dict[tuple[str, int], dict[str, int]] = {}
    lag_ignored: dict[tuple[str, int], str] = {}
    for alert_type, cid in sorted({(c.alert_type, c.cluster_id) for c in configs if c.cluster_id is not None}):
        groups = group_lag.get(cid, {})
        if alert_type == _CONSUMER_LAG_ALERT_TYPE:
            if groups:
                lag_names[(alert_type, cid)] = dict(groups)
            else:
                lag_ignored[(alert_type, cid)] = f"no consumer group lag rows updated in the last {window_min} min"
            continue
        rows = snapshot_rows.get(cid, [])
        if not rows:
            lag_ignored[(alert_type, cid)] = f"no connector snapshot in the last {window_min} min"
            continue
        previous_rows = rows[0].previous_rows
        if previous_rows is not None and len(rows) * 2 < previous_rows:
            lag_ignored[(alert_type, cid)] = (
                f"latest connector snapshot looks partial: {len(rows)} rows vs {previous_rows} in the previous one"
            )
            continue
        names = {
            r.connector_name: groups[f"connect-{r.connector_name}"]
            for r in rows if f"connect-{r.connector_name}" in groups
        }
        if names:
            lag_names[(alert_type, cid)] = names
        else:
            lag_ignored[(alert_type, cid)] = (
                f"no connector has a consumer group row (connect-<name>) updated in the last {window_min} min"
            )

    # ---- Decide, entirely in memory, no DB session open ----
    now = datetime.now(timezone.utc)
    to_insert: list[KafkaAlertTrigger] = []
    to_update: list[tuple[int, str | None, str]] = []  # (trigger id, new severity or None, metric_value)
    to_resolve: dict[int, KafkaAlertConfig] = {}  # trigger id -> rule: grouped resolve card/email
    to_resolve_silently: list[int] = []
    # (config, kind, severity, summary, offenders, new triggers) -- one grouped card per rule
    to_post: list[tuple[KafkaAlertConfig, str, str, str, list[dict], list[KafkaAlertTrigger]]] = []
    names_evaluated = 0

    for config in configs:
        cluster_id = config.cluster_id
        if cluster_id is None:
            logger.warning("_evaluate_cluster_lag: alert_config_id=%s has no cluster_id; skipped", config.id)
            continue
        names = lag_names.get((config.alert_type, cluster_id))
        if names is None:
            continue  # reason logged from lag_ignored
        try:
            noun = _LAG_NOUNS[config.alert_type]
            generic, custom, blacklist = _lag_rule_parts(config)
            rule_open = {
                subject: value for (cfg_id, cid, subject), value in open_triggers.items()
                if cfg_id == config.id and cid == cluster_id
            }
            offenders: list[dict] = []
            new_triggers: list[KafkaAlertTrigger] = []
            escalations = 0
            evaluated: set[str] = set()
            for name, lag in names.items():
                if name in custom:
                    tiers, source = custom[name], "custom"
                elif name in blacklist:
                    continue
                elif generic:
                    tiers, source = generic, "generic"
                else:
                    continue
                evaluated.add(name)
                names_evaluated += 1
                key = (config.id, cluster_id, name)
                tier = _threshold_tier(tiers, lag)
                open_trigger = rule_open.get(name)
                if tier is None:
                    _threshold_breach_counts.pop(key, None)
                    if open_trigger is not None:
                        to_resolve[open_trigger[0]] = config
                    continue

                metric_value = str(lag)
                description = f"{noun.capitalize()} {name} lag is {lag:,} ({tier}, {source} threshold {float(tiers[tier]):g})"
                if open_trigger is not None:
                    trigger_id, open_severity = open_trigger
                    old_tier = open_severity or config.severity
                    old_rank = _TIER_RANK.get(old_tier, -1)
                    if _TIER_RANK[tier] > old_rank:
                        escalations += 1
                        to_update.append((trigger_id, tier, metric_value))
                    elif _TIER_RANK[tier] < old_rank:
                        to_update.append((trigger_id, tier, metric_value))
                    else:
                        to_update.append((trigger_id, None, metric_value))
                    offenders.append({"name": name, "lag": lag, "tier": tier, "source": source})
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
                if is_recurrence:
                    description += " -- RECURRING: this same issue resolved recently and has now fired again"
                trigger = KafkaAlertTrigger(
                    alert_config_id=config.id,
                    cluster_id=cluster_id,
                    subject=name,
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
                new_triggers.append(trigger)
                offenders.append({"name": name, "lag": lag, "tier": tier, "source": source})

            # Open but no longer evaluated (blacklisted, no fresh row, no tiers): silent.
            for name, (trigger_id, _severity) in rule_open.items():
                if name not in evaluated:
                    to_resolve_silently.append(trigger_id)

            if not offenders:
                continue
            reminder_hours = (config.config or {}).get("reminder_hours") or _LAG_REMINDER_DEFAULT_HOURS
            last_card_at = _lag_last_card_at.get(config.id)
            reminder_due = last_card_at is None or now - last_card_at >= timedelta(hours=reminder_hours)
            if not new_triggers and not escalations and not reminder_due:
                continue
            offenders.sort(key=lambda o: (-o["lag"], o["name"]))
            severity = max((o["tier"] for o in offenders), key=lambda t: _TIER_RANK[t])
            kind = "fire" if new_triggers else "escalate" if escalations else "reminder"
            parts = [f"{len(new_triggers)} new"] if new_triggers else []
            if escalations:
                parts.append(f"{escalations} escalated")
            summary = f"{_plural(len(offenders), noun)} above their lag tiers" + (
                f" ({', '.join(parts)})" if parts else f" (reminder: still above after {reminder_hours} h)"
            )
            to_post.append((config, kind, severity, summary, offenders, new_triggers))
        except Exception as exc:
            logger.warning(
                "_evaluate_cluster_lag: alert_config_id=%s cluster_id=%s failed: %s",
                config.id, cluster_id, exc,
            )

    for (alert_type, cid), reason in sorted(lag_ignored.items()):
        logger.info("evaluate_alerts: %s ignored cluster %s: %s", alert_type, cid, reason)
    logger.info(
        "evaluate_alerts: cluster lag rules=%d names=%d fired=%d cards=%d resolved=%d resolved_silently=%d",
        len(configs), names_evaluated, len(to_insert), len(to_post), len(to_resolve), len(to_resolve_silently),
    )

    # ---- Grouped Teams cards, no DB session open ----
    cluster_names: dict[int, str] = {}
    if to_post or to_resolve:
        try:
            cluster_names = await _get_cluster_names()
        except Exception as exc:
            logger.warning("_evaluate_cluster_lag: cluster name lookup failed: %s", exc)
    emails: list[dict | None] = []
    for config, kind, severity, summary, offenders, new_triggers in to_post:
        try:
            cluster_name = cluster_names.get(config.cluster_id, str(config.cluster_id))
            from shared.escalation.notifier import build_lag_card
            card = build_lag_card(
                "Kafka Analyser", cluster_name, config.alert_type, severity, summary, offenders,
                _LAG_ACTION, max_rows=_LAG_CARD_MAX_ROWS,
            )
            webhook_url = config.webhook_url or _config.get("teams_webhook_url", "")
            if webhook_url:
                success = await send_to_teams(webhook_url=webhook_url, card=card)
                error_detail = None
            else:
                success = False
                error_detail = "No webhook URL configured (per-alert or agent-level)"
            for trigger in new_triggers:
                trigger.teams_post_success = success
                trigger.error_detail = error_detail
            if not success and not new_triggers:
                logger.warning(
                    "_evaluate_cluster_lag: %s card not sent for alert_config_id=%s: %s",
                    kind, config.id, error_detail or "Teams post failed",
                )
            # The reminder clock restarts on every grouped notice, sent or not,
            # so a failing webhook does not turn reminders into every run.
            _lag_last_card_at[config.id] = now
            emails.append(_rule_email(
                config, cluster_name, kind, severity, summary, _LAG_ACTION, offenders=offenders,
            ))
        except Exception as exc:
            logger.warning("_evaluate_cluster_lag: card for alert_config_id=%s failed: %s", config.id, exc)
    _queue_rule_emails(emails)

    if not to_insert and not to_update and not to_resolve and not to_resolve_silently:
        return

    # ---- Single write session: each row in its own savepoint ----
    # Grouped resolve cards are sent after this session has closed, from plain
    # snapshots taken before each commit (a later rollback expires ORM objects).
    cleared: dict[int, list[dict]] = {}  # rule id -> cleared groups
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
                        "_evaluate_cluster_lag: failed to record trigger for alert_config_id=%s "
                        "cluster_id=%s subject=%s (card was already sent): %s",
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
                    logger.warning("_evaluate_cluster_lag: failed to update trigger id=%s: %s", trigger_id, exc)
            for trigger_id in [*to_resolve, *to_resolve_silently]:
                try:
                    snapshot = None
                    async with session.begin_nested():
                        trigger = await session.get(KafkaAlertTrigger, trigger_id)
                        if trigger is not None and trigger.resolved_at is None:
                            trigger.resolved_at = now
                            open_minutes = (
                                (now - trigger.triggered_at).total_seconds() / 60
                                if trigger.triggered_at is not None else None
                            )
                            snapshot = {
                                "name": trigger.subject,
                                "was": trigger.severity,
                                "open_minutes": open_minutes,
                                "teams_post_success": trigger.teams_post_success,
                            }
                    await session.commit()
                    if snapshot is not None and trigger_id in to_resolve:
                        config = to_resolve[trigger_id]
                        snapshot["was"] = snapshot["was"] or config.severity
                        cleared.setdefault(config.id, []).append(snapshot)
                except Exception as exc:
                    await session.rollback()
                    logger.warning("_evaluate_cluster_lag: failed to resolve trigger id=%s: %s", trigger_id, exc)
    except Exception as exc:
        logger.warning("_evaluate_cluster_lag: write session failed entirely: %s", exc)

    if cleared:
        await _send_lag_resolves(
            {c.id: c for c in to_resolve.values()}, cleared, cluster_names,
            _config.get("teams_webhook_url", ""),
        )


async def _send_lag_resolves(
    configs: dict[int, KafkaAlertConfig],
    cleared: dict[int, list[dict]],
    cluster_names: dict[int, str],
    default_webhook_url: str,
) -> None:
    """One grouped resolve card per rule (only the groups whose opening card
    reached Teams, and only if the rule's send_resolve_card allows it; capped
    at _RESOLVE_CARDS_TIMEOUT_SECS in total) and one grouped resolve email
    per rule listing every group cleared. Never raises."""
    emails: list[dict | None] = []
    cards: list[tuple[str, dict]] = []
    for rule_id, items in cleared.items():
        try:
            config = configs[rule_id]
            items = sorted(items, key=lambda i: str(i["name"]))
            noun = _LAG_NOUNS.get(config.alert_type, "group")
            cluster_name = cluster_names.get(config.cluster_id, str(config.cluster_id))
            was = max((i["was"] for i in items if i["was"] in _TIER_RANK), key=lambda t: _TIER_RANK[t], default=config.severity)
            emails.append(_rule_email(
                config, cluster_name, "resolve", was,
                f"{_plural(len(items), noun)} back below their lag tiers.",
                cleared=[{k: i[k] for k in ("name", "was", "open_minutes")} for i in items],
            ))
            card_items = [i for i in items if i["teams_post_success"] is True]
            webhook_url = config.webhook_url or default_webhook_url
            if card_items and webhook_url and (config.config or {}).get("send_resolve_card", True):
                from shared.escalation.notifier import build_lag_card
                cards.append((webhook_url, build_lag_card(
                    "Kafka Analyser", cluster_name, config.alert_type, was,
                    f"{_plural(len(card_items), noun)} back below their lag tiers.",
                    [{"name": i["name"], "was": i["was"]} for i in card_items],
                    resolved=True, max_rows=_LAG_CARD_MAX_ROWS,
                )))
        except Exception as exc:
            logger.warning("_send_lag_resolves: rule %s failed: %s", rule_id, exc)

    async def _post_all() -> None:
        for webhook_url, card in cards:
            try:
                await send_to_teams(webhook_url=webhook_url, card=card)
            except Exception as exc:
                logger.warning("_send_lag_resolves: card failed: %s", exc)
    try:
        await asyncio.wait_for(_post_all(), timeout=_RESOLVE_CARDS_TIMEOUT_SECS)
    except asyncio.TimeoutError:
        logger.warning("_send_lag_resolves: stopped after %s s cap", _RESOLVE_CARDS_TIMEOUT_SECS)
    except Exception as exc:
        logger.warning("_send_lag_resolves failed: %s", exc)
    _queue_rule_emails(emails)
