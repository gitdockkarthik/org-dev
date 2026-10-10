"""Every shadow-mode hook records exactly one kafka_notification_outbox row per
notice, and the inline sends are unchanged when recording fails or is off.

The real evaluators and owner-notice functions run against a SCRATCH Postgres
(DATABASE_URL; it must already have the live schema plus the 0058 tables)
with a fake SMTP server and a fake Teams webhook on 127.0.0.1. Only data
sources outside the database are patched: cluster names, the enabled-cluster
list, the broker TCP check and the health-summary gather. The scenario runs
three times -- recording on, recording failing (outbox._insert raises), mode
off -- and the messages the fakes received and the trigger rows written must
be identical across the three runs.

Run inside the agent image, never against a real database (it truncates
kafka_alert_configs, kafka_alert_triggers and the outbox tables):
  docker run --rm --network <scratch net> -v $PWD/agents/kafka-analyser:/app:ro \
    -v $PWD/shared:/opt/shared:ro -e PYTHONPATH=/app:/opt -e DATABASE_URL=<scratch> \
    -w /app org-dev-kafka-analyser python -B tests/test_outbox_hooks.py
"""
import asyncio
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

assert "scratch" in os.environ.get("OUTBOX_TEST_CONFIRM", ""), \
    "set OUTBOX_TEST_CONFIRM=scratch to confirm DATABASE_URL is a throwaway database"

from sqlalchemy import text  # noqa: E402

import collectors  # noqa: E402
import outbox  # noqa: E402
import routes_dashboard  # noqa: E402
import teams_alerts  # noqa: E402
from _fakes import FakeSMTP, FakeTeams, free_port  # noqa: E402
from database import SessionLocal, engine  # noqa: E402
from routes_settings import _config  # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s — %(message)s")
PASSED = []
SMTP, TEAMS = FakeSMTP(), FakeTeams()
CLUSTER = 1


def ok(name):
    PASSED.append(name)
    print(f"PASS {name}")


# ── Patched data sources outside the database ───────────────────────────────

REACHABLE = {"value": False}


async def _cluster_names():
    return {CLUSTER: "c1", 2: "c2"}


async def _enabled_clusters():
    return [{"id": CLUSTER, "name": "c1", "enabled": True}]


async def _reachability(cluster_id):
    up = REACHABLE["value"]
    return {"cluster_name": "c1", "total": 1, "reachable_count": 1 if up else 0,
            "brokers": [{"host": "b1", "port": 9092, "reachable": up}]}


async def _gather():
    return (datetime.now(timezone.utc), "alive",
            [{"name": "c1", "data_age_minutes": 1.0, "success_count": 5, "failed_count": 0}])


teams_alerts._get_cluster_names = _cluster_names
collectors._get_enabled_clusters = _enabled_clusters
routes_dashboard.get_broker_reachability = _reachability
collectors._gather_health_summary = _gather


# ── Helpers ──────────────────────────────────────────────────────────────────

def configure(mode):
    _config.update({
        "notification_outbox_mode": mode,
        "teams_enabled": True,
        "teams_webhook_url": TEAMS.url,
        "email_enabled": True,
        "smtp_host": "127.0.0.1",
        "smtp_port": SMTP.port,
        "smtp_username": "",
        "smtp_password": "",
        "smtp_from_address": "alerts@sintecmedia.com",
        "email_recipients": "ops@operative.com",
        "health_email_recipients": "owner@operative.com",
    })


async def sql(statement, **params):
    async with SessionLocal() as s:
        result = await s.execute(text(statement), params)
        await s.commit()
        return result


async def reset_db():
    await sql("TRUNCATE kafka_notification_outbox, kafka_notification_attempts, kafka_alert_triggers, "
              "kafka_alert_configs, kafka_broker_metrics, kafka_topic_metrics, kafka_consumer_group_lag, "
              "kafka_job_runs RESTART IDENTITY CASCADE")
    rules = [
        # name, alert_type, cluster_id, severity, config, email_enabled
        ("cw", "close_wait_spike", None, "warning", {"threshold": 1}, False),
        ("reach", "broker_unreachable", None, "critical", {}, True),
        ("cpu", "broker.cpu_pct", CLUSTER, "warning", {"mode": "simple", "warning": 50, "critical": 90}, True),
        ("urp", "cluster.urp_total", CLUSTER, "warning", {"mode": "simple", "warning": 1}, True),
        ("lag", "cluster.consumer_lag", CLUSTER, "warning", {"warning": 100}, True),
    ]
    for name, alert_type, cid, sev, cfg, email_on in rules:
        await sql("INSERT INTO kafka_alert_configs (name, alert_type, cluster_id, severity, config, cooldown_minutes, "
                  "enabled, email_enabled, created_at, updated_at) VALUES (:n, :t, :c, :s, CAST(:cfg AS jsonb), 0, "
                  "true, :e, now(), now())", n=name, t=alert_type, c=cid, s=sev, cfg=json.dumps(cfg), e=email_on)
    await sql("INSERT INTO kafka_job_runs (job_id, triggered_by, status, logs, created_at, started_at, ended_at) "
              "VALUES ('kafka-topic-structure-1', 'test', 'success', '', now(), now(), now())")
    await sql("INSERT INTO kafka_topic_metrics (time, cluster_id, topic, partition_count, replication_factor, "
              "messages_in_per_sec, bytes_in_per_sec, bytes_out_per_sec, total_messages, size_bytes, retention_bytes, "
              "retention_pct, urp_count) VALUES (now(), 1, 't1', 3, 3, 0, 0, 0, 0, 0, 0, 0, 2)")
    await set_cpu(60)
    await sql("INSERT INTO kafka_consumer_group_lag (cluster_id, group_id, total_lag, updated_at) "
              "VALUES (1, 'g1', 5000, now())")
    teams_alerts._broker_fail_counts.clear()
    teams_alerts._threshold_breach_counts.clear()
    teams_alerts._lag_last_card_at.clear()


async def set_cpu(value):
    await sql("DELETE FROM kafka_broker_metrics")
    await sql("INSERT INTO kafka_broker_metrics (time, cluster_id, broker_id, heap_pct, gc_pause_ms, "
              "request_handler_idle_pct, urp_count, messages_in_per_sec, cpu_pct, disk_pct) "
              "VALUES (now(), 1, '1', 10, 0, 90, 0, 0, :v, 10)", v=value)


async def settle():
    """Wait for queued rule emails and background records."""
    for _ in range(3):
        if teams_alerts._rule_email_tasks:
            await asyncio.wait(list(teams_alerts._rule_email_tasks), timeout=30)
        await outbox.drain(30)
        await asyncio.sleep(0.05)


async def outbox_rows():
    return (await sql("SELECT id, kind, source, routing, origin, state, email_status, teams_status, "
                      "email_last_error, teams_last_error, alert_config_id, severity FROM kafka_notification_outbox "
                      "ORDER BY id")).all()


def transcript_mark(t):
    return (len(TEAMS.cards), len(SMTP.messages))


def _norm(value) -> str:
    """Card or email as comparable text: digits, times and ports removed."""
    return re.sub(r"\d", "#", value)


def transcript_since(mark):
    cards = [_norm(json.dumps(c, sort_keys=True)) for c in TEAMS.cards[mark[0]:]]
    mails = [(tuple(r), _norm(m["Subject"])) for r, m in SMTP.messages[mark[1]:]]
    return cards, mails


# ── The scenario: one step per hook ──────────────────────────────────────────
# Each step: (name, coroutine factory, expected new rows as (kind, source, state, email, teams)).

def steps():
    async def unconfigured_summary():
        saved = dict(_config)
        _config.update({"health_email_recipients": "", "teams_enabled": False})
        try:
            await collectors._send_health_summary(9)
        finally:
            _config.update(saved)

    async def lag_resolve():
        await sql("UPDATE kafka_consumer_group_lag SET total_lag = 0, updated_at = now()")
        await teams_alerts._evaluate_cluster_lag()

    async def cpu_up():
        await set_cpu(95)
        await teams_alerts._evaluate_broker_thresholds()

    async def cpu_down():
        await set_cpu(5)
        await teams_alerts._evaluate_broker_thresholds()

    async def reach_up():
        REACHABLE["value"] = True
        await teams_alerts._evaluate_broker_reachability()
        REACHABLE["value"] = False

    async def no_webhook_fire():
        saved = _config["teams_webhook_url"]
        _config["teams_webhook_url"] = ""
        try:
            await teams_alerts.fire_close_wait_alerts({2: 3})
        finally:
            _config["teams_webhook_url"] = saved

    async def watchdog_smtp_down():
        SMTP.down()
        try:
            await collectors._post_watchdog_notice("critical", "Process count 25 exceeded the safety threshold of 20")
        finally:
            SMTP.up()

    async def twice(fn):
        await fn()
        await settle()
        await fn()

    return [
        ("health summary (email sent)", lambda: collectors._send_health_summary(9),
         [("owner_summary", "_send_health_summary", "delivered", "sent", "skipped")]),
        ("health summary, nothing configured", unconfigured_summary,
         [("owner_summary", "_send_health_summary", "failed", "skipped", "skipped")]),
        ("watchdog notice, SMTP down -> Teams fallback", watchdog_smtp_down,
         [("owner_watchdog", "check_process_count_watchdog", "degraded", "failed", "sent")]),
        ("close-wait fire (rule email off)", lambda: teams_alerts.fire_close_wait_alerts({CLUSTER: 3}),
         [("rule_fire", "fire_close_wait_alerts", "delivered", "skipped", "sent")]),
        ("close-wait fire, no webhook configured", no_webhook_fire,
         [("rule_fire", "fire_close_wait_alerts", "failed", "skipped", "skipped")]),
        ("close-wait resolve (_send_resolve_cards)", lambda: teams_alerts.resolve_cleared_close_wait_triggers({CLUSTER: 0, 2: 0}),
         [("rule_resolve", "_send_resolve_cards", "delivered", "skipped", "sent"),
          ("rule_resolve", "_send_resolve_cards", "suppressed", "skipped", "skipped")]),
        ("broker reachability fire (2 checks)", lambda: twice(teams_alerts._evaluate_broker_reachability),
         [("rule_fire", "_evaluate_broker_reachability", "delivered", "sent", "sent")]),
        ("broker reachability resolve", reach_up,
         [("rule_resolve", "_send_resolve_cards", "delivered", "sent", "sent")]),
        ("broker threshold fire (2 checks)", lambda: twice(teams_alerts._evaluate_broker_thresholds),
         [("rule_fire", "_evaluate_broker_thresholds", "delivered", "sent", "sent")]),
        ("broker threshold escalate", cpu_up,
         [("rule_escalate", "_evaluate_broker_thresholds", "delivered", "sent", "sent")]),
        ("broker threshold resolve", cpu_down,
         [("rule_resolve", "_send_resolve_cards", "delivered", "sent", "sent")]),
        ("cluster metric fire (2 checks)", lambda: twice(teams_alerts._evaluate_cluster_metrics),
         [("rule_fire", "_evaluate_cluster_metrics", "delivered", "sent", "sent")]),
        ("lag grouped fire (2 checks)", lambda: twice(teams_alerts._evaluate_cluster_lag),
         [("rule_fire", "_evaluate_cluster_lag", "delivered", "sent", "sent")]),
        ("lag grouped resolve (_send_lag_resolves)", lag_resolve,
         [("rule_resolve", "_send_lag_resolves", "delivered", "sent", "sent")]),
    ]


async def run_scenario(mode, label, break_recording=False):
    configure(mode)
    await reset_db()
    original_insert = outbox._insert
    if break_recording:
        async def _broken(*args, **kwargs):
            raise RuntimeError("recording broken on purpose")
        outbox._insert = _broken
    per_step = []
    try:
        for name, factory, expected in steps():
            before = len(await outbox_rows())
            mark = transcript_mark(None)
            await factory()
            await settle()
            rows = (await outbox_rows())[before:]
            per_step.append((name, transcript_since(mark)))
            if mode == "off" or break_recording:
                assert rows == [], (label, name, rows)
                continue
            got = [(r.kind, r.source, r.state, r.email_status, r.teams_status) for r in rows]
            assert sorted(got) == sorted(expected), f"{name}: expected {expected}, got {got}"
            assert all(r.origin == "inline" for r in rows)
            ok(f"[{label}] {name}: {len(rows)} row(s) {got}")
    finally:
        outbox._insert = original_insert
    triggers = (await sql("SELECT alert_config_id, cluster_id, subject, severity, metric_value, teams_post_success, "
                          "error_detail, resolved_at IS NULL AS open FROM kafka_alert_triggers ORDER BY id")).all()
    return per_step, [tuple(t) for t in triggers]


async def main():
    shadow_steps, shadow_triggers = await run_scenario("shadow", "shadow")

    rows = await outbox_rows()
    reasons = {(r.source, r.email_last_error, r.teams_last_error) for r in rows}
    assert ("_send_health_summary", "health_email_recipients is blank (Teams-only)",
            "not configured: teams_enabled off or no teams_webhook_url") in reasons, reasons
    assert ("fire_close_wait_alerts", "rule email off (not queued)",
            "no webhook URL configured (per-alert or agent-level)") in reasons, reasons
    watchdog = [r for r in rows if r.kind == "owner_watchdog"][0]
    assert watchdog.email_last_error in ("connection refused", "connection error (ConnectionRefusedError)"), watchdog
    attempts = (await sql("SELECT o.kind, a.channel, a.attempt_no, a.outcome, a.duration_ms IS NOT NULL AS timed "
                          "FROM kafka_notification_attempts a JOIN kafka_notification_outbox o ON o.id = a.outbox_id "
                          "WHERE o.kind = 'owner_watchdog' ORDER BY a.channel")).all()
    assert [tuple(a) for a in attempts] == [("owner_watchdog", "email", 1, "failed_transient", True),
                                            ("owner_watchdog", "teams", 1, "sent", True)], attempts
    dump = repr((await sql("SELECT row_to_json(o)::text FROM kafka_notification_outbox o")).all()) + \
        repr((await sql("SELECT row_to_json(a)::text FROM kafka_notification_attempts a")).all())
    assert "secret-signature" not in dump and TEAMS.url not in dump, "webhook URL leaked into the outbox"
    ok("skip reasons recorded (Teams-only owner, nothing configured, no webhook, rule email off); no webhook URL stored")

    broken_steps, broken_triggers = await run_scenario("shadow", "recording fails", break_recording=True)
    off_steps, off_triggers = await run_scenario("off", "mode off")
    for (name, a), (_n2, b), (_n3, c) in zip(shadow_steps, broken_steps, off_steps):
        assert a == b == c, f"{name}: inline sends differ\nshadow={a}\nbroken={b}\noff={c}"
    assert shadow_triggers == broken_triggers == off_triggers, (shadow_triggers, broken_triggers, off_triggers)
    total_cards = sum(len(a[0]) for _n, a in shadow_steps)
    total_mails = sum(len(a[1]) for _n, a in shadow_steps)
    ok(f"inline sends identical with recording on / failing / off: {total_cards} Teams cards, "
       f"{total_mails} emails, {len(shadow_triggers)} trigger rows, per step and in order")
    await engine.dispose()
    print(f"\n{len(PASSED)} checks passed")


if __name__ == "__main__":
    asyncio.run(main())
