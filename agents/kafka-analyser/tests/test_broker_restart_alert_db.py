"""cluster.broker_restarted: the real teams_alerts._evaluate_cluster_metrics against a SCRATCH Postgres
(the live schema plus Alembic 0059) with real rows in kafka_broker_restarts, kafka_broker_metrics,
kafka_alert_configs and kafka_alert_triggers: fire on the 2nd check, escalate, window boundary,
the window setting, resolve when events age out, the stale guard, and a missing kafka_broker_restarts
table (renamed) not breaking the URP rule on the same cluster. Only the Teams post, the rule email
queue and the cluster-name lookup are replaced. Plain asserts, no pytest needed.

Run inside the agent image, never against a real database (it truncates kafka_alert_configs,
kafka_alert_triggers, kafka_broker_metrics, kafka_broker_restarts, kafka_topic_metrics and kafka_job_runs):
  docker run --rm --network <scratch net> -v $PWD/agents/kafka-analyser:/app:ro \\
    -v $PWD/shared:/opt/shared:ro -e PYTHONPATH=/app:/opt -e DATABASE_URL=<scratch> \\
    -e OUTBOX_TEST_CONFIRM=scratch -w /app org-dev-kafka-analyser python -B tests/test_broker_restart_alert_db.py
"""
import asyncio
import json
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

assert "scratch" in os.environ.get("OUTBOX_TEST_CONFIRM", ""), \
    "set OUTBOX_TEST_CONFIRM=scratch to confirm DATABASE_URL is a throwaway database"

from sqlalchemy import text  # noqa: E402

import teams_alerts as ta  # noqa: E402
from database import SessionLocal, engine  # noqa: E402
from routes_settings import _config  # noqa: E402

CID = 1
PASSED = []
CARDS, LOGS = [], []


class Cap(logging.Handler):
    def emit(self, record):
        LOGS.append((record.levelno, record.getMessage()))


logging.getLogger("teams_alerts").addHandler(Cap())
logging.getLogger("teams_alerts").setLevel(logging.INFO)


def check(name, cond, detail=""):
    assert cond, f"FAIL {name} {detail}"
    PASSED.append(name)
    print(f"PASS {name}")


async def sql(statement, **params):
    async with SessionLocal() as s:
        result = await s.execute(text(statement), params)
        await s.commit()
        return result


async def _names():
    return {CID: "scratch-1"}


async def _post(webhook_url, card):
    CARDS.append(card)
    return True, ta.outbox.sent()


ta._get_cluster_names = _names
ta._post_card = _post
ta._queue_rule_emails = lambda emails: None
ta.outbox.enabled = lambda: False
_config.update(teams_enabled=True, teams_webhook_url="https://hook.invalid/x", email_enabled=False)

TIERS = {"mode": "simple", "info": 1, "warning": 3, "critical": 5}


async def reset(window=None):
    await sql("TRUNCATE kafka_alert_triggers, kafka_alert_configs, kafka_broker_metrics, kafka_broker_restarts, "
              "kafka_topic_metrics, kafka_job_runs RESTART IDENTITY CASCADE")
    _config.pop("broker_restart_window_minutes", None)
    if window is not None:
        _config["broker_restart_window_minutes"] = window
    for name, t, cfg in (("restarts", "cluster.broker_restarted", TIERS), ("urp", "cluster.urp_total", {"mode": "simple", "warning": 1})):
        await sql("INSERT INTO kafka_alert_configs (name, alert_type, cluster_id, severity, config, cooldown_minutes, enabled, "
                  "email_enabled, created_at, updated_at) VALUES (:n, :t, :c, 'warning', CAST(:cfg AS jsonb), 0, true, false, now(), now())",
                  n=name, t=t, c=CID, cfg=json.dumps(cfg))
    await healthy()
    await sql("INSERT INTO kafka_job_runs (job_id, triggered_by, status, logs, created_at, started_at, ended_at) "
              "VALUES ('kafka-topic-structure-1', 'test', 'success', '', now(), now(), now())")
    await sql("INSERT INTO kafka_topic_metrics (time, cluster_id, topic, partition_count, replication_factor, messages_in_per_sec, "
              "bytes_in_per_sec, bytes_out_per_sec, total_messages, size_bytes, retention_bytes, retention_pct, urp_count) "
              "VALUES (now(), 1, 't1', 3, 3, 0, 0, 0, 0, 0, 0, 0, 2)")
    ta._threshold_breach_counts.clear()


async def healthy(minutes_old=1):
    await sql("DELETE FROM kafka_broker_metrics")
    for b in ("1", "2", "3"):
        await sql("INSERT INTO kafka_broker_metrics (time, cluster_id, broker_id, heap_pct, gc_pause_ms, request_handler_idle_pct, "
                  "urp_count, messages_in_per_sec, cpu_pct, disk_pct) VALUES (now() - make_interval(mins => :m), 1, :b, 10, 0, 90, 0, 0, 10, 10)",
                  m=minutes_old, b=b)


async def restart(broker, minutes_ago, uptime_minutes=13 * 1440 + 125):
    await sql("INSERT INTO kafka_broker_restarts (cluster_id, broker_id, started_at, previous_started_at, detected_at) "
              "VALUES (1, :b, now() - make_interval(mins => :m) - interval '45 seconds', "
              "now() - make_interval(mins => :m) - interval '45 seconds' - make_interval(mins => :u), now() - make_interval(mins => :m))",
              b=broker, m=minutes_ago, u=uptime_minutes)


async def run(label=""):
    CARDS.clear()
    LOGS.clear()
    await ta._evaluate_cluster_metrics()


async def trigger(config_name="restarts"):
    row = (await sql("SELECT t.severity, t.metric_value, t.message_sent, t.subject, t.resolved_at FROM kafka_alert_triggers t "
                     "JOIN kafka_alert_configs c ON c.id = t.alert_config_id WHERE c.name = :n ORDER BY t.id DESC LIMIT 1", n=config_name)).first()
    return row


def cards_for(kind):
    return [c for c in CARDS if kind in json.dumps(c)]


async def main():
    # below the lowest tier, then at it: fires on the 2nd check
    await reset()
    await restart(2, 45)
    await run(); await run()
    check("an event outside the window: no trigger", await trigger() is None)
    await restart(2, 10)
    await run()
    check("count at the tier: no trigger after 1 check", await trigger() is None)
    await run()
    t = await trigger()
    check("fires on the 2nd check: info, value 1, subject cluster, description names broker 2 after 13 d 2 h",
          t is not None and t.severity == "info" and t.metric_value == "1" and t.subject == "cluster"
          and "Broker 2 restarted at" in t.message_sent and " UTC after 13 d 2 h. 1 broker restart in the last 30 min (info, threshold 1)" in t.message_sent, t)
    check("one card with the action text", len(CARDS) == 1 and "why the broker restarted" in json.dumps(CARDS[0]), json.dumps(CARDS)[:300])
    await run()
    check("no repeat card while open", not CARDS)

    # escalate 1 -> 3 -> 5
    await restart(3, 6); await restart(1, 3)
    await run()
    t = await trigger()
    check("escalates to warning with the latest restart (broker 1)", t.severity == "warning" and t.metric_value == "3" and len(CARDS) == 1
          and "Broker 1 restarted at" in json.dumps(CARDS[0]), t)
    await restart(2, 2); await restart(3, 1)
    await run()
    check("escalates to critical, value 5", (await trigger()).severity == "critical" and (await trigger()).metric_value == "5" and len(CARDS) == 1)

    # events age out of the window: resolve
    await sql("UPDATE kafka_broker_restarts SET detected_at = now() - interval '31 minutes'")
    await run()
    t = await trigger()
    check("resolves when the events age out, one resolve card", t.resolved_at is not None and len(CARDS) == 1 and "RESOLVE" in json.dumps(CARDS[0]).upper(), t)

    # window boundary (real timestamptz comparison) and the setting
    await reset(); await restart(2, 29)
    await run(); await run()
    check("29 min old counts with N=30", (await trigger()) is not None and (await trigger()).metric_value == "1")
    await reset(); await restart(2, 31)
    await run(); await run()
    check("31 min old does not count with N=30", await trigger() is None)
    await reset(window=60); await restart(2, 31)
    await run(); await run()
    t = await trigger()
    check("setting 60: the 31 min old event counts and the text says 60 min", t is not None and "in the last 60 min" in t.message_sent, t)
    _config["broker_restart_window_minutes"] = 15
    await run()
    check("window lowered to 15 at evaluation time: the event ages out, resolves", (await trigger()).resolved_at is not None)

    # stale guard: every broker row older than 6 minutes -> skipped, open trigger untouched
    await reset(); await restart(2, 5)
    await run(); await run()
    check("fired before the guard test", (await trigger()) is not None)
    await healthy(minutes_old=10)
    await sql("DELETE FROM kafka_broker_restarts")
    await run()
    t = await trigger()
    check("stale broker metrics: not resolved although there are no restarts, INFO names the cluster",
          t.resolved_at is None and any("broker restarts ignored clusters" in m and "no broker metrics in the last 6 min" in m for _, m in LOGS), LOGS)

    # a missing kafka_broker_restarts must skip only this type; URP on the same cluster still fires
    await reset(); await restart(2, 5)
    await run(); await run()
    check("both rules fired on real rows", (await trigger()) is not None and (await trigger("urp")) is not None)
    await sql("UPDATE kafka_alert_triggers SET resolved_at = now() WHERE alert_config_id = (SELECT id FROM kafka_alert_configs WHERE name = 'urp')")
    await sql("ALTER TABLE kafka_broker_restarts RENAME TO kafka_broker_restarts_x")
    try:
        ta._threshold_breach_counts.clear()
        await run(); await run()
        urp = await trigger("urp")
        t = await trigger()
        check("missing table: URP re-fired on its 2nd check (other types still evaluated)", urp is not None and urp.resolved_at is None
              and any(c for c in CARDS), (urp, CARDS))
        check("missing table: restart trigger neither resolved nor changed, WARNING names the type",
              t.resolved_at is None and t.severity == "info"
              and any(lv == logging.WARNING and "broker restart read failed" in m and "cluster.broker_restarted" in m for lv, m in LOGS)
              and not any("setup failed" in m for _, m in LOGS), LOGS)
    finally:
        await sql("ALTER TABLE kafka_broker_restarts_x RENAME TO kafka_broker_restarts")
    await sql("DELETE FROM kafka_broker_restarts")
    await run()
    check("table back, no restarts: the restart trigger resolves", (await trigger()).resolved_at is not None)

    await engine.dispose()
    print(f"\n{len(PASSED)} checks passed")


asyncio.run(main())
