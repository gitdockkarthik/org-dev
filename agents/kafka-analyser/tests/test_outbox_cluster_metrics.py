"""Cluster metrics through the notification outbox (step 2, chunk C):
teams_alerts._evaluate_cluster_metrics (here cluster.urp_total, subject
"cluster") with the setting
rule_outbox_evaluators off (inline, as before) and on (enqueued, then sent by
the real worker), against a SCRATCH Postgres with the agent's tables, the
fake SMTP server and fake Teams webhook of tests/_delivery_fakes.py. Plain
asserts, no pytest needed.

Run inside the agent image, never against a real database (it truncates
kafka_notification_outbox, kafka_notification_attempts, kafka_alert_configs,
kafka_alert_triggers, kafka_topic_metrics and kafka_job_runs):
  docker run --rm --network <scratch net> -v $PWD/agents/kafka-analyser:/app:ro \\
    -v $PWD/shared:/opt/shared:ro -e PYTHONPATH=/app:/opt -e DATABASE_URL=<scratch> \\
    -e OUTBOX_TEST_CONFIRM=scratch -w /app org-dev-kafka-analyser python -B tests/test_outbox_cluster_metrics.py
"""
import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

assert "scratch" in os.environ.get("OUTBOX_TEST_CONFIRM", ""), \
    "set OUTBOX_TEST_CONFIRM=scratch to confirm DATABASE_URL is a throwaway database"

from sqlalchemy import text  # noqa: E402

import outbox_worker as W  # noqa: E402
import teams_alerts as ta  # noqa: E402
from _delivery_fakes import ScriptedSMTP, ScriptedTeams  # noqa: E402
from database import SessionLocal, engine  # noqa: E402
from routes_settings import _config  # noqa: E402

PASSED = []
SMTP, TEAMS = ScriptedSMTP(), ScriptedTeams()
KICKS = []


class Capture(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


LOGS = Capture()
logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s - %(message)s")
logging.getLogger().addHandler(LOGS)


def ok(name):
    PASSED.append(name)
    print(f"PASS {name}", flush=True)


async def _names():
    return {4: "prod-4"}


ta._get_cluster_names = _names
_real_kick = W.kick
W.kick = lambda: KICKS.append(1)


async def sql(statement, **params):
    async with SessionLocal() as s:
        result = await s.execute(text(statement), params)
        await s.commit()
        return result


async def reset(switch_on=True):
    await W.drain(30)
    if ta._rule_email_tasks:
        await asyncio.gather(*list(ta._rule_email_tasks))
    await sql("TRUNCATE kafka_notification_outbox, kafka_notification_attempts, kafka_alert_configs, "
              "kafka_alert_triggers, kafka_topic_metrics, kafka_job_runs RESTART IDENTITY CASCADE")
    SMTP.reset()
    TEAMS.reset()
    KICKS.clear()
    LOGS.lines.clear()
    ta._threshold_breach_counts.clear()
    _config.update({
        "notification_outbox_mode": "shadow", "teams_enabled": True, "teams_webhook_url": TEAMS.url(),
        "email_enabled": True, "smtp_host": "127.0.0.1", "smtp_port": SMTP.port, "smtp_username": "",
        "smtp_password": "", "smtp_from_address": "alerts@sintecmedia.com", "email_recipients": "ops@operative.com",
        "rule_teams_fallback": True, "rule_outbox_evaluators": ["cluster_metrics"] if switch_on else [],
    })


async def add_rule(rule_id, teams=True, email=True):
    await sql("INSERT INTO kafka_alert_configs (id, name, alert_type, cluster_id, severity, config, cooldown_minutes, "
              "enabled, email_enabled, created_at, updated_at) VALUES (:id, :n, 'cluster.urp_total', 4, 'warning', "
              "CAST(:cfg AS jsonb), 0, :t, :e, now(), now())", id=rule_id, n=f"URP rule {rule_id}",
              cfg='{"mode": "simple", "warning": 10, "critical": 100}', t=teams, e=email)


async def urp(value):
    """Cluster 4's under-replicated partitions, with a fresh topic-structure run (the evaluator's guard)."""
    await sql("INSERT INTO kafka_job_runs (job_id, triggered_by, status, started_at, ended_at, logs, created_at) "
              "VALUES ('kafka-topic-structure-4', 'test', 'success', now(), now(), '', now())")
    updated = (await sql("UPDATE kafka_topic_metrics SET urp_count = :u, time = now()", u=value)).rowcount
    if not updated:
        await sql("INSERT INTO kafka_topic_metrics (time, cluster_id, topic, partition_count, replication_factor, "
                  "messages_in_per_sec, bytes_in_per_sec, bytes_out_per_sec, total_messages, size_bytes, "
                  "retention_bytes, retention_pct, urp_count) VALUES (now(), 4, 'orders', 6, 3, 1, 1, 1, 1, 1, 1, 1, :u)",
                  u=value)


async def evaluate(n=1):
    for _ in range(n):
        await ta._evaluate_cluster_metrics()
    if ta._rule_email_tasks:
        await asyncio.gather(*list(ta._rule_email_tasks))


async def deliver():
    await W.run_once()
    await W.drain(30)


async def rows(kind=None):
    q = "SELECT * FROM kafka_notification_outbox WHERE origin = 'outbox'" + (" AND kind = :k" if kind else "") + " ORDER BY id"
    return [dict(r) for r in (await sql(q, k=kind)).mappings().all()]


async def triggers():
    return {r["alert_config_id"]: dict(r) for r in (await sql(
        "SELECT id, alert_config_id, severity, resolved_at, teams_post_success, error_detail "
        "FROM kafka_alert_triggers ORDER BY id")).mappings().all()}


async def inline_records():
    return (await sql("SELECT count(*) FROM kafka_notification_outbox WHERE origin = 'inline'")).scalar()


# ── Tests ────────────────────────────────────────────────────────────────────

async def test_switch_off_inline():
    await reset(switch_on=False)
    await add_rule(1)
    await urp(150)
    await evaluate(2)
    await asyncio.sleep(0.3)  # the background shadow record
    t = (await triggers())[1]
    assert len(TEAMS.posts) == 1 and len(SMTP.messages) == 1, (TEAMS.posts, SMTP.messages)
    assert (t["teams_post_success"], t["error_detail"]) == (True, None), t
    assert await rows() == [] and await inline_records() == 1 and KICKS == []
    ok("switch off: the inline path as before (card and email sent inline, trigger posted, one shadow record, "
       "no outbox row, no kick)")


async def test_switch_on_fire_and_deliver():
    await reset()
    for rule_id, teams, email in ((1, True, True), (2, True, False), (3, False, True), (4, False, False)):
        await add_rule(rule_id, teams, email)
    await urp(25)
    await evaluate()
    assert await rows() == [] and KICKS == []
    await evaluate()
    fired, trig = await rows("rule_fire"), await triggers()
    assert len(fired) == 3 and 4 not in trig, (fired, trig)
    by_rule = {r["alert_config_id"]: r for r in fired}
    assert [(by_rule[i]["email_status"], by_rule[i]["teams_status"]) for i in (1, 2, 3)] == [
        ("pending", "pending"), ("skipped", "pending"), ("pending", "standby")], by_rule
    for i, (teams, email) in ((1, (True, True)), (2, (True, False)), (3, (False, True))):
        r = by_rule[i]
        assert (r["routing"], r["severity"], r["trigger_ids"], r["dedupe_key"], r["payload"]["subject"]) == (
            "rule", "warning", [trig[i]["id"]], f"rule_fire:{trig[i]['id']}", "cluster"), r
        assert r["payload"]["policy"] == {"rule_id": i, "teams_toggle": teams, "email_toggle": email}
        assert (trig[i]["teams_post_success"], trig[i]["error_detail"]) == (False, f"queued (outbox row {r['id']})")
    assert KICKS == [1] and TEAMS.posts == [] and SMTP.messages == [] and await inline_records() == 0
    ok("switch on, fire: one rule_fire row per rule (subject 'cluster') with routing rule, its policy and trigger_ids [trigger]; "
       "Teams+Email both pending, Teams only email skipped, Email only standby; both off not evaluated; triggers "
       "marked queued; kick() once after the commit; nothing posted, emailed or shadow-recorded inline")

    await evaluate()
    assert len(await rows()) == 3 and KICKS == [1]
    ok("switch on: the same condition evaluated again enqueues nothing more")

    await deliver()
    trig = await triggers()
    assert len(TEAMS.posts) == 2, TEAMS.posts
    assert len(SMTP.messages) == 2 and all(m[0] == ["ops@operative.com"] for m in SMTP.messages)
    assert [(trig[i]["teams_post_success"], trig[i]["error_detail"]) for i in (1, 2, 3)] == [
        (True, None), (True, None), (False, "Teams off for this rule")], trig
    assert all(r["state"] == "delivered" for r in await rows()), await rows()
    ok("the worker delivers them: cards for the Teams rules, emails for the Email rules; teams_post_success "
       "written back (Email-only rule: 'Teams off for this rule'); all delivered")
    return trig


async def test_switch_on_escalate_and_resolve():
    await urp(150)
    await evaluate()
    esc, trig = await rows("rule_escalate"), await triggers()
    assert len(esc) == 3 and all(trig[i]["severity"] == "critical" for i in (1, 2, 3)), (esc, trig)
    assert all(r["dedupe_key"].startswith(f"rule_escalate:{r['trigger_ids'][0]}:critical:") and r["severity"] == "critical"
               and r["priority"] == 1 for r in esc), esc
    assert trig[1]["error_detail"] == f"queued (outbox row {[r for r in esc if r['alert_config_id'] == 1][0]['id']})"
    assert KICKS == [1, 1] and len(TEAMS.posts) == 2 and len(SMTP.messages) == 2
    await deliver()
    assert len(TEAMS.posts) == 4 and len(SMTP.messages) == 4
    ok("switch on, escalate: one rule_escalate row per open trigger (critical, priority 1), trigger severity "
       "updated and marked queued in the same savepoint; delivered by the worker")

    await urp(0)
    await evaluate()
    res, trig = await rows("rule_resolve"), await triggers()
    assert len(res) == 3 and all(trig[i]["resolved_at"] is not None for i in (1, 2, 3)), (res, trig)
    assert all(r["dedupe_key"] == f"rule_resolve:{r['trigger_ids'][0]}" for r in res)
    assert len(TEAMS.posts) == 4 and len(SMTP.messages) == 4  # nothing inline
    await deliver()
    by_rule = {r["alert_config_id"]: r for r in await rows("rule_resolve")}
    assert [(by_rule[i]["email_status"], by_rule[i]["teams_status"]) for i in (1, 2, 3)] == [
        ("sent", "sent"), ("skipped", "sent"), ("sent", "skipped")], by_rule
    assert len(TEAMS.posts) == 6 and len(SMTP.messages) == 6
    resolve_cards = [p for p in TEAMS.posts[4:]]
    assert all("Open for" in str(p[1]) for p in resolve_cards)
    ok("switch on, resolve: one rule_resolve row per trigger in its resolve savepoint, no inline resolve card or "
       "email; the worker sends the resolve cards (fire cards reached Teams) and the resolve emails")


async def test_enqueue_failure_rolls_back_one():
    await reset()
    for rule_id in (1, 2, 3):
        await add_rule(rule_id)
    real = ta.enqueue_rule_notice

    async def failing(session, config, *a, **kw):
        if config.id == 2:
            raise RuntimeError("simulated enqueue failure")
        return await real(session, config, *a, **kw)
    ta.enqueue_rule_notice = failing
    try:
        await urp(25)
        await evaluate(2)
    finally:
        ta.enqueue_rule_notice = real
    trig, fired = await triggers(), await rows("rule_fire")
    assert sorted(trig) == [1, 3] and sorted(r["alert_config_id"] for r in fired) == [1, 3], (trig, fired)
    assert any("_evaluate_cluster_metrics: trigger and notice not recorded for alert_config_id=2" in l
               for l in LOGS.lines), LOGS.lines
    assert KICKS == [1]
    await evaluate()
    assert sorted(await triggers()) == [1, 2, 3] and len(await rows("rule_fire")) == 3
    ok("a failing enqueue rolls back that trigger's savepoint only (no trigger, no row, a warning); the other "
       "rules are recorded; the next run records the missing one")


async def test_mixed_inline_fire_outbox_resolve():
    await reset(switch_on=False)
    await add_rule(1)
    await add_rule(2)
    await urp(25)
    await evaluate(2)
    await asyncio.sleep(0.3)
    await sql("UPDATE kafka_alert_triggers SET teams_post_success = false, error_detail = 'HTTP 500' "
              "WHERE alert_config_id = 2")  # rule 2's inline card did not reach Teams
    posts_before, mails_before = len(TEAMS.posts), len(SMTP.messages)
    _config["rule_outbox_evaluators"] = ["cluster_metrics"]
    await urp(0)
    await evaluate()
    res = {r["alert_config_id"]: r for r in await rows("rule_resolve")}
    assert len(res) == 2 and len(TEAMS.posts) == posts_before and len(SMTP.messages) == mails_before
    await deliver()
    res = {r["alert_config_id"]: r for r in await rows("rule_resolve")}
    assert (res[1]["email_status"], res[1]["teams_status"]) == ("sent", "sent"), res[1]
    assert (res[2]["email_status"], res[2]["teams_status"], res[2]["teams_last_error"]) == (
        "sent", "skipped", "opening card never reached Teams"), res[2]
    assert len(TEAMS.posts) == posts_before + 1 and len(SMTP.messages) == mails_before + 2
    ok("mixed: triggers fired inline before the switch and resolved after it -- the resolve rows have no parent "
       "row; the card follows the trigger's own teams_post_success (sent for rule 1, skipped for rule 2 whose "
       "inline card failed); both resolve emails sent")


async def main():
    await test_switch_off_inline()
    await test_switch_on_fire_and_deliver()
    await test_switch_on_escalate_and_resolve()
    await test_enqueue_failure_rolls_back_one()
    await test_mixed_inline_fire_outbox_resolve()
    await reset(switch_on=False)
    W.kick = _real_kick
    await engine.dispose()
    print(f"\nALL {len(PASSED)} CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
