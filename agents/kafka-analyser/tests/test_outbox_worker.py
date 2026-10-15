"""Failure-injection tests for the outbox delivery worker (outbox_worker.py,
step 2a) against a SCRATCH Postgres with the 0058 tables, a scriptable fake
SMTP server and a fake Teams webhook on 127.0.0.1 (tests/_delivery_fakes.py).
Rows are enqueued directly with outbox_worker.enqueue; no live notice and no
evaluator is involved. Plain asserts, no pytest needed.

Run inside the agent image, never against a real database (it truncates
kafka_notification_outbox, kafka_notification_attempts, kafka_alert_configs,
kafka_alert_triggers, kafka_job_runs and kafka_job_schedules). Rule rows with a
policy (enqueue_in) cover the rule fallback, the dependency gate, the send-time
checks and the trigger write-back:
  docker run --rm --network <scratch net> -v $PWD/agents/kafka-analyser:/app:ro \
    -v $PWD/shared:/opt/shared:ro -e PYTHONPATH=/app:/opt -e DATABASE_URL=<scratch> \
    -e OUTBOX_TEST_CONFIRM=scratch -w /app org-dev-kafka-analyser python -B tests/test_outbox_worker.py
Takes about two minutes (the slow-SMTP and timeout cases run in real time).
"""
import asyncio
import json
import logging
import os
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

assert "scratch" in os.environ.get("OUTBOX_TEST_CONFIRM", ""), \
    "set OUTBOX_TEST_CONFIRM=scratch to confirm DATABASE_URL is a throwaway database"

from sqlalchemy import text  # noqa: E402

import outbox  # noqa: E402
import outbox_worker as W  # noqa: E402
from _delivery_fakes import ScriptedSMTP, ScriptedTeams, free_port  # noqa: E402
from database import SessionLocal, engine  # noqa: E402
from routes_settings import _config  # noqa: E402

PASSED = []
PASSWORD = "pw-Secret-9876"
SMTP, TEAMS = ScriptedSMTP(), ScriptedTeams()
LABEL = W.RESEND_LABEL


class Capture(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


LOGS = Capture()
logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s - %(message)s")
logging.getLogger().addHandler(LOGS)
logging.getLogger("outbox_worker").setLevel(logging.INFO)


def ok(name):
    PASSED.append(name)
    print(f"PASS {name}", flush=True)


def configure(**over):
    _config.update({
        "notification_outbox_mode": "shadow",
        "teams_enabled": True,
        "teams_webhook_url": TEAMS.url(),
        "email_enabled": True,
        "smtp_host": "127.0.0.1",
        "smtp_port": SMTP.port,
        "smtp_username": "relay-user",
        "smtp_password": PASSWORD,
        "smtp_from_address": "alerts@sintecmedia.com",
        "email_recipients": "ops@operative.com, outsider@example.com",
        "health_email_recipients": "owner@operative.com",
        "rule_teams_fallback": True,
        **over,
    })


async def sql(statement, **params):
    async with SessionLocal() as s:
        result = await s.execute(text(statement), params)
        await s.commit()
        return result


async def assert_no_secrets_stored():
    dump = (await sql("SELECT coalesce(string_agg(o::text, ' '), '') FROM kafka_notification_outbox o")).scalar()
    dump += (await sql("SELECT coalesce(string_agg(a::text, ' '), '') FROM kafka_notification_attempts a")).scalar()
    for secret in (PASSWORD, "secret-signature", "owner@operative.com", "ops@operative.com", "outsider@"):
        assert secret not in dump, secret


async def reset(**over):
    await W.drain(60)
    await assert_no_secrets_stored()  # checked on every test's rows before they are truncated
    await sql("TRUNCATE kafka_notification_outbox, kafka_notification_attempts, kafka_alert_configs, "
              "kafka_alert_triggers, kafka_job_runs, kafka_job_schedules RESTART IDENTITY CASCADE")
    if SMTP._server is None:
        SMTP.up()
    if TEAMS._server is None:
        TEAMS.up()
    SMTP.reset()
    TEAMS.reset()
    configure(**over)


async def row(row_id):
    return dict((await sql("SELECT *, extract(epoch FROM now()) AS db_now FROM kafka_notification_outbox "
                           "WHERE id = :id", id=row_id)).mappings().one())


async def attempts(row_id):
    return [dict(r) for r in (await sql(
        "SELECT * FROM kafka_notification_attempts WHERE outbox_id = :id ORDER BY id", id=row_id)).mappings().all()]


def due_in(r, channel):
    """Seconds from DB now to the channel's next attempt."""
    from datetime import datetime
    due = datetime.fromisoformat(r["payload"]["worker"]["due"][channel])
    return due.timestamp() - float(r["db_now"])


async def force_due():
    await sql("UPDATE kafka_notification_outbox SET next_attempt_at = now() - interval '1 second', "
              "payload = CASE WHEN payload ? 'worker' THEN jsonb_set(payload, '{worker,due}', '{}'::jsonb) "
              "ELSE payload END WHERE finished_at IS NULL")


def card(n):
    return {"type": "message", "attachments": [{
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {"type": "AdaptiveCard", "version": "1.4",
                    "body": [{"type": "TextBlock", "text": f"card {n}"}]}}]}


def mail(n):
    return {"subject": f"notice {n}", "text": f"text {n}", "html": f"<p>html {n}</p>"}


async def enq_owner(n, kind="owner_watchdog", **kw):
    return await W.enqueue(kind, "owner", f"t:{n}", email=mail(n), teams_card=card(n), severity="critical",
                           source="test", kick_after=False, **kw)


async def enq_rule(n, kind="rule_fire", severity="warning", email=True, teams=True, ref="agent_default"):
    return await W.enqueue(kind, "rule", f"t:{n}", email=mail(n) if email else None,
                           teams_card=card(n) if teams else None, teams_ref=ref, severity=severity,
                           source="test", kick_after=False)


def first_texts():
    return [t[0] for t in TEAMS.texts()]


# ── Pure rules ───────────────────────────────────────────────────────────────

def test_rules():
    half = lambda: 0.5  # noqa: E731 -- no jitter
    assert W.backoff_secs(1, "failed_transient", rand=half) == 30
    assert W.backoff_secs(2, "timeout", rand=half) == 60
    assert W.backoff_secs(5, "failed_transient", rand=half) == 480
    assert W.backoff_secs(6, "failed_transient", rand=half) == 900  # 960 capped
    assert W.backoff_secs(1, "failed_config", rand=half) == 900
    assert W.backoff_secs(1, "failed_transient", rand=lambda: 0) == 24
    assert abs(W.backoff_secs(1, "failed_transient", rand=lambda: 1) - 36) < 1e-9
    assert W.backoff_secs(1, "failed_transient", retry_after=120, rand=half) == 120
    assert W.backoff_secs(1, "failed_transient", retry_after=99999, rand=half) == 3600
    ok("backoff: 30 s doubling to a 15 min cap, +/-20% jitter, config errors at the cap, Retry-After honoured")
    assert [W.http_outcome(s) for s in (200, 202)] == ["sent", "sent"]
    assert [W.http_outcome(s) for s in (429, 500, 503)] == ["failed_transient"] * 3
    assert [W.http_outcome(s) for s in (400, 401, 403, 404)] == ["failed_permanent"] * 4
    assert W.parse_retry_after("120") == 120 and W.parse_retry_after(None) is None
    ok("Teams: 200/202 sent, 429/5xx transient, 400/401/403/404 permanent; Retry-After parsed")
    labelled = W.label_card(card(1))
    assert labelled["attachments"][0]["content"]["body"][0]["text"] == LABEL
    assert card(1)["attachments"][0]["content"]["body"][0]["text"] == "card 1"
    assert W.label_subject("s") == f"s {LABEL}"
    assert LABEL == "(re-sent after an unconfirmed delivery, may be a duplicate)"
    ok("resend label: first line of the card, suffix of the subject; the stored card is untouched")


# ── Scenarios ────────────────────────────────────────────────────────────────

async def test_shadow_finds_nothing():
    await reset()
    rid = await outbox.record_inline(
        None, "owner_summary", "owner", "inline:x", None, None, None, None, "t",
        {"email": outbox.failed("connection refused"), "teams": outbox.sent(5)}, source="test")
    before = await row(rid)
    stats = await W.run_once()
    after = await row(rid)
    assert stats == {"recovered": 0, "claimed": 0}, stats
    assert before["updated_at"] == after["updated_at"] and after["state"] == "degraded"
    assert SMTP.messages == [] and TEAMS.posts == []
    ok("shadow mode: inline rows are finished on insert; a run claims nothing and sends nothing")


async def test_smtp_down():
    await reset(smtp_port=free_port())
    rid = await enq_owner(1)
    await W.run_once()
    r, a = await row(rid), await attempts(rid)
    assert (r["email_status"], r["teams_status"], r["state"]) == ("pending", "sent", "degraded"), r
    assert r["finished_at"] is None and r["email_last_error"] == "connection refused"
    assert [(x["channel"], x["outcome"], x["phase_reached"]) for x in a] == [
        ("email", "failed_transient", "connect"), ("teams", "sent", "done")], a
    assert 24 <= due_in(r, "email") <= 36 and len(TEAMS.posts) == 1
    ok("SMTP down (owner): email fails at connect, the Teams fallback goes out in the same run; degraded")

    await force_due()
    await W.run_once()
    r = await row(rid)
    assert r["email_attempts"] == 2 and 48 <= due_in(r, "email") <= 72, (r["email_attempts"], due_in(r, "email"))
    assert len(TEAMS.posts) == 1 and r["state"] == "degraded"
    ok("SMTP down: email retries on backoff (30 s, then 60 s, jittered); Teams is not sent again")

    await sql("UPDATE kafka_notification_outbox SET expires_at = now() + interval '20 seconds' WHERE id = :id", id=rid)
    await force_due()
    await W.run_once()
    r = await row(rid)
    assert r["email_status"] == "abandoned" and r["finished_at"] is not None and r["state"] == "degraded", r
    assert r["email_last_error"] == "3 attempts, last error: connection refused", r["email_last_error"]
    assert len(TEAMS.posts) == 1 and SMTP.messages == []
    ok("SMTP down: at max age the email is abandoned with '3 attempts, last error ...'; row finished, degraded")


async def test_teams_down():
    await reset()
    rid = await enq_rule(1, email=False)
    TEAMS.script = [(500, {})]
    await W.run_once()
    r = await row(rid)
    assert (r["teams_status"], r["teams_last_error"], r["state"]) == ("pending", "HTTP 500", "pending"), r
    assert 24 <= due_in(r, "teams") <= 36
    TEAMS.script = [(429, {"Retry-After": "120"})]
    await force_due()
    await W.run_once()
    r = await row(rid)
    assert r["teams_last_error"] == "HTTP 429" and 118 <= due_in(r, "teams") <= 121, due_in(r, "teams")
    TEAMS.script = [(404, {})]
    await force_due()
    await W.run_once()
    r, a = await row(rid), await attempts(rid)
    assert (r["teams_status"], r["state"], r["teams_attempts"]) == ("failed", "failed", 3), r
    assert r["finished_at"] is not None
    assert [(x["outcome"], (x["detail"] or {}).get("http_status")) for x in a] == [
        ("failed_transient", 500), ("failed_transient", 429), ("failed_permanent", 404)], a
    ok("Teams down: 500 retried on backoff, 429 waits its Retry-After (120 s), 404 fails permanently")

    rid2 = await enq_rule(2, email=False)
    TEAMS.script = [(400, {})]
    await W.run_once()
    r = await row(rid2)
    assert (r["teams_status"], r["teams_attempts"], r["state"]) == ("failed", 1, "failed"), r
    ok("Teams 400 (content error) fails at once, no retry")


async def test_both_down_then_recover():
    await reset(smtp_port=free_port())
    TEAMS.down()
    ids = {
        "resolve": await enq_rule(1, kind="rule_resolve"),
        "summary": await enq_owner(2, kind="owner_summary"),
        "watchdog": await enq_owner(3),
        "fire_crit": await enq_rule(4, severity="critical"),
        "fire_warn": await enq_rule(5),
    }
    await W.run_once()
    for name, rid in ids.items():
        r = await row(rid)
        assert r["email_status"] == "pending" and r["teams_status"] == "pending", (name, r)
        assert r["finished_at"] is None and r["state"] == "pending", (name, r["state"])
    configure()
    TEAMS.up()
    await force_due()
    await W.run_once()
    assert SMTP.subjects() == ["notice 3", "notice 4", "notice 5", "notice 1", "notice 2"], SMTP.subjects()
    assert first_texts() == ["card 4", "card 5", "card 1"], first_texts()
    for name, rid in ids.items():
        r = await row(rid)
        assert r["state"] == "delivered" and r["finished_at"] is not None, (name, r["state"])
        if name in ("summary", "watchdog"):
            assert r["teams_status"] == "superseded", r["teams_status"]
    ok("both down then recovering: delivered in priority order (watchdog, critical, warning, resolve, summary); "
       "owner Teams fallbacks dropped once the email went out")


async def test_slow_smtp():
    await reset()
    SMTP.delay = {"DATA_END": 8}
    rid = await enq_owner(1)
    t0 = time.monotonic()
    await W.run_once()
    r, a = await row(rid), await attempts(rid)
    assert time.monotonic() - t0 >= 8 and r["state"] == "delivered", r
    assert (r["email_status"], r["teams_status"]) == ("sent", "skipped") and TEAMS.posts == []
    assert a[0]["duration_ms"] >= 8000 and a[0]["outcome"] == "sent", a
    ok("slow SMTP, 8 s: sent within the 30 s cap; no Teams fallback")

    await reset()
    SMTP.delay = {"banner": 8, "EHLO": 8, "MAIL": 8, "RCPT": 8, "DATA_END": 8}  # each step < 10 s, 40 s in all
    rid = await enq_owner(2)
    t0 = time.monotonic()
    await W.run_once()
    took = time.monotonic() - t0
    r = await row(rid)
    assert 30 <= took < 39, took
    assert (r["email_status"], r["teams_status"], r["state"]) == ("sending", "sent", "degraded"), r
    assert len(TEAMS.posts) == 1 and SMTP.messages == []
    await W.drain(30)
    r, a = await row(rid), await attempts(rid)
    assert (r["email_status"], r["teams_status"], r["state"]) == ("sent", "sent", "delivered"), r
    assert r["claimed_by"] is None and r["finished_at"] is not None
    email_attempt = [x for x in a if x["channel"] == "email"][0]
    assert email_attempt["outcome"] == "sent" and email_attempt["duration_ms"] >= 39000, email_attempt
    assert email_attempt["detail"]["late_result"] is True and email_attempt["detail"]["cap_fired_after_s"] == 30
    await force_due()
    await W.run_once()
    assert len(SMTP.messages) == 1 and len(TEAMS.posts) == 1
    ok("slow SMTP, 40 s: the 30 s cap fires, Teams fallback once, the late 'sent' is recorded by the "
       "done-callback; one email, one card")


async def test_restart_mid_send():
    await reset()
    SMTP.delay = {"DATA_END": 3}
    rid = await enq_rule(1, teams=False)
    old_boot = outbox.BOOT_ID
    task = asyncio.create_task(W.run_once())
    while not SMTP.data_started.is_set():
        await asyncio.sleep(0.02)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    outbox.BOOT_ID = uuid.uuid4().hex  # the agent restarted
    try:
        stats = await W.run_once()
        await W.drain(30)  # the old boot's late result arrives after the new boot took over
    finally:
        new_boot, outbox.BOOT_ID = outbox.BOOT_ID, old_boot
    r, a = await row(rid), await attempts(rid)
    assert stats["recovered"] == 1, stats
    assert len(SMTP.messages) == 2 and len(set(SMTP.message_ids())) == 1, SMTP.message_ids()
    assert SMTP.subjects() == ["notice 1", f"notice 1 {LABEL}"], SMTP.subjects()
    assert (r["email_status"], r["email_attempts"], r["state"]) == ("sent", 2, "delivered"), r
    assert [(x["boot_id"], x["outcome"]) for x in a] == [(old_boot, "sent"), (new_boot, "sent")], a
    assert a[0]["detail"]["late_result"] is True and a[0]["detail"]["resend_after_unknown"] is True
    assert a[1]["detail"]["resend_after_unknown"] is True
    ok("restart mid-send: the new boot marks the attempt unknown and resends once, labelled, same "
       "Message-ID (2 deliveries); the old boot's late result is kept on its attempt and changes nothing")

    await reset()
    SMTP.drop_at_data_end = True  # connection lost after the body: outcome unknown every time
    rid = await enq_rule(2, teams=False)
    await W.run_once()
    await W.run_once()
    r = await row(rid)
    assert len(SMTP.messages) == 2 and SMTP.subjects()[1].endswith(LABEL), SMTP.subjects()
    assert r["email_status"] == "abandoned" and r["state"] == "failed", r
    assert r["email_last_error"].startswith("2 attempts, last error: outcome unknown twice"), r["email_last_error"]
    ok("unknown twice: resent once, then abandoned; the stub counts at most two deliveries")


async def test_duplicates():
    await reset()
    SMTP.delay = {"DATA_END": 0.02}
    ids = [await enq_rule(n) for n in range(50)]

    async def kicked():
        W.kick()
        W.kick()
        await asyncio.sleep(0.01)
        await W.drain(120)

    await asyncio.gather(W.deliver_due_notifications(), W.run_once(), kicked(), W.deliver_due_notifications(),
                         W.run_once())
    await W.drain(60)
    assert len(SMTP.messages) == 50 and len(set(SMTP.message_ids())) == 50, len(SMTP.messages)
    assert sorted(SMTP.subjects()) == sorted(f"notice {n}" for n in range(50))
    assert sorted(first_texts()) == sorted(f"card {n}" for n in range(50))
    counts = (await sql("SELECT channel, outcome, count(*) FROM kafka_notification_attempts "
                        "GROUP BY 1, 2 ORDER BY 1, 2")).all()
    assert [tuple(c) for c in counts] == [("email", "sent", 50), ("teams", "sent", 50)], counts
    states = (await sql("SELECT state, count(*) FROM kafka_notification_outbox GROUP BY 1")).all()
    assert [tuple(s) for s in states] == [("delivered", 50)], states
    assert all(SMTP.messages[i][0] == ["ops@operative.com"] for i in range(50))  # outsider dropped
    ok("duplicates: two cron runs, two manual runs and two kicks over 50 rows -> exactly one email and one "
       "card per row; recipients limited to the allowed domains")

    import jobs
    await reset()
    SMTP.delay = {"DATA_END": 8}
    rid = await enq_owner(99)
    jobs.register_job(W.JOB_ID, "Notification Delivery", "test", W.deliver_due_notifications, default_timeout_secs=5)
    run = await jobs._create_run(W.JOB_ID, "test")
    await jobs._execute_job(jobs._jobs[W.JOB_ID], run)
    job_run = (await sql("SELECT status, logs FROM kafka_job_runs WHERE id = :id", id=run.id)).one()
    assert job_run.status == "success" and job_run.logs.startswith("Retry succeeded"), job_run
    await W.drain(30)
    await force_due()
    await W.run_once()
    r = await row(rid)
    assert len(SMTP.messages) == 1 and TEAMS.posts == [], (len(SMTP.messages), TEAMS.posts)
    assert (r["email_status"], r["teams_status"], r["email_attempts"], r["state"]) == ("sent", "skipped", 1, "delivered")
    ok("job wrapper: a 5 s timeout during an 8 s send forces the wrapper's retry; the retry finds the live "
       "claim and sends nothing; the late result records the one email; no Teams card")


async def test_crash_after_claim():
    await reset()
    rid = await enq_rule(1)
    claimed = await W._claim(10)  # the worker dies right after its claim commit
    assert len(claimed) == 1
    stats = await W.run_once()
    assert stats == {"recovered": 0, "claimed": 0} and SMTP.messages == [] and TEAMS.posts == [], stats
    ok("crash after claim: a channel in 'sending' with a live lease is never claimed again")

    await sql("UPDATE kafka_notification_outbox SET claim_expires_at = now() - interval '1 second' WHERE id = :id",
              id=rid)
    stats = await W.run_once()
    r, a = await row(rid), await attempts(rid)
    assert stats["recovered"] == 2 and r["state"] == "delivered", (stats, r)
    assert SMTP.subjects() == [f"notice 1 {LABEL}"] and TEAMS.texts() == [[LABEL, "card 1"]], TEAMS.texts()
    assert [(x["channel"], x["outcome"]) for x in a] == [
        ("email", "unknown"), ("teams", "unknown"), ("email", "sent"), ("teams", "sent")], a
    ok("crash after claim: after lease expiry both channels are recovered as unknown and resent once, labelled")


async def test_config_healing():
    await reset(smtp_host="nonexistent-host.invalid")
    rid = await enq_rule(1, teams=False)
    await W.run_once()
    r, a = await row(rid), await attempts(rid)
    assert (r["email_status"], r["email_last_error"]) == ("pending", "could not resolve SMTP host"), r
    assert a[0]["outcome"] == "failed_config" and 720 <= due_in(r, "email") <= 1080, (a, due_in(r, "email"))
    configure()  # the host is fixed in Settings
    await force_due()
    await W.run_once()
    r = await row(rid)
    assert (r["email_status"], r["email_attempts"], r["state"]) == ("sent", 2, "delivered") and len(SMTP.messages) == 1
    ok("config healing: a wrong host retries at the 15 min cap and the next attempt uses the corrected setting")


async def test_not_configured():
    await reset(smtp_host="")
    rid = await enq_owner(1)
    await W.run_once()
    r, a = await row(rid), await attempts(rid)
    assert (r["email_status"], r["email_last_error"]) == ("skipped", "not configured: smtp_host missing"), r
    assert (r["teams_status"], r["state"]) == ("sent", "degraded") and r["finished_at"] is not None, r
    assert a[0]["outcome"] == "skipped" and a[0]["error"] == "not configured: smtp_host missing"
    ok("owner, email not configured: skipped with the missing key named, Teams sent the same run; degraded")

    await reset(health_email_recipients="")
    rid = await enq_owner(2)
    await W.run_once()
    r = await row(rid)
    assert (r["email_status"], r["teams_status"], r["state"]) == ("skipped", "sent", "delivered"), r
    assert r["email_last_error"] == "health_email_recipients is blank (Teams-only)"
    ok("owner, Teams-only by setting: email skipped (not a fault), Teams sent; delivered")

    await reset(health_email_recipients="", teams_enabled=False)
    rid = await enq_owner(3)
    await W.run_once()
    r = await row(rid)
    assert (r["teams_status"], r["teams_last_error"]) == (
        "pending", "not configured: teams_enabled off or no teams_webhook_url"), r
    assert 720 <= due_in(r, "teams") <= 1080
    await sql("UPDATE kafka_notification_outbox SET expires_at = now() WHERE id = :id", id=rid)
    await force_due()
    await W.run_once()
    r = await row(rid)
    assert (r["email_status"], r["teams_status"], r["state"]) == ("skipped", "abandoned", "failed"), r
    assert r["teams_last_error"] == ("1 attempt, last error: not configured: teams_enabled off or no "
                                     "teams_webhook_url"), r["teams_last_error"]
    ok("owner, nothing configured: Teams retries at the cap, then failed with both reasons recorded")

    await reset(email_recipients="", teams_webhook_url="")
    await sql("INSERT INTO kafka_alert_configs (id, name, alert_type, severity, config, cooldown_minutes, enabled, "
              "email_enabled, webhook_url, created_at, updated_at) VALUES (7, 'r7', 'x', 'warning', '{}', 0, true, "
              "true, :u, now(), now()), (8, 'r8', 'x', 'warning', '{}', 0, true, true, NULL, now(), now())",
              u=TEAMS.url("rule7"))
    rid = await enq_rule(4, ref="rule:7")
    rid8 = await enq_rule(5, email=False, ref="rule:8")
    rid9 = await enq_rule(6, email=False, ref="rule:9")
    await W.run_once()
    r, r8, r9 = await row(rid), await row(rid8), await row(rid9)
    assert (r["email_status"], r["email_last_error"]) == ("pending", "not configured: email_recipients missing"), r
    assert r["teams_status"] == "sent" and TEAMS.posts[0][0].startswith("/webhook/rule7/"), TEAMS.posts
    assert 720 <= due_in(r, "email") <= 1080 and r["state"] == "degraded"
    assert (r8["teams_status"], r8["teams_last_error"]) == (
        "pending", "no webhook URL configured (per-alert or agent-level)"), r8
    assert (r9["teams_status"], r9["teams_last_error"], r9["state"]) == (
        "skipped", "rule 9 no longer exists", "suppressed"), r9
    ok("rule rows: email not configured retries at the cap with the reason; the webhook is resolved at send "
       "time from the rule reference (rule 7's own URL, rule 8 none, rule 9 deleted)")


async def test_default_executor_saturation():
    await reset()
    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
    blockers = [loop.run_in_executor(None, time.sleep, 4) for _ in range(3)]
    rid = await enq_owner(1)
    t0 = time.monotonic()
    await W.run_once()
    took = time.monotonic() - t0
    r = await row(rid)
    assert r["email_status"] == "sent" and took < 2, (r["email_status"], took)
    assert any(t.name.startswith("outbox-smtp") for t in threading.enumerate())
    await asyncio.gather(*blockers)
    ok(f"default executor saturated (12 s of work queued): email sent in {took:.2f} s on the outbox-smtp pool")


async def test_superseded_summary_and_kick():
    await reset(smtp_port=free_port(), teams_enabled=False)
    older = await enq_owner(1, kind="owner_summary")
    await W.run_once()
    configure()
    newer = await W.enqueue("owner_summary", "owner", "t:2", email=mail(2), teams_card=card(2), source="test")
    await W.drain(30)  # enqueue kicked the worker; no explicit run
    a, b = await row(older), await row(newer)
    assert b["state"] == "delivered" and SMTP.subjects() == ["notice 2"], (b, SMTP.subjects())
    assert (a["email_status"], a["teams_status"], a["finished_at"] is not None) == ("superseded", "superseded", True), a
    assert a["email_last_error"] == f"superseded: a newer summary (row {newer}) was delivered"
    ok("kick: an enqueue starts a run at once; a delivered summary supersedes an older one still retrying")

    rows = [await W.enqueue(k, "rule", f"age:{k}", teams_card=card(k), kick_after=False) for k in outbox._MAX_AGE]
    ages = (await sql("SELECT kind, extract(epoch FROM expires_at - created_at) FROM kafka_notification_outbox "
                      "WHERE id = ANY(:ids)", ids=rows)).all()
    assert {k: timedelta(seconds=float(s)) for k, s in ages} == outbox._MAX_AGE, ages
    ok("max age by kind: watchdog 12 h, fire/escalate/resolve 24 h, reminder 6 h, summary 2 h, heartbeat and "
       "deadman 12 h")


# ── Rule rows with a policy (rule notices through the outbox, step 1) ────────

async def add_rule(rule_id, teams=True, email=True, webhook=None):
    await sql("INSERT INTO kafka_alert_configs (id, name, alert_type, severity, config, cooldown_minutes, enabled, "
              "email_enabled, webhook_url, created_at, updated_at) VALUES (:id, :n, 'cluster.urp_total', 'warning', "
              "'{}', 0, :t, :e, :u, now(), now())", id=rule_id, n=f"rule {rule_id}", t=teams, e=email, u=webhook)


async def add_trigger(rule_id, posted=False, detail=None):
    return (await sql("INSERT INTO kafka_alert_triggers (alert_config_id, cluster_id, subject, severity, triggered_at, "
                      "metric_value, message_sent, teams_post_success, error_detail, is_recurrence) VALUES "
                      "(:r, 4, 'cluster', 'warning', now(), '5', 'm', :p, :d, false) RETURNING id",
                      r=rule_id, p=posted, d=detail)).scalar_one()


async def trigger(tid):
    return dict((await sql("SELECT teams_post_success, error_detail FROM kafka_alert_triggers WHERE id = :id",
                           id=tid)).mappings().one())


async def enq_policy(n, rule_id, kind="rule_fire", teams=True, email=True, trigger_ids=None, queued=True):
    """One rule row on its own session, as an evaluator would: the triggers
    in trigger_ids are marked queued in the same transaction."""
    async with SessionLocal() as s:
        rid = await W.enqueue_in(s, kind, f"p:{n}", policy={"rule_id": rule_id, "teams_toggle": teams,
                                                             "email_toggle": email},
                                 email=mail(n), teams_card=card(n), rule_name=f"rule {rule_id}", severity="warning",
                                 cluster_id=4, trigger_ids=trigger_ids, source="test", payload={"subject": "cluster"})
        if rid is not None and trigger_ids and queued:
            await s.execute(text("UPDATE kafka_alert_triggers SET error_detail = :d WHERE id = ANY(:ids)"),
                            {"d": W.queued_detail(rid), "ids": trigger_ids})
        await s.commit()
    return rid


async def run_until_idle(n=1):
    for _ in range(n):
        await force_due()
        await W.run_once()


async def test_rule_channel_plan():
    await reset()
    for rule_id, teams, email in ((1, True, True), (2, True, False), (3, False, True), (4, False, False)):
        await add_rule(rule_id, teams, email)
    both, teams_only, email_only = [await enq_policy(n, n, teams=t, email=e)
                                    for n, t, e in ((1, True, True), (2, True, False), (3, False, True))]
    none = await enq_policy(4, 4, teams=False, email=False)
    rows = {k: await row(v) for k, v in (("both", both), ("teams", teams_only), ("email", email_only))}
    assert none is None and (await sql("SELECT count(*) FROM kafka_notification_outbox")).scalar() == 3
    assert (rows["both"]["email_status"], rows["both"]["teams_status"]) == ("pending", "pending"), rows["both"]
    assert (rows["teams"]["email_status"], rows["teams"]["email_last_error"], rows["teams"]["teams_status"]) == (
        "skipped", "rule email off", "pending") and rows["teams"]["email_message_id"] is None, rows["teams"]
    assert (rows["email"]["email_status"], rows["email"]["teams_status"]) == ("pending", "standby"), rows["email"]
    pl = rows["email"]["payload"]
    assert pl["teams_card"] == card(3) and pl["email"] == mail(3) and pl["teams_ref"] == "rule:3" and pl["title"] == "notice 3"
    assert pl["policy"] == {"rule_id": 3, "teams_toggle": False, "email_toggle": True} and pl["rule_name"] == "rule 3"
    assert pl["subject"] == "cluster" and pl["mode"] == "on" and rows["email"]["alert_config_id"] == 3
    ok("rule rows at enqueue: both on -> both pending; Teams only -> email skipped 'rule email off'; Email only -> "
       "email pending, card stored in standby; both off -> no row; payload holds email, card, policy, rule_name, rule:<id>")
    await W.run_once()
    rows = {k: await row(v) for k, v in (("both", both), ("teams", teams_only), ("email", email_only))}
    assert (rows["both"]["email_status"], rows["both"]["teams_status"], rows["both"]["state"]) == ("sent", "sent", "delivered")
    assert (rows["teams"]["teams_status"], rows["teams"]["state"]) == ("sent", "delivered"), rows["teams"]
    assert (rows["email"]["email_status"], rows["email"]["teams_status"], rows["email"]["teams_last_error"],
            rows["email"]["state"]) == ("sent", "skipped", "fallback not needed: email sent", "delivered"), rows["email"]
    assert sorted(SMTP.subjects()) == ["notice 1", "notice 3"] and sorted(first_texts()) == ["card 1", "card 2"]
    ok("rule rows sent: each channel independent when Teams is on; Email-only: email sent, the standby card "
       "skipped 'fallback not needed: email sent', no card; all delivered")


async def test_enqueue_in_transaction():
    await reset()
    await add_rule(1)
    async with SessionLocal() as s:
        tid = (await s.execute(text("INSERT INTO kafka_alert_triggers (alert_config_id, triggered_at, teams_post_success, "
                                    "is_recurrence) VALUES (1, now(), false, false) RETURNING id"))).scalar_one()
        rid = await W.enqueue_in(s, "rule_fire", W.rule_dedupe_key("rule_fire", trigger_id=tid),
                                 policy={"rule_id": 1, "teams_toggle": True, "email_toggle": True},
                                 email=mail(1), teams_card=card(1), trigger_ids=[tid])
        assert rid is not None
        await s.rollback()
    counts = (await sql("SELECT (SELECT count(*) FROM kafka_notification_outbox), (SELECT count(*) FROM kafka_alert_triggers)")).one()
    assert tuple(counts) == (0, 0), counts
    await W.run_once()
    assert SMTP.messages == [] and TEAMS.posts == []
    ok("enqueue_in joins the caller's transaction: a rollback removes the trigger and the row together; "
       "nothing is committed or kicked by enqueue_in")
    async with SessionLocal() as s:
        tid = (await s.execute(text("INSERT INTO kafka_alert_triggers (alert_config_id, triggered_at, teams_post_success, "
                                    "is_recurrence) VALUES (1, now(), false, false) RETURNING id"))).scalar_one()
        key = W.rule_dedupe_key("rule_fire", trigger_id=tid)
        pol = {"rule_id": 1, "teams_toggle": True, "email_toggle": True}
        first = await W.enqueue_in(s, "rule_fire", key, policy=pol, email=mail(1), trigger_ids=[tid])
        again = await W.enqueue_in(s, "rule_fire", key, policy=pol, email=mail(1), trigger_ids=[tid])
        await s.commit()
    r = await row(first)
    assert first is not None and again is None and r["trigger_ids"] == [tid] and r["routing"] == "rule" and r["origin"] == "outbox"
    from datetime import datetime, timezone
    ts = datetime(2026, 10, 15, 6, 45, tzinfo=timezone.utc)
    assert [W.rule_dedupe_key("rule_fire", trigger_id=7), W.rule_dedupe_key("rule_resolve", trigger_id=7),
            W.rule_dedupe_key("rule_escalate", trigger_id=7, tier="critical", eval_ts=ts),
            W.rule_dedupe_key("rule_reminder", config_id=3, eval_ts=ts, grouped=True),
            W.rule_dedupe_key("rule_resolve", config_id=3, eval_ts=ts, grouped=True)] == [
        "rule_fire:7", "rule_resolve:7", "rule_escalate:7:critical:2026-10-15T06:45:00+00:00",
        "lag:3:reminder:2026-10-15T06:45:00+00:00", "lag_resolve:3:2026-10-15T06:45:00+00:00"]
    ok("enqueue_in after a commit: one row with its trigger_ids; the same dedupe key again inserts nothing; "
       "dedupe keys rule_fire:{t}, rule_resolve:{t}, rule_escalate:{t}:{tier}:{ts}, lag:{rule}:{kind}:{ts}, "
       "lag_resolve:{rule}:{ts}")


async def test_rule_fallback_after_three():
    await reset(smtp_port=free_port())
    await add_rule(1, teams=False)
    rid = await enq_policy(1, 1, teams=False)
    for attempt in (1, 2):
        await run_until_idle()
        r = await row(rid)
        assert (r["email_status"], r["email_attempts"], r["teams_status"]) == ("pending", attempt, "standby"), r
        assert TEAMS.posts == [] and r["state"] == "pending", r
    ok("rule fallback: after 1 and 2 failed email attempts the card stays in standby; nothing posted")
    await run_until_idle()
    r = await row(rid)
    assert (r["email_status"], r["email_attempts"], r["teams_status"], r["state"]) == ("pending", 3, "sent", "degraded"), r
    assert first_texts() == ["card 1"] and TEAMS.posts[0][0].startswith("/webhook/"), TEAMS.posts
    ok("rule fallback: the 3rd failed attempt turns standby into pending and the card goes out in the same run; "
       "email keeps retrying; degraded")
    configure()
    await run_until_idle()
    r = await row(rid)
    assert (r["email_status"], r["teams_status"], r["state"]) == ("sent", "sent", "delivered") and len(TEAMS.posts) == 1, r
    ok("rule fallback: the email retried later is sent; the card is not posted again; both channels sent")


async def test_rule_fallback_at_once():
    cases = []
    # abandoned: max age reached on the 2nd attempt
    await reset(smtp_port=free_port())
    await add_rule(1, teams=False)
    rid = await enq_policy(1, 1, teams=False)
    await W.run_once()
    await sql("UPDATE kafka_notification_outbox SET expires_at = now() + interval '20 seconds' WHERE id = :id", id=rid)
    await run_until_idle()
    r = await row(rid)
    cases.append(("abandoned", r))
    assert (r["email_status"], r["email_attempts"], r["teams_status"], r["state"]) == ("abandoned", 2, "sent", "degraded"), r
    # permanently failed (1 attempt). Not reachable end to end today: smtplib's data() returns a 5xx final
    # reply instead of raising, and _smtp_send_sync records it as sent (reported, not changed here).
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    fake = {"id": 1, "routing": "rule", "kind": "rule_fire", "email_status": "sending", "teams_status": "standby",
            "email_attempts": 1, "teams_attempts": 0, "email_last_error": None, "teams_last_error": None,
            "email_sent_at": None, "teams_sent_at": None, "email_warning": None, "expires_at": now + timedelta(hours=1),
            "payload": {"policy": {"rule_id": 1, "teams_toggle": False, "email_toggle": True}}}
    W._apply(fake, "email", {"outcome": "failed_permanent", "error": "554 rejected"}, now)
    assert (fake["email_status"], fake["teams_status"]) == ("failed", "pending"), fake
    # not configured: no SMTP host; and email switched off globally
    for over, reason in (({"smtp_host": ""}, "not configured: smtp_host missing"),
                         ({"email_recipients": ""}, "not configured: email_recipients missing"),
                         ({"email_enabled": False}, "not configured: email_enabled (global) off")):
        await reset(**over)
        await add_rule(1, teams=False)
        rid = await enq_policy(1, 1, teams=False)
        await W.run_once()
        r = await row(rid)
        assert (r["email_status"], r["email_last_error"], r["teams_status"], r["state"]) == (
            "skipped", reason, "sent", "degraded"), (over, r)
        assert len(TEAMS.posts) == 1 and SMTP.messages == []
    ok("rule fallback at once: email abandoned (2 attempts), permanently failed (1 attempt, via _apply), not "
       "configured (no SMTP host, no recipients) or global email off -> the card goes out in the same run; degraded")


async def test_rule_no_fallback():
    for over, reason in (({"teams_enabled": False}, "Teams switched off globally"),
                         ({"rule_teams_fallback": False}, "Teams off for this rule (fallback off)")):
        await reset(smtp_host="", **over)
        await add_rule(1, teams=False)
        rid = await enq_policy(1, 1, teams=False)
        await W.run_once()
        r = await row(rid)
        assert (r["email_status"], r["teams_status"], r["teams_last_error"], r["state"]) == (
            "skipped", "skipped", reason, "failed"), (over, r)
        assert r["finished_at"] is not None and TEAMS.posts == []
    ok("no rule fallback with global Teams off ('Teams switched off globally') or rule_teams_fallback off "
       "('Teams off for this rule (fallback off)'): nothing posted, the row ends failed")
    await reset(smtp_host="", teams_webhook_url="")
    await add_rule(1, teams=False)
    rid = await enq_policy(1, 1, teams=False)
    await W.run_once()
    r = await row(rid)
    assert (r["teams_status"], r["teams_last_error"], r["state"], r["finished_at"] is not None) == (
        "skipped", "no webhook URL configured (per-alert or agent-level)", "failed", True), r
    ok("no rule fallback without a webhook: the card is skipped with the reason, not retried")
    await reset()
    await add_rule(1, teams=False, email=False)  # the rule's Email toggle was turned off after the enqueue
    rid = await enq_policy(1, 1, teams=False)
    await W.run_once()
    r = await row(rid)
    assert (r["email_status"], r["email_last_error"], r["teams_status"], r["teams_last_error"], r["state"]) == (
        "skipped", "rule email off", "skipped", "Teams off for this rule", "suppressed"), r
    assert TEAMS.posts == [] and SMTP.messages == []
    ok("rule Email toggle off at send time: email skipped 'rule email off'; that is not a delivery failure, so "
       "no fallback (card skipped 'Teams off for this rule'); suppressed")


async def test_rule_email_supersedes():
    await reset(smtp_port=free_port())
    await add_rule(1, teams=False)
    rid = await enq_policy(1, 1, teams=False)
    await run_until_idle(2)
    TEAMS.script = [(500, {})]
    await run_until_idle()
    r = await row(rid)
    assert (r["email_attempts"], r["teams_status"], r["teams_last_error"]) == (3, "pending", "HTTP 500"), r
    configure()
    await run_until_idle()
    r = await row(rid)
    assert (r["email_status"], r["teams_status"], r["teams_last_error"], r["state"]) == (
        "sent", "superseded", "fallback no longer needed: email sent", "delivered"), r
    assert len(TEAMS.posts) == 1 and SMTP.subjects() == ["notice 1"]
    ok("rule fallback pending but not yet delivered (Teams 500) when the email goes out: superseded, no card; delivered")


async def test_rule_dependency_gate():
    await reset()
    await add_rule(1)
    t1 = await add_trigger(1)
    TEAMS.status = 500
    fire = await enq_policy(1, 1, trigger_ids=[t1])
    resolve = await enq_policy(2, 1, kind="rule_resolve", trigger_ids=[t1])
    await W.run_once()
    f, r = await row(fire), await row(resolve)
    assert (f["email_status"], f["teams_status"]) == ("sent", "pending"), f
    assert (r["email_status"], r["teams_status"]) == ("pending", "pending") and r["email_attempts"] == 0, r
    await run_until_idle()  # the fire's email has finished: the resolve email may go; its card still waits
    r = await row(resolve)
    assert (r["email_status"], r["teams_status"], r["teams_attempts"]) == ("sent", "pending", 0), r
    assert SMTP.subjects() == ["notice 1", "notice 2"], SMTP.subjects()
    ok("dependency gate: a resolve waits per channel for the earlier fire sharing its trigger; its email goes once "
       "the fire's email is done while its card keeps waiting for the fire's card")
    TEAMS.status = 202
    await run_until_idle()
    r = await row(resolve)
    assert r["teams_status"] == "pending" and r["teams_attempts"] == 0, r  # the fire's card was only being sent
    await run_until_idle()
    f, r = await row(fire), await row(resolve)
    assert (f["teams_status"], r["teams_status"], r["state"]) == ("sent", "sent", "delivered"), (f, r)
    texts = first_texts()  # card 1 also holds the posts answered 500
    assert texts.count("card 2") == 1 and max(i for i, t in enumerate(texts) if t == "card 1") < texts.index("card 2"), texts
    ok("dependency gate: the resolve card goes out only after the fire's card was sent, in that order")

    await reset(smtp_port=free_port())
    await add_rule(1)
    t1 = await add_trigger(1)
    fire = await enq_policy(1, 1, trigger_ids=[t1])
    resolve = await enq_policy(2, 1, kind="rule_resolve", trigger_ids=[t1])
    await run_until_idle()
    await sql("UPDATE kafka_notification_outbox SET expires_at = now() + interval '20 seconds' WHERE id = :id", id=fire)
    await run_until_idle()
    f, r = await row(fire), await row(resolve)
    assert (f["email_status"], f["teams_status"]) == ("abandoned", "sent"), f
    assert (r["email_status"], r["email_attempts"], r["teams_status"]) == ("pending", 0, "sent"), r
    configure()
    await run_until_idle()
    r = await row(resolve)
    assert (r["email_status"], r["state"]) == ("sent", "delivered") and SMTP.subjects() == ["notice 2"], (r, SMTP.subjects())
    ok("dependency gate: a fire email that was never delivered (abandoned) does not block the resolve email once "
       "it is done; the resolve card went as soon as the fire's card was sent")

    await reset()
    await add_rule(1)
    t1 = await add_trigger(1)
    TEAMS.status = 500
    fire = await enq_policy(1, 1, trigger_ids=[t1])
    resolve = await enq_policy(2, 1, kind="rule_resolve", trigger_ids=[t1])
    await sql("UPDATE kafka_notification_outbox SET created_at = now() - interval '61 minutes' WHERE id = :id", id=resolve)
    await W.run_once()
    r = await row(resolve)
    assert (r["email_status"], r["teams_status"], r["teams_last_error"]) == (
        "sent", "skipped", "opening card never reached Teams"), r
    assert (await row(fire))["teams_status"] == "pending"
    ok("dependency gate capped at 60 min: the resolve proceeds; its card is skipped 'opening card never reached "
       "Teams' since the fire's card was not sent; its email goes out")

    await reset()
    await add_rule(1)
    t_posted, t_not = await add_trigger(1, posted=True), await add_trigger(1, posted=False, detail="HTTP 500")
    r_posted = await enq_policy(1, 1, kind="rule_resolve", trigger_ids=[t_posted], queued=False)
    r_not = await enq_policy(2, 1, kind="rule_resolve", trigger_ids=[t_not], queued=False)
    await W.run_once()
    a, b = await row(r_posted), await row(r_not)
    assert (a["email_status"], a["teams_status"]) == ("sent", "sent"), a
    assert (b["email_status"], b["teams_status"], b["teams_last_error"]) == ("sent", "skipped", "opening card never reached Teams"), b
    assert first_texts() == ["card 1"]
    ok("no parent row (the fire went inline before the switch): the resolve proceeds; its card follows the "
       "trigger's own teams_post_success (sent for the posted one, skipped for the other)")


async def test_rule_send_time_checks():
    await reset()
    await add_rule(9)
    await add_rule(1)
    gone = await enq_policy(1, 9)
    both = await enq_policy(2, 1)
    await sql("DELETE FROM kafka_alert_configs WHERE id = 9")
    await sql("UPDATE kafka_alert_configs SET enabled = false WHERE id = 1")  # Teams toggle off after the enqueue
    await W.run_once()
    g, b = await row(gone), await row(both)
    assert (g["email_status"], g["email_last_error"], g["teams_status"], g["teams_last_error"], g["state"]) == (
        "skipped", "rule 9 no longer exists", "skipped", "rule 9 no longer exists", "suppressed"), g
    assert (b["email_status"], b["teams_status"], b["teams_last_error"], b["state"]) == (
        "sent", "skipped", "Teams off for this rule", "delivered"), b
    assert SMTP.subjects() == ["notice 2"] and TEAMS.posts == []
    ok("send-time checks: a deleted rule skips both channels; a Teams toggle turned off after the enqueue "
       "skips the card (not a fallback row); the email still goes")
    await reset(teams_enabled=False)
    await add_rule(1)
    rid = await enq_policy(1, 1)
    legacy = await enq_rule(2, email=False)
    await W.run_once()
    r, l = await row(rid), await row(legacy)
    assert (r["email_status"], r["teams_status"], r["teams_last_error"]) == ("sent", "skipped", "Teams switched off globally"), r
    assert (l["teams_status"], l["teams_last_error"]) == ("skipped", "Teams switched off globally"), l
    await reset(email_enabled=False)
    await add_rule(1)
    rid = await enq_policy(1, 1)
    await W.run_once()
    r = await row(rid)
    assert (r["email_status"], r["email_last_error"], r["teams_status"], r["state"]) == (
        "skipped", "not configured: email_enabled (global) off", "sent", "degraded"), r
    ok("send-time checks: global Teams off skips every rule card ('Teams switched off globally'); global email "
       "off skips the email as not configured (degraded) while the card goes")


async def test_rule_trigger_writeback():
    await reset()
    await add_rule(1)
    await add_rule(2, teams=False)
    t_sent, t_off, t_other = await add_trigger(1), await add_trigger(2), await add_trigger(1, detail="HTTP 500")
    sent_row = await enq_policy(1, 1, trigger_ids=[t_sent])
    off_row = await enq_policy(2, 2, teams=False, trigger_ids=[t_off])
    other_row = await enq_policy(3, 1, trigger_ids=[t_other], queued=False)
    assert (await trigger(t_sent)) == {"teams_post_success": False, "error_detail": f"queued (outbox row {sent_row})"}
    await W.run_once()
    assert (await trigger(t_sent)) == {"teams_post_success": True, "error_detail": None}
    assert (await trigger(t_off)) == {"teams_post_success": False, "error_detail": "Teams off for this rule"}
    assert (await trigger(t_other)) == {"teams_post_success": False, "error_detail": "HTTP 500"}
    assert (await row(other_row))["teams_status"] == "sent"
    await W._writeback(await row(sent_row))  # again: nothing still shows "queued", nothing changes
    assert (await trigger(t_sent)) == {"teams_post_success": True, "error_detail": None}
    ok("trigger write-back: sent -> teams_post_success true; an Email-only rule's card not needed -> false "
       "'Teams off for this rule'; a trigger not showing 'queued (outbox row N)' is left alone; idempotent")
    await reset(teams_enabled=False)
    await add_rule(1)
    t = await add_trigger(1)
    await enq_policy(1, 1, trigger_ids=[t])
    await W.run_once()
    assert (await trigger(t)) == {"teams_post_success": False, "error_detail": "Teams switched off globally"}
    await reset()
    await add_rule(1)
    t = await add_trigger(1)
    rid = await enq_policy(1, 1, trigger_ids=[t])
    await sql("ALTER TABLE kafka_alert_triggers RENAME TO kafka_alert_triggers_moved")
    try:
        await W.run_once()
    finally:
        await sql("ALTER TABLE kafka_alert_triggers_moved RENAME TO kafka_alert_triggers")
    r = await row(rid)
    assert (r["email_status"], r["teams_status"], r["state"]) == ("sent", "sent", "delivered"), r
    assert any("trigger write-back failed" in line for line in LOGS.lines)
    ok("trigger write-back: a skipped card writes its reason; a failing write-back is logged and never fails the send")


async def test_rule_settings():
    import httpx
    import routes_settings
    from fastapi import FastAPI
    from storage import init_storage
    from config import settings as agent_settings
    init_storage(agent_settings.agent_slug)
    assert routes_settings._DEFAULTS["rule_teams_fallback"] is True and routes_settings._DEFAULTS["rule_outbox_evaluators"] == []
    saved = {k: _config.get(k) for k in ("rule_teams_fallback", "rule_outbox_evaluators")}
    _config["rule_outbox_evaluators"] = []
    assert not any(outbox.rule_via_outbox(n) for n in outbox.RULE_EVALUATORS + ("nope",))
    app = FastAPI()
    app.include_router(routes_settings.router)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://agent") as c:
            bad = [await c.post("/settings", json={"rule_outbox_evaluators": v}) for v in (["lag", "nope"], "lag", [1])]
            bad.append(await c.post("/settings", json={"rule_teams_fallback": "yes"}))
            assert [r.status_code for r in bad] == [422] * 4, [r.status_code for r in bad]
            assert _config["rule_outbox_evaluators"] == [] and _config["rule_teams_fallback"] is True
            good = await c.post("/settings", json={"rule_outbox_evaluators": ["lag", "close_wait", "lag"],
                                                   "rule_teams_fallback": False})
            assert good.status_code == 200 and _config["rule_outbox_evaluators"] == ["lag", "close_wait"]
            assert [outbox.rule_via_outbox(n) for n in ("lag", "close_wait", "cluster_metrics", "nope")] == [
                True, True, False, False]
            stored = dict((await sql("SELECT key, value FROM agent_config WHERE key IN "
                                     "('rule_outbox_evaluators', 'rule_teams_fallback')")).all())
            assert json.loads(stored["rule_outbox_evaluators"]) == ["lag", "close_wait"], stored
            assert json.loads(stored["rule_teams_fallback"]) is False, stored
            got = (await c.get("/settings")).json()
            assert got["rule_outbox_evaluators"] == ["lag", "close_wait"] and got["rule_teams_fallback"] is False
            await c.post("/settings", json={"rule_outbox_evaluators": [], "rule_teams_fallback": True})
    finally:
        _config.update(saved)
        await sql("DELETE FROM agent_config WHERE key IN ('rule_outbox_evaluators', 'rule_teams_fallback')")
    ok("settings: rule_teams_fallback (default true) and rule_outbox_evaluators (default empty) are validated "
       "(422 for an unknown name, a non-list or a non-bool), de-duplicated, stored and returned; "
       "outbox.rule_via_outbox is false for an empty list and an unknown name")


async def test_no_secrets():
    await assert_no_secrets_stored()
    logs = "\n".join(LOGS.lines)
    assert "outbox worker:" in logs
    for secret in (PASSWORD, "secret-signature", "owner@operative.com", "ops@operative.com", "outsider@"):
        assert secret not in logs, secret
    ok("no password, webhook URL or address in the outbox rows, the attempts or the worker's log lines")


async def main():
    test_rules()
    for test in (test_shadow_finds_nothing, test_smtp_down, test_teams_down, test_both_down_then_recover,
                 test_slow_smtp, test_restart_mid_send, test_duplicates, test_crash_after_claim,
                 test_config_healing, test_not_configured, test_superseded_summary_and_kick,
                 test_default_executor_saturation, test_rule_channel_plan, test_enqueue_in_transaction,
                 test_rule_fallback_after_three, test_rule_fallback_at_once, test_rule_no_fallback,
                 test_rule_email_supersedes, test_rule_dependency_gate, test_rule_send_time_checks,
                 test_rule_trigger_writeback, test_rule_settings, test_no_secrets):
        await test()
    await W.drain(30)
    await sql("TRUNCATE kafka_notification_outbox, kafka_notification_attempts, kafka_alert_configs, "
              "kafka_alert_triggers, kafka_job_runs, kafka_job_schedules RESTART IDENTITY CASCADE")
    await engine.dispose()
    print(f"\nALL {len(PASSED)} CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
