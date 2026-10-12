"""Failure-injection tests for the outbox delivery worker (outbox_worker.py,
step 2a) against a SCRATCH Postgres with the 0058 tables, a scriptable fake
SMTP server and a fake Teams webhook on 127.0.0.1 (tests/_delivery_fakes.py).
Rows are enqueued directly with outbox_worker.enqueue; no live notice and no
evaluator is involved. Plain asserts, no pytest needed.

Run inside the agent image, never against a real database (it truncates
kafka_notification_outbox, kafka_notification_attempts, kafka_alert_configs,
kafka_job_runs and kafka_job_schedules):
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
              "kafka_job_runs, kafka_job_schedules RESTART IDENTITY CASCADE")
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
                 test_default_executor_saturation, test_no_secrets):
        await test()
    await W.drain(30)
    await sql("TRUNCATE kafka_notification_outbox, kafka_notification_attempts, kafka_alert_configs, "
              "kafka_job_runs, kafka_job_schedules RESTART IDENTITY CASCADE")
    await engine.dispose()
    print(f"\nALL {len(PASSED)} CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
