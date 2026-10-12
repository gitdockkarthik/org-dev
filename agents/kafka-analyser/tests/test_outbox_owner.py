"""Outbox step 2b-1: owner notices through the outbox in mode "on" -- the
health summary, the could-not-be-built notice, both watchdog notices (durable
before the exit), the spool, the startup check, the dead-man check and the
worker-stalled direct notice -- against a SCRATCH Postgres with the live
schema, a scriptable fake SMTP server and a fake Teams webhook on 127.0.0.1.
Only the summary's data gathering (_gather_health_summary), os._exit and the
/proc listing are patched. Plain asserts, no pytest needed.

Run inside the agent image, never against a real database (it truncates the
outbox tables, kafka_watchdog_events and kafka_job_runs):
  docker run --rm --network <scratch net> -v $PWD/agents/kafka-analyser:/app:ro \
    -v $PWD/shared:/opt/shared:ro -e PYTHONPATH=/app:/opt -e DATABASE_URL=<scratch> \
    -e OUTBOX_TEST_CONFIRM=scratch -w /app org-dev-kafka-analyser python -B tests/test_outbox_owner.py
"""
import asyncio
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

assert "scratch" in os.environ.get("OUTBOX_TEST_CONFIRM", ""), \
    "set OUTBOX_TEST_CONFIRM=scratch to confirm DATABASE_URL is a throwaway database"

from sqlalchemy import text  # noqa: E402

import collectors  # noqa: E402
import outbox  # noqa: E402
import outbox_worker as W  # noqa: E402
from _delivery_fakes import ScriptedSMTP, ScriptedTeams, free_port  # noqa: E402
from database import SessionLocal, engine  # noqa: E402
from routes_settings import _config  # noqa: E402

PASSED = []
PASSWORD = "pw-Secret-9876"
SMTP, TEAMS = ScriptedSMTP(), ScriptedTeams()
SPOOL = "/tmp/outbox_spool_test.jsonl"
W.SPOOL_PATH = SPOOL
SECRETS = (PASSWORD, "secret-signature", "owner@operative.com", "ops@operative.com", "u:pw@")


class Capture(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def text(self):
        return "\n".join(r.getMessage() for r in self.records)


LOGS = Capture()
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
logging.getLogger().addHandler(LOGS)
for name in ("httpx", "httpcore"):  # as main.py does
    logging.getLogger(name).setLevel(logging.WARNING)


class Exited(BaseException):
    pass


def plain(message) -> str:
    """The text/plain part of a message the fake SMTP server received."""
    for part in message.walk():
        if part.get_content_type() == "text/plain":
            return part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8")
    return ""


def ok(name):
    PASSED.append(name)
    print(f"PASS {name}", flush=True)


def configure(**over):
    _config.update({
        "notification_outbox_mode": "on",
        "teams_enabled": True,
        "teams_webhook_url": TEAMS.url(),
        "email_enabled": True,
        "smtp_host": "127.0.0.1",
        "smtp_port": SMTP.port,
        "smtp_username": "relay-user",
        "smtp_password": PASSWORD,
        "smtp_from_address": "alerts@sintecmedia.com",
        "email_recipients": "ops@operative.com",
        "health_email_recipients": "owner@operative.com",
        **over,
    })


async def sql(statement, **params):
    async with SessionLocal() as s:
        result = await s.execute(text(statement), params)
        await s.commit()
        return result


async def assert_no_secrets_stored():
    dump = ""
    for table in ("kafka_notification_outbox", "kafka_notification_attempts", "kafka_watchdog_events"):
        dump += (await sql(f"SELECT coalesce(string_agg(t::text, ' '), '') FROM {table} t")).scalar()
    for secret in SECRETS:
        assert secret not in dump, secret


async def reset(**over):
    await W.drain(60)
    await outbox.drain(10)
    await assert_no_secrets_stored()
    await sql("TRUNCATE kafka_notification_outbox, kafka_notification_attempts, kafka_watchdog_events, "
              "kafka_job_runs RESTART IDENTITY CASCADE")
    await sql("DROP TRIGGER IF EXISTS test_fail_owner_watchdog ON kafka_notification_outbox")
    if os.path.exists(SPOOL):
        os.remove(SPOOL)
    if SMTP._server is None:
        SMTP.up()
    SMTP.reset()
    TEAMS.reset()
    W._raw_dsn = ORIGINAL_RAW_DSN
    collectors._gather_health_summary = fake_gather()
    configure(**over)


async def rows(where="true"):
    return [dict(r) for r in (await sql(
        f"SELECT * FROM kafka_notification_outbox WHERE {where} ORDER BY id")).mappings().all()]


ORIGINAL_RAW_DSN = W._raw_dsn
_GATHER_N = [0]


def fake_gather(fail=None, sleep=0):
    """Stand-in for _gather_health_summary (a fresh 'now' per call, so each
    summary gets its own dedupe key)."""
    async def gather():
        if sleep:
            await asyncio.sleep(sleep)
        if fail:
            raise fail
        _GATHER_N[0] += 1
        now = datetime(2026, 10, 12, 5, 0, tzinfo=timezone.utc) + timedelta(minutes=_GATHER_N[0])
        return now, "alive", [{"name": "c1", "data_age_minutes": 1.0, "success_count": 5, "failed_count": 0,
                               "brokers_online_agent": "3/3", "brokers_online_real": "3/3",
                               "broker_connections": None}]
    return gather


# ── Summary ──────────────────────────────────────────────────────────────────

async def test_summary_on():
    await reset()
    had_issue = await collectors._send_health_summary(9)
    await W.drain(30)
    [r] = await rows()
    assert had_issue is False and r["origin"] == "outbox" and r["kind"] == "owner_summary", (had_issue, r)
    assert (r["email_status"], r["teams_status"], r["state"]) == ("sent", "skipped", "delivered"), r
    assert SMTP.subjects() == ["[OK] Kafka Analyser health summary"] and TEAMS.posts == []
    assert collectors._health_card_window_start == datetime.fromisoformat(
        r["dedupe_key"].split(":", 1)[1] + "+00:00").timestamp()
    ok("summary, mode on: one outbox row, delivered by email and not Teams; the window starts at enqueue")

    await reset(smtp_port=free_port())
    await collectors._send_health_summary(9)
    await W.drain(30)
    [r] = await rows()
    assert (r["email_status"], r["teams_status"], r["state"]) == ("pending", "sent", "degraded"), r
    assert len(TEAMS.posts) == 1 and r["email_last_error"] == "connection refused"
    ok("summary, email down: the Teams fallback goes out and the email stays pending for retry")


async def test_summary_could_not_be_built():
    await reset()
    collectors._gather_health_summary = fake_gather(fail=RuntimeError("boom at postgresql://u:pw@db:5432/x"))
    had_issue = await collectors._send_health_summary(9)
    await W.drain(30)
    [r] = await rows()
    assert had_issue is True and r["state"] == "delivered", (had_issue, r)
    assert SMTP.subjects() == ["[ISSUE] Kafka Analyser health summary could not be built"], SMTP.subjects()
    body = plain(SMTP.messages[0][1])
    assert "Health summary could not be built: RuntimeError: boom at <url>" in body, body
    ok("gather failure: a 'Health summary could not be built: <reason>' notice is enqueued (URL masked); "
       "it counts as an issue, so the 10-minute follow-up applies")

    await reset()
    collectors._SUMMARY_GATHER_TIMEOUT_SECS, saved = 0.5, collectors._SUMMARY_GATHER_TIMEOUT_SECS
    collectors._gather_health_summary = fake_gather(sleep=2)
    try:
        assert await collectors._send_health_summary(9) is True
    finally:
        collectors._SUMMARY_GATHER_TIMEOUT_SECS = saved
    await W.drain(30)
    body = plain(SMTP.messages[0][1])
    assert "could not be built: gathering timed out after 0.5s" in body, body
    ok("gather timeout: the could-not-be-built notice names the timeout")


async def test_mode_switch_without_restart():
    await reset(notification_outbox_mode="shadow")
    await collectors._send_health_summary(9)
    await outbox.drain(10)
    assert len(SMTP.messages) == 1, SMTP.subjects()  # sent inline, inside the call
    assert [(r["origin"], r["state"]) for r in await rows()] == [("inline", "delivered")]
    configure(notification_outbox_mode="on")
    await collectors._send_health_summary(9)
    await W.drain(30)
    assert [r["origin"] for r in await rows()] == ["inline", "outbox"] and len(SMTP.messages) == 2
    configure(notification_outbox_mode="shadow")
    await collectors._send_health_summary(9)
    await outbox.drain(10)
    assert [r["origin"] for r in await rows()] == ["inline", "outbox", "inline"] and len(SMTP.messages) == 3
    ok("mode is read per call: shadow sends inline, on enqueues, back to shadow sends inline again")


async def test_restart_mid_send():
    await reset()
    SMTP.delay = {"DATA_END": 3}
    W_kick, W.kick = W.kick, (lambda: None)  # run the worker by hand
    try:
        await collectors._send_health_summary(9)
    finally:
        W.kick = W_kick
    old_boot = outbox.BOOT_ID
    task = asyncio.create_task(W.run_once())
    while not SMTP.data_started.is_set():
        await asyncio.sleep(0.02)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    outbox.BOOT_ID = uuid.uuid4().hex
    try:
        await W.run_once()
        await W.drain(30)
    finally:
        outbox.BOOT_ID = old_boot
    [r] = await rows()
    assert len(SMTP.messages) == 2 and len(set(m["Message-ID"] for _r, m in SMTP.messages)) == 1
    resent = " ".join(SMTP.subjects()[1].split())  # long subjects are folded on the wire
    assert resent == f"[OK] Kafka Analyser health summary {W.RESEND_LABEL}", SMTP.subjects()
    assert (r["email_status"], r["email_attempts"], r["teams_status"]) == ("sent", 2, "skipped"), r
    ok("restart mid-send: the summary is resent once by the new boot, labelled, same Message-ID; no Teams")


# ── Watchdogs ────────────────────────────────────────────────────────────────

async def run_process_watchdog():
    """check_process_count_watchdog over the threshold, with os._exit
    simulated; returns how many emails had gone out when it 'exited'."""
    real_exit, real_listdir = os._exit, os.listdir
    seen = {}

    def fake_exit(code):
        seen["emails_at_exit"] = len(SMTP.messages)
        seen["cards_at_exit"] = len(TEAMS.posts)
        raise Exited(code)

    os._exit = fake_exit
    os.listdir = lambda p: [str(i) for i in range(30)] if p == "/proc" else real_listdir(p)
    try:
        await collectors.check_process_count_watchdog()
    except Exited:
        pass
    finally:
        os._exit, os.listdir = real_exit, real_listdir
    assert "emails_at_exit" in seen, "watchdog did not exit"
    return seen


async def test_watchdog_durable():
    await reset()
    before_send = {}
    real_send = W.send_claimed

    async def spy(row_id, attempts, budget, email_cap=None):
        # As written by insert_claimed_with_event, before the send updates anything.
        before_send["xmins"] = [x for (x,) in (await sql(
            "SELECT xmin::text FROM kafka_watchdog_events UNION ALL "
            "SELECT xmin::text FROM kafka_notification_outbox WHERE id = :id UNION ALL "
            "SELECT xmin::text FROM kafka_notification_attempts WHERE outbox_id = :id", id=row_id)).all()]
        before_send["row"] = (await rows(f"id = {row_id}"))[0]
        return await real_send(row_id, attempts, budget, email_cap)

    W.send_claimed = spy
    try:
        seen = await run_process_watchdog()
    finally:
        W.send_claimed = real_send
    [r] = await rows()
    events = (await sql("SELECT watchdog, reason, details FROM kafka_watchdog_events")).mappings().all()
    assert seen == {"emails_at_exit": 1, "cards_at_exit": 0}, seen
    assert [(e["watchdog"], e["reason"], e["details"]["process_count"]) for e in events] == [
        ("process_count", "process_count", 30)], events
    xmins = before_send["xmins"]
    assert len(xmins) == 3 and len(set(xmins)) == 1, xmins  # event, outbox row, attempt: one transaction
    claimed = before_send["row"]
    assert (claimed["email_status"], claimed["claimed_by"]) == ("sending", outbox.BOOT_ID), claimed
    assert r["kind"] == "owner_watchdog" and r["dedupe_key"] == f"owner_watchdog:check_process_count_watchdog:{outbox.BOOT_ID}"
    assert (r["email_status"], r["state"], r["claimed_by"]) == ("sent", "delivered", None), r
    ok("process-count watchdog, mode on: event row, outbox row and attempt in one transaction (same xmin); "
       "the email went out before the (simulated) exit")

    await reset(smtp_port=free_port())
    seen = await run_process_watchdog()
    [r] = await rows()
    assert seen == {"emails_at_exit": 0, "cards_at_exit": 1}, seen
    assert (r["email_status"], r["teams_status"], r["state"]) == ("pending", "sent", "degraded"), r
    ok("watchdog, SMTP down: the Teams fallback goes out inside the 5 s budget, before the exit; email "
       "left pending for the next boot")


async def test_watchdog_one_transaction_or_nothing():
    await reset()
    await sql("CREATE OR REPLACE FUNCTION test_fail_owner_watchdog() RETURNS trigger LANGUAGE plpgsql AS "
              "$$ BEGIN RAISE EXCEPTION 'injected failure'; END $$")
    await sql("CREATE TRIGGER test_fail_owner_watchdog BEFORE INSERT ON kafka_notification_outbox "
              "FOR EACH ROW WHEN (NEW.kind = 'owner_watchdog') EXECUTE FUNCTION test_fail_owner_watchdog()")
    await collectors._watchdog_notice_and_event(
        "critical", "stale data", "data_freshness_watchdog", "check_data_freshness_watchdog",
        ("data_freshness", "stale_data", {"stale_clusters": [["c1", 7.5]]}))
    events = (await sql("SELECT count(*) FROM kafka_watchdog_events")).scalar()
    assert events == 0 and await rows() == [], events
    assert len(SMTP.messages) == 1 and os.path.exists(SPOOL)
    ok("outbox insert fails after the event insert: both roll back; the notice is sent inline and spooled")


async def test_database_down():
    await reset()
    W._raw_dsn = lambda: f"postgresql://u:x@127.0.0.1:{free_port()}/none"
    t0 = time.monotonic()
    await asyncio.wait_for(collectors._watchdog_notice_and_event(
        "critical", "Process count 30 exceeded", "process_watchdog", "check_process_count_watchdog",
        ("process_count", "process_count", {"process_count": 30, "threshold": 20})), timeout=5)
    took = time.monotonic() - t0
    assert len(SMTP.messages) == 1 and SMTP.subjects()[0].startswith("[ISSUE]"), SMTP.subjects()
    lines = open(SPOOL).read().splitlines()
    entry = json.loads(lines[0])
    assert len(lines) == 1 and entry["kind"] == "owner_watchdog" and entry["channels"]["email"]["status"] == "sent"
    assert entry["event"] == {"watchdog": "process_count", "reason": "process_count",
                              "details": {"process_count": 30, "threshold": 20}}
    assert await rows() == [] and took < 5, took
    spooled = open(SPOOL).read()
    for secret in SECRETS + ("127.0.0.1", "://"):
        assert secret not in spooled, secret
    ok(f"database down: the notice is sent inline ({took:.2f} s) and one spool line is written")


# ── Startup, dead-man, stalled worker ────────────────────────────────────────

async def test_spool_ingest():
    await reset()
    base = {"v": 1, "at": "2026-10-12T05:00:00+00:00", "boot_id": "b0", "kind": "owner_watchdog",
            "source": "check_process_count_watchdog", "severity": "critical", "title": "[ISSUE] watchdog",
            "event": {"watchdog": "process_count", "reason": "process_count", "details": {"process_count": 30}},
            "channels": {"email": {"status": "sent", "duration_ms": 40, "started_at": "2026-10-12T05:00:00+00:00"},
                         "teams": {"status": "skipped", "reason": "fallback not needed: email sent"}}}
    with open(SPOOL, "w") as f:
        for key in ("owner_watchdog:a:b0", "owner_watchdog:a:b0", "owner_watchdog:b:b0"):
            f.write(json.dumps(dict(base, dedupe_key=key)) + "\n")
    first = await W.startup()
    assert first["spool"] == 2 and not os.path.exists(SPOOL), first
    with open(SPOOL, "w") as f:
        f.write(json.dumps(dict(base, dedupe_key="owner_watchdog:a:b0")) + "\n")
    second = await W.startup()
    third = await W.startup()
    assert (second["spool"], third["spool"]) == (0, 0) and not os.path.exists(SPOOL), (second, third)
    got = [(r["dedupe_key"], r["origin"], r["state"]) for r in await rows()]
    assert got == [("owner_watchdog:a:b0", "inline", "delivered"), ("owner_watchdog:b:b0", "inline", "delivered")], got
    events = (await sql("SELECT count(*) FROM kafka_watchdog_events")).scalar()
    assert events == 2 and SMTP.messages == [], events
    ok("spool: ingested once at startup (records and event rows), deduped by key, file removed; nothing resent")


async def test_startup_check():
    await reset()
    await sql("INSERT INTO kafka_job_runs (job_id, triggered_by, status, logs, created_at, started_at, ended_at) "
              "VALUES ('x', 'schedule', 'success', '', now() - interval '3 minutes', now() - interval '3 minutes', "
              "now() - interval '2 minutes')")
    await sql("INSERT INTO kafka_watchdog_events (watchdog, reason, details, created_at) "
              "VALUES ('process_count', 'process_count', '{}', now() - interval '2 minutes')")
    result = await W.startup()
    assert result["check"].startswith("normal") and await rows() == [], result
    ok("startup check: previous run ended through a watchdog restart, nothing abandoned -> no notice")

    await sql("TRUNCATE kafka_watchdog_events")
    result = await W.startup()
    await W.drain(30)
    [r] = await rows()
    assert result["check"].startswith("notice enqueued") and r["kind"] == "deadman", result
    assert SMTP.subjects() == ["[ISSUE] Kafka Analyser restarted unexpectedly"], SMTP.subjects()
    body = plain(SMTP.messages[0][1])
    assert "Agent restarted at " in body and "previous activity ended about " in body and \
        "0 pending, 0 abandoned" in body and "No watchdog event" in body, body
    ok("startup check: previous run ended with no watchdog event -> 'agent restarted at X; previous activity "
       "ended about Y; N pending, M abandoned'")

    await reset(notification_outbox_mode="shadow")
    result = await W.startup()
    assert result["check"] == "skipped (mode not on)" and await rows() == []
    ok("startup check: nothing in shadow mode")


async def test_clean_shutdown_marker():
    await reset()
    await sql("INSERT INTO kafka_job_runs (job_id, triggered_by, status, logs, created_at, started_at, ended_at) "
              "VALUES ('x', 'schedule', 'success', '', now() - interval '3 minutes', now() - interval '3 minutes', "
              "now() - interval '2 minutes')")
    assert await W.write_clean_shutdown_marker() is True
    marker = (await sql("SELECT watchdog, reason, details FROM kafka_watchdog_events")).mappings().one()
    assert (marker["watchdog"], marker["reason"], marker["details"]["boot_id"]) == (
        "clean_shutdown", "lifespan_shutdown", outbox.BOOT_ID), marker
    result = await W.startup()
    assert result["check"].startswith("normal") and await rows() == [], result
    ok("clean shutdown: the lifespan marker is written, and the next startup gives no notice")

    await sql("UPDATE kafka_watchdog_events SET created_at = now() - interval '11 minutes'")
    await sql("UPDATE kafka_job_runs SET started_at = now() - interval '13 minutes', "
              "ended_at = now() - interval '12 minutes'")
    result = await W.startup()
    await W.drain(30)
    assert result["check"].startswith("notice enqueued"), result
    assert SMTP.subjects() == ["[ISSUE] Kafka Analyser restarted unexpectedly"], SMTP.subjects()
    ok("clean shutdown marker older than 10 minutes: not counted; with no watchdog event the notice is sent")

    await reset()
    real = W._raw_dsn
    for dsn in (f"postgresql://u:x@127.0.0.1:{free_port()}/none", "postgresql://u:x@10.255.255.1:5432/none"):
        W._raw_dsn = lambda dsn=dsn: dsn
        t0 = time.monotonic()
        assert await W.write_clean_shutdown_marker() is False
        took = time.monotonic() - t0
        assert took < 1.3, took
    W._raw_dsn = lambda: ""
    assert await W.write_clean_shutdown_marker() is False
    W._raw_dsn = real
    ok(f"marker write failing (refused, unreachable, no database) returns False within 1 s and never raises")


async def test_deadman():
    await reset(smtp_port=free_port(), teams_enabled=False)
    stuck = await W.enqueue("owner_summary", "owner", "stuck:1", email={"subject": "s1", "text": "t"},
                            kick_after=False)
    await sql("UPDATE kafka_notification_outbox SET created_at = now() - interval '20 minutes', "
              "next_attempt_at = now() + interval '1 hour' WHERE id = :id", id=stuck)
    gone = await W.enqueue("rule_fire", "rule", "gone:1", teams_card={"type": "message"}, kick_after=False)
    await sql("UPDATE kafka_notification_outbox SET teams_status = 'abandoned', teams_last_error = "
              "'3 attempts, last error: HTTP 500', state = 'failed', finished_at = now() WHERE id = :id", id=gone)
    fresh = await W.enqueue("owner_summary", "owner", "fresh:1", email={"subject": "s2", "text": "t"},
                            kick_after=False)
    await sql("UPDATE kafka_notification_outbox SET next_attempt_at = now() + interval '1 hour' WHERE id = :id",
              id=fresh)
    stats = await W.run_once()
    await W.drain(30)
    dead = await rows("kind = 'deadman'")
    assert stats["deadman"] == 2 and len(dead) == 1, (stats, dead)
    assert dead[0]["dedupe_key"] == f"deadman:{datetime.now(timezone.utc):%Y-%m-%dT%H}"
    reported = {r["id"]: r["reported_at"] is not None for r in await rows("kind <> 'deadman'")}
    assert reported == {stuck: True, gone: True, fresh: False}, reported
    text_body = dead[0]["payload"]["email"]["text"]
    assert f"row {stuck} owner_summary" in text_body and "teams abandoned (3 attempts, last error: HTTP 500)" in text_body

    await sql("UPDATE kafka_notification_outbox SET created_at = now() - interval '20 minutes' WHERE id = :id", id=fresh)
    stats = await W.run_once()
    await W.drain(30)
    assert stats.get("deadman", 0) == 0 and len(await rows("kind = 'deadman'")) == 1, stats
    assert (await rows(f"id = {fresh}"))[0]["reported_at"] is None
    ok("dead-man: an unfinished row over 15 min and an abandoned channel -> one notice this hour, rows marked; "
       "a new problem in the same hour waits for the next hour's notice")

    await sql("UPDATE kafka_notification_outbox SET dedupe_key = 'deadman:previous-hour' WHERE kind = 'deadman'")
    stats = await W.run_once()
    await W.drain(30)
    dead = await rows("kind = 'deadman'")
    assert stats["deadman"] >= 1 and len(dead) == 2, (stats, dead)
    assert (await rows(f"id = {fresh}"))[0]["reported_at"] is not None
    ok("dead-man: the next hour's notice reports what was left (at most one notice per hour)")


async def test_worker_stalled():
    await reset()
    collectors._last_worker_stall_notice_at = None
    saved = W._last_success
    W._last_success = time.time() - 700
    try:
        mark = len(LOGS.records)
        await collectors._check_outbox_worker_stalled()
        await collectors._check_outbox_worker_stalled()
        critical = [r for r in LOGS.records[mark:] if r.levelno == logging.CRITICAL]
        assert len(critical) == 2 and "outbox worker stalled" in critical[0].getMessage()
        assert SMTP.subjects() == ["[ISSUE] Kafka Analyser notification delivery stalled"], SMTP.subjects()
        assert await rows() == []
        ok("worker stalled (> 10 min): CRITICAL each check, ONE direct owner notice per hour, nothing in the outbox")
        configure(notification_outbox_mode="shadow")
        collectors._last_worker_stall_notice_at = None
        await collectors._check_outbox_worker_stalled()
        assert len(SMTP.messages) == 1
        W._last_success = time.time()
        configure()
        await collectors._check_outbox_worker_stalled()
        assert len(SMTP.messages) == 1
        ok("worker-stalled check: silent in shadow mode and when the worker ran recently")
    finally:
        W._last_success = saved


async def test_teams_error_body_truncated():
    from shared.escalation import notifier
    TEAMS.script = [(500, {}, "E" * 1000)]
    mark = len(LOGS.records)
    assert await notifier.send_to_teams(TEAMS.url(), {"type": "message"}) is False
    line = [r.getMessage() for r in LOGS.records[mark:] if "Teams webhook returned" in r.getMessage()][0]
    assert line == "Teams webhook returned 500: " + "E" * 200, len(line)
    ok("Teams error body logged at most 200 characters")


async def test_no_secrets():
    await reset()
    logs = LOGS.text()
    assert "owner notice" in logs and "outbox" in logs
    for secret in SECRETS:
        assert secret not in logs, secret
    ok("no password, webhook URL, address or DSN credential in rows, events, spool lines or log lines")


async def main():
    for test in (test_summary_on, test_summary_could_not_be_built, test_mode_switch_without_restart,
                 test_restart_mid_send, test_watchdog_durable, test_watchdog_one_transaction_or_nothing,
                 test_database_down, test_spool_ingest, test_startup_check, test_clean_shutdown_marker, test_deadman, test_worker_stalled,
                 test_teams_error_body_truncated, test_no_secrets):
        await test()
    await sql("TRUNCATE kafka_notification_outbox, kafka_notification_attempts, kafka_watchdog_events, "
              "kafka_job_runs RESTART IDENTITY CASCADE")
    await engine.dispose()
    print(f"\nALL {len(PASSED)} CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
