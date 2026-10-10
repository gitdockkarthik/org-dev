"""Tests for outbox.py (step 1, shadow mode): row building, state and outcome
rules, secret-free errors, skipped reasons, never-raises, mode off, and --
when DATABASE_URL points at a scratch database that has the 0058 tables --
idempotent recording on dedupe_key. Plain asserts, no pytest needed.
Run inside the agent image with the working tree mounted, e.g.:
  docker run --rm -v $PWD/agents/kafka-analyser:/app:ro -v $PWD/shared:/opt/shared:ro \
    -e PYTHONPATH=/app:/opt -e DATABASE_URL=... -w /app org-dev-kafka-analyser python -B tests/test_outbox.py
NEVER point DATABASE_URL at a real database: the DB part inserts and deletes rows."""
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import outbox  # noqa: E402
from routes_settings import _config  # noqa: E402

PASSED = []
WEBHOOK = "https://example.webhook.office.com/webhookb2/abc/IncomingWebhook/def/SIGNATURE123"
PASSWORD = "hunter2-secret"
T0 = datetime(2026, 10, 10, 3, 27, tzinfo=timezone.utc)


def ok(name):
    PASSED.append(name)
    print(f"PASS {name}")


class Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


def row_for(results, kind="owner_summary", routing="owner", severity=None):
    return outbox.build_row(kind, routing, "k", None, None, None, severity, "title", results,
                            source="test", now=T0, secrets=[PASSWORD, WEBHOOK])


def test_states_and_outcomes():
    row, att = row_for({
        "email": outbox.sent(812, T0, detail={"recipients": 1}),
        "teams": outbox.skipped("fallback not needed: email sent"),
    })
    assert row["state"] == "delivered", row["state"]
    assert (row["email_status"], row["teams_status"]) == ("sent", "skipped")
    assert (row["email_attempts"], row["teams_attempts"]) == (1, 0)
    assert row["teams_last_error"] == "fallback not needed: email sent"
    assert row["email_sent_at"] == T0 + timedelta(milliseconds=812) == row["delivered_at"]
    assert [(a["channel"], a["attempt_no"], a["outcome"]) for a in att] == [("email", 1, "sent"), ("teams", 0, "skipped")]
    assert att[0]["duration_ms"] == 812 and att[0]["phase_reached"] == "done" and att[1]["started_at"] is None
    assert row["origin"] == "inline" and row["finished_at"] == T0 and row["priority"] == 4
    ok("owner email sent, Teams fallback not needed -> delivered")

    row, att = row_for({
        "email": outbox.failed("timed out after 5s", 5003, T0),
        "teams": outbox.sent(640, T0 + timedelta(seconds=5), detail={"http_status": 202}),
    })
    assert row["state"] == "degraded" and row["email_last_error"] == "timed out after 5s"
    assert att[0]["outcome"] == "timeout" and att[1]["outcome"] == "sent"
    ok("owner email timed out, Teams sent -> degraded, outcome timeout")

    row, att = row_for({
        "email": outbox.skipped("not configured: smtp_host missing", config_problem=True),
        "teams": outbox.skipped("not configured: teams_enabled off or no teams_webhook_url", config_problem=True),
    })
    assert row["state"] == "failed", row["state"]
    assert row["email_last_error"] == "not configured: smtp_host missing"
    assert att[0]["detail"] == {"config_problem": True} and att[0]["error"] == row["email_last_error"]
    ok("nothing configured -> failed with both reasons recorded")

    row, _ = row_for({"email": outbox.skipped("rule email off"), "teams": outbox.skipped("resolve card off for this rule")},
                     kind="rule_resolve", routing="rule")
    assert row["state"] == "suppressed" and row["priority"] == 3
    ok("every channel skipped by a setting -> suppressed")

    row, _ = row_for({"email": outbox.sent(10, T0, warning="dropped 1 invalid address(es)"),
                      "teams": outbox.skipped("fallback not needed: email sent")})
    assert row["state"] == "degraded" and row["email_warning"] == "dropped 1 invalid address(es)"
    ok("email sent with a warning -> degraded, warning kept")

    cases = [
        (outbox.failed("HTTP 404", detail={"http_status": 404}), "failed_permanent"),
        (outbox.failed("HTTP 429", detail={"http_status": 429}), "failed_transient"),
        (outbox.failed("HTTP 500", detail={"http_status": 500}), "failed_transient"),
        (outbox.failed("authentication failed (SMTP 535)"), "failed_config"),
        (outbox.failed("connection refused"), "failed_transient"),
        (outbox.failed("no allowed recipients", config_problem=True), "failed_config"),
    ]
    for result, expected in cases:
        assert outbox.outcome(result) == expected, (result, outbox.outcome(result))
    row, _ = row_for({"email": outbox.skipped("rule email off"), "teams": outbox.sent(5, T0)},
                     kind="rule_fire", routing="rule", severity="critical")
    assert row["state"] == "delivered" and row["priority"] == 1
    ok("outcome classification (permanent / transient / config) and critical priority")


def test_secret_free():
    nasty = f"post to {WEBHOOK} failed\r\nwith password {PASSWORD} and http://10.0.0.1:25/x " + "x" * 600
    text = outbox.clean_error(nasty, secrets=[PASSWORD, WEBHOOK])
    assert WEBHOOK not in text and "SIGNATURE123" not in text and PASSWORD not in text
    assert "\r" not in text and "\n" not in text and "http" not in text and len(text) <= 300, text
    _config["smtp_password"], _config["teams_webhook_url"] = PASSWORD, WEBHOOK
    row, att = outbox.build_row("rule_fire", "rule", "k", 1, 2, None, "warning", f"title {PASSWORD}",
                                {"email": outbox.failed(nasty), "teams": outbox.skipped(f"bad {WEBHOOK}")})
    blob = repr((row, att))
    assert PASSWORD not in blob and "SIGNATURE123" not in blob, blob
    assert outbox.clean_error(None) is None and outbox.clean_error("") is None
    ok("errors, reasons and title are single-line, capped and secret-free")


def test_modes():
    for value, expected in (("off", "off"), ("OFF ", "off"), ("shadow", "shadow"), ("on", "shadow"),
                            ("bogus", "shadow"), (None, "shadow"), ("", "shadow")):
        _config["notification_outbox_mode"] = value
        assert outbox.mode() == expected, (value, outbox.mode())
    _config["notification_outbox_mode"] = "shadow"
    ok("mode: off / shadow, 'on' and unknown values behave as shadow")


class BoomSession:
    def __init__(self, message):
        self.message, self.touched = message, False

    def begin_nested(self):
        self.touched = True
        raise RuntimeError(self.message)


async def test_never_raises():
    cap = Capture()
    logging.getLogger("outbox").addHandler(cap)
    _config["notification_outbox_mode"] = "shadow"
    session = BoomSession(f"connection to {WEBHOOK} lost, password={PASSWORD}")
    result = await outbox.record_inline(session, "rule_fire", "rule", "k1", 1, 1, None, "warning", "t",
                                        {"teams": outbox.sent(1, T0)}, source="test")
    assert result is None and session.touched
    warnings = [r for r in cap.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, [r.getMessage() for r in cap.records]
    msg = warnings[0].getMessage()
    assert "could not record rule_fire k1" in msg and PASSWORD not in msg and "SIGNATURE123" not in msg, msg
    ok("record_inline: a failing session -> None, one secret-free WARNING, no exception")

    _config["notification_outbox_mode"] = "off"
    session = BoomSession("must not be touched")
    assert await outbox.record_inline(session, "rule_fire", "rule", "k2", 1, 1, None, None, "t", {}) is None
    assert not session.touched
    outbox.record_inline_soon(session_or_none=None)  # wrong kwargs: must still not raise
    _config["notification_outbox_mode"] = "shadow"
    ok("mode off: record_inline touches nothing")

    cap.records.clear()
    outbox.record_inline_soon(kind="rule_fire", routing="rule", dedupe_key="k3", alert_config_id=1, cluster_id=1,
                              trigger_ids=None, severity=None, subject_or_title="t", channel_results={},
                              unknown_argument=True)
    await outbox.drain()
    assert any("could not" in r.getMessage() for r in cap.records), [r.getMessage() for r in cap.records]
    logging.getLogger("outbox").removeHandler(cap)
    ok("record_inline_soon: a bad call in the background task -> WARNING only")


def test_soon_without_loop():
    outbox.record_inline_soon(kind="rule_fire", routing="rule", dedupe_key="k4", alert_config_id=1, cluster_id=1,
                              trigger_ids=None, severity=None, subject_or_title="t", channel_results={})
    ok("record_inline_soon with no running event loop -> no exception")


async def test_db():
    from sqlalchemy import text
    from database import SessionLocal, engine
    if SessionLocal is None:
        print("SKIP database tests (DATABASE_URL not set)")
        return
    _config["notification_outbox_mode"] = "shadow"
    async with SessionLocal() as s:
        await s.execute(text("DELETE FROM kafka_notification_outbox WHERE dedupe_key LIKE 'test:%'"))
        await s.commit()
    args = dict(kind="owner_watchdog", routing="owner", alert_config_id=None, cluster_id=None,
                trigger_ids=[7, 8], severity="critical", subject_or_title="[ISSUE] watchdog",
                channel_results={"email": outbox.failed("connection refused", 3, T0),
                                 "teams": outbox.sent(40, T0, detail={"http_status": 202})},
                source="test")
    first = await outbox.record_inline(None, dedupe_key="test:idem", **args)
    second = await outbox.record_inline(None, dedupe_key="test:idem", **args)
    assert isinstance(first, int) and second is None, (first, second)
    async with SessionLocal() as s:
        n = (await s.execute(text("SELECT count(*) FROM kafka_notification_outbox WHERE dedupe_key='test:idem'"))).scalar()
        rows = (await s.execute(text(
            "SELECT a.channel, a.attempt_no, a.outcome, a.error, o.state, o.trigger_ids, o.priority, o.origin "
            "FROM kafka_notification_attempts a JOIN kafka_notification_outbox o ON o.id=a.outbox_id "
            "WHERE o.id=:id ORDER BY a.channel"), {"id": first})).all()
    assert n == 1 and len(rows) == 2, (n, rows)
    assert [tuple(r[:4]) for r in rows] == [("email", 1, "failed_transient", "connection refused"),
                                            ("teams", 1, "sent", None)], rows
    assert rows[0].state == "degraded" and rows[0].trigger_ids == [7, 8] and rows[0].priority == 0
    assert rows[0].origin == "inline"
    ok("DB: same dedupe_key twice -> one row, two attempts, second call returns None")

    # With a caller session: savepoint only, the caller decides.
    async with SessionLocal() as s:
        rid = await outbox.record_inline(s, dedupe_key="test:rollback", **args)
        assert isinstance(rid, int)
        await s.rollback()
    async with SessionLocal() as s:
        rid = await outbox.record_inline(s, dedupe_key="test:commit", **args)
        await s.commit()
        got = (await s.execute(text(
            "SELECT dedupe_key FROM kafka_notification_outbox WHERE dedupe_key IN ('test:rollback','test:commit')"
        ))).scalars().all()
    assert got == ["test:commit"], got
    ok("DB: with a caller session the row follows the caller's commit / rollback")

    # A CHECK violation (bad kind) is a recording failure: None, no exception, session still usable.
    async with SessionLocal() as s:
        bad = await outbox.record_inline(s, "not_a_kind", "rule", "test:bad", None, None, None, None, "t", {})
        assert bad is None
        assert (await s.execute(text("SELECT 1"))).scalar() == 1
    ok("DB: a constraint violation -> None, caller's session still usable")
    async with SessionLocal() as s:
        await s.execute(text("DELETE FROM kafka_notification_outbox WHERE dedupe_key LIKE 'test:%'"))
        await s.commit()
    await engine.dispose()


async def main():
    test_states_and_outcomes()
    test_secret_free()
    test_modes()
    await test_never_raises()
    await test_db()


if __name__ == "__main__":
    test_soon_without_loop()
    asyncio.run(main())
    print(f"\n{len(PASSED)} checks passed")
