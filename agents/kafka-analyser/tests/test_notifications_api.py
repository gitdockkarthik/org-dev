"""Tests for routes_notifications.py (notification delivery log, read-only)
against a SCRATCH Postgres with the live schema of kafka_notification_outbox,
kafka_notification_attempts (0058), kafka_alert_configs and kafka_job_runs.
The router runs in-process (httpx ASGITransport); cluster names are faked.
Covers: default window and since_hours up to 168, each filter alone and
combined, pagination, deleted rules, shadow and worker rows, the detail
endpoint, the summary, the 5 s statement timeout, bad parameters, and that
no payload body, address, URL, message id or attempt detail ever reaches a
response. Plain asserts, no pytest needed.

Run inside the agent image, never against a real database (it truncates the
four tables above):
  docker run --rm --network <scratch net> -v $PWD/agents/kafka-analyser:/app:ro \
    -v $PWD/shared:/opt/shared:ro -e PYTHONPATH=/app:/opt -e DATABASE_URL=<scratch> \
    -e OUTBOX_TEST_CONFIRM=scratch -w /app org-dev-kafka-analyser python -B tests/test_notifications_api.py
Takes about 10 seconds (the timeout case waits the real 5 s).
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

assert "scratch" in os.environ.get("OUTBOX_TEST_CONFIRM", ""), \
    "set OUTBOX_TEST_CONFIRM=scratch to confirm DATABASE_URL is a throwaway database"

import httpx  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from sqlalchemy import text  # noqa: E402

import database  # noqa: E402
import routes_notifications as RN  # noqa: E402
from routes_alerts import router as alerts_router  # noqa: E402

PASSED = []
CLUSTERS = {4: "prod-4", 8: "prod-8"}  # 9 is unknown on purpose
# Markers planted where the API must never look; none may appear in any response.
MARKERS = ("SECRET-ADDR@example.com", "hook.example", "SIGNATURE123", "BODY-MARKER", "MSGID-MARKER",
           "DETAIL-MARKER", "CARD-MARKER", "DEDUPE-MARKER", "agent_default")
FORBIDDEN_KEYS = {"payload", "dedupe_key", "email_message_id", "detail_json", "teams_ref", "webhook_url",
                  "email_warning", "claimed_by", "trigger_ids", "to", "html", "teams_card"}
ITEM_KEYS = {"id", "created_at", "kind", "source", "origin", "routing", "alert_config_id", "rule_name",
             "cluster_id", "cluster_name", "severity", "title", "state", "delivered_at",
             "email_status", "email_attempts", "email_last_error", "email_sent_at",
             "teams_status", "teams_attempts", "teams_last_error", "teams_sent_at"}
RESPONSES = []


def ok(name):
    PASSED.append(name)
    print(f"PASS {name}", flush=True)


def secret_payload(n, **extra):
    return {"title": f"Notice {n}", "mode": "on", "boot_id": "b",
            "email": {"to": ["SECRET-ADDR@example.com"], "subject": "s", "html": "<p>BODY-MARKER</p>",
                      "message_id": "<MSGID-MARKER@x>"},
            "teams_card": {"body": [{"text": "CARD-MARKER"}]}, "teams_ref": "agent_default",
            "webhook_url": "https://hook.example/webhookb2/SIGNATURE123", **extra}


# name, hours ago, kind, origin, config, cluster, email, teams, state, finished, extra payload, errors
ROWS = [
    dict(n="fire4", h=1, kind="rule_fire", origin="inline", cfg=1, cl=4, e="sent", t="sent", st="delivered"),
    dict(n="resolve8", h=2, kind="rule_resolve", origin="inline", cfg=99, cl=8, e="skipped", t="failed", st="failed",
         p={"rule_name": "Old rule"}, te="T" * 500),
    dict(n="fire9", h=3, kind="rule_fire", origin="inline", cfg=98, cl=9, e="failed", t="sent", st="degraded",
         ee="SMTP 535 authentication failed"),
    dict(n="summary", h=0.5, kind="owner_summary", origin="outbox", cfg=None, cl=None, e="sent", t="superseded",
         st="delivered", ea=2),
    dict(n="watchdog", h=0.2, kind="owner_watchdog", origin="outbox", cfg=None, cl=None, e="pending", t="standby",
         st="pending", fin=False),
    dict(n="old-pending", h=5, kind="owner_watchdog", origin="outbox", cfg=None, cl=None, e="pending", t="standby",
         st="pending", fin=False),
    dict(n="heartbeat", h=20, kind="heartbeat", origin="outbox", cfg=None, cl=None, e="skipped", t="sent", st="delivered"),
    dict(n="day2", h=30, kind="rule_fire", origin="inline", cfg=1, cl=4, e="sent", t="sent", st="delivered"),
    dict(n="day5", h=100, kind="rule_escalate", origin="inline", cfg=2, cl=8, e="sent", t="skipped", st="delivered"),
    dict(n="day9", h=200, kind="rule_fire", origin="inline", cfg=1, cl=4, e="sent", t="sent", st="delivered"),
    dict(n="longtitle", h=4, kind="rule_reminder", origin="inline", cfg=2, cl=8, e="sent", t="sent", st="delivered",
         p={"title": "L" * 300}),
] + [dict(n=f"page{i}", h=6 + i * 0.25, kind="rule_reminder", origin="inline", cfg=1, cl=4, e="sent", t="skipped",
          st="delivered") for i in range(30)]


async def setup():
    async with database.SessionLocal() as s:
        await s.execute(text("TRUNCATE kafka_notification_attempts, kafka_notification_outbox, kafka_alert_configs, "
                             "kafka_job_runs RESTART IDENTITY CASCADE"))
        await s.execute(text("INSERT INTO kafka_alert_configs (id, name, alert_type, cluster_id) VALUES "
                             "(1, 'CPU rule', 'broker.cpu_pct', 4), (2, 'URP rule', 'cluster.urp_total', 8)"))
        for r in ROWS:
            payload = secret_payload(r["n"], **r.get("p", {}))
            fin = r.get("fin", True)
            row_id = (await s.execute(text(
                "INSERT INTO kafka_notification_outbox (created_at, updated_at, kind, source, routing, origin, dedupe_key, "
                "alert_config_id, cluster_id, severity, payload, email_status, teams_status, email_attempts, teams_attempts, "
                "email_last_error, teams_last_error, email_sent_at, teams_sent_at, email_message_id, expires_at, state, "
                "delivered_at, finished_at, email_warning) VALUES (now() - make_interval(secs => :secs), now(), :kind, 'test', "
                ":routing, :origin, :dedupe, :cfg, :cl, 'warning', CAST(:payload AS jsonb), :e, :t, :ea, :ta, :ee, :te, "
                ":esent, :tsent, '<MSGID-MARKER@x>', now() + interval '1 day', :st, :dlv, :fin, 'SECRET-ADDR@example.com') "
                "RETURNING id"),
                {"secs": r["h"] * 3600, "kind": r["kind"], "routing": "rule" if r["cfg"] else "owner", "origin": r["origin"],
                 "dedupe": f"DEDUPE-MARKER:{r['n']}", "cfg": r["cfg"], "cl": r["cl"], "payload": json.dumps(payload),
                 "e": r["e"], "t": r["t"], "ea": r.get("ea", 1 if r["e"] in ("sent", "failed") else 0),
                 "ta": 1 if r["t"] in ("sent", "failed") else 0, "ee": r.get("ee"), "te": r.get("te"),
                 "esent": None, "tsent": None, "st": r["st"], "dlv": None,
                 "fin": None})).scalar_one()
            if fin:
                await s.execute(text("UPDATE kafka_notification_outbox SET finished_at = created_at, "
                                     "email_sent_at = CASE WHEN email_status = 'sent' THEN created_at END, "
                                     "teams_sent_at = CASE WHEN teams_status = 'sent' THEN created_at END, "
                                     "delivered_at = CASE WHEN state = 'delivered' THEN created_at END WHERE id = :id"),
                                {"id": row_id})
            r["id"] = row_id
        summary = next(r for r in ROWS if r["n"] == "summary")["id"]
        await s.execute(text(
            "INSERT INTO kafka_notification_attempts (outbox_id, channel, attempt_no, started_at, duration_ms, phase_reached, "
            "outcome, error, detail) VALUES "
            "(:id, 'email', 1, now() - interval '31 minutes', 30000, 'data', 'failed_transient', :err1, "
            " '{\"note\": \"DETAIL-MARKER SECRET-ADDR@example.com\"}'), "
            "(:id, 'email', 2, now() - interval '30 minutes', 900, 'done', 'sent', NULL, NULL), "
            "(:id, 'teams', 0, NULL, NULL, NULL, 'skipped', 'email sent; Teams fallback not needed', NULL)"),
            {"id": summary, "err1": "421 try again later " + "x" * 400})
        await s.execute(text(
            "INSERT INTO kafka_job_runs (job_id, triggered_by, status, started_at, ended_at, duration_seconds, logs, created_at) VALUES "
            "(:j, 'schedule', 'success', now() - interval '10 minutes', now() - interval '599 seconds', 1.0, 'SECRET-ADDR@example.com', now()), "
            "(:j, 'schedule', 'success', now() - interval '3 minutes', now() - interval '178 seconds', 2.5, '', now()), "
            "('other-job', 'schedule', 'success', now() - interval '1 minute', now(), 1.0, '', now()), "
            "(:j, 'schedule', 'failed', now() - interval '2 minutes', now() - interval '119 seconds', 1.0, '', now())"),
            {"j": RN._DELIVERY_JOB_ID})
        await s.commit()


def app():
    a = FastAPI()
    a.include_router(alerts_router)  # included first, as in main.py: must not shadow the new paths
    a.include_router(RN.router)
    return a


async def get(client, path, **params):
    resp = await client.get(path, params=params)
    RESPONSES.append((path, params, resp.status_code, resp.text))
    return resp


def ids(resp):
    return [n["id"] for n in resp.json()["notifications"]]


def expect(pred, hours=24):
    rows = [r for r in ROWS if r["h"] < hours and pred(r)]
    return [r["id"] for r in sorted(rows, key=lambda r: (r["h"], -r["id"]))]


def by_name(n):
    return next(r for r in ROWS if r["n"] == n)


async def main():
    await setup()

    async def fake_names():
        return dict(CLUSTERS)
    RN._get_cluster_names = fake_names
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app()), base_url="http://t") as c:
        L = "/alerts/notifications"

        # Default window, ordering, total_count, item shape
        r = await get(c, L, limit=200)
        assert r.status_code == 200, r.text
        assert ids(r) == expect(lambda r: True), (ids(r), expect(lambda r: True))
        body = r.json()
        assert body["total_count"] == len(expect(lambda r: True)) and body["since_hours"] == 24 and body["limit"] == 200
        assert all(set(n) == ITEM_KEYS for n in body["notifications"]), set(body["notifications"][0])
        assert [n["created_at"] for n in body["notifications"]] == sorted((n["created_at"] for n in body["notifications"]), reverse=True)
        r = await get(c, L)
        assert len(ids(r)) == 25 and r.json()["limit"] == 25 and r.json()["offset"] == 0
        ok("default window: last 24 h only, newest first, default limit 25, total_count, exact item keys")

        for hours in (1, 48, 167, 168):
            r = await get(c, L, since_hours=hours, limit=200)
            assert ids(r) == expect(lambda r: True, hours) and r.json()["total_count"] == len(expect(lambda r: True, hours)), hours
        assert by_name("day5")["id"] in ids(r) and by_name("day9")["id"] not in ids(r)
        ok("since_hours 1, 48, 167, 168: matching rows; 168 includes 100 h, excludes 200 h")

        # Each filter alone
        cases = {
            "kind=rule_fire": ({"kind": "rule_fire"}, lambda r: r["kind"] == "rule_fire"),
            "state=pending": ({"state": "pending"}, lambda r: r["st"] == "pending"),
            "state=failed": ({"state": "failed"}, lambda r: r["st"] == "failed"),
            "channel=email": ({"channel": "email"}, lambda r: r["e"] != "skipped"),
            "channel=teams": ({"channel": "teams"}, lambda r: r["t"] != "skipped"),
            "channel_status=failed": ({"channel_status": "failed"}, lambda r: "failed" in (r["e"], r["t"])),
            "channel_status=standby": ({"channel_status": "standby"}, lambda r: "standby" in (r["e"], r["t"])),
            "channel=teams&channel_status=failed": ({"channel": "teams", "channel_status": "failed"}, lambda r: r["t"] == "failed"),
            "channel=email&channel_status=failed": ({"channel": "email", "channel_status": "failed"}, lambda r: r["e"] == "failed"),
            "cluster_id=8": ({"cluster_id": 8}, lambda r: r["cl"] == 8),
            "alert_config_id=1": ({"alert_config_id": 1}, lambda r: r["cfg"] == 1),
            "alert_config_id=99 (deleted)": ({"alert_config_id": 99}, lambda r: r["cfg"] == 99),
        }
        for label, (params, pred) in cases.items():
            r = await get(c, L, limit=200, **params)
            assert r.status_code == 200 and ids(r) == expect(pred) and r.json()["total_count"] == len(expect(pred)), \
                (label, ids(r), expect(pred))
            assert expect(pred), f"{label}: fixture gives no rows"
        ok(f"each filter alone ({len(cases)} cases), ids and total_count")

        combined = {
            "kind+cluster+channel+status": ({"kind": "rule_fire", "cluster_id": 4, "channel": "email", "channel_status": "sent"},
                                            lambda r: r["kind"] == "rule_fire" and r["cl"] == 4 and r["e"] == "sent"),
            "config+state+168h": ({"alert_config_id": 1, "state": "delivered", "since_hours": 168},
                                  lambda r: r["cfg"] == 1 and r["st"] == "delivered"),
            "kind+channel=teams": ({"kind": "rule_reminder", "channel": "teams"}, lambda r: r["kind"] == "rule_reminder" and r["t"] != "skipped"),
            "no match": ({"kind": "deadman"}, lambda r: False),
        }
        for label, (params, pred) in combined.items():
            hours = params.get("since_hours", 24)
            r = await get(c, L, limit=200, **params)
            assert ids(r) == expect(pred, hours) and r.json()["total_count"] == len(expect(pred, hours)), (label, ids(r))
        ok("combined filters (4 cases, incl. one with no match)")

        # Pagination
        full = expect(lambda r: True)
        pages, offset = [], 0
        while True:
            r = await get(c, L, limit=10, offset=offset)
            assert r.json()["total_count"] == len(full) and r.json()["offset"] == offset
            if not ids(r):
                break
            pages += ids(r)
            offset += 10
        assert pages == full and offset == 40, (len(pages), offset)
        r = await get(c, L, limit=7, offset=35)
        assert ids(r) == full[35:42] and r.json()["total_count"] == len(full)
        ok(f"pagination: limit 10 pages concatenate to all {len(full)} rows; offset past the end is empty; total_count stable")

        # Names, deleted rules, origins, titles, errors
        r = await get(c, L, limit=200, since_hours=168)
        items = {n["id"]: n for n in r.json()["notifications"]}
        it = lambda name: items[by_name(name)["id"]]  # noqa: E731
        assert it("fire4")["rule_name"] == "CPU rule" and it("fire4")["cluster_name"] == "prod-4"
        assert it("resolve8")["rule_name"] == "Old rule" and it("fire9")["rule_name"] == "(deleted rule)"
        assert it("summary")["rule_name"] is None and it("summary")["cluster_id"] is None and it("summary")["cluster_name"] is None
        assert it("fire9")["cluster_name"] == "9"
        ok("rule names: live rule, deleted rule with stored name, deleted without ('(deleted rule)'), owner notice None; cluster names")
        assert {n["origin"] for n in items.values()} == {"inline", "outbox"}
        assert it("summary")["origin"] == "outbox" and it("fire4")["origin"] == "inline"
        assert it("summary")["email_attempts"] == 2 and it("summary")["teams_status"] == "superseded"
        ok("shadow (origin inline) and worker (origin outbox) rows both listed")
        assert it("longtitle")["title"] == "L" * 200 and it("fire4")["title"] == "Notice fire4"
        assert it("resolve8")["teams_last_error"] == "T" * 200 and it("fire9")["email_last_error"] == "SMTP 535 authentication failed"
        assert it("fire4")["email_sent_at"] and it("fire4")["delivered_at"] and it("resolve8")["email_sent_at"] is None
        ok("title and last errors cut to 200; sent_at and delivered_at set as stored")

        # Detail
        sid = by_name("summary")["id"]
        r = await get(c, f"{L}/{sid}")
        d = r.json()
        assert r.status_code == 200 and set(d) == ITEM_KEYS | {"attempts"} and d["id"] == sid
        assert [(a["channel"], a["attempt_no"], a["outcome"]) for a in d["attempts"]] == \
            [("email", 1, "failed_transient"), ("email", 2, "sent"), ("teams", 0, "skipped")]
        assert set(d["attempts"][0]) == {"channel", "attempt_no", "started_at", "duration_ms", "phase_reached", "outcome", "error"}
        assert len(d["attempts"][0]["error"]) == 200 and d["attempts"][0]["duration_ms"] == 30000 and d["attempts"][0]["phase_reached"] == "data"
        assert d["attempts"][2]["started_at"] is None
        r = await get(c, f"{L}/{by_name('resolve8')['id']}")
        assert r.json()["attempts"] == [] and r.json()["rule_name"] == "Old rule"
        r = await get(c, f"{L}/{by_name('day9')['id']}")
        assert r.status_code == 200, "detail is not limited to 168 h"
        r = await get(c, f"{L}/999999")
        assert r.status_code == 404
        ok("detail: same fields plus attempts (ordered, error cut to 200), no attempts -> [], 404 for a missing id")

        # Summary
        r = await get(c, f"{L}/summary")
        s = r.json()
        day = [x for x in ROWS if x["h"] < 24]
        assert r.status_code == 200 and s["total"] == len(day) and s["window_hours"] == 24
        for state in ("pending", "delivered", "degraded", "failed", "suppressed"):
            assert s["by_state"][state] == sum(1 for x in day if x["st"] == state), state
        for ch, key in (("email", "e"), ("teams", "t")):
            for status, count in s["by_channel_status"][ch].items():
                assert count == sum(1 for x in day if x[key] == status), (ch, status)
        assert len(s["by_channel_status"]["email"]) == 8
        old = by_name("old-pending")
        assert s["oldest_unfinished"]["id"] == old["id"] and abs(s["oldest_unfinished"]["age_seconds"] - 5 * 3600) < 120
        assert s["delivery_job"]["last_success_duration_seconds"] == 2.5 and s["delivery_job"]["job_id"] == "kafka-notification-delivery"
        assert s["last_heartbeat"]["id"] == by_name("heartbeat")["id"] and s["last_heartbeat"]["teams_status"] == "sent"
        ok("summary: 24 h counts by state and channel status, oldest unfinished (5 h), last delivery success "
           "(newest success, not the later failure), last heartbeat")

        # Bad parameters
        bad = [{"since_hours": 169}, {"since_hours": 0}, {"since_hours": "abc"}, {"since_hours": 24.5}, {"kind": "bogus"},
               {"state": "sent"}, {"channel": "sms"}, {"channel_status": "done"}, {"limit": 0}, {"limit": 201},
               {"offset": -1}, {"offset": 100001}, {"cluster_id": 0}, {"cluster_id": "x"}, {"alert_config_id": -3},
               {"kind": "rule_fire'; DROP TABLE kafka_notification_outbox; --"}]
        for params in bad:
            r = await get(c, L, **params)
            assert r.status_code == 422, (params, r.status_code)
        for path in (f"{L}/0", f"{L}/abc", f"{L}/-1", f"{L}/99999999999999999999"):
            r = await get(c, path)
            assert r.status_code == 422, (path, r.status_code)
        ok(f"bad parameters -> 422 ({len(bad)} list cases, 4 detail ids)")

        # Statement timeout: hold a lock so the query waits; it must be cancelled after 5 s.
        async with database.SessionLocal() as locker:
            await locker.execute(text("LOCK TABLE kafka_notification_outbox IN ACCESS EXCLUSIVE MODE"))
            t0 = asyncio.get_running_loop().time()
            r = await get(c, L)
            took = asyncio.get_running_loop().time() - t0
            await locker.rollback()
        assert r.status_code == 503 and 4.5 < took < 8, (r.status_code, took, r.text)
        async with database.DashboardSessionLocal() as s2:
            assert (await s2.execute(text("SHOW statement_timeout"))).scalar_one() == "0"
        r = await get(c, L)
        assert r.status_code == 200
        ok(f"statement timeout: a blocked query is cancelled after {took:.1f} s (503); the pooled connection keeps its default")

        # Dashboard pool only: with the main pool gone the routes still work.
        main_pool = database.SessionLocal
        database.SessionLocal = None
        try:
            for path in (L, f"{L}/summary", f"{L}/{sid}"):
                assert (await get(c, path)).status_code == 200, path
        finally:
            database.SessionLocal = main_pool
        ok("runs on the dashboard pool (main SessionLocal unset: all three endpoints still 200)")

    # Nothing secret in any response
    leaks = [(p, m) for p, _, _, body in RESPONSES for m in MARKERS if m in body]
    assert not leaks, leaks[:5]

    def keys(o):
        if isinstance(o, dict):
            for k, v in o.items():
                yield k
                yield from keys(v)
        elif isinstance(o, list):
            for v in o:
                yield from keys(v)
    found = {k for _, _, code, body in RESPONSES if code == 200 for k in keys(json.loads(body))} & FORBIDDEN_KEYS
    assert not found, found
    ok(f"no payload, address, URL, message id, dedupe key or attempt detail in any of {len(RESPONSES)} responses")

    await database.engine.dispose()
    await database.dashboard_engine.dispose()
    print(f"\n{len(PASSED)} checks passed")


asyncio.run(main())
