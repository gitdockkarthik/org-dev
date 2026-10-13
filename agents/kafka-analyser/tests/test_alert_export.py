"""Tests for routes_alert_export.py (GET /alerts/configs/export, CSV, read-only)
against a SCRATCH Postgres with the live schema of kafka_alert_configs, ideally
holding a copy of the live rules. The router runs in-process (httpx
ASGITransport); cluster names are faked, or taken from EXPORT_CLUSTER_NAMES
(JSON {"id": "name"}).

Covers: header and column order, one row per rule ordered by id and the count
(EXPECT_RULES, if set, is the number of rules before the test plants its own),
booleans, empty cells for columns that do not apply, tiers of above and below
types, the lag columns, threshold of the two general types, number formats, the filename and headers, UTF-8,
formula injection in the name column, the webhook flag with no URL in the
file, no custom or blacklist names in the file, the 5 s statement timeout
(503), that no rule changes, and the round trip through the csv module.
Plain asserts, no pytest needed.

The planted rules use ids from 900001 and are deleted at the end; the rules
already in the database are only read. Run inside the agent image, never
against a real database:
  docker run --rm --network <scratch net> -v $PWD/agents/kafka-analyser:/app:ro \
    -v $PWD/shared:/opt/shared:ro -e PYTHONPATH=/app:/opt -e DATABASE_URL=<scratch> \
    -e OUTBOX_TEST_CONFIRM=scratch -e EXPECT_RULES=65 -w /app org-dev-kafka-analyser python -B tests/test_alert_export.py
Takes about 10 seconds (the timeout case waits the real 5 s).
"""
import asyncio
import csv
import io
import json
import os
import re
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

assert "scratch" in os.environ.get("OUTBOX_TEST_CONFIRM", ""), \
    "set OUTBOX_TEST_CONFIRM=scratch to confirm DATABASE_URL is a throwaway database"

import httpx  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from sqlalchemy import select, text  # noqa: E402

import database  # noqa: E402
import routes_alert_export as RE  # noqa: E402
from models import KafkaAlertConfig  # noqa: E402

PASSED = []
URL = "/alerts/configs/export"
COLUMNS = ["id", "name", "alert_type", "alert_type_label", "cluster_id", "cluster_name",
           "enabled", "severity", "direction", "info", "warning", "critical", "min_rf",
           "cooldown_minutes", "send_resolve_card", "email_enabled", "has_custom_webhook",
           "reminder_hours", "custom_count", "blacklist_count", "created_at", "updated_at", "threshold"]
BOOLS = ("enabled", "send_resolve_card", "email_enabled", "has_custom_webhook")
NUMS = ("id", "cluster_id", "info", "warning", "critical", "min_rf", "cooldown_minutes",
        "reminder_hours", "custom_count", "blacklist_count", "threshold")
TIERED = set(RE._TIERED_ALERT_TYPES)
LAG = {"cluster.consumer_lag", "cluster.connector_lag"}
BELOW = {"cluster.msg_rate_in", "cluster.slo_compliance"}
# Planted where the export must never look; none may appear in the file.
MARKERS = ("hook.example", "WEBHOOK-SIGNATURE-MARKER", "CUSTOM-NAME-MARKER", "BLACKLIST-NAME-MARKER",
           "SECRET-ADDR@example.com")
P = 900000  # planted ids are P + n

# id offset, name, alert_type, cluster_id, severity, config, webhook_url, enabled, email_enabled
PLANTED = [
    (1, "=SUM(A1:A2)", "broker.cpu_pct", 4, "critical", {"mode": "simple", "warning": 70, "critical": 90}, None, True, False),
    (2, "+cmd|' /C calc'!A0", "cluster.urp_total", 4, "warning", {"mode": "simple", "info": 1, "warning": 5, "critical": 20, "send_resolve_card": False}, None, False, True),
    (3, "-2+3", "cluster.leader_skew", 8, "warning", {"mode": "simple", "warning": 1.5, "critical": 3}, None, True, True),
    (4, "@SUM(1)", "close_wait_spike", None, "info", {"threshold": 7}, None, True, False),
    (5, "\t=HYPERLINK(\"http://x\")", "broker_unreachable", None, "critical", {"threshold": 1}, None, True, False),
    (6, "  =padded", "cluster.msg_rate_in", 8, "warning", {"mode": "simple", "direction": "below", "info": 100, "warning": 0.5, "critical": 0}, None, True, False),
    (7, "\tplain after tab", "cluster.slo_compliance", 4, "warning", {"mode": "simple", "direction": "below", "warning": 99.5, "critical": 90}, None, True, False),
    (8, "'=already quoted", "cluster.data_gb_max", 4, "warning", {"mode": "simple", "warning": 1e20, "critical": 1.5e21}, None, True, False),
    (9, "a=b, \"quoted\"\nnewline", "cluster.data_spread_pct", 999, "warning", {"mode": "simple", "warning": 0.00000015}, None, True, False),
    (10, "Überwachung – Verzögerung ✓ 日本", "cluster.consumer_lag", 4, "warning",
     {"mode": "simple", "warning": 10000, "critical": 100000, "reminder_hours": 12,
      "custom": [{"name": "CUSTOM-NAME-MARKER-a", "tiers": {"warning": 5}}, {"name": "=CUSTOM-NAME-MARKER-b", "tiers": {"critical": 9}},
                 {"name": "CUSTOM-NAME-MARKER-c", "tiers": {"info": 1}}],
      "blacklist": ["BLACKLIST-NAME-MARKER-1", "BLACKLIST-NAME-MARKER-2"]},
     "https://hook.example/webhookb2/WEBHOOK-SIGNATURE-MARKER", True, True),
    (11, "Connector lag defaults", "cluster.connector_lag", 8, "warning", {"mode": "simple", "warning": 1}, "", True, False),
    (12, "RF rule", "cluster.rf_below_min", 4, "warning", {"mode": "simple", "warning": 1, "critical": 3, "min_rf": 3}, "   ", True, False),
    (13, "Negative tier (DB edit)", "cluster.urp_total", 8, "warning", {"mode": "simple", "warning": -5, "critical": "-x"}, None, True, False),
    (14, "Unknown type", "future.type", None, "info", {}, "https://hook.example/b?SECRET-ADDR@example.com", False, False),
]


def ok(name):
    PASSED.append(name)
    print(f"PASS {name}", flush=True)


def unformula(v: str) -> str:
    """What an import does to a text cell (see routes_alert_export docstring)."""
    return v[1:] if v.startswith("'") and RE._FORMULA.match(v[1:]) else v


def parse_num(v: str):
    if v == "":
        return None
    return int(v) if re.fullmatch(r"-?\d+", v) else float(v)


def app() -> FastAPI:
    a = FastAPI()
    a.include_router(RE.router)
    return a


async def table_digest(s) -> str:
    return (await s.execute(text(
        "SELECT md5(string_agg(t::text, '|' ORDER BY id)) FROM kafka_alert_configs t"))).scalar_one()


async def main():
    names = {4: "prod-4", 8: "prod-8"}  # 999 is unknown on purpose
    if os.environ.get("EXPORT_CLUSTER_NAMES"):
        names = {int(k): v for k, v in json.loads(os.environ["EXPORT_CLUSTER_NAMES"]).items()}

    async def fake_names():
        return dict(names)
    RE._get_cluster_names = fake_names

    async with database.SessionLocal() as s:
        await s.execute(text("DELETE FROM kafka_alert_configs WHERE id > :p"), {"p": P})
        before = (await s.execute(text("SELECT count(*) FROM kafka_alert_configs"))).scalar_one()
        if os.environ.get("EXPECT_RULES"):
            assert before == int(os.environ["EXPECT_RULES"]), (before, os.environ["EXPECT_RULES"])
        for n, name, typ, cl, sev, cfg, hook, en, em in PLANTED:
            await s.execute(text(
                "INSERT INTO kafka_alert_configs (id, name, alert_type, cluster_id, severity, config, webhook_url, "
                "cooldown_minutes, enabled, email_enabled, created_at, updated_at) VALUES (:id, :name, :typ, :cl, :sev, "
                "CAST(:cfg AS jsonb), :hook, :cd, :en, :em, now() - make_interval(days => :age), now())"),
                {"id": P + n, "age": n, "name": name, "typ": typ, "cl": cl, "sev": sev, "cfg": json.dumps(cfg),
                 "hook": hook, "cd": 15 + n, "en": en, "em": em})
        await s.commit()
        digest_before = await table_digest(s)
    ok(f"scratch database: {before} existing rules, {len(PLANTED)} planted")

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app()), base_url="http://t") as c:
            t0 = datetime.now(timezone.utc)
            r = await c.get(URL)
            t1 = datetime.now(timezone.utc)
            assert r.status_code == 200, (r.status_code, r.text)
            raw = r.content

            # Headers and file name
            assert r.headers["content-type"] == "text/csv; charset=utf-8", r.headers["content-type"]
            cd = r.headers["content-disposition"]
            m = re.fullmatch(r'attachment; filename="(kafka-alert-rules-(\d{8})-(\d{4})\.csv)"', cd)
            assert m, cd
            assert m.group(1) in {f"kafka-alert-rules-{t:%Y%m%d-%H%M}.csv" for t in (t0, t1)}, (m.group(1), t0)
            assert RE.export_filename(datetime(2026, 1, 2, 23, 59, tzinfo=timezone.utc)) == "kafka-alert-rules-20260102-2359.csv"
            assert r.headers.get("cache-control") == "no-store"
            ok(f"text/csv; charset=utf-8, attachment {m.group(1)} (UTC now), no-store")

            # UTF-8, no BOM
            body = raw.decode("utf-8")  # strict
            assert not body.startswith("﻿")
            assert "Überwachung – Verzögerung ✓ 日本" in body and "Überwachung".encode() in raw
            ok("body is strict UTF-8 without BOM; non-ASCII name intact")

            rows = list(csv.reader(io.StringIO(body, newline="")))
            assert rows[0] == COLUMNS, rows[0]
            assert body.split("\r\n", 1)[0] == ",".join(COLUMNS)
            ok(f"header row is the {len(COLUMNS)} columns in order")

            data = [dict(zip(COLUMNS, row)) for row in rows[1:]]
            assert all(len(row) == len(COLUMNS) for row in rows)
            async with database.SessionLocal() as s:
                cfgs = (await s.execute(select(KafkaAlertConfig).order_by(KafkaAlertConfig.id))).scalars().all()
            assert len(data) == len(cfgs) == before + len(PLANTED), (len(data), len(cfgs))
            assert [int(d["id"]) for d in data] == [cfg.id for cfg in cfgs]
            ok(f"one row per rule: {len(data)} rows = {before} + {len(PLANTED)}, ordered by id")

            by_id = {int(d["id"]): d for d in data}
            pl = {n: by_id[P + n] for n, *_ in PLANTED}

            # Booleans
            for d in data:
                for col in BOOLS:
                    assert d[col] in ("true", "false"), (d["id"], col, d[col])
            assert [pl[n]["enabled"] for n in (1, 2, 14)] == ["true", "false", "false"]
            assert [pl[n]["email_enabled"] for n in (1, 2, 3)] == ["false", "true", "true"]
            assert pl[2]["send_resolve_card"] == "false" and pl[1]["send_resolve_card"] == "true"
            ok("booleans are true/false only; enabled, email_enabled, send_resolve_card (missing = true)")

            # Webhook flag, URL never in the file
            hooks = {d["id"]: d["has_custom_webhook"] for d in data}
            assert pl[10]["has_custom_webhook"] == "true" and pl[14]["has_custom_webhook"] == "true"
            assert pl[11]["has_custom_webhook"] == "false" and pl[12]["has_custom_webhook"] == "false"  # "" and spaces
            want = {str(cfg.id): "true" if (cfg.webhook_url or "").strip() else "false" for cfg in cfgs}
            assert hooks == want
            leaks = [mk for mk in MARKERS if mk in body]
            assert not leaks, leaks
            assert "https://" not in body  # the only http text in the data is planted rule 5's own name
            ok(f"has_custom_webhook true only for the {sum(v == 'true' for v in hooks.values())} rules with a URL; "
               "no URL, custom name, blacklist name or address in the file")

            # Lag columns
            assert (pl[10]["reminder_hours"], pl[10]["custom_count"], pl[10]["blacklist_count"]) == ("12", "3", "2")
            assert (pl[11]["reminder_hours"], pl[11]["custom_count"], pl[11]["blacklist_count"]) == ("", "0", "0")
            for d in data:
                if d["alert_type"] not in LAG:
                    assert d["reminder_hours"] == d["custom_count"] == d["blacklist_count"] == "", d
            assert pl[10]["direction"] == "above" and pl[10]["warning"] == "10000" and pl[10]["critical"] == "100000"
            # threshold: the two general types only (last column)
            assert pl[4]["threshold"] == "7" and pl[5]["threshold"] == "1"
            general = {"close_wait_spike", "broker_unreachable"}
            for d, cfg in zip(data, cfgs):
                if cfg.alert_type in general:
                    assert d["threshold"] == str((cfg.config or {})["threshold"]), (d["id"], d["threshold"])
                else:
                    assert d["threshold"] == "", (d["id"], d["alert_type"], d["threshold"])
            live_general = [d for d in data if d["alert_type"] in general and int(d["id"]) < P]
            ok(f"threshold: planted general rules 7 and 1, the {len(live_general)} existing general rules "
               f"{[d['threshold'] for d in live_general]}, empty for every other type")

            ok("lag rules: reminder_hours, custom_count, blacklist_count (missing lists count 0); empty for other types")

            # Tiers: above and below types
            assert [pl[1][k] for k in ("direction", "info", "warning", "critical")] == ["above", "", "70", "90"]
            assert [pl[2][k] for k in ("direction", "info", "warning", "critical")] == ["above", "1", "5", "20"]
            assert [pl[3][k] for k in ("warning", "critical")] == ["1.5", "3"]
            assert [pl[6][k] for k in ("direction", "info", "warning", "critical")] == ["below", "100", "0.5", "0"]
            assert [pl[7][k] for k in ("direction", "info", "warning", "critical")] == ["below", "", "99.5", "90"]
            ok("tiers: above types (direction above, unset tier empty) and below types (direction below, 0 kept)")

            # Columns that do not apply
            for n in (4, 5):
                d = pl[n]
                assert d["cluster_id"] == "" and d["cluster_name"] == "All clusters"
                assert all(d[k] == "" for k in ("direction", "info", "warning", "critical", "min_rf")), d
                assert d["severity"] in ("info", "critical")
            for d in data:
                tiered = d["alert_type"] in TIERED
                assert (d["severity"] == "") == tiered, (d["id"], d["alert_type"], d["severity"])
                assert (d["direction"] != "") == tiered
                if d["alert_type"] != "cluster.rf_below_min":
                    assert d["min_rf"] == "", d
            assert pl[12]["min_rf"] == "3"
            assert pl[4]["alert_type_label"] == "CLOSE_WAIT spike" and pl[5]["alert_type_label"] == "Broker reachability (live check)"
            assert pl[14]["alert_type_label"] == "future.type" and pl[14]["severity"] == "info"
            assert pl[9]["cluster_name"] == "999" and pl[1]["cluster_name"] == names.get(4, "4")
            ok("not applicable -> empty: general rules (no cluster_id, tiers, direction), severity empty for tier types, "
               "min_rf only on rf_below_min; labels incl. general and unknown types; unknown cluster shows its id")

            # Formula injection in the name column
            want_names = {1: "'=SUM(A1:A2)", 2: "'+cmd|' /C calc'!A0", 3: "'-2+3", 4: "'@SUM(1)",
                          5: "'\t=HYPERLINK(\"http://x\")", 6: "'  =padded", 7: "'\tplain after tab",
                          8: "''=already quoted", 9: "a=b, \"quoted\"\nnewline"}
            for n, want_name in want_names.items():
                assert pl[n]["name"] == want_name, (n, pl[n]["name"], want_name)
            for d in data:
                for col in COLUMNS:
                    if col not in NUMS:
                        v = d[col]
                        assert not re.match(r"[ \t]*[=+\-@]", v) and not v.startswith(("\t", "\r")), (d["id"], col, v)
            ok("formula injection: = + - @, tab, spaces-then-= and an existing quote are prefixed; "
               "no text cell starts like a formula")

            # Numbers: plain decimals, negatives not prefixed
            assert pl[13]["warning"] == "-5" and pl[13]["critical"] == "'-x"
            assert pl[8]["warning"] == "100000000000000000000" and pl[8]["critical"] == "1500000000000000000000"
            assert pl[9]["warning"] == "0.00000015"
            for d in data:
                for col in NUMS:
                    v = d[col]
                    assert v == "" or re.fullmatch(r"-?\d+(\.\d+)?", v) or (d["id"], col) == (str(P + 13), "critical"), (d["id"], col, v)
            ok("numbers are plain decimals (1e20, 1.5e-7 expanded); -5 is not prefixed; a non-number in a "
               "numeric slot is written as text and prefixed")

            # Round trip through the csv module equals the source data
            mismatches = []
            for cfg, d in zip(cfgs, data):
                config = cfg.config or {}
                tiered, lag = cfg.alert_type in TIERED, cfg.alert_type in LAG
                got = {
                    "id": int(d["id"]), "name": unformula(d["name"]), "alert_type": unformula(d["alert_type"]),
                    "cluster_id": parse_num(d["cluster_id"]),
                    "enabled": d["enabled"] == "true", "email_enabled": d["email_enabled"] == "true",
                    "severity": unformula(d["severity"]) or None,
                    "tiers": {t: parse_num(d[t]) if re.fullmatch(r"-?\d+(\.\d+)?|", d[t]) else unformula(d[t]) for t in ("info", "warning", "critical")},
                    "min_rf": parse_num(d["min_rf"]), "cooldown_minutes": int(d["cooldown_minutes"]),
                    "send_resolve_card": d["send_resolve_card"] == "true",
                    "has_custom_webhook": d["has_custom_webhook"] == "true",
                    "reminder_hours": parse_num(d["reminder_hours"]),
                    "custom_count": parse_num(d["custom_count"]), "blacklist_count": parse_num(d["blacklist_count"]),
                    "created_at": datetime.fromisoformat(d["created_at"]), "updated_at": datetime.fromisoformat(d["updated_at"]),
                    "threshold": parse_num(d["threshold"]),
                }
                src = {
                    "id": cfg.id, "name": cfg.name, "alert_type": cfg.alert_type, "cluster_id": cfg.cluster_id,
                    "enabled": cfg.enabled, "email_enabled": cfg.email_enabled,
                    "severity": None if tiered else cfg.severity,
                    "tiers": {t: config.get(t) if tiered else None for t in ("info", "warning", "critical")},
                    "min_rf": config.get("min_rf") if cfg.alert_type == "cluster.rf_below_min" else None,
                    "cooldown_minutes": cfg.cooldown_minutes,
                    "send_resolve_card": config.get("send_resolve_card", True) is not False,
                    "has_custom_webhook": bool((cfg.webhook_url or "").strip()),
                    "reminder_hours": config.get("reminder_hours") if lag else None,
                    "custom_count": len(config.get("custom", [])) if lag else None,
                    "blacklist_count": len(config.get("blacklist", [])) if lag else None,
                    "created_at": cfg.created_at, "updated_at": cfg.updated_at,
                    "threshold": config.get("threshold") if cfg.alert_type in ("close_wait_spike", "broker_unreachable") else None,
                }
                if got != src:
                    mismatches.append((cfg.id, {k: (got[k], src[k]) for k in src if got[k] != src[k]}))
            assert not mismatches, mismatches[:3]
            assert all(d["created_at"].endswith("+00:00") and d["updated_at"].endswith("+00:00") for d in data)
            ok(f"round trip: all {len(data)} rows read back with csv equal the database (names after un-prefixing, "
               "tiers, timestamps to the microsecond in UTC)")

            # Page filters do not apply: query parameters are ignored
            r2 = await c.get(URL, params={"cluster_id": 4, "enabled": "false", "q": "urp"})
            assert r2.status_code == 200 and len(list(csv.reader(io.StringIO(r2.text, newline="")))) == len(rows)
            ok("query parameters (page filters) are ignored: same row count")

            # Statement timeout: hold a lock so the query waits; it must be cancelled after 5 s.
            async with database.SessionLocal() as locker:
                await locker.execute(text("LOCK TABLE kafka_alert_configs IN ACCESS EXCLUSIVE MODE"))
                t0 = asyncio.get_running_loop().time()
                r = await c.get(URL)
                took = asyncio.get_running_loop().time() - t0
                await locker.rollback()
            assert r.status_code == 503 and 4.5 < took < 8, (r.status_code, took, r.text)
            assert "5s" in r.json()["detail"]
            async with database.DashboardSessionLocal() as s2:
                assert (await s2.execute(text("SHOW statement_timeout"))).scalar_one() == "0"
            assert (await c.get(URL)).status_code == 200
            ok(f"statement timeout: a blocked query is cancelled after {took:.1f} s (503); the pooled connection keeps its default")

            # Other database errors: 500 without detail
            real = RE._get_session_factory
            RE._get_session_factory = lambda dashboard=False: (_ for _ in ()).throw(RuntimeError("boom secret"))
            try:
                r = await c.get(URL)
            finally:
                RE._get_session_factory = real
            assert r.status_code == 500 and r.json() == {"detail": "Export failed"}, r.text
            ok("other failures: 500 'Export failed' (no exception text)")

            # Dashboard pool only
            main_pool = database.SessionLocal
            database.SessionLocal = None
            try:
                assert (await c.get(URL)).status_code == 200
            finally:
                database.SessionLocal = main_pool
            ok("runs on the dashboard pool (main SessionLocal unset: still 200)")

        async with database.SessionLocal() as s:
            assert await table_digest(s) == digest_before
        ok("no rule changed (table digest identical before and after all requests)")

        sample = body.split("\r\n")
        print("\nSAMPLE (first rows of the existing rules):")
        for line in sample[:8]:
            print("  " + line)
    finally:
        async with database.SessionLocal() as s:
            await s.execute(text("DELETE FROM kafka_alert_configs WHERE id > :p"), {"p": P})
            await s.commit()
            left = (await s.execute(text("SELECT count(*) FROM kafka_alert_configs"))).scalar_one()
        print(f"planted rules removed; {left} rules left")
        await database.engine.dispose()
        await database.dashboard_engine.dispose()
    print(f"\n{len(PASSED)} checks passed")


asyncio.run(main())
