"""Tests for tools/incident_report.py with canned database rows and a fake session (no database is touched).
Plain asserts, no pytest needed.
Run from the repo root:  python3 -B agents/alert-analyser/tests/test_incident_report.py
The SQL statements themselves are NOT exercised here; the first call of the endpoint on the real database does that."""
import asyncio
import json
import os
import re
import sys
import uuid
from collections import namedtuple
from datetime import datetime, timedelta, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
_ns = {"__name__": "incident_report_under_test"}
for _f in ("zabbix_recovery.py", "zabbix_closure.py", "incident_report.py"):
    _p = os.path.join(_HERE, "..", "tools", _f)
    exec(compile(open(_p, encoding="utf-8").read(), _p, "exec"), _ns)
build_report, get_report, get_report_cached, bucket_of = _ns["build_report"], _ns["get_report"], _ns["get_report_cached"], _ns["bucket_of"]

OpenRow = namedtuple("OpenRow", "id alert_id message created_at outcome has_outcome action_taken_at fix_applied priority incident_created_at")
EvRow = namedtuple("EvRow", "id message created_at")
SrcRow = namedtuple("SrcRow", "message source")
SchedRow = namedtuple("SchedRow", "job_id cron_expression enabled")
RunRow = namedtuple("RunRow", "job_id status triggered_by started_at duration_seconds logs")
ResRow = namedtuple("ResRow", "outcome resolution_type n")
NOW = datetime(2026, 10, 7, 12, 0, 0, tzinfo=timezone.utc)
STG = "o1stgslavedb1.cztn97v5hefa.us-east-1.rds.amazonaws.com"


def om(host, check, env="o1prod"):
    return "[Zabbix] [Open] [Critical] [O1] [%s] [Env:%s] %s" % (host, env, check)


def cm(host, check, env="o1prod"):
    return "[Zabbix] : [Closed]  : [O1] [%s] [Env:%s]  %s" % (host, env, check)


def iid(n):
    return str(uuid.UUID(int=n))


def source_key(a):
    return "zabbix" if "[Zabbix]" in (a.get("message") or "") else (a.get("source") or "unknown").strip().lower()


def scenario():
    o, e = [], []

    def add(n, host, hours_ago, outcome=None, has=False, recovered=False, check="Jetty service is down"):
        ts = (NOW - timedelta(hours=hours_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")
        m = om(host, check)
        o.append(OpenRow(iid(n), "alert-%d" % n, m, ts, outcome, has, "2026-10-03T11:47:11+00:00" if has else None,
                         "Jenkins restart via ProdOps/job/jetty_control_service" if has else None, "P2", NOW - timedelta(hours=hours_ago)))
        e.append(EvRow("alert-%d" % n, m, ts))
        if recovered:
            e.append(EvRow("closed-%d" % n, cm(host, check), (NOW - timedelta(hours=hours_ago - 0.1)).strftime("%Y-%m-%dT%H:%M:%SZ")))

    add(1, "h1", 5, recovered=True)                            # pending closure
    add(2, "h2", 4, "FAILED", True, recovered=True)             # pending closure, fix failed
    add(3, "h3", 3, "SUCCESS", True, recovered=True)            # agent outcome
    add(4, "h4", 50, "FAILED", True)                            # no recovery, fix failed
    add(5, "h5", 20)                                            # no recovery
    m = om(STG, "Replica Lag on is greater tha", "o1stg")
    ts = (NOW - timedelta(hours=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    o.append(OpenRow(iid(6), "alert-6", m, ts, None, False, None, None, "P2", NOW - timedelta(hours=10)))
    e += [EvRow("alert-6", m, ts), EvRow("c4", cm(STG, "Replica Lag on is greater than 4", "o1stg"), (NOW - timedelta(hours=9)).strftime("%Y-%m-%dT%H:%M:%SZ")),
          EvRow("c1", cm(STG, "Replica Lag on is greater than 1", "o1stg"), (NOW - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%SZ"))]
    add(7, "h7", 30)                                            # older Open ...
    add(8, "h7", 6)                                             # ... superseded by this newer one (no recovery)
    bad = "[Zabbix] [Open] [Critical] [O1] [h9] [{$PRIMARY_HOSTGROUP1}] is unreachable"
    ts = (NOW - timedelta(hours=40)).strftime("%Y-%m-%dT%H:%M:%SZ")
    o.append(OpenRow(iid(9), "alert-9", bad, ts, None, False, None, None, "P2", NOW - timedelta(hours=40)))
    e.append(EvRow("alert-9", bad, ts))
    return o, e


def build(**kw):
    o, e = scenario()
    args = dict(opens_rows=o, event_rows=e, source_rows=[], schedule_rows=[], run_rows=[], resolved_rows=[],
                enabled_sources={"zabbix"}, source_key=source_key, now=NOW)
    args.update(kw)
    return build_report(**args)


# ---- open incidents -------------------------------------------------------
def test_every_open_incident_gets_the_right_reason():
    by = {i["alert_id"]: i for i in build()["open"]["incidents"]}
    assert by["alert-1"]["reason_code"] == "pending_closure" and "within 5 minutes" in by["alert-1"]["reason"], by["alert-1"]
    assert by["alert-2"]["reason_code"] == "pending_closure", "a failed fix is still closed by the recovery notice"
    assert by["alert-3"]["reason_code"] == "agent_outcome" and "SUCCESS" in by["alert-3"]["reason"], by["alert-3"]
    assert by["alert-4"]["reason_code"] == "no_recovery" and by["alert-5"]["reason_code"] == "no_recovery"
    assert by["alert-6"]["reason_code"] == "ambiguous_prefix"
    assert by["alert-7"]["reason_code"] == "older_open_superseded" and by["alert-8"]["reason_code"] == "no_recovery"
    assert by["alert-9"]["reason_code"] == "unparsed_title"


def test_the_fix_attempt_is_shown_next_to_the_incident():
    by = {i["alert_id"]: i for i in build()["open"]["incidents"]}
    assert by["alert-4"]["fix_attempt"]["outcome"] == "FAILED" and "Jenkins" in by["alert-4"]["fix_attempt"]["what"], by["alert-4"]
    assert by["alert-2"]["fix_attempt"]["outcome"] == "FAILED" and by["alert-5"]["fix_attempt"] is None


def test_totals_ordering_and_age():
    r = build()
    inc = r["open"]["incidents"]
    assert r["open"]["total"] == 9 == len(inc)
    assert r["open"]["by_reason"] == {"pending_closure": 2, "agent_outcome": 1, "no_recovery": 3, "ambiguous_prefix": 1,
                                      "older_open_superseded": 1, "unparsed_title": 1}, r["open"]["by_reason"]
    assert [i["created_at"] for i in inc] == sorted(i["created_at"] for i in inc), "oldest first"
    assert {i["alert_id"]: i["age_hours"] for i in inc}["alert-4"] == 50.0
    assert inc[0]["alert_id"] == "alert-4" and inc[0]["title"].startswith("[Zabbix] [Open]")


# ---- sources, closure job, resolved --------------------------------------
def test_sources_count_genuine_alerts_per_source_and_flag_paused_ones():
    rows = [SrcRow(om("h", "x"), "Email")] * 5 + [SrcRow("[New Relic] x", "New Relic")] * 3 + [SrcRow("y", "LightStep")]
    s = build(source_rows=rows)["sources"]
    assert s["enabled"] == ["zabbix"]
    assert s["alerts_24h"] == [{"source": "zabbix", "enabled": True, "genuine_alerts": 5},
                               {"source": "new relic", "enabled": False, "genuine_alerts": 3},
                               {"source": "lightstep", "enabled": False, "genuine_alerts": 1}], s["alerts_24h"]


def test_closure_job_mode_comes_from_the_enabled_schedule():
    S = SchedRow
    assert build(schedule_rows=[S("zabbix-closure", "*/5 * * * *", True), S("zabbix-closure-dry-run", "*/5 * * * *", False)])["closure_job"] == {"mode": "live", "schedule": "*/5 * * * *", "runs": []}
    assert build(schedule_rows=[S("zabbix-closure", "*/5 * * * *", False), S("zabbix-closure-dry-run", "*/10 * * * *", True)])["closure_job"]["mode"] == "dry run"
    off = build(schedule_rows=[S("zabbix-closure", "*/5 * * * *", False), S("zabbix-closure-dry-run", "*/5 * * * *", False)])["closure_job"]
    assert off["mode"] == "off"
    assert build(schedule_rows=[])["closure_job"]["mode"] == "off"
    assert build(schedule_rows=[S("zabbix-closure", "*/5 * * * *", True), S("zabbix-closure-dry-run", "*/5 * * * *", True)])["closure_job"]["mode"] == "live", "if both were enabled the live job is the one that changes data"


def test_runs_are_newest_first_limited_and_labelled():
    runs = [RunRow("zabbix-closure" if i % 2 else "zabbix-closure-dry-run", "success", "schedule", NOW - timedelta(minutes=5 * i), 1.0, "note %d" % i) for i in range(14)]
    out = build(run_rows=runs)["closure_job"]["runs"]
    assert len(out) == 10 and out[0]["note"] == "note 0" and out[0]["job"] == "closure (dry run)" and out[1]["job"] == "closure", out[:2]
    assert [r["started_at"] for r in out] == sorted((r["started_at"] for r in out), reverse=True)


def test_resolved_incidents_are_split_by_how_they_were_resolved():
    rows = [ResRow("SUCCESS", "action_resolved", 3), ResRow("ALREADY_HEALTHY", "action_resolved", 2), ResRow("CLOSED_IN_OPSGENIE", "action_resolved", 54),
            ResRow("FAILED", "closure_alert_correlation", 4), ResRow(None, "closure_alert_correlation", 100), ResRow(None, "self_healed", 7),
            ResRow(None, "pre_launch_bulk_cleanup", 5)]
    r = build(resolved_rows=rows)["resolved_30d"]
    got = {b["key"]: (b["count"], b["kind"]) for b in r["buckets"]}
    assert got == {"fixed_by_action": (3, "pipeline"), "already_healthy": (2, "outside"), "closed_in_opsgenie": (54, "outside"),
                   "recovered_after_failed_fix": (4, "outside"), "recovered_by_notice": (100, "outside"), "self_healed": (7, "outside"), "other": (5, "outside")}, got
    assert r["total"] == 175 and "agent_other" not in got, "empty buckets are left out"
    assert bucket_of("WEIRD", "action_resolved") == "agent_other" and bucket_of(None, "action_resolved") == "agent_other"
    assert [b["key"] for b in r["buckets"]][0] == "fixed_by_action", "buckets keep their fixed order"


# ---- fetching, caching, read-only ----------------------------------------
class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class FakeSession:
    def __init__(self, world):
        self.w = world

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def execute(self, stmt, params=None):
        s = stmt.strip()
        self.w["executed"].append(s)
        for name in ("OPENS_SQL", "EVENTS_SQL", "SOURCES_SQL", "SCHEDULES_SQL", "RUNS_SQL", "RESOLVED_SQL"):
            if s == _ns[name].strip():
                return FakeResult(self.w[name])
        raise AssertionError("unexpected sql: " + s)


def fake_world():
    o, e = scenario()
    return {"executed": [], "OPENS_SQL": o, "EVENTS_SQL": e, "SOURCES_SQL": [], "RESOLVED_SQL": [],
            "SCHEDULES_SQL": [SchedRow("zabbix-closure", "*/5 * * * *", True)],
            "RUNS_SQL": [RunRow("zabbix-closure", "success", "schedule", NOW, 0.8, "live: resolved 0")]}


def test_get_report_reads_only_and_serialises_to_json():
    w = fake_world()
    r = asyncio.run(get_report(lambda: FakeSession(w), lambda s: s, {"zabbix"}, source_key, now=NOW))
    assert len(w["executed"]) == 6 and all(s.upper().startswith("SELECT") for s in w["executed"]), w["executed"]
    assert r["open"]["total"] == 9 and r["closure_job"]["mode"] == "live"
    json.dumps(r)  # no datetime or other non-JSON value may remain


def test_the_report_is_cached_for_a_minute():
    _ns["_cache"].update({"at": None, "data": None})
    w = fake_world()
    clock = {"t": 1000.0}
    call = lambda: asyncio.run(get_report_cached(lambda: FakeSession(w), lambda s: s, {"zabbix"}, source_key, ttl=60, clock=lambda: clock["t"]))
    first = call()
    n = len(w["executed"])
    clock["t"] = 1030.0
    assert call() is first and len(w["executed"]) == n, "within the TTL no query may run"
    clock["t"] = 1061.0
    call()
    assert len(w["executed"]) == 2 * n, "after the TTL the report is rebuilt"
    _ns["_cache"].update({"at": None, "data": None})


def test_every_sql_statement_is_read_only():
    for name in ("OPENS_SQL", "SOURCES_SQL", "SCHEDULES_SQL", "RUNS_SQL", "RESOLVED_SQL"):
        sql = _ns[name]
        assert sql.strip().upper().startswith("SELECT"), name
        assert not re.search(r"\b(insert|update|delete|drop|alter|truncate)\b", sql, re.I), name


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print("PASS", name)
        except Exception as e:
            failed += 1
            print("FAIL", name, "->", repr(e))
    print("%d passed, %d failed" % (len(tests) - failed, failed))
    sys.exit(1 if failed else 0)
