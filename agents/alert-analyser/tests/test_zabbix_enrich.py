"""Tests for tools/zabbix_enrich.py with a fake database session and a fake OpsGenie source (no database and no
network are touched). Plain asserts, no pytest needed.
Run from the repo root:  python3 -B agents/alert-analyser/tests/test_zabbix_enrich.py
The SQL itself is NOT exercised here; it was run against a real PostgreSQL when the module was written, and the first
manual run of the job on the box is its check on the live database."""
import asyncio
import os
import re
import sys
from collections import namedtuple

_HERE = os.path.dirname(os.path.abspath(__file__))
_ns = {"__name__": "zabbix_enrich_under_test"}
for _f in ("zabbix_recovery.py", "zabbix_closure.py", "zabbix_detail.py", "zabbix_enrich.py"):
    _p = os.path.join(_HERE, "..", "tools", _f)
    exec(compile(open(_p, encoding="utf-8").read(), _p, "exec"), _ns)
run_enrichment, build_enrich_row = _ns["run_enrichment"], _ns["build_enrich_row"]
CAND, COUNT, UPSERT = _ns["ENRICH_CANDIDATES_SQL"], _ns["ENRICH_COUNT_SQL"], _ns["ENRICH_UPSERT_SQL"]

Row = namedtuple("Row", "alert_id message")
HOST = "o1confpbapgsql.cztn97v5hefa.us-east-1.rds.amazonaws.com"
OPEN_MSG = "[Zabbix] [Open] [Severe] [O1] [%s] [Env:o1conf] Write IOPS on is greater than" % HOST
CLOSED_MSG = "[Zabbix] : [Closed]  : [O1] [%s] [Env:o1conf]  Write IOPS on is gre" % HOST
UNCUT_MSG = "[Zabbix] [Open] [Critical] [O1] [h] [Env:x] Jetty service is down"


def desc(kind, name, pid, severity="Severe"):
    head = "Problem started at 04:27:58 on 2026.09.26" if kind == "open" else "Problem has been resolved at 06:14:07 on 2026.10.03"
    return "%s\r\nProblem name: %s\r\n\r\nHost: %s\r\nSeverity: %s\r\n\r\nOriginal problem ID: %s\r\nCAUTION: x" % (head, name, HOST, severity, pid)


class FakeResult:
    def __init__(self, rows=None, value=None):
        self._rows, self._value = rows or [], value

    def fetchall(self):
        return self._rows

    def scalar(self):
        return self._value


class FakeSession:
    def __init__(self, world):
        self.w = world

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def execute(self, stmt, params=None):
        self.w["statements"].append(stmt.strip())
        if stmt == CAND:
            self.w["limit"] = params["n"]
            return FakeResult(rows=self.w["rows"][:params["n"]])
        if stmt == COUNT:
            return FakeResult(value=len(self.w["rows"]))
        if stmt == UPSERT:
            self.w["upserts"].append(dict(params))
            return FakeResult()
        raise AssertionError("unexpected statement: " + stmt[:60])

    async def commit(self):
        self.w["commits"] += 1


def world(rows):
    return {"rows": rows, "statements": [], "upserts": [], "commits": 0, "limit": None}


class FakeSource:
    def __init__(self, behaviours, delay=0.0):
        self.b, self.calls, self.live, self.peak, self.delay = behaviours, [], 0, 0, delay

    async def get_alert_description(self, alert_id):
        self.calls.append(alert_id)
        self.live += 1
        self.peak = max(self.peak, self.live)
        await asyncio.sleep(self.delay)
        self.live -= 1
        v = self.b.get(alert_id, ("ok", None))
        if v == "raise":
            raise RuntimeError("boom")
        return v


def run(rows, behaviours, **kw):
    w = world(rows)
    src = FakeSource(behaviours, kw.pop("delay", 0.0))
    note = asyncio.run(run_enrichment(src, lambda: FakeSession(w), lambda s: s, **kw))
    return note, w, src


# ---- what is stored for one alert ----------------------------------------
def test_an_open_and_a_closed_notice_get_their_problem_id_and_full_title():
    note, w, _ = run([Row("o1", OPEN_MSG), Row("c1", CLOSED_MSG)],
                     {"o1": ("ok", desc("open", "Write IOPS on is greater than 6", "276731495")),
                      "c1": ("ok", desc("closed", "Write IOPS on is greater than 6", "276731495"))})
    by = {u["alert_id"]: u for u in w["upserts"]}
    assert by["o1"]["problem_id"] == by["c1"]["problem_id"] == "276731495", by
    assert by["o1"]["full_message"] == OPEN_MSG + " 6" and by["o1"]["title_status"] == "extended" and by["o1"]["kind"] == "open"
    assert by["c1"]["full_message"] == CLOSED_MSG + "ater than 6" and by["c1"]["title_status"] == "extended", by["c1"]
    assert by["c1"]["kind"] == "closed" and by["c1"]["fetch_status"] == "ok" and w["commits"] >= 1


def test_a_title_that_was_not_cut_is_stored_as_it_is():
    _, w, _ = run([Row("u1", UNCUT_MSG)], {"u1": ("ok", desc("open", "Jetty service is down", "5"))})
    u = w["upserts"][0]
    assert u["title_status"] == "same" and u["full_message"] == UNCUT_MSG and u["problem_id"] == "5", u


def test_an_alert_without_description_a_missing_alert_and_a_failed_lookup_are_told_apart():
    _, w, _ = run([Row("a", OPEN_MSG), Row("b", OPEN_MSG), Row("c", OPEN_MSG), Row("d", OPEN_MSG)],
                  {"a": ("ok", None), "b": ("not_found", None), "c": ("error", None), "d": "raise"})
    by = {u["alert_id"]: u for u in w["upserts"]}
    assert by["a"]["fetch_status"] == "no_description" and by["b"]["fetch_status"] == "not_found"
    assert by["c"]["fetch_status"] == "error" and by["d"]["fetch_status"] == "error", "an exception counts as an error and the run goes on"
    for u in by.values():
        assert u["problem_id"] is None and u["full_message"] is None, u


def test_a_description_without_a_problem_id_is_ok_with_an_empty_id():
    p = build_enrich_row("x", UNCUT_MSG, "ok", "Problem started at 1\nProblem name: Jetty service is down\n")
    assert p["fetch_status"] == "ok" and p["problem_id"] is None and p["title_status"] == "same", p


def test_a_description_that_does_not_extend_the_title_keeps_its_id_and_no_title():
    p = build_enrich_row("x", OPEN_MSG, "ok", desc("open", "Memory is low", "77"))
    assert p["title_status"] == "mismatch" and p["full_message"] is None and p["problem_id"] == "77", p


# ---- how the batch behaves ------------------------------------------------
def test_throttling_stops_the_batch_and_the_throttled_alert_is_not_stored():
    rows = [Row("a%d" % i, OPEN_MSG) for i in range(1, 7)]
    note, w, src = run(rows, {"a3": ("throttled", None)}, concurrency=2)
    assert src.calls == ["a1", "a2", "a3", "a4"], src.calls
    assert sorted(u["alert_id"] for u in w["upserts"]) == ["a1", "a2", "a4"], "a3 was throttled and must be fetched again next time"
    assert "stopped by: OpsGenie throttling" in note, note


def test_the_time_budget_stops_the_batch():
    t = {"v": 0.0}

    def clock():
        t["v"] += 50.0
        return t["v"]
    note, w, src = run([Row("a%d" % i, OPEN_MSG) for i in range(1, 6)], {}, concurrency=1, budget_seconds=80, clock=clock)
    assert src.calls == ["a1"] and len(w["upserts"]) == 1 and "stopped by: the time budget" in note, (src.calls, note)


def test_requests_in_flight_never_exceed_the_concurrency():
    _, _, src = run([Row("a%d" % i, OPEN_MSG) for i in range(1, 13)], {}, concurrency=3, delay=0.01)
    assert src.peak == 3 and len(src.calls) == 12, (src.peak, src.calls)
    _, _, src1 = run([Row("a%d" % i, OPEN_MSG) for i in range(1, 5)], {}, delay=0.005)
    assert src1.peak == _ns["ENRICH_CONCURRENCY"] == 4, "the default is 4 at a time"


def test_only_the_limit_is_asked_for_and_the_note_counts_what_is_left():
    note, w, _ = run([Row("a%d" % i, OPEN_MSG) for i in range(1, 11)], {}, limit=4)
    assert w["limit"] == 4 and len(w["upserts"]) == 4, w["limit"]
    assert "fetched 4:" in note and "still to fetch: 6" in note and "stopped by: none" in note, note


def test_nothing_to_fetch():
    note, w, src = run([], {})
    assert note.startswith("nothing to fetch") and w["upserts"] == [] and src.calls == [], note


def test_the_note_counts_every_kind_of_result():
    note, _, _ = run([Row("a", OPEN_MSG), Row("b", OPEN_MSG), Row("c", OPEN_MSG), Row("d", UNCUT_MSG), Row("e", OPEN_MSG)],
                     {"a": ("ok", desc("open", "Write IOPS on is greater than 6", "1")), "b": ("not_found", None), "c": ("error", None),
                      "d": ("ok", desc("open", "Jetty service is down", "2")), "e": ("ok", None)})
    assert "fetched 5: ok 2, no description 1, not found 1, error 1" in note, note
    assert "titles: extended 1, same 1, mismatch 0, other 0" in note, note


# ---- the SQL contract and namespace safety ------------------------------
def test_the_job_writes_only_to_its_own_table_and_everything_else_is_read_only():
    for sql in (CAND, COUNT):
        assert sql.strip().upper().startswith("SELECT"), sql[:40]
        assert not re.search(r"\b(insert|update|delete|drop|alter|truncate)\b", sql, re.I)
    assert re.match(r"\s*INSERT INTO zabbix_alert_detail\b", UPSERT) and "ON CONFLICT (alert_id) DO UPDATE" in UPSERT
    assert not re.search(r"\b(incidents|alert_sync_history)\b", UPSERT), "the upsert must not touch incidents or the history"
    note, w, _ = run([Row("a", OPEN_MSG)], {})
    assert all(s.upper().startswith(("SELECT", "INSERT INTO ZABBIX_ALERT_DETAIL")) for s in w["statements"]), w["statements"]


def test_the_order_and_the_retry_rule_are_in_the_selection():
    assert "ESCALATED" in CAND and "ORDER BY w.prio, w.ts DESC" in CAND and "LIMIT :n" in CAND
    assert "fetch_status = 'error'" in CAND and "interval '10 minutes'" in CAND and "DISTINCT ON (alert_id)" in CAND
    assert "LIMIT" not in COUNT and "ORDER BY w.prio" not in COUNT


def test_the_modules_can_share_one_namespace():
    # scripts send several modules to the container on stdin: no name used by two modules may be overwritten
    assert _ns["RUN_TIMEOUT_SECONDS"] == 120, "the closure module's own constant is intact"
    assert _ns["ENRICH_TIMEOUT_SECONDS"] == 110
    p = _ns["parse_message"]("[Zabbix] [Open] [Critical] [O1] [h] [Env:x] CPU")
    assert p["host"] == "h" and p["env"] == "x" and callable(_ns["run_job"]) and callable(_ns["run_enrichment"]) and _ns["run_job"] is not _ns["run_enrichment"]


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
