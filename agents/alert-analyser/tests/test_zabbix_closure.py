"""Tests for tools/zabbix_closure.py: the plan, the transaction flow and the job entry point, with a fake
database session (no real database is touched). Plain asserts, no pytest needed.
Run from the repo root:  python3 -B agents/alert-analyser/tests/test_zabbix_closure.py
The SQL statements themselves are NOT exercised here; the dry-run job does that on every cycle."""
import asyncio
import os
import sys
import uuid
from collections import namedtuple

_HERE = os.path.dirname(os.path.abspath(__file__))
_ns = {"__name__": "zabbix_closure_under_test"}
for _p in (os.path.join(_HERE, "..", "tools", "zabbix_recovery.py"), os.path.join(_HERE, "..", "tools", "zabbix_closure.py")):
    exec(compile(open(_p, encoding="utf-8").read(), _p, "exec"), _ns)
plan, apply, run_job, summarise, parse_ts = _ns["plan"], _ns["apply"], _ns["run_job"], _ns["summarise"], _ns["parse_ts"]

OpenRow = namedtuple("OpenRow", "id alert_id message created_at outcome has_outcome")
EvRow = namedtuple("EvRow", "id message created_at")
STG = "o1stgslavedb1.cztn97v5hefa.us-east-1.rds.amazonaws.com"


def om(host, check, env="o1prod"):
    return "[Zabbix] [Open] [Critical] [O1] [%s] [Env:%s] %s" % (host, env, check)


def cm(host, check, env="o1prod"):
    return "[Zabbix] : [Closed]  : [O1] [%s] [Env:%s]  %s" % (host, env, check)


def iid(n):
    return str(uuid.UUID(int=n))


def scenario():
    """1 recovered (no outcome), 2 recovered with FAILED / failed, 3 recovered with SUCCESS / unknown outcome,
    1 recovered after 30 days, 1 unrecovered, 1 ambiguous."""
    o, e = [], []

    def add(n, host, ts, outcome=None, has=False, closed=None):
        m = om(host, "Jetty service is down")
        o.append(OpenRow(iid(n), "alert-%d" % n, m, ts, outcome, has))
        e.append(EvRow("alert-%d" % n, m, ts))
        if closed:
            e.append(EvRow("closed-%d" % n, cm(host, "Jetty service is down"), closed))

    add(1, "h1", "2026-10-06T10:00:00Z", closed="2026-10-06T10:05:00Z")
    add(2, "h2", "2026-10-06T10:00:00Z", "FAILED", True, closed="2026-10-06T11:30:00Z")
    add(3, "h3", "2026-10-06T10:00:00Z", "failed", True, closed="2026-10-06T11:30:00Z")
    add(4, "h4", "2026-10-06T10:00:00Z", "SUCCESS", True, closed="2026-10-06T10:20:00Z")
    add(5, "h5", "2026-10-06T10:00:00Z", None, True, closed="2026-10-06T10:20:00Z")
    add(6, "h6", "2026-09-01T10:00:00Z", closed="2026-10-01T10:00:00Z")
    add(7, "h7", "2026-10-06T10:00:00Z")
    m = om(STG, "Replica Lag on is greater tha", "o1stg")
    o.append(OpenRow(iid(8), "alert-8", m, "2026-10-06T12:00:00Z", None, False))
    e += [EvRow("alert-8", m, "2026-10-06T12:00:00Z"),
          EvRow("c4", cm(STG, "Replica Lag on is greater than 4", "o1stg"), "2026-10-06T12:05:00Z"),
          EvRow("c1", cm(STG, "Replica Lag on is greater than 1", "o1stg"), "2026-10-06T12:06:00Z")]
    return o, e


# ---- fake database --------------------------------------------------------
class FakeResult:
    def __init__(self, rowcount=1, scalar=0, rows=None):
        self.rowcount, self._scalar, self._rows = rowcount, scalar, rows

    def scalar(self):
        return self._scalar

    def fetchall(self):
        return self._rows


class World:
    def __init__(self, opens, events):
        self.opens, self.events = opens, events
        self.not_escalated, self.updated, self.history, self.outcome = set(), [], [], None
        self.history_rowcount, self.other_stamped, self.extra_stamped, self.raise_on_update = 1, 0, 0, False
        self.select_delay = 0


class _Tx:
    def __init__(self, w):
        self.w = w

    async def __aenter__(self):
        self.w.outcome = None
        return self

    async def __aexit__(self, et, ev, tb):
        self.w.outcome = "ROLLBACK" if et else "COMMIT"
        return False


class FakeSession:
    def __init__(self, w):
        self.w = w

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def begin(self):
        return _Tx(self.w)

    async def execute(self, stmt, params=None):
        w, s = self.w, stmt.strip()
        if s == _ns["OPENS_SQL"].strip():
            if w.select_delay:
                await asyncio.sleep(w.select_delay)
            return FakeResult(rows=w.opens)
        if s == _ns["EVENTS_SQL"].strip():
            return FakeResult(rows=w.events)
        if s.startswith("SET LOCAL"):
            return FakeResult()
        if "UPDATE incident_management.incidents" in s:
            if w.raise_on_update:
                raise RuntimeError("simulated database error")
            i = str(params["id"])
            assert params["closed_at"].tzinfo is not None, "closed_at must be timezone-aware"
            if i in w.not_escalated:
                return FakeResult(rowcount=0)
            w.updated.append((i, params["closed_at"]))
            return FakeResult(rowcount=1)
        if "INSERT INTO incident_management.incident_status_history" in s:
            w.history.append(str(params["id"]))
            return FakeResult(rowcount=w.history_rowcount)
        if "NOT ILIKE" in s and "updated_at = now()" in s:
            return FakeResult(scalar=w.other_stamped)
        if "updated_at = now()" in s:
            return FakeResult(scalar=len(w.updated) + w.extra_stamped)
        raise AssertionError("unexpected sql: " + s)


def go(commit, w, timeout=5):
    return asyncio.run(run_job(commit, session_factory=lambda: FakeSession(w), text=lambda s: s, timeout=timeout))


def world():
    o, e = scenario()
    return World(o, e)


# ---- plan -----------------------------------------------------------------
def test_plan_actions_and_the_agent_outcome_rule():
    o, e = scenario()
    by = {r["alert_id"]: r["action"] for r in plan(o, e)}
    assert by["alert-1"] == "resolve", by
    assert by["alert-2"] == "resolve" and by["alert-3"] == "resolve", "a FAILED outcome (any case) must still resolve"
    assert by["alert-4"] == "skip_agent_outcome", "a SUCCESS outcome belongs to the other agent"
    assert by["alert-5"] == "skip_agent_outcome", "an outcome of unknown shape is left alone"
    assert by["alert-7"] == "keep" and by["alert-8"] == "review", by


def test_there_is_no_gap_limit():
    o, e = scenario()
    r = {x["alert_id"]: x for x in plan(o, e)}["alert-6"]
    assert r["action"] == "resolve" and r["reason"] == "closed_exact", r


# ---- transaction flow -----------------------------------------------------
def test_dry_run_rolls_back_but_runs_every_write():
    w = world()
    s = go(False, w)
    assert w.outcome == "ROLLBACK" and len(w.updated) == 4 and len(w.history) == 4, (w.outcome, w.updated)
    assert s.startswith("dry run: would resolve 4 (exact 4, prefix 0)"), s


def test_live_run_commits_only_resolve_rows():
    w = world()
    s = go(True, w)
    ids = {iid(1), iid(2), iid(3), iid(6)}
    assert w.outcome == "COMMIT" and {u[0] for u in w.updated} == ids and set(w.history) == ids, (w.outcome, w.updated)
    assert s.startswith("live: resolved 4 (exact 4, prefix 0)") and "agent outcome not FAILED 2" in s, s


def test_nothing_to_resolve_writes_nothing():
    o, e = scenario()
    w = World([x for x in o if x.alert_id in ("alert-7", "alert-8")], e)
    s = go(True, w)
    assert w.updated == [] and w.outcome is None, "no transaction should be opened when there is nothing to do"
    assert s.startswith("live: resolved 0"), s


def test_one_incident_resolved_meanwhile_is_skipped_not_fatal():
    w = world()
    w.not_escalated.add(iid(1))
    s = go(True, w)
    assert w.outcome == "COMMIT" and len(w.updated) == 3 and "skipped (no longer ESCALATED) 1" in s, (w.outcome, s)


def _fails(w, commit=True):
    try:
        go(commit, w)
    except RuntimeError as e:
        return str(e)
    raise AssertionError("expected the run to fail")


def test_stamp_mismatch_rolls_back_and_fails_the_run():
    w = world()
    w.extra_stamped = 1
    assert "differ from rows updated" in _fails(w) and w.outcome == "ROLLBACK"


def test_non_zabbix_row_touched_rolls_back_and_fails_the_run():
    w = world()
    w.other_stamped = 1
    assert "non-Zabbix" in _fails(w) and w.outcome == "ROLLBACK"


def test_history_failure_rolls_back_and_fails_the_run():
    w = world()
    w.history_rowcount = 0
    assert "status history insert" in _fails(w) and w.outcome == "ROLLBACK"


def test_database_error_rolls_back_and_fails_the_run():
    w = world()
    w.raise_on_update = True
    assert _fails(w).startswith("ROLLED BACK on error") and w.outcome == "ROLLBACK"


def test_too_many_skipped_rolls_back_and_fails_the_run():
    o, e = [], []
    for n in range(1, 13):  # 12 recoverable incidents; the guard tolerates at most max(5, 1%) that changed meanwhile
        m = om("h%d" % n, "Jetty service is down")
        o.append(OpenRow(iid(n), "alert-%d" % n, m, "2026-10-06T10:00:00Z", None, False))
        e += [EvRow("alert-%d" % n, m, "2026-10-06T10:00:00Z"), EvRow("closed-%d" % n, cm("h%d" % n, "Jetty service is down"), "2026-10-06T10:05:00Z")]
    w = World(o, e)
    for n in range(1, 8):
        w.not_escalated.add(iid(n))
    assert "no longer ESCALATED" in _fails(w) and w.outcome == "ROLLBACK"
    w2 = World(o, e)
    for n in range(1, 6):
        w2.not_escalated.add(iid(n))
    s = go(True, w2)
    assert w2.outcome == "COMMIT" and "skipped (no longer ESCALATED) 5" in s, s


def test_run_is_refused_above_the_per_run_limit():
    w = world()
    old = _ns["MAX_ROWS_PER_RUN"]
    _ns["MAX_ROWS_PER_RUN"] = 3
    try:
        assert "REFUSED" in _fails(w) and w.updated == []
    finally:
        _ns["MAX_ROWS_PER_RUN"] = old


def test_a_stuck_run_times_out():
    w = world()
    w.select_delay = 1.0
    try:
        go(True, w, timeout=0.05)
    except asyncio.TimeoutError:
        return
    raise AssertionError("expected a timeout")


def test_sql_keeps_its_guards_and_reads_the_agent_outcome():
    u = _ns["UPDATE_SQL"]
    assert "status = 'ESCALATED'" in u and "[Zabbix]" in u and "GREATEST(" in u and "created_at" in u
    assert "resolution_type = 'closure_alert_correlation'" in u and "detected_via = 'message_parse'" in u
    assert "action_outcome" in _ns["OPENS_SQL"] and "ESCALATED" in _ns["OPENS_SQL"] and "[Zabbix]" in _ns["OPENS_SQL"]
    assert "RESOLVED" in _ns["HISTORY_SQL"]


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
