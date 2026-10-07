"""Tests for scripts/recovery_cleanup.py: the planning and the transaction control flow, with a fake
database session (no real database is touched). Plain asserts, no pytest needed.
Run from the repo root:  python3 -B agents/alert-analyser/tests/test_recovery_cleanup.py
The SQL statements themselves are NOT exercised here; the rehearsal on the real database does that."""
import asyncio
import io
import os
import sys
import uuid
from collections import namedtuple

_HERE = os.path.dirname(os.path.abspath(__file__))
_ns = {"__name__": "recovery_cleanup_under_test"}
for _p in (os.path.join(_HERE, "..", "tools", "zabbix_recovery.py"), os.path.join(_HERE, "..", "scripts", "recovery_cleanup.py")):
    exec(compile(open(_p, encoding="utf-8").read(), _p, "exec"), _ns)
plan, apply, write_csv, parse_ts = _ns["plan"], _ns["apply"], _ns["write_csv"], _ns["parse_ts"]

OpenRow = namedtuple("OpenRow", "id alert_id message created_at")
EvRow = namedtuple("EvRow", "id message created_at")


def om(host, check, env="o1prod"):
    return "[Zabbix] [Open] [Critical] [O1] [%s] [Env:%s] %s" % (host, env, check)


def cm(host, check, env="o1prod"):
    return "[Zabbix] : [Closed]  : [O1] [%s] [Env:%s]  %s" % (host, env, check)


STG = "o1stgslavedb1.cztn97v5hefa.us-east-1.rds.amazonaws.com"


def scenario(n_recovered=3):
    """n recovered Opens (5 min gap), 1 recovered after 3 days, 1 keep, 1 ambiguous review."""
    opens, events = [], []
    for i in range(n_recovered):
        iid = str(uuid.UUID(int=i + 1))
        msg = om("h%d" % i, "Jetty service is down")
        opens.append(OpenRow(iid, "alert-%d" % i, msg, "2026-10-06T10:00:00Z"))
        events.append(EvRow("alert-%d" % i, msg, "2026-10-06T10:00:00Z"))
        events.append(EvRow("closed-%d" % i, cm("h%d" % i, "Jetty service is down"), "2026-10-06T10:05:00Z"))
    long_id = str(uuid.UUID(int=900))
    m = om("hlong", "CPU Utilization is greater than 90%")
    opens.append(OpenRow(long_id, "alert-long", m, "2026-10-01T10:00:00Z"))
    events += [EvRow("alert-long", m, "2026-10-01T10:00:00Z"), EvRow("closed-long", cm("hlong", "CPU Utilization is greater than 90%"), "2026-10-04T10:00:00Z")]
    keep_id = str(uuid.UUID(int=901))
    m = om("hkeep", "Jetty service is down")
    opens.append(OpenRow(keep_id, "alert-keep", m, "2026-10-06T11:00:00Z")); events.append(EvRow("alert-keep", m, "2026-10-06T11:00:00Z"))
    amb_id = str(uuid.UUID(int=902))
    m = om(STG, "Replica Lag on is greater tha", "o1stg")
    opens.append(OpenRow(amb_id, "alert-amb", m, "2026-10-06T12:00:00Z"))
    events += [EvRow("alert-amb", m, "2026-10-06T12:00:00Z"),
               EvRow("c4", cm(STG, "Replica Lag on is greater than 4", "o1stg"), "2026-10-06T12:05:00Z"),
               EvRow("c1", cm(STG, "Replica Lag on is greater than 1", "o1stg"), "2026-10-06T12:06:00Z")]
    return opens, events


# ---- fake database --------------------------------------------------------
class FakeResult:
    def __init__(self, rowcount=1, scalar=0):
        self.rowcount, self._scalar = rowcount, scalar

    def scalar(self):
        return self._scalar


class World:
    def __init__(self):
        self.not_escalated, self.updated, self.history, self.outcome = set(), [], [], None
        self.history_rowcount, self.other_stamped, self.extra_stamped, self.raise_on_update = 1, 0, 0, False


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
        if s.startswith("SET LOCAL"):
            return FakeResult()
        if "UPDATE incident_management.incidents" in s:
            if w.raise_on_update:
                raise RuntimeError("simulated database error")
            iid = str(params["id"])
            assert params["closed_at"].tzinfo is not None, "closed_at must be timezone-aware"
            if iid in w.not_escalated:
                return FakeResult(rowcount=0)
            w.updated.append((iid, params["closed_at"]))
            return FakeResult(rowcount=1)
        if "INSERT INTO incident_management.incident_status_history" in s:
            w.history.append(str(params["id"]))
            return FakeResult(rowcount=w.history_rowcount)
        if "NOT ILIKE" in s and "updated_at = now()" in s:
            return FakeResult(scalar=w.other_stamped)
        if "updated_at = now()" in s:
            return FakeResult(scalar=len(w.updated) + w.extra_stamped)
        raise AssertionError("unexpected sql: " + s)


def run_apply(rows, commit, w):
    return asyncio.run(apply(rows, commit, lambda: FakeSession(w), lambda s: s))


def planned(n=3, max_gap=24.0):
    opens, events = scenario(n)
    return plan(opens, events, max_gap)


# ---- planning -------------------------------------------------------------
def test_plan_actions_and_gap_threshold():
    rows = planned(3, 24.0)
    by = {r["alert_id"]: r for r in rows}
    assert [by["alert-%d" % i]["action"] for i in range(3)] == ["resolve"] * 3, rows
    assert by["alert-long"]["decision"] == "resolve" and by["alert-long"]["action"] == "held_gap", by["alert-long"]
    assert by["alert-long"]["gap_seconds"] == 3 * 86400
    assert by["alert-keep"]["action"] == "keep" and by["alert-amb"]["action"] == "review", rows
    assert by["alert-0"]["gap_seconds"] == 300 and by["alert-0"]["closed_alert_id"] == "closed-0"
    assert len(rows) == 6


def test_plan_threshold_is_adjustable():
    by = {r["alert_id"]: r for r in planned(3, 100.0)}
    assert by["alert-long"]["action"] == "resolve"
    by = {r["alert_id"]: r for r in planned(3, 0.01)}
    assert by["alert-0"]["action"] == "held_gap"


# ---- transaction control flow ---------------------------------------------
def test_rehearsal_does_every_write_then_rolls_back():
    rows, w = planned(), World()
    ok, note, stats = run_apply(rows, False, w)
    assert ok and w.outcome == "ROLLBACK" and note.startswith("REHEARSAL OK"), (ok, note, w.outcome)
    assert stats == {"updated": 3, "skipped": 0} and len(w.history) == 3
    assert [r["applied"] for r in rows if r["action"] == "resolve"] == ["rehearsal"] * 3
    assert all(r["applied"] == "" for r in rows if r["action"] != "resolve")


def test_commit_applies_only_resolve_rows():
    rows, w = planned(), World()
    ok, note, stats = run_apply(rows, True, w)
    assert ok and w.outcome == "COMMIT" and note == "COMMITTED", (ok, note, w.outcome)
    assert stats["updated"] == 3 and len(w.updated) == 3
    ids = {r["incident_id"] for r in rows if r["action"] == "resolve"}
    assert {u[0] for u in w.updated} == ids and set(w.history) == ids
    assert w.updated[0][1] == parse_ts("2026-10-06T10:05:00Z"), w.updated[0]
    assert [r["applied"] for r in rows if r["action"] == "resolve"] == ["yes"] * 3


def test_a_few_incidents_resolved_meanwhile_are_skipped_not_fatal():
    rows, w = planned(), World()
    w.not_escalated.add(str(uuid.UUID(int=1)))
    ok, note, stats = run_apply(rows, True, w)
    assert ok and w.outcome == "COMMIT" and stats == {"updated": 2, "skipped": 1}, (ok, note, stats)
    assert [r["applied"] for r in rows if r["action"] == "resolve"] == ["skipped_not_escalated", "yes", "yes"]


def test_too_many_skipped_rolls_back():
    opens, events = scenario(12)
    rows, w = plan(opens, events, 24.0), World()
    for i in range(1, 8):
        w.not_escalated.add(str(uuid.UUID(int=i)))
    ok, note, stats = run_apply(rows, True, w)
    assert not ok and w.outcome == "ROLLBACK" and "no longer ESCALATED" in note, (ok, note)
    assert "yes" not in [r["applied"] for r in rows], "nothing may be reported as applied after a rollback"
    assert "rolled_back" in [r["applied"] for r in rows]


def test_stamp_mismatch_rolls_back():
    rows, w = planned(), World()
    w.extra_stamped = 1
    ok, note, _ = run_apply(rows, True, w)
    assert not ok and w.outcome == "ROLLBACK" and "differ from rows updated" in note, (ok, note)


def test_non_zabbix_row_touched_rolls_back():
    rows, w = planned(), World()
    w.other_stamped = 1
    ok, note, _ = run_apply(rows, True, w)
    assert not ok and w.outcome == "ROLLBACK" and "non-Zabbix" in note, (ok, note)


def test_history_insert_failure_rolls_back():
    rows, w = planned(), World()
    w.history_rowcount = 0
    ok, note, _ = run_apply(rows, True, w)
    assert not ok and w.outcome == "ROLLBACK" and "status history insert" in note, (ok, note)


def test_database_error_rolls_back_and_reports():
    rows, w = planned(), World()
    w.raise_on_update = True
    ok, note, _ = run_apply(rows, True, w)
    assert not ok and w.outcome == "ROLLBACK" and note.startswith("ROLLED BACK on error"), (ok, note)


def test_update_sql_keeps_its_guards():
    u = _ns["UPDATE_SQL"]
    assert "status = 'ESCALATED'" in u and "[Zabbix]" in u, "update must only touch ESCALATED Zabbix incidents"
    assert "GREATEST(" in u and "created_at" in u, "resolved_at must never be earlier than created_at"
    assert "resolution_type = 'closure_alert_correlation'" in u and "detected_via = 'message_parse'" in u
    assert "RESOLVED" in _ns["HISTORY_SQL"] and "ESCALATED" in _ns["HISTORY_SQL"]


def test_summary_lists_skipped_incident_ids():
    rows, w = planned(), World()
    w.not_escalated.add(str(uuid.UUID(int=1)))
    ok, note, stats = run_apply(rows, True, w)
    err = io.StringIO()
    old, sys.stderr = sys.stderr, err
    try:
        _ns["summarise"](rows, True, ok, note, stats, 24.0)
    finally:
        sys.stderr = old
    assert str(uuid.UUID(int=1)) in err.getvalue() and "skipped incident ids" in err.getvalue(), err.getvalue()


def test_decision_list_is_written_before_any_database_write():
    """If the process dies inside apply() (for example after a commit), the CSV must already exist."""
    opens, events = scenario(3)
    state = {"selects": 0}

    class _Rows:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class _ReadSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def execute(self, stmt):
            state["selects"] += 1
            return _Rows(opens if stmt == _ns["OPENS_SQL"] else events)

    import types
    fake_sa, fake_db = types.ModuleType("sqlalchemy"), types.ModuleType("database")
    fake_sa.text = lambda s: s
    fake_db.SessionLocal = lambda: _ReadSession()
    saved = {k: sys.modules.get(k) for k in ("sqlalchemy", "database")}
    sys.modules["sqlalchemy"], sys.modules["database"] = fake_sa, fake_db
    real_apply = _ns["apply"]

    async def dying_apply(*a, **k):
        raise RuntimeError("process died inside apply")

    _ns["apply"] = dying_apply
    out, old = io.StringIO(), sys.stdout
    sys.stdout = out
    try:
        try:
            asyncio.run(_ns["main"]())
            raise AssertionError("main() should have propagated the simulated failure")
        except RuntimeError as e:
            assert "process died" in str(e)
    finally:
        sys.stdout = old
        _ns["apply"] = real_apply
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
    lines = out.getvalue().strip().splitlines()
    assert state["selects"] == 2 and lines[0].startswith("incident_id,alert_id") and len(lines) == 1 + 6, (state, lines[:2])


def test_csv_has_every_judged_incident():
    rows = planned()
    buf = io.StringIO()
    write_csv(rows, buf)
    lines = buf.getvalue().strip().splitlines()
    assert lines[0].startswith("incident_id,alert_id,open_created_at") and len(lines) == 1 + len(rows), lines[:2]


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
