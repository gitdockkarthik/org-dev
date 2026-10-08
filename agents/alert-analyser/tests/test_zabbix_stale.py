"""Tests for tools/zabbix_stale.py: the decision per incident, the transaction flow and the job entry point, with a fake
database session (no real database is touched). Plain asserts, no pytest needed.
Run from the repo root:  python3 -B agents/alert-analyser/tests/test_zabbix_stale.py
The SQL statements themselves are NOT exercised here; the dry-run job does that on every cycle."""
import asyncio
import os
import sys
import uuid
from collections import namedtuple
from datetime import datetime, timedelta, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
_ns = {"__name__": "zabbix_stale_under_test"}
_p = os.path.join(_HERE, "..", "tools", "zabbix_stale.py")
exec(compile(open(_p, encoding="utf-8").read(), _p, "exec"), _ns)
plan, apply, run_job, summarise, is_production = _ns["plan"], _ns["apply"], _ns["run_job"], _ns["summarise"], _ns["is_production"]

NOW = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
Row = namedtuple("Row", "id alert_id created_at message has_outcome outcome detail_alert host problem_name problem_id own_notice_at later_activity_at")


def iid(n):
    return str(uuid.UUID(int=n))


def msg(env, host):
    return "[Zabbix] [Open] [Critical] [O1] [%s] [Env:%s] Lack of available memory is less than 10%% on server" % (host, env)


def row(n, days, env="o1stg", details=True, notice=None, later=None, outcome=None, has=False):
    created = NOW - timedelta(days=days)
    return Row(iid(n), "alert-%d" % n, created, msg(env, "host%d" % n), has, outcome, ("alert-%d" % n) if details else None,
               "host%d" % n, "Lack of available memory", str(1000 + n), notice, later)


def scenario():
    """1 stale (staging, 8 d), 2 inferred (9 d, its trigger acted again), 3 production 10 d (kept), 4 production 15 d (stale),
    5 has its own notice, 6 no details, 7 agent outcome SUCCESS."""
    return [row(1, 8), row(2, 9, later=NOW - timedelta(days=5)), row(3, 10, env="nvaprod"), row(4, 15, env="o1prod"),
            row(5, 12, notice=NOW - timedelta(days=1)), row(6, 12, details=False), row(7, 12, outcome="SUCCESS", has=True)]


# ---- the decision ---------------------------------------------------------
def test_is_production_reads_the_environment_tag():
    assert is_production(msg("o1prod", "h")) and is_production(msg("nvaprod", "h"))
    assert not is_production(msg("o1stg", "h")) and not is_production(msg("o1pre", "h")) and not is_production(msg("o1conf", "h"))
    assert not is_production("[Zabbix] [Open] no environment tag here") and not is_production(None)


def test_a_stale_incident_with_nothing_known_is_closed_at_its_creation_time():
    r = {x["alert_id"]: x for x in plan(scenario(), NOW)}["alert-1"]
    assert r["action"] == "close_stale" and r["label"] == "stale_no_recovery", r
    assert r["resolved_at_will_be"] == NOW - timedelta(days=8), "a stale closure is stamped at created_at, outside every current window"


def test_later_activity_on_the_trigger_closes_it_as_inferred_at_that_time():
    r = {x["alert_id"]: x for x in plan(scenario(), NOW)}["alert-2"]
    assert r["action"] == "close_inferred" and r["label"] == "closure_recovery_inferred", r
    assert r["resolved_at_will_be"] == NOW - timedelta(days=5), "resolved_at is the time of the later activity, an upper bound"
    early = row(8, 9, later=NOW - timedelta(days=30))
    assert plan([early], NOW)[0]["resolved_at_will_be"] == NOW - timedelta(days=9), "never earlier than created_at"


def test_production_waits_14_days_and_the_rest_7():
    by = {x["alert_id"]: x["action"] for x in plan(scenario(), NOW)}
    assert by["alert-3"] == "keep_younger", "a production incident of 10 days is not stale yet"
    assert by["alert-4"] == "close_stale", "a production incident of 15 days is"
    assert plan([row(9, 6)], NOW)[0]["action"] == "keep_younger" and plan([row(10, 7.5)], NOW)[0]["action"] == "close_stale"


def test_an_incident_with_its_own_notice_is_left_to_the_closure_job():
    r = {x["alert_id"]: x for x in plan(scenario(), NOW)}["alert-5"]
    assert r["action"] == "skip_has_notice" and r["resolved_at_will_be"] is None, r
    assert plan([row(11, 40, notice=NOW)], NOW)[0]["action"] == "skip_has_notice", "however old it is"


def test_an_incident_without_stored_details_is_held():
    assert {x["alert_id"]: x["action"] for x in plan(scenario(), NOW)}["alert-6"] == "held_no_details"


def test_an_agent_outcome_other_than_failed_is_left_alone():
    by = {x["alert_id"]: x["action"] for x in plan(scenario(), NOW)}
    assert by["alert-7"] == "skip_agent_outcome"
    assert plan([row(12, 12, outcome="failed", has=True)], NOW)[0]["action"] == "close_stale", "a FAILED outcome (any case) still closes"
    assert plan([row(13, 12, outcome=None, has=True)], NOW)[0]["action"] == "skip_agent_outcome", "an outcome of unknown shape is left alone"


def test_the_same_incident_twice_counts_once():
    r = row(1, 8)
    assert len(plan([r, r], NOW)) == 1


def test_summarise_names_both_modes_and_every_count():
    rows = plan(scenario(), NOW)
    s = summarise(rows, False, {"updated": 0, "skipped": 0})
    assert s.startswith("dry run: would close 3 (inferred 1, stale 2) | judged 7: kept younger 1, own notice 1, agent outcome not FAILED 1, no details 1"), s
    s = summarise(rows, True, {"updated": 3, "skipped": 0})
    assert s.startswith("live: closed 3 (inferred 1, stale 2)") and s.endswith("skipped (no longer ESCALATED) 0"), s


# ---- fake database --------------------------------------------------------
class FakeResult:
    def __init__(self, rowcount=1, scalar=0, rows=None):
        self.rowcount, self._scalar, self._rows = rowcount, scalar, rows

    def scalar(self):
        return self._scalar

    def fetchall(self):
        return self._rows


class World:
    def __init__(self, candidates):
        self.candidates = candidates
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
        if s == _ns["CANDIDATES_SQL"].strip():
            assert params == {"min_days": 7}, params
            if w.select_delay:
                await asyncio.sleep(w.select_delay)
            return FakeResult(rows=w.candidates)
        if s.startswith("SET LOCAL"):
            return FakeResult()
        if "UPDATE incident_management.incidents" in s:
            if w.raise_on_update:
                raise RuntimeError("simulated database error")
            i = str(params["id"])
            assert params["closed_at"].tzinfo is not None, "closed_at must be timezone-aware"
            if i in w.not_escalated:
                return FakeResult(rowcount=0)
            w.updated.append((i, params["label"], params["closed_at"]))
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
    """The scenario with ages relative to the real clock, so that the job's own clock agrees."""
    now = datetime.now(timezone.utc)
    return World([r._replace(created_at=now - (NOW - r.created_at), later_activity_at=(now - (NOW - r.later_activity_at)) if r.later_activity_at else None,
                             own_notice_at=(now - (NOW - r.own_notice_at)) if r.own_notice_at else None) for r in scenario()])


# ---- transaction flow -----------------------------------------------------
def test_dry_run_rolls_back_but_runs_every_write():
    w = world()
    s = go(False, w)
    assert w.outcome == "ROLLBACK" and len(w.updated) == 3 and len(w.history) == 3, (w.outcome, w.updated)
    assert s.startswith("dry run: would close 3 (inferred 1, stale 2)"), s


def test_live_run_commits_only_the_close_rows_with_their_labels():
    w = world()
    s = go(True, w)
    labels = {i: lab for i, lab, _ in w.updated}
    assert w.outcome == "COMMIT" and set(labels) == {iid(1), iid(2), iid(4)} and set(w.history) == set(labels), (w.outcome, w.updated)
    assert labels[iid(1)] == "stale_no_recovery" and labels[iid(4)] == "stale_no_recovery" and labels[iid(2)] == "closure_recovery_inferred", labels
    assert s.startswith("live: closed 3 (inferred 1, stale 2)"), s


def test_nothing_to_close_writes_nothing():
    w = world()
    w.candidates = [r for r in w.candidates if r.alert_id in ("alert-3", "alert-5", "alert-6", "alert-7")]
    s = go(True, w)
    assert w.updated == [] and w.outcome is None, "no transaction should be opened when there is nothing to do"
    assert s.startswith("live: closed 0"), s


def test_one_incident_resolved_meanwhile_is_skipped_not_fatal():
    w = world()
    w.not_escalated = {iid(1)}
    s = go(True, w)
    assert w.outcome == "COMMIT" and {u[0] for u in w.updated} == {iid(2), iid(4)} and s.endswith("skipped (no longer ESCALATED) 1"), (w.outcome, s)


def test_stamp_mismatch_rolls_back_and_fails_the_run():
    w = world()
    w.extra_stamped = 1
    try:
        go(True, w)
    except RuntimeError as e:
        assert "ROLLED BACK" in str(e) and "stamped" in str(e), str(e)
    else:
        raise AssertionError("the run must fail")
    assert w.outcome == "ROLLBACK"


def test_non_zabbix_row_touched_rolls_back_and_fails_the_run():
    w = world()
    w.other_stamped = 1
    try:
        go(True, w)
    except RuntimeError as e:
        assert "non-Zabbix" in str(e), str(e)
    else:
        raise AssertionError("the run must fail")
    assert w.outcome == "ROLLBACK"


def test_history_failure_rolls_back_and_fails_the_run():
    w = world()
    w.history_rowcount = 0
    try:
        go(True, w)
    except RuntimeError as e:
        assert "status history" in str(e), str(e)
    else:
        raise AssertionError("the run must fail")
    assert w.outcome == "ROLLBACK"


def test_database_error_rolls_back_and_fails_the_run():
    w = world()
    w.raise_on_update = True
    try:
        go(True, w)
    except RuntimeError as e:
        assert "ROLLED BACK on error" in str(e), str(e)
    else:
        raise AssertionError("the run must fail")
    assert w.outcome == "ROLLBACK"


def test_too_many_skipped_rolls_back_and_fails_the_run():
    w = world()
    w.candidates = [row(n, 8) for n in range(100, 118)]
    w.not_escalated = {iid(n) for n in range(100, 107)}
    try:
        go(True, w)
    except RuntimeError as e:
        assert "no longer ESCALATED" in str(e), str(e)
    else:
        raise AssertionError("the run must fail")
    assert w.outcome == "ROLLBACK"


def test_run_is_refused_above_the_per_run_limit():
    w = world()
    now = datetime.now(timezone.utc)
    w.candidates = [r._replace(created_at=now - timedelta(days=9)) for r in (row(n, 9) for n in range(200, 232))]
    try:
        go(True, w)
    except RuntimeError as e:
        assert "REFUSED" in str(e) and "limit of 30" in str(e), str(e)
    else:
        raise AssertionError("the run must be refused")
    assert w.updated == [] and w.outcome is None


def test_a_stuck_run_times_out():
    w = world()
    w.select_delay = 1
    try:
        go(True, w, timeout=0.05)
    except asyncio.TimeoutError:
        return
    raise AssertionError("the run must time out")


def test_sql_keeps_its_guards_and_reads_the_stored_details():
    sql = _ns["CANDIDATES_SQL"]
    assert "ESCALATED" in sql and "[Zabbix] [Open]" in sql and "LEFT JOIN zabbix_alert_detail" in sql and "own_notice_at" in sql, "candidate query guards"
    assert "action_outcome" in sql and ":min_days" in sql and sql.strip().upper().startswith("WITH")
    upd = _ns["UPDATE_SQL"]
    assert "status = 'ESCALATED'" in upd and "[Zabbix]" in upd and ":label" in upd and "GREATEST" in upd
    assert "RESOLVED" in _ns["HISTORY_SQL"]
    assert (_ns["STALE_DAYS"], _ns["STALE_DAYS_PROD"], _ns["MAX_ROWS_PER_RUN"]) == (7, 14, 30)


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
