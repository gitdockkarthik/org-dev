"""Tests for tools/zabbix_recovery.py. Plain asserts, no pytest needed.
Run from the repo root:  python3 -B agents/alert-analyser/tests/test_zabbix_recovery.py
The module is loaded by file path so the tools package (and its imports) is not needed."""
import importlib.util
import os
import random
import sys
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("zabbix_recovery", os.path.join(_HERE, "..", "tools", "zabbix_recovery.py"))
zr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(zr)


def om(host, check, env="o1prod", sev="Critical"):
    return "[Zabbix] [Open] [%s] [O1] [%s] [Env:%s] %s" % (sev, host, env, check)


def cm(host, check, env="o1prod"):
    return "[Zabbix] : [Closed]  : [O1] [%s] [Env:%s]  %s" % (host, env, check)


def ev(id_, msg, ts):
    return {"id": id_, "message": msg, "createdAt": ts}


def run(opens, events):
    return {d["id"]: d for d in zr.decide(opens, events)}


# ---- parsing -------------------------------------------------------------
def test_parse_open_and_closed_and_maintenance():
    p = zr.parse_message(om("awo1-stg-api03", "Jetty service is down", "o1stg"))
    assert p["kind"] == "open" and p["host"] == "awo1-stg-api03" and p["env"] == "o1stg", p
    assert p["check"] == "Jetty service is down", p
    c = zr.parse_message(cm("awo1-stg-api03", "Jetty service is down", "o1stg"))
    assert c["kind"] == "closed" and c["check"] == "Jetty service is down", c
    m = zr.parse_message("[Maintenance] " + cm("awo1-stg-api03", "Jetty service is down", "o1stg"))
    assert m["kind"] == "closed" and m["host"] == "awo1-stg-api03", m


def test_parse_rejects_non_zabbix_and_flags_unparseable():
    assert zr.parse_message("[New Relic]  [open]  [critical] [AOS] [x] [Env:stg] Lambda Latency") is None
    assert zr.parse_message("Alert - SintecmediaAtlas-Prod - 2026-09-21T13:34Z") is None
    assert zr.parse_message(None) is None
    assert zr.parse_message("[Zabbix] [Open] [Critical] something without env")["host"] is None
    assert zr.parse_message("[Zabbix] [Open] [Closed] [h] [Env:x] c")["kind"] is None  # both tags: never guess


def test_parse_timestamps():
    a = zr.parse_ts("2026-10-06T16:04:13.23Z")
    b = zr.parse_ts("2026-10-06T16:04:13.230000+00:00")
    assert a == b and a.tzinfo is not None
    assert zr.parse_ts(datetime(2026, 10, 6, 16, 4, 13)).tzinfo is not None


# ---- resolve / keep ------------------------------------------------------
def test_open_with_later_closed_resolves_and_cites_it():
    o = ev("o1", om("h1", "Jetty service is down"), "2026-10-06T10:00:00Z")
    c = ev("c1", cm("h1", "Jetty service is down"), "2026-10-06T10:06:00Z")
    d = run([o], [o, c])["o1"]
    assert d["decision"] == "resolve" and d["reason"] == "closed_exact", d
    assert d["closed_id"] == "c1" and d["closed_at"].startswith("2026-10-06T10:06:00"), d


def test_closed_before_or_at_the_same_time_does_not_resolve():
    o = ev("o1", om("h1", "x"), "2026-10-06T10:00:00Z")
    before = ev("c0", cm("h1", "x"), "2026-10-06T09:59:59Z")
    same = ev("c1", cm("h1", "x"), "2026-10-06T10:00:00Z")
    d = run([o], [o, before, same])["o1"]
    assert d["decision"] == "keep" and d["reason"] == "newest_open_no_recovery", d


def test_different_host_or_environment_or_check_never_matches():
    o = ev("o1", om("h1", "Jetty service is down", "o1prod"), "2026-10-06T10:00:00Z")
    others = [ev("c1", cm("h2", "Jetty service is down", "o1prod"), "2026-10-06T10:05:00Z"),
              ev("c2", cm("h1", "Jetty service is down", "o1stg"), "2026-10-06T10:05:00Z"),
              ev("c3", cm("h1", "CPU Utilization is greater than 90%", "o1prod"), "2026-10-06T10:05:00Z")]
    assert run([o], [o] + others)["o1"]["decision"] == "keep"


def test_flapping_resolves_each_recovered_open_and_keeps_the_newest():
    a = ev("a", om("h1", "x"), "2026-10-06T10:00:00Z")
    c1 = ev("c1", cm("h1", "x"), "2026-10-06T10:05:00Z")
    b = ev("b", om("h1", "x"), "2026-10-06T10:10:00Z")
    r = run([a, b], [a, c1, b])
    assert r["a"]["decision"] == "resolve" and r["a"]["closed_id"] == "c1", r
    assert r["b"]["decision"] == "keep", r


def test_two_opens_then_one_closed_resolves_both_with_the_same_notice():
    a = ev("a", om("h1", "x"), "2026-10-06T10:00:00Z")
    b = ev("b", om("h1", "x"), "2026-10-06T10:02:00Z")
    c = ev("c", cm("h1", "x"), "2026-10-06T10:09:00Z")
    r = run([a, b], [a, b, c])
    assert r["a"]["decision"] == "resolve" and r["b"]["decision"] == "resolve", r
    assert r["a"]["closed_id"] == "c" and r["b"]["closed_id"] == "c", r


def test_older_open_without_recovery_is_review_not_resolved():
    a = ev("a", om("h1", "x"), "2026-10-06T10:00:00Z")
    b = ev("b", om("h1", "x"), "2026-10-06T10:30:00Z")
    r = run([a, b], [a, b])
    assert r["a"]["decision"] == "review" and r["a"]["reason"] == "older_open_superseded", r
    assert r["b"]["decision"] == "keep", r


def test_first_matching_closed_is_cited():
    o = ev("o", om("h1", "x"), "2026-10-06T10:00:00Z")
    late = ev("c2", cm("h1", "x"), "2026-10-06T12:00:00Z")
    early = ev("c1", cm("h1", "x"), "2026-10-06T10:20:00Z")
    assert run([o], [o, late, early])["o"]["closed_id"] == "c1"


def test_same_problem_requires_same_host_and_environment():
    a = zr.parse_message(om("h1", "Jetty service is down", "o1prod"))
    assert zr.same_problem(a, zr.parse_message(cm("h1", "Jetty service is down", "o1prod")))
    assert not zr.same_problem(a, zr.parse_message(cm("h1", "Jetty service is down", "o1stg")))
    assert not zr.same_problem(a, zr.parse_message(cm("h2", "Jetty service is down", "o1prod")))


# ---- truncation ----------------------------------------------------------
RDS = "nvaprod2slavedb01.cztn97v5hefa.us-east-1.rds.amazonaws.com"
STG = "o1stgslavedb1.cztn97v5hefa.us-east-1.rds.amazonaws.com"


def test_truncated_rds_titles_match_by_prefix():
    open_msg = om(RDS, "Disk Queue Depth on is", "nvaprod")
    closed_msg = cm(RDS, "Disk Queue Depth on is gre", "nvaprod")
    assert len(open_msg) == 129 and len(closed_msg) == 130, (len(open_msg), len(closed_msg))  # real lengths
    o = ev("o", open_msg, "2026-09-15T01:13:07Z")
    c = ev("c", closed_msg, "2026-09-15T01:40:00Z")
    d = run([o], [o, c])["o"]
    assert d["decision"] == "resolve" and d["reason"] == "closed_prefix", d


def test_prefix_does_not_match_when_titles_are_short():
    o = ev("o", om("h1", "Replica Lag on is greater tha"), "2026-10-06T10:00:00Z")
    c = ev("c", cm("h1", "Replica Lag on is greater than 4"), "2026-10-06T10:05:00Z")
    assert len(o["message"]) < 125 and len(c["message"]) < 125
    assert run([o], [o, c])["o"]["decision"] == "keep"


def test_ambiguous_prefix_goes_to_review():
    o = ev("o", om(STG, "Replica Lag on is greater tha", "o1stg"), "2026-10-06T10:00:00Z")
    c4 = ev("c4", cm(STG, "Replica Lag on is greater than 4", "o1stg"), "2026-10-06T10:05:00Z")
    c1 = ev("c1", cm(STG, "Replica Lag on is greater than 1", "o1stg"), "2026-10-06T10:06:00Z")
    assert min(len(o["message"]), len(c4["message"]), len(c1["message"])) >= 125
    d = run([o], [o, c4, c1])["o"]
    assert d["decision"] == "review" and d["reason"] == "ambiguous_prefix", d


def test_one_prefix_text_repeated_is_not_ambiguous():
    o = ev("o", om(STG, "Replica Lag on is greater tha", "o1stg"), "2026-10-06T10:00:00Z")
    c1 = ev("c1", cm(STG, "Replica Lag on is greater than 4", "o1stg"), "2026-10-06T10:05:00Z")
    c2 = ev("c2", cm(STG, "Replica Lag on is greater than 4", "o1stg"), "2026-10-06T10:07:00Z")
    d = run([o], [o, c1, c2])["o"]
    assert d["decision"] == "resolve" and d["closed_id"] == "c1", d


def test_exact_match_wins_over_other_prefix_matches():
    o = ev("o", om(STG, "Replica Lag on is greater tha", "o1stg"), "2026-10-06T10:00:00Z")
    other = ev("c4", cm(STG, "Replica Lag on is greater than 4", "o1stg"), "2026-10-06T10:05:00Z")
    exact = ev("cx", cm(STG, "Replica Lag on is greater tha", "o1stg"), "2026-10-06T10:08:00Z")
    d = run([o], [o, other, exact])["o"]
    assert d["decision"] == "resolve" and d["reason"] == "closed_exact" and d["closed_id"] == "cx", d


# ---- safety --------------------------------------------------------------
def test_unparsed_open_is_never_resolved():
    bad = ev("b", "[Zabbix] [Open] [Critical] no env here", "2026-10-06T10:00:00Z")
    c = ev("c", cm("h1", "no env here"), "2026-10-06T10:05:00Z")
    d = run([bad], [bad, c])["b"]
    assert d["decision"] == "review" and d["reason"] == "unparsed_title", d
    badts = ev("t", om("h1", "x"), "not a time")
    assert run([badts], [badts])["t"]["reason"] == "unparsed_time"


def test_non_zabbix_events_are_ignored():
    o = ev("o", om("h1", "x"), "2026-10-06T10:00:00Z")
    nr = ev("n", "[New Relic]  [closed] [critical] [AOS] [h1] [Env:o1prod] x", "2026-10-06T10:05:00Z")
    assert run([o], [o, nr])["o"]["decision"] == "keep"


def test_result_does_not_depend_on_input_order_and_keeps_open_order():
    events = []
    opens = []
    for i in range(30):
        host = "h%d" % (i % 4)
        o = ev("o%d" % i, om(host, "Jetty service is down"), "2026-10-06T%02d:00:00Z" % (i % 20))
        opens.append(o)
        events.append(o)
        if i % 3 == 0:
            events.append(ev("c%d" % i, cm(host, "Jetty service is down"), "2026-10-06T%02d:30:00Z" % ((i % 20) + 1 if i % 20 < 22 else 23)))
    base = zr.decide(opens, events)
    rnd = random.Random(7)
    for _ in range(5):
        shuffled = events[:]
        rnd.shuffle(shuffled)
        assert zr.decide(opens, shuffled) == base
    assert [d["id"] for d in base] == [o["id"] for o in opens]


def test_jetty_pattern_from_live_data():
    # 3 recovered Opens on a host that flaps, 1 with no Closed at all (as in the 232 stale Jetty incidents)
    opens = [ev("j%d" % i, om("api02", "Jetty service is down", "nvaprod"), "2026-10-0%dT08:00:00Z" % i) for i in (1, 2, 3)]
    opens.append(ev("lone", om("api09", "Jetty service is down", "nvaprod"), "2026-10-04T08:00:00Z"))
    closed = [ev("c%d" % i, cm("api02", "Jetty service is down", "nvaprod"), "2026-10-0%dT08:06:00Z" % i) for i in (1, 2, 3)]
    r = run(opens, opens + closed)
    assert [r["j%d" % i]["decision"] for i in (1, 2, 3)] == ["resolve"] * 3, r
    assert [r["j%d" % i]["closed_id"] for i in (1, 2, 3)] == ["c1", "c2", "c3"], r
    assert r["lone"]["decision"] == "keep", r


def test_accepts_datetime_objects():
    o = ev("o", om("h1", "x"), datetime(2026, 10, 6, 10, 0, 0, tzinfo=timezone.utc))
    c = ev("c", cm("h1", "x"), datetime(2026, 10, 6, 10, 5, 0))
    assert run([o], [o, c])["o"]["decision"] == "resolve"


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
