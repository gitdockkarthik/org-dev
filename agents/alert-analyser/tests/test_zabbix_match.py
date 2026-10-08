"""Tests for tools/zabbix_match.py (matching Zabbix Open alerts to their recovery notices by problem ID).
Plain asserts, no pytest needed. The cases are the real ones found on 7-8 Oct 2026.
Run from the repo root:  python3 -B agents/alert-analyser/tests/test_zabbix_match.py"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ns = {"__name__": "zabbix_match_under_test"}
for _f in ("zabbix_recovery.py", "zabbix_closure.py", "zabbix_detail.py", "zabbix_enrich.py", "zabbix_match.py"):
    _p = os.path.join(_HERE, "..", "tools", _f)
    exec(compile(open(_p, encoding="utf-8").read(), _p, "exec"), _ns)
decide_by_id = _ns["decide_by_id"]

HOST = "nvaprod2slavedb01.cztn97v5hefa.us-east-1.rds.amazonaws.com"


def title(kind, host, check, env="nvaprod", cut=None):
    m = ("[Zabbix] [Open] [Critical] [O1] [%s] [Env:%s] %s" if kind == "open" else "[Zabbix] : [Closed]  : [O1] [%s] [Env:%s]  %s") % (host, env, check)
    return m[:cut] if cut else m


def ev(aid, kind, host, check, at, pid, env="nvaprod", cut=None, status="ok", dkind="same", full=None):
    """dkind: 'same' = the description agrees with the title tag; or 'open'/'closed'/None to set it explicitly."""
    return {"id": aid, "message": title(kind, host, check, env, cut), "createdAt": at, "problem_id": pid,
            "detail_status": status, "detail_kind": (kind if dkind == "same" else dkind), "full_message": full}


def one(opens, events):
    return decide_by_id(opens, events)


# ---- the basic pairing -----------------------------------------------------
def test_an_open_is_resolved_by_the_closed_notice_with_its_problem_id():
    o = ev("o1", "open", "h1", "Jetty service is down", "2026-10-06T10:00:00Z", "500")
    c = ev("c1", "closed", "h1", "Jetty service is down", "2026-10-06T10:05:00Z", "500")
    r = one([o], [o, c])[0]
    assert r == {"id": "o1", "decision": "resolve", "reason": "closed_by_id", "closed_id": "c1", "closed_at": "2026-10-06T10:05:00+00:00"}, r


def test_sibling_triggers_with_identical_cut_titles_are_told_apart_by_id():
    # a '> 3' and a '> 5' alert on one host: OpsGenie cut both Closed titles at the same character (141 of 317 prefix matches were paired wrongly)
    chk = "Disk Queue Depth on is greater than %s"
    o3 = ev("o3", "open", HOST, chk % 3, "2026-10-06T10:00:00Z", "103", cut=129)
    o5 = ev("o5", "open", HOST, chk % 5, "2026-10-06T10:00:01Z", "105", cut=129)
    c5 = ev("c5", "closed", HOST, chk % 5, "2026-10-06T10:20:00Z", "105", cut=130)
    c3 = ev("c3", "closed", HOST, chk % 3, "2026-10-06T10:45:00Z", "103", cut=130)
    r = {x["id"]: x for x in one([o3, o5], [o3, o5, c5, c3])}
    assert r["o3"]["closed_id"] == "c3" and r["o3"]["closed_at"].startswith("2026-10-06T10:45"), "the '> 3' alert waits for its own notice"
    assert r["o5"]["closed_id"] == "c5" and r["o5"]["closed_at"].startswith("2026-10-06T10:20"), r["o5"]


def test_a_sibling_notice_alone_never_closes_an_open():
    o3 = ev("o3", "open", HOST, "Disk Queue Depth on is greater than 3", "2026-10-06T10:00:00Z", "103", cut=129)
    c5 = ev("c5", "closed", HOST, "Disk Queue Depth on is greater than 5", "2026-10-06T10:20:00Z", "105", cut=130)
    r = one([o3], [o3, c5])[0]
    assert r["decision"] == "keep" and r["reason"] == "no_recovery_for_problem" and r["closed_id"] is None, r


def test_overlapping_problems_with_the_same_title_each_wait_for_their_own_notice():
    b = ev("ob", "open", "hd", "Jetty service is down", "2026-10-06T09:55:00Z", "5000")
    a = ev("oa", "open", "hd", "Jetty service is down", "2026-10-06T10:00:00Z", "5001")
    cb = ev("cb", "closed", "hd", "Jetty service is down", "2026-10-06T10:05:00Z", "5000")
    ca = ev("ca", "closed", "hd", "Jetty service is down", "2026-10-06T10:30:00Z", "5001")
    r = {x["id"]: x for x in one([a, b], [a, b, cb, ca])}
    assert r["oa"]["closed_id"] == "ca" and r["ob"]["closed_id"] == "cb", r
    # and when A's own notice never arrived, B's notice does not close it
    r2 = {x["id"]: x for x in one([a, b], [a, b, cb])}
    assert r2["ob"]["decision"] == "resolve" and r2["oa"]["decision"] == "keep" and r2["oa"]["reason"] == "no_recovery_for_problem", r2


def test_a_notice_created_before_its_own_open_still_closes_it():
    # o1prod-amagent03: the notice of problem 277437169 was created at 14:11, a minute before its Open at 14:12
    o = ev("o", "open", "o1prod-amagent03", "Timeout Error in Apache Logs", "2026-10-01T14:12:00Z", "277437169", env="o1prod")
    c = ev("c", "closed", "o1prod-amagent03", "Timeout Error in Apache Logs", "2026-10-01T14:11:00Z", "277437169", env="o1prod")
    r = one([o], [o, c])[0]
    assert r["decision"] == "resolve" and r["closed_id"] == "c", r


def test_a_newer_open_with_the_same_title_does_not_close_an_older_one_whose_notice_never_came():
    old = ev("old", "open", "h1", "Jetty service is down", "2026-09-17T10:00:00Z", "100")
    new = ev("new", "open", "h1", "Jetty service is down", "2026-09-20T10:00:00Z", "200")
    r = {x["id"]: x for x in one([old, new], [old, new])}
    assert r["old"]["decision"] == "keep" and r["old"]["reason"] == "no_recovery_for_problem", r["old"]


def test_two_opens_of_one_problem_are_both_closed_by_its_one_notice():
    a = ev("a", "open", "h1", "Jetty service is down", "2026-10-06T10:00:00Z", "700")
    b = ev("b", "open", "h1", "Jetty service is down", "2026-10-06T10:10:00Z", "700")
    c = ev("c", "closed", "h1", "Jetty service is down", "2026-10-06T10:30:00Z", "700")
    r = one([a, b], [a, b, c])
    assert [x["decision"] for x in r] == ["resolve", "resolve"] and {x["closed_id"] for x in r} == {"c"}, r


def test_the_earliest_of_two_notices_with_one_id_is_cited():
    o = ev("o", "open", "h1", "x", "2026-10-06T10:00:00Z", "9")
    c2 = ev("c2", "closed", "h1", "x", "2026-10-06T11:00:00Z", "9")
    c1 = ev("c1", "closed", "h1", "x", "2026-10-06T10:30:00Z", "9")
    assert one([o], [o, c2, c1])[0]["closed_id"] == "c1"


# ---- alerts whose title cannot be read, or whose details are missing -------
def test_the_unexpanded_placeholder_title_is_matched_by_id_too():
    o = {"id": "o", "message": "[Zabbix] [Open] [Critical] [O1] [o1stg-um-amagent02] [{$PRIMARY_HOSTGROUP1}] is unreachable",
         "createdAt": "2026-09-21T05:16:00Z", "problem_id": "276059801", "detail_status": "ok", "detail_kind": "open", "full_message": None}
    c = {"id": "c", "message": "[Zabbix] : [Closed]  : [O1] [o1stg-um-amagent02] [{$PRIMARY_HOSTGROUP1}]  is unreachable",
         "createdAt": "2026-09-22T05:00:00Z", "problem_id": "276059801", "detail_status": "ok", "detail_kind": "closed", "full_message": None}
    assert one([o], [o, c])[0]["decision"] == "resolve"
    assert one([o], [o])[0] == {"id": "o", "decision": "keep", "reason": "no_recovery_for_problem", "closed_id": None, "closed_at": None}


def test_an_open_without_stored_details_waits_even_when_a_title_matches():
    o = ev("o", "open", "h1", "Jetty service is down", "2026-10-06T10:00:00Z", None, status=None, dkind=None)
    c = ev("c", "closed", "h1", "Jetty service is down", "2026-10-06T10:05:00Z", "5")
    r = one([o], [o, c])[0]
    assert r["decision"] == "keep" and r["reason"] == "details_pending", r


def test_a_closed_notice_without_details_is_not_used_yet():
    o = ev("o", "open", "h1", "x", "2026-10-06T10:00:00Z", "5")
    c = ev("c", "closed", "h1", "x", "2026-10-06T10:05:00Z", None, status=None, dkind=None)
    assert one([o], [o, c])[0]["reason"] == "no_recovery_for_problem"


def test_an_open_with_details_but_no_problem_id_falls_back_to_an_exact_title_only():
    o = ev("o", "open", "h1", "tungsten replicator is not ONLINE", "2026-10-06T10:00:00Z", None, status="no_description", dkind=None)
    c = ev("c", "closed", "h1", "tungsten replicator is not ONLINE", "2026-10-06T10:30:00Z", None, status=None, dkind=None)
    r = one([o], [o, c])[0]
    assert r["decision"] == "resolve" and r["reason"] == "closed_exact" and r["closed_id"] == "c", r
    # a prefix-only match is not trusted without an ID
    long_chk = "Disk Queue Depth on is greater than 5"
    op = ev("op", "open", HOST, long_chk, "2026-10-06T10:00:00Z", None, status="no_description", dkind=None, cut=129)
    cp = ev("cp", "closed", HOST, long_chk, "2026-10-06T10:20:00Z", None, status=None, dkind=None, cut=130)
    assert one([op], [op, cp])[0]["reason"] == "prefix_without_id" and one([op], [op, cp])[0]["decision"] == "review"
    # and with nothing to match it is simply kept
    assert one([o], [o])[0]["decision"] == "keep"


def test_the_rebuilt_full_title_is_used_by_the_fallback():
    o = ev("o", "open", HOST, "Disk Queue Depth on is greater than 5", "2026-10-06T10:00:00Z", None, status="no_description", dkind=None, cut=129,
           full=title("open", HOST, "Disk Queue Depth on is greater than 5"))
    c = ev("c", "closed", HOST, "Disk Queue Depth on is greater than 5", "2026-10-06T10:20:00Z", None, status=None, dkind=None, cut=130,
           full=title("closed", HOST, "Disk Queue Depth on is greater than 5"))
    r = one([o], [o, c])[0]
    assert r["decision"] == "resolve" and r["reason"] == "closed_exact", r


# ---- conflicting or broken input -------------------------------------------
def test_an_open_whose_description_says_resolved_goes_to_review():
    o = ev("o", "open", "h1", "x", "2026-10-06T10:00:00Z", "5", dkind="closed")
    assert one([o], [o])[0]["decision"] == "review" and one([o], [o])[0]["reason"] == "kind_conflict"


def test_a_closed_notice_whose_description_says_started_is_not_used():
    o = ev("o", "open", "h1", "x", "2026-10-06T10:00:00Z", "5")
    c = ev("c", "closed", "h1", "x", "2026-10-06T10:05:00Z", "5", dkind="open")
    assert one([o], [o, c])[0]["decision"] == "keep"


def test_a_closed_notice_with_an_unreadable_time_is_ignored_and_the_next_one_is_used():
    o = ev("o", "open", "h1", "x", "2026-10-06T10:00:00Z", "5")
    bad = ev("bad", "closed", "h1", "x", "not a time", "5")
    good = ev("good", "closed", "h1", "x", "2026-10-06T10:30:00Z", "5")
    assert one([o], [o, bad, good])[0]["closed_id"] == "good"


def test_a_notice_with_only_an_unreadable_time_cannot_close_an_open():
    o = ev("o", "open", "h1", "x", "2026-10-06T10:00:00Z", "5")
    bad = ev("bad", "closed", "h1", "x", "not a time", "5")
    r = one([o], [o, bad])[0]
    assert r["decision"] == "keep" and r["closed_id"] is None and r["closed_at"] is None, r


def test_an_item_that_is_not_an_open_notice_is_not_resolved():
    c = ev("c", "closed", "h1", "x", "2026-10-06T10:05:00Z", "5")
    r = one([c], [c])[0]
    assert r["decision"] == "review" and r["reason"] == "unparsed_title"


def test_the_output_has_the_same_shape_and_order_as_the_title_rules():
    a = ev("a", "open", "h1", "x", "2026-10-06T10:00:00Z", "1")
    b = ev("b", "open", "h2", "y", "2026-10-06T10:00:00Z", "2")
    ca = ev("ca", "closed", "h1", "x", "2026-10-06T10:05:00Z", "1")
    r = one([b, a], [a, b, ca])
    assert [x["id"] for x in r] == ["b", "a"] and all(set(x) == {"id", "decision", "reason", "closed_id", "closed_at"} for x in r)
    assert one([], []) == []


def test_the_modules_can_share_one_namespace():
    assert callable(_ns["decide"]) and callable(_ns["decide_by_id"]) and _ns["decide"] is not _ns["decide_by_id"]
    assert _ns["RUN_TIMEOUT_SECONDS"] == 120 and _ns["ENRICH_TIMEOUT_SECONDS"] == 110
    p = _ns["parse_message"]("[Zabbix] [Open] [Critical] [O1] [h] [Env:x] CPU")
    assert p["host"] == "h" and p["env"] == "x"


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
