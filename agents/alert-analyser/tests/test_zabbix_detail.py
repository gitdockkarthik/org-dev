"""Tests for tools/zabbix_detail.py: plain asserts, no pytest needed. The descriptions and titles are real
ones seen on 7 Oct 2026 (Windows line endings included).
Run from the repo root:  python3 -B agents/alert-analyser/tests/test_zabbix_detail.py"""
import importlib.util
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ns = {"__name__": "zabbix_detail_under_test"}
for _f in ("zabbix_recovery.py", "zabbix_detail.py"):
    _p = os.path.join(_HERE, "..", "tools", _f)
    exec(compile(open(_p, encoding="utf-8").read(), _p, "exec"), _ns)
parse_description, full_title, extract, is_zabbix = _ns["parse_description"], _ns["full_title"], _ns["extract"], _ns["is_zabbix"]
parse_message, same_problem = _ns["parse_message"], _ns["same_problem"]

CAUTION = "CAUTION: External Sender. Please do not click on links or open attachments from senders you do not trust."
OPEN_CRLF = ("Problem started at 04:27:58 on 2026.09.26\r\nProblem name: Write IOPS on is greater than 6\r\n\r\n"
             "Host: o1confpbapgsql.cztn97v5hefa.us-east-1.rds.amazonaws.com\r\nSeverity: Severe\r\nTag:\r\nEnvironment: Env: o1conf\r\n\r\n"
             "Original problem ID: 276731495\r\n" + CAUTION)
CLOSED_CRLF = ("Problem has been resolved at 06:14:07 on 2026.10.03\r\nProblem name: Disk Queue Depth on is greater than 5\r\n"
               "Host: o1confmasterdb01.cztn97v5hefa.us-east-1.rds.amazonaws.com\r\nSeverity: Critical\r\n\r\n"
               "Original problem ID: 277705288\r\n" + CAUTION)
OPEN_LF = ("Problem started at 04:14:06 on 2026.10.07\nProblem name: Disk Queue Depth on is greater than 3\n\n"
           "Host: o1confmasterdb01.cztn97v5hefa.us-east-1.rds.amazonaws.com\nSeverity: Critical\n\nOriginal problem ID: 277705111\n" + CAUTION)
OPEN_MSG = "[Zabbix] [Open] [Severe] [O1] [o1confpbapgsql.cztn97v5hefa.us-east-1.rds.amazonaws.com] [Env:o1conf] Write IOPS on is greater than"
CLOSED_MSG = "[Zabbix] : [Closed]  : [O1] [o1confmasterdb01.cztn97v5hefa.us-east-1.rds.amazonaws.com] [Env:o1conf]  Disk Queue Depth on is great"


# ---- parse_description ----------------------------------------------------
def test_open_description_with_windows_line_endings():
    d = parse_description(OPEN_CRLF)
    assert d == {"kind": "open", "problem_id": "276731495", "problem_name": "Write IOPS on is greater than 6",
                 "host": "o1confpbapgsql.cztn97v5hefa.us-east-1.rds.amazonaws.com", "severity": "Severe"}, d


def test_closed_description():
    d = parse_description(CLOSED_CRLF)
    assert d["kind"] == "closed" and d["problem_id"] == "277705288" and d["severity"] == "Critical", d
    assert d["problem_name"] == "Disk Queue Depth on is greater than 5", d


def test_unix_line_endings_and_untidy_spacing():
    assert parse_description(OPEN_LF)["problem_id"] == "277705111"
    d = parse_description("Problem started at 1\nProblem name:    CPU   Utilization is greater than 90%   \nOriginal problem ID:   42  \n")
    assert d["problem_name"] == "CPU Utilization is greater than 90%" and d["problem_id"] == "42", d


def test_old_style_carriage_return_only_line_endings():
    d = parse_description("Problem started at 1\rProblem name: Disk Queue Depth on is greater than 3\rHost: h1\rOriginal problem ID: 77\r")
    assert d["kind"] == "open" and d["problem_id"] == "77" and d["problem_name"] == "Disk Queue Depth on is greater than 3" and d["host"] == "h1", d


def test_missing_pieces_come_back_as_none_and_never_raise():
    empty = {"kind": None, "problem_id": None, "problem_name": None, "host": None, "severity": None}
    assert parse_description(None) == empty and parse_description("") == empty
    d = parse_description("Problem started at 1\nHost: h\n")
    assert d["kind"] == "open" and d["problem_id"] is None and d["problem_name"] is None and d["host"] == "h", d
    assert parse_description("Problem name:   \nHost: h")["problem_name"] is None
    assert parse_description("Original problem ID: abc")["problem_id"] is None


def test_the_id_is_taken_from_its_own_line_only():
    d = parse_description("Problem started at 1\nSome text mentioning Original problem ID: 999 in the middle\nOriginal problem ID: 123\n")
    assert d["problem_id"] == "123", d


# ---- full_title -----------------------------------------------------------
def test_open_title_is_extended_with_the_threshold():
    title, status = full_title(OPEN_MSG, OPEN_CRLF)
    assert status == "extended" and title == OPEN_MSG + " 6", (status, title)


def test_closed_title_keeps_its_double_spaces():
    title, status = full_title(CLOSED_MSG, CLOSED_CRLF)
    assert status == "extended" and title == CLOSED_MSG + "er than 5", (status, title)
    assert "[Env:o1conf]  Disk Queue Depth on is greater than 5" in title


def test_a_title_that_was_not_cut_is_left_alone():
    msg = "[Zabbix] [Open] [Critical] [O1] [h] [Env:x] CPU Utilization is greater than 90%"
    d = "Problem started at 1\nProblem name: CPU Utilization is greater than 90%\nOriginal problem ID: 5\n"
    assert full_title(msg, d) == (msg, "same")


def test_a_description_that_does_not_extend_the_title_is_rejected():
    msg = "[Zabbix] [Open] [Critical] [O1] [h] [Env:x] CPU Utilization is gr"
    assert full_title(msg, "Problem started at 1\nProblem name: Memory is low\n") == (None, "mismatch")
    assert full_title(msg, "Problem started at 1\nProblem name: Free CPU Utilization is greater than 90%\n") == (None, "mismatch"), "contains is not prefix"


def test_missing_inputs_give_a_status_and_no_title():
    msg = "[Zabbix] [Open] [Critical] [O1] [h] [Env:x] CPU Utilization is gr"
    assert full_title(msg, None) == (None, "no_description") and full_title(msg, "") == (None, "no_description")
    assert full_title(msg, "Problem started at 1\nHost: h\n") == (None, "no_problem_name")
    assert full_title("[Zabbix] [Open] no env tag", "Problem name: x\n") == (None, "no_env_tag")
    assert full_title(None, "Problem name: x\n") == (None, "no_env_tag")


# ---- extract and the link with the recovery rules ------------------------
def test_extract_returns_what_is_stored():
    e = extract(OPEN_MSG, OPEN_CRLF)
    assert e == {"problem_id": "276731495", "problem_name": "Write IOPS on is greater than 6", "full_message": OPEN_MSG + " 6",
                 "title_status": "extended", "kind": "open", "host": "o1confpbapgsql.cztn97v5hefa.us-east-1.rds.amazonaws.com", "severity": "Severe"}, e
    n = extract(OPEN_MSG, None)
    assert n["problem_id"] is None and n["full_message"] is None and n["title_status"] == "no_description"


def test_an_open_and_its_closed_notice_carry_the_same_id():
    o = extract("[Zabbix] [Open] [Critical] [O1] [h1] [Env:x] Disk Queue Depth on is", OPEN_LF.replace("277705111", "500"))
    c = extract("[Zabbix] : [Closed]  : [O1] [h1] [Env:x]  Disk Queue Depth on is gre", CLOSED_CRLF.replace("277705288", "500"))
    assert o["problem_id"] == c["problem_id"] == "500" and o["kind"] == "open" and c["kind"] == "closed"


def test_full_titles_separate_sibling_triggers_that_cut_titles_could_not():
    # two thresholds on one host: OpsGenie cut both Closed titles at the same character
    host = "nvaprod2slavedb01.cztn97v5hefa.us-east-1.rds.amazonaws.com"
    def closed(th):
        msg = ("[Zabbix] : [Closed]  : [O1] [%s] [Env:nvaprod]  Disk Queue Depth on is greater than %s" % (host, th))[:130]
        return extract(msg, "Problem has been resolved at 1\nProblem name: Disk Queue Depth on is greater than %s\nOriginal problem ID: %s\n" % (th, 100 + int(th)))
    c3, c5 = closed("3"), closed("5")
    assert parse_message(c3["full_message"] or "")["check"] != parse_message(c5["full_message"] or "")["check"]
    cut3 = parse_message(("[Zabbix] : [Closed]  : [O1] [%s] [Env:nvaprod]  Disk Queue Depth on is greater than 3" % host)[:130])
    cut5 = parse_message(("[Zabbix] : [Closed]  : [O1] [%s] [Env:nvaprod]  Disk Queue Depth on is greater than 5" % host)[:130])
    assert cut3["check"] == cut5["check"], "the cut titles were identical: that is the old ambiguity"
    open3 = parse_message(("[Zabbix] [Open] [Critical] [O1] [%s] [Env:nvaprod] Disk Queue Depth on is greater than 3" % host))
    assert same_problem(open3, parse_message(c3["full_message"])) and not same_problem(open3, parse_message(c5["full_message"]))


def test_the_two_modules_can_share_one_namespace():
    # scripts send several modules to the container on stdin, and the tests load them into one namespace:
    # a private name used by both (for example a regex) would silently break the first one loaded
    p = parse_message("[Zabbix] [Open] [Critical] [O1] [h] [Env:x] CPU")
    assert p["host"] == "h" and p["env"] == "x" and p["check"] == "CPU", p


def test_is_zabbix():
    assert is_zabbix("[Zabbix] [Open] x") and is_zabbix("[Maintenance] [zabbix] : [Closed]") and not is_zabbix("[New Relic] [open]") and not is_zabbix(None)


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
