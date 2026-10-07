"""Reads the details OpsGenie keeps for a Zabbix alert in its description (alert-analyser).

OpsGenie cuts an alert's message at 130 characters, so the check text in the title is often incomplete.
The alert's description (returned by Get Alert, not by the list call the sync uses) carries the full
"Problem name:", the "Host:", the "Severity:" and "Original problem ID:", the same on Open and Closed
notices, for example:
    Problem started at 04:27:58 on 2026.09.26            (Closed: Problem has been resolved at ...)
    Problem name: Write IOPS on is greater than 6
    Host: o1confpbapgsql.cztn97v5hefa.us-east-1.rds.amazonaws.com
    Severity: Severe
    Original problem ID: 276731495

Pure functions, no database and no network. parse_description() reads the fields; full_title() rebuilds
the title with the whole check text, but only when the cut text is a prefix of the problem name;
extract() returns what is stored for an alert.
"""
import re

_DESC_ID_RE = re.compile(r"^[ \t]*Original problem ID:[ \t]*(\d+)", re.M)
_DESC_NAME_RE = re.compile(r"^[ \t]*Problem name:[ \t]*(.+?)[ \t]*$", re.M)
_DESC_HOST_RE = re.compile(r"^[ \t]*Host:[ \t]*(.+?)[ \t]*$", re.M)
_DESC_SEVERITY_RE = re.compile(r"^[ \t]*Severity:[ \t]*(.+?)[ \t]*$", re.M)
_TITLE_ENV_RE = re.compile(r"\[Env:[^\]]*\]\s*")
_ZABBIX_TAG_RE = re.compile(r"\[zabbix\]", re.I)


def is_zabbix(message):
    return bool(_ZABBIX_TAG_RE.search(message or ""))


def parse_description(description):
    """-> {'kind': 'open'|'closed'|None, 'problem_id': str|None, 'problem_name', 'host', 'severity'}"""
    out = {"kind": None, "problem_id": None, "problem_name": None, "host": None, "severity": None}
    if not description:
        return out
    text = description.replace("\r\n", "\n").replace("\r", "\n")
    first = text.strip().split("\n", 1)[0].strip().lower()
    if first.startswith("problem started"):
        out["kind"] = "open"
    elif first.startswith("problem has been resolved"):
        out["kind"] = "closed"
    for key, rx in (("problem_id", _DESC_ID_RE), ("problem_name", _DESC_NAME_RE), ("host", _DESC_HOST_RE), ("severity", _DESC_SEVERITY_RE)):
        m = rx.search(text)
        if m:
            value = " ".join(m.group(1).split())
            out[key] = value or None
    return out


def full_title(message, description):
    """Rebuild the alert title with the whole check text. Returns (title, status); the title is None
    unless the status is 'same' (the title was not cut) or 'extended' (the cut check text is a prefix of
    the problem name). Other statuses: no_description, no_problem_name, no_env_tag, mismatch."""
    msg = message or ""
    if not description:
        return None, "no_description"
    name = parse_description(description)["problem_name"]
    if not name:
        return None, "no_problem_name"
    e = _TITLE_ENV_RE.search(msg)
    if not e:
        return None, "no_env_tag"
    cut = " ".join(msg[e.end():].split())
    if cut == name:
        return msg, "same"
    if name.startswith(cut):
        return msg[:e.end()] + name, "extended"
    return None, "mismatch"


def extract(message, description):
    """What is stored for an alert: ids and names from the description plus the rebuilt title."""
    d = parse_description(description)
    title, status = full_title(message, description)
    return {"problem_id": d["problem_id"], "problem_name": d["problem_name"], "full_message": title,
            "title_status": status, "kind": d["kind"], "host": d["host"], "severity": d["severity"]}
