"""Pure matching rules for Zabbix recovery (alert-analyser).

No database, no network, no writes. For each ESCALATED Zabbix Open alert it decides
whether a later Zabbix Closed notice for the same problem exists.

Rules (decided 2026-10-07):
- Same problem = same host and environment, and the same check text. Titles that OpsGenie
  cut at its ~130 character limit also match by prefix, but only when both messages are at
  least PREFIX_MIN_LEN long.
- A Closed notice resolves an Open only when the Closed alert's own createdAt is later
  (strictly) than the Open's. Our sync time and OpsGenie's live status are not used.
- The first matching Closed notice after the Open is the one cited.
- If an Open only prefix-matches Closed notices whose check texts differ from each other,
  the decision is "review" (different Zabbix triggers can share a truncated prefix).
- An Open with no later Closed is kept; if a newer Open for the same problem exists the
  older one is "review" (superseded), never resolved without a recovery notice.
- A title that cannot be parsed is never resolved: "review".
"""
import re
from collections import defaultdict
from datetime import datetime, timezone

PREFIX_MIN_LEN = 125

_HOST_RE = re.compile(r"\[([^\]]+)\]\s*\[Env:")
_ENV_RE = re.compile(r"\[Env:([^\]]*)\]")
_CHECK_RE = re.compile(r"\[Env:[^\]]*\]\s*(.*)$", re.DOTALL)


def parse_ts(value):
    """ISO string (Z or offset, 1-9 fractional digits) or datetime -> aware datetime in UTC."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).replace("Z", "+00:00")
    s = re.sub(r"\.(\d+)", lambda m: "." + (m.group(1) + "000000")[:6], s, count=1)
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def parse_message(message):
    """Return {'kind','host','env','check','mlen'} for a Zabbix title, or None if it is not a
    Zabbix title. kind is None when the title has neither or both of the Open/Closed tags;
    host is None when the '[host] [Env:..]' pattern is absent."""
    msg = message or ""
    tokens = [t.strip().lower() for t in re.findall(r"\[([^\]]+)\]", msg)]
    if "zabbix" not in tokens:
        return None
    has_open, has_closed = "open" in tokens, "closed" in tokens
    kind = "open" if (has_open and not has_closed) else ("closed" if (has_closed and not has_open) else None)
    host_m, env_m, chk_m = _HOST_RE.search(msg), _ENV_RE.search(msg), _CHECK_RE.search(msg)
    check = re.sub(r"\s+", " ", chk_m.group(1)).strip() if chk_m else ""
    return {"kind": kind, "host": host_m.group(1) if host_m else None,
            "env": env_m.group(1) if env_m else "", "check": check, "mlen": len(msg)}


def same_problem(a, b):
    """True when two parsed titles describe the same Zabbix problem."""
    if a["host"] != b["host"] or a["env"] != b["env"]:
        return False
    if a["check"] == b["check"]:
        return True
    if min(a["mlen"], b["mlen"]) >= PREFIX_MIN_LEN:
        return a["check"].startswith(b["check"]) or b["check"].startswith(a["check"])
    return False


def decide(opens, events):
    """opens: the ESCALATED Zabbix Open alerts to judge. events: every Zabbix alert known
    (Open and Closed). Each is a dict with 'id', 'message', 'createdAt'.
    Returns one dict per item of opens, in the same order:
      {'id', 'decision': 'resolve'|'keep'|'review', 'reason', 'closed_id', 'closed_at'}"""
    closeds, open_events = defaultdict(list), defaultdict(list)
    for e in events:
        p = parse_message(e.get("message"))
        if not p or not p["kind"] or not p["host"]:
            continue
        try:
            ts = parse_ts(e["createdAt"])
        except Exception:
            continue
        (closeds if p["kind"] == "closed" else open_events)[(p["host"], p["env"])].append((e, p, ts))

    out = []
    for o in opens:
        d = {"id": o.get("id"), "decision": None, "reason": None, "closed_id": None, "closed_at": None}
        out.append(d)
        po = parse_message(o.get("message"))
        if not po or po["kind"] != "open" or not po["host"]:
            d["decision"], d["reason"] = "review", "unparsed_title"
            continue
        try:
            ots = parse_ts(o["createdAt"])
        except Exception:
            d["decision"], d["reason"] = "review", "unparsed_time"
            continue
        key = (po["host"], po["env"])
        later = [x for x in closeds.get(key, []) if x[2] > ots and same_problem(po, x[1])]
        if later:
            exact = [x for x in later if x[1]["check"] == po["check"]]
            if exact:
                pick, reason = min(exact, key=lambda x: (x[2], str(x[0].get("id")))), "closed_exact"
            elif len({x[1]["check"] for x in later}) == 1:
                pick, reason = min(later, key=lambda x: (x[2], str(x[0].get("id")))), "closed_prefix"
            else:
                d["decision"], d["reason"] = "review", "ambiguous_prefix"
                continue
            d["decision"], d["reason"] = "resolve", reason
            d["closed_id"], d["closed_at"] = pick[0].get("id"), pick[2].isoformat()
            continue
        newer = any(x[0].get("id") != o.get("id") and x[2] > ots and same_problem(po, x[1])
                    for x in open_events.get(key, []))
        if newer:
            d["decision"], d["reason"] = "review", "older_open_superseded"
        else:
            d["decision"], d["reason"] = "keep", "newest_open_no_recovery"
    return out
