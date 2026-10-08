"""Matches Zabbix Open alerts to their recovery notices by problem ID (alert-analyser).

Pure functions: no database, no network, no writes. Each Zabbix alert's description carries "Original problem ID",
Zabbix's own ID of the problem, and an Open notice and the Closed notice that recovers it carry the same one. Checked on
7-8 Oct 2026: 3,882 problems had exactly one Open and one Closed notice. Matching by title had paired an Open with the
notice of a different problem for 141 of 317 prefix matches and about 3% of exact matches (sibling triggers on one host
whose Closed titles OpsGenie cut at the same character, and overlapping problems with the same title).

decide_by_id(opens, events) returns one dict per Open, in order, shaped like tools/zabbix_recovery.decide():
    {'id', 'decision': 'resolve'|'keep'|'review', 'reason', 'closed_id', 'closed_at'}
Rules:
- An Open with a problem ID is resolved by the earliest Closed notice that carries the same ID ('closed_by_id'), in
  whatever order the two notices arrived (a notice can be created a minute before its own Open). With no such notice it
  is kept ('no_recovery_for_problem'): it is never closed by another problem's notice, and a newer alert with the same
  title does not close it either.
- An Open with no stored details yet is kept ('details_pending'); the enrichment job fetches them within minutes.
- An Open whose details were fetched but carry no problem ID falls back to the title rules, and only an EXACT title
  match resolves it; a prefix-only match goes to review ('prefix_without_id').
- A notice is used only when its kind is clear: the title tag and the description must not disagree.
Each dict in opens and events has: id, message, createdAt, and problem_id, detail_status (None when no detail row
exists), detail_kind ('open'/'closed'/None) and full_message (None when the title was not rebuilt).
"""
from collections import defaultdict

try:
    from tools.zabbix_recovery import decide, parse_message, parse_ts
except ImportError:
    pass  # the tests load the modules into one namespace


def _match_kind(e):
    """('open'|'closed'|None, conflict): the kind from the title tag and from the description; a disagreement is a conflict."""
    t = (parse_message(e.get("message")) or {}).get("kind")
    d = e.get("detail_kind")
    if t and d and t != d:
        return None, True
    return (t or d), False


def _closed_by_problem(events):
    index = defaultdict(list)
    for e in events:
        pid = e.get("problem_id")
        if not pid:
            continue
        kind, _conflict = _match_kind(e)
        if kind != "closed":
            continue
        try:
            ts = parse_ts(e["createdAt"])
        except Exception:
            continue
        index[str(pid)].append((ts, str(e.get("id"))))
    for hits in index.values():
        hits.sort()
    return index


def decide_by_id(opens, events):
    closed = _closed_by_problem(events)
    out, fallback = [], []
    for o in opens:
        d = {"id": o.get("id"), "decision": None, "reason": None, "closed_id": None, "closed_at": None}
        out.append(d)
        if o.get("detail_status") is None:
            d["decision"], d["reason"] = "keep", "details_pending"
            continue
        kind, conflict = _match_kind(o)
        if conflict:
            d["decision"], d["reason"] = "review", "kind_conflict"
            continue
        if kind != "open":
            d["decision"], d["reason"] = "review", "unparsed_title"
            continue
        pid = o.get("problem_id")
        if pid:
            hits = closed.get(str(pid))
            if hits:
                d["decision"], d["reason"] = "resolve", "closed_by_id"
                d["closed_at"], d["closed_id"] = hits[0][0].isoformat(), hits[0][1]
            else:
                d["decision"], d["reason"] = "keep", "no_recovery_for_problem"
            continue
        fallback.append((d, o))
    if fallback:
        title_events = [{"id": e.get("id"), "message": e.get("full_message") or e.get("message"), "createdAt": e.get("createdAt")}
                        for e in events]
        title_opens = [{"id": o.get("id"), "message": o.get("full_message") or o.get("message"), "createdAt": o.get("createdAt")}
                       for _d, o in fallback]
        for (d, _o), t in zip(fallback, decide(title_opens, title_events)):
            if t["decision"] == "resolve" and t["reason"] == "closed_prefix":
                d["decision"], d["reason"] = "review", "prefix_without_id"
            else:
                d["decision"], d["reason"] = t["decision"], t["reason"]
                d["closed_id"], d["closed_at"] = t["closed_id"], t["closed_at"]
    return out
