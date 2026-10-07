"""Dry run of the Zabbix recovery rules on live data. Read-only: SELECTs only, writes nothing.

Run (no rebuild needed; the container image does not contain tools/zabbix_recovery.py yet,
so the module is sent ahead of this script on stdin):
  cat agents/alert-analyser/tools/zabbix_recovery.py agents/alert-analyser/scripts/recovery_dry_run.py | docker exec -i org-dev-alert-analyser-1 python -
After the next image rebuild the module is importable and this also works:
  docker exec -i org-dev-alert-analyser-1 python - < agents/alert-analyser/scripts/recovery_dry_run.py
Env: SAMPLES (examples shown per group, default 5).
"""
import asyncio
import os
import random
import sys
import time
from collections import Counter
from datetime import datetime, timezone

try:
    from tools.zabbix_recovery import decide, parse_message, parse_ts
except ImportError:
    pass
if "decide" not in globals():
    sys.exit("zabbix_recovery is not available. Run: cat agents/alert-analyser/tools/zabbix_recovery.py "
             "agents/alert-analyser/scripts/recovery_dry_run.py | docker exec -i org-dev-alert-analyser-1 python -")

SAMPLES = int(os.environ.get("SAMPLES", "5"))

OPENS_SQL = """
SELECT id::text AS id, alert_id, alert_payload->>'message' AS message, alert_payload->>'createdAt' AS created_at
FROM incident_management.incidents
WHERE status = 'ESCALATED' AND alert_payload->>'message' ILIKE '%[Zabbix]%'
"""

EVENTS_SQL = """
SELECT alert_id AS id, alert_payload->>'message' AS message, alert_payload->>'createdAt' AS created_at
FROM incident_management.incidents
WHERE alert_payload->>'message' ILIKE '%[Zabbix]%' AND alert_payload->>'createdAt' IS NOT NULL
UNION
SELECT alert_id, alert_data->>'message', alert_data->>'createdAt'
FROM alert_sync_history
WHERE alert_data->>'message' ILIKE '%[Zabbix]%' AND alert_data->>'createdAt' IS NOT NULL
"""


def short(msg, n=118):
    return " ".join((msg or "").split())[:n]


def pct(n, total):
    return "%.1f%%" % (100.0 * n / total) if total else "n/a"


def fmt(sec):
    sec = int(sec)
    if sec < 3600:
        return "%dm%02ds" % (sec // 60, sec % 60)
    if sec < 86400:
        return "%.1fh" % (sec / 3600.0)
    return "%.1fd" % (sec / 86400.0)


def report(opens_rows, event_rows, out=print):
    t0 = time.time()
    events_by_id = {}
    for r in event_rows:
        events_by_id.setdefault(r.id, {"id": r.id, "message": r.message, "createdAt": r.created_at})
    events = list(events_by_id.values())
    opens = [{"id": r.id, "alert_id": r.alert_id, "message": r.message, "createdAt": r.created_at} for r in opens_rows]
    opens_by_id = {o["id"]: o for o in opens}

    kinds = Counter()
    for e in events:
        p = parse_message(e["message"])
        kinds[p["kind"] if (p and p["kind"] and p["host"]) else "unparsed"] += 1
    out("ZABBIX RECOVERY DRY RUN (read-only; nothing is written)")
    out("ESCALATED Zabbix incidents to judge: %d" % len(opens))
    out("Zabbix alerts known (incidents + sync history, de-duplicated by alert_id): %d | %s" % (len(events), dict(kinds)))

    decisions = decide(opens, events)
    out("decide() took %.1fs" % (time.time() - t0))

    out("")
    out("--- decisions ---")
    by = Counter((d["decision"], d["reason"]) for d in decisions)
    for (dec, rea), n in sorted(by.items(), key=lambda kv: (-kv[1], kv[0])):
        out("  %-8s %-26s %6d  %s" % (dec, rea, n, pct(n, len(decisions))))
    out("  totals: %s" % dict(Counter(d["decision"] for d in decisions)))

    resolves = [d for d in decisions if d["decision"] == "resolve"]
    rows = []
    for d in resolves:
        o = opens_by_id[d["id"]]
        gap = (parse_ts(d["closed_at"]) - parse_ts(o["createdAt"])).total_seconds()
        rows.append((gap, d, o))

    if rows:
        gs = sorted(r[0] for r in rows)

        def q(p):
            return gs[min(len(gs) - 1, int(p * len(gs)))]

        out("")
        out("--- resolve: time from the Open to the cited Closed notice ---")
        out("  median %s | p90 %s | max %s" % (fmt(q(0.5)), fmt(q(0.9)), fmt(gs[-1])))
        b = Counter()
        for g in gs:
            b["< 1 hour" if g < 3600 else "1 hour - 1 day" if g < 86400 else "1 - 7 days" if g < 7 * 86400 else "> 7 days"] += 1
        for k in ("< 1 hour", "1 hour - 1 day", "1 - 7 days", "> 7 days"):
            out("  %-15s %5d" % (k, b.get(k, 0)))

        chk = Counter(parse_message(opens_by_id[d["id"]]["message"])["check"][:45] for d in resolves)
        out("")
        out("--- resolve by check text (top 10) ---")
        for k, n in chk.most_common(10):
            out("  %5d  %s" % (n, k))

        def pair(gap, d, o):
            c = events_by_id.get(d["closed_id"], {})
            out("  [%s] gap %s" % (d["reason"], fmt(gap)))
            out("      OPEN   %s  %s" % (str(o["createdAt"])[:19], short(o["message"])))
            out("      CLOSED %s  %s" % (str(c.get("createdAt"))[:19], short(c.get("message"))))

        rnd = random.Random(11)
        exact = [r for r in rows if r[1]["reason"] == "closed_exact"]
        prefix = [r for r in rows if r[1]["reason"] == "closed_prefix"]
        out("")
        out("--- resolve samples ---")
        for label, group in (("random, exact title match", rnd.sample(exact, min(SAMPLES, len(exact)))),
                             ("random, prefix match (truncated titles)", rnd.sample(prefix, min(SAMPLES, len(prefix)))),
                             ("longest gaps", sorted(rows, key=lambda r: -r[0])[:SAMPLES])):
            out(" %s:" % label)
            for gap, d, o in group:
                pair(gap, d, o)

    for reason in ("ambiguous_prefix", "older_open_superseded", "unparsed_title", "unparsed_time"):
        grp = [d for d in decisions if d["reason"] == reason]
        if grp:
            out("")
            out("--- review: %s (%d) ---" % (reason, len(grp)))
            for d in grp[:40]:
                o = opens_by_id[d["id"]]
                out("  %s | %s | %s" % (str(o["createdAt"])[:19], o["alert_id"], short(o["message"])))

    keeps = [d for d in decisions if d["decision"] == "keep"]
    now = datetime.now(timezone.utc)
    old = 0
    for d in keeps:
        try:
            if (now - parse_ts(opens_by_id[d["id"]]["createdAt"])).total_seconds() > 86400:
                old += 1
        except Exception:
            pass
    out("")
    out("--- keep: newest Open with no later Closed (%d, of which %d are older than 1 day) ---" % (len(keeps), old))
    for d in sorted(keeps, key=lambda d: str(opens_by_id[d["id"]]["createdAt"]))[:60]:
        o = opens_by_id[d["id"]]
        out("  %s | %s" % (str(o["createdAt"])[:19], short(o["message"])))


async def main():
    from sqlalchemy import text
    from database import SessionLocal
    async with SessionLocal() as s:
        opens_rows = (await s.execute(text(OPENS_SQL))).fetchall()
        event_rows = (await s.execute(text(EVENTS_SQL))).fetchall()
    report(opens_rows, event_rows)


if __name__ == "__main__":
    asyncio.run(main())
