"""One-off cleanup: resolve the ESCALATED Zabbix incidents whose recovery notice exists.

Uses tools/zabbix_recovery.decide(). REHEARSAL by default: it performs every write inside one
transaction and then rolls back. Set CONFIRM=RESOLVE to commit.

Run (the container image does not contain tools/zabbix_recovery.py yet, so it is sent ahead on
stdin; stdout is the full decision list as CSV, written BEFORE any database write; stderr is the
summary):
  cat agents/alert-analyser/tools/zabbix_recovery.py agents/alert-analyser/scripts/recovery_cleanup.py | \
    docker exec -i org-dev-alert-analyser-1 python - > /data/backups/recovery_cleanup_$(date -u +%Y%m%d_%H%M%S).csv
Env: CONFIRM=RESOLVE (commit); MAX_GAP_HOURS (default 24: a resolve whose Closed notice came later
than this after the Open is held for review); MAX_ROWS (default 2500: refuse to touch more rows).

A resolved incident gets (decided 2026-10-07): status RESOLVED; resolved_at = the Closed notice's own
time (never earlier than the incident's created_at); resolution_type closure_alert_correlation (an
existing value); detected_via message_parse; one incident_status_history row ESCALATED -> RESOLVED
with the same time. Rows are updated only while still ESCALATED and still Zabbix.
"""
import asyncio
import csv
import os
import sys
import uuid
from collections import Counter

try:
    from tools.zabbix_recovery import decide, parse_message, parse_ts
except ImportError:
    pass
if "decide" not in globals():
    sys.exit("zabbix_recovery is not available. Run: cat agents/alert-analyser/tools/zabbix_recovery.py "
             "agents/alert-analyser/scripts/recovery_cleanup.py | docker exec -i org-dev-alert-analyser-1 python -")

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

UPDATE_SQL = """
UPDATE incident_management.incidents
SET status = 'RESOLVED',
    resolved_at = GREATEST(CAST(:closed_at AS timestamptz), created_at),
    resolution_type = 'closure_alert_correlation',
    detected_via = 'message_parse',
    updated_at = now()
WHERE id = :id AND status = 'ESCALATED' AND alert_payload->>'message' ILIKE '%[Zabbix]%'
"""

HISTORY_SQL = """
INSERT INTO incident_management.incident_status_history (incident_id, from_status, to_status, changed_at)
SELECT id, 'ESCALATED', 'RESOLVED', resolved_at FROM incident_management.incidents WHERE id = :id
"""

STAMPED_SQL = "SELECT count(*) FROM incident_management.incidents WHERE updated_at = now()"
STAMPED_OTHER_SQL = ("SELECT count(*) FROM incident_management.incidents "
                     "WHERE updated_at = now() AND alert_payload->>'message' NOT ILIKE '%[Zabbix]%'")

CSV_COLUMNS = ["incident_id", "alert_id", "open_created_at", "closed_alert_id", "closed_created_at",
               "gap_seconds", "decision", "reason", "action", "open_message"]


def log(msg=""):
    print(msg, file=sys.stderr)


def short(msg, n=110):
    return " ".join((msg or "").split())[:n]


def fmt(sec):
    sec = int(sec)
    if sec < 3600:
        return "%dm%02ds" % (sec // 60, sec % 60)
    if sec < 86400:
        return "%.1fh" % (sec / 3600.0)
    return "%.1fd" % (sec / 86400.0)


def plan(opens_rows, event_rows, max_gap_hours):
    """One row per judged incident. action is resolve / held_gap / keep / review."""
    events_by_id = {}
    for r in event_rows:
        events_by_id.setdefault(r.id, {"id": r.id, "message": r.message, "createdAt": r.created_at})
    events = list(events_by_id.values())
    opens = [{"id": r.id, "alert_id": r.alert_id, "message": r.message, "createdAt": r.created_at} for r in opens_rows]
    by_id = {o["id"]: o for o in opens}
    rows = []
    for d in decide(opens, events):
        o = by_id[d["id"]]
        gap, action = None, d["decision"]
        if d["decision"] == "resolve":
            gap = (parse_ts(d["closed_at"]) - parse_ts(o["createdAt"])).total_seconds()
            if gap > max_gap_hours * 3600:
                action = "held_gap"
        rows.append({"incident_id": d["id"], "alert_id": o["alert_id"], "open_created_at": o["createdAt"],
                     "closed_alert_id": d["closed_id"], "closed_created_at": d["closed_at"],
                     "gap_seconds": None if gap is None else int(gap), "decision": d["decision"],
                     "reason": d["reason"], "action": action, "applied": "",
                     "open_message": " ".join((o["message"] or "").split())})
    return rows


class _Stop(Exception):
    pass


class _Rehearsal(Exception):
    pass


async def apply(rows, commit, session_factory, text):
    """Resolve the rows whose action is 'resolve' in ONE transaction. Rolls back unless every check
    passes; with commit=False it always rolls back (rehearsal). Returns (ok, note, stats)."""
    todo = [r for r in rows if r["action"] == "resolve"]
    stats = {"updated": 0, "skipped": 0}
    async with session_factory() as s:
        try:
            async with s.begin():
                await s.execute(text("SET LOCAL lock_timeout = '30s'"))
                await s.execute(text("SET LOCAL statement_timeout = '300s'"))
                for r in todo:
                    iid = uuid.UUID(r["incident_id"])
                    res = await s.execute(text(UPDATE_SQL), {"id": iid, "closed_at": parse_ts(r["closed_created_at"])})
                    if res.rowcount == 1:
                        h = await s.execute(text(HISTORY_SQL), {"id": iid})
                        if h.rowcount != 1:
                            raise _Stop("status history insert affected %s rows for incident %s" % (h.rowcount, r["incident_id"]))
                        r["applied"] = "yes" if commit else "rehearsal"
                        stats["updated"] += 1
                    else:
                        r["applied"] = "skipped_not_escalated"
                        stats["skipped"] += 1
                stamped = (await s.execute(text(STAMPED_SQL))).scalar()
                stamped_other = (await s.execute(text(STAMPED_OTHER_SQL))).scalar()
                problems = []
                if stamped != stats["updated"]:
                    problems.append("rows stamped in this transaction (%s) differ from rows updated (%s)" % (stamped, stats["updated"]))
                if stamped_other != 0:
                    problems.append("%s non-Zabbix rows were stamped" % stamped_other)
                if stats["skipped"] > max(5, len(todo) // 100):
                    problems.append("%s incidents were no longer ESCALATED (limit %s)" % (stats["skipped"], max(5, len(todo) // 100)))
                if problems:
                    raise _Stop("; ".join(problems))
                if not commit:
                    raise _Rehearsal()
        except _Rehearsal:
            return True, "REHEARSAL OK - everything was rolled back, nothing changed", stats
        except _Stop as e:
            for r in todo:
                if r["applied"] in ("yes", "rehearsal"):
                    r["applied"] = "rolled_back"
            return False, "ROLLED BACK: %s" % e, stats
        except Exception as e:
            for r in todo:
                if r["applied"] in ("yes", "rehearsal"):
                    r["applied"] = "rolled_back"
            return False, "ROLLED BACK on error: %r" % (e,), stats
    return True, "COMMITTED", stats


def write_csv(rows, stream):
    w = csv.DictWriter(stream, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)


def summarise(rows, commit, ok, note, stats, max_gap_hours):
    log("MODE: %s | MAX_GAP_HOURS=%s" % ("COMMIT" if commit else "REHEARSAL (rolls back, changes nothing)", max_gap_hours))
    log("incidents judged: %d" % len(rows))
    for (action, reason), n in sorted(Counter((r["action"], r["reason"]) for r in rows).items(), key=lambda kv: (-kv[1], kv[0])):
        log("  %-9s %-26s %6d" % (action, reason, n))
    log("resolve applied: %s | skipped (no longer ESCALATED): %s" % (stats["updated"], stats["skipped"]))
    skipped = [r["incident_id"] for r in rows if r["applied"] == "skipped_not_escalated"]
    if skipped:
        log("skipped incident ids (resolved by something else meanwhile): %s" % ", ".join(skipped))
    held = sorted((r for r in rows if r["action"] == "held_gap"), key=lambda r: -r["gap_seconds"])
    if held:
        log("")
        log("--- held for review: Closed notice came more than %sh after the Open (%d) ---" % (max_gap_hours, len(held)))
        for r in held:
            log("  gap %-6s open %s closed %s | %s" % (fmt(r["gap_seconds"]), str(r["open_created_at"])[:16],
                                                       str(r["closed_created_at"])[:16], short(r["open_message"])))
    log("")
    log(note)


async def main():
    commit = os.environ.get("CONFIRM") == "RESOLVE"
    max_gap = float(os.environ.get("MAX_GAP_HOURS", "24"))
    max_rows = int(os.environ.get("MAX_ROWS", "2500"))
    from sqlalchemy import text
    from database import SessionLocal
    async with SessionLocal() as s:
        opens_rows = (await s.execute(text(OPENS_SQL))).fetchall()
        event_rows = (await s.execute(text(EVENTS_SQL))).fetchall()
    rows = plan(opens_rows, event_rows, max_gap)
    todo = [r for r in rows if r["action"] == "resolve"]
    if not todo:
        write_csv(rows, sys.stdout)
        summarise(rows, commit, True, "nothing to resolve", {"updated": 0, "skipped": 0}, max_gap)
        return 0
    if len(todo) > max_rows:
        write_csv(rows, sys.stdout)
        summarise(rows, commit, False, "REFUSED: %d rows to resolve exceeds MAX_ROWS=%d" % (len(todo), max_rows),
                  {"updated": 0, "skipped": 0}, max_gap)
        return 1
    write_csv(rows, sys.stdout)  # the decision list is written BEFORE any write to the database
    sys.stdout.flush()
    ok, note, stats = await apply(rows, commit, SessionLocal, text)
    summarise(rows, commit, ok, note, stats, max_gap)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
