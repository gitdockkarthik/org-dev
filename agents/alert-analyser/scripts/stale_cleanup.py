"""One-off manual cleanup of stale ESCALATED Zabbix incidents (alert-analyser). Decided by the owner on 2026-10-08.

An open Zabbix incident that has had no recovery notice for MIN_AGE_DAYS (default 7) or more is closed, so the open list shows fresh
issues. Nothing in the application code changes; this only writes the incident rows, like the earlier cleanup scripts.
  close_inferred  the same trigger (host + check) acted again later with another problem ID: Zabbix keeps one open problem per
                  trigger, so this problem had recovered. resolution_type closure_recovery_inferred; resolved_at = the time of
                  that later activity, an UPPER BOUND of the real recovery (never earlier than created_at).
  close_stale     nothing is known. resolution_type stale_cleanup_2026-10-08; resolved_at = created_at, so the closure falls
                  outside every current date window and cannot move the MTTR / MTTA cards (the cards count every resolution_type).
  held_no_details no stored alert details: the trigger cannot be checked, so the incident is NOT closed.
detected_via is the existing value message_parse (no new value); resolution_type carries the real label. One
incident_status_history row ESCALATED -> RESOLVED is written with the same time as resolved_at.
REHEARSAL by default: every write runs in one transaction that is then rolled back. CONFIRM=CLOSE commits.

Run (stdout is the full decision list as CSV, written BEFORE any database write; stderr is the summary):
  docker exec -i org-dev-alert-analyser-1 python - < agents/alert-analyser/scripts/stale_cleanup.py > /data/backups/stale_cleanup_$(date -u +%Y%m%d_%H%M%S).csv
  add  -e CONFIRM=CLOSE  after -i to commit.   Env: MIN_AGE_DAYS (default 7), MAX_ROWS (default 30: refuse to close more).
Rows are updated only while still ESCALATED and still Zabbix; the whole run rolls back if any check fails.
"""
import asyncio
import csv
import os
import sys
import uuid
from collections import Counter
from datetime import datetime, timezone

LABEL_INFERRED = "closure_recovery_inferred"
LABEL_STALE = "stale_cleanup_2026-10-08"

CTE = """
WITH ev AS (
    SELECT alert_id, CASE WHEN alert_data->>'createdAt' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN (alert_data->>'createdAt')::timestamptz END AS at
    FROM alert_sync_history WHERE alert_data->>'message' ILIKE '%[Zabbix]%'
    UNION
    SELECT alert_id, CASE WHEN alert_payload->>'createdAt' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN (alert_payload->>'createdAt')::timestamptz END
    FROM incident_management.incidents WHERE alert_payload->>'message' ILIKE '%[Zabbix]%'
), det AS (
    SELECT d.alert_id, d.problem_id, d.kind, d.host, d.problem_name, ev.at
    FROM zabbix_alert_detail d JOIN ev ON ev.alert_id = d.alert_id WHERE ev.at IS NOT NULL
)
"""

CANDIDATES_SQL = CTE + """
SELECT i.id::text AS id, i.alert_id, i.created_at, oa.at AS alert_at, d.alert_id AS detail_alert, d.host, d.problem_name, d.problem_id,
       i.alert_payload->>'message' AS message,
       (SELECT count(*) FROM det x WHERE x.kind = 'open' AND x.host = d.host AND x.problem_name = d.problem_name
                                       AND x.problem_id <> d.problem_id AND x.at > oa.at) AS newer_opens,
       (SELECT count(*) FROM det x WHERE x.kind = 'closed' AND x.host = d.host AND x.problem_name = d.problem_name
                                       AND x.problem_id <> d.problem_id AND x.at > oa.at) AS later_closed,
       (SELECT min(x.at) FROM det x WHERE x.host = d.host AND x.problem_name = d.problem_name
                                       AND x.problem_id <> d.problem_id AND x.at > oa.at) AS later_activity_at
FROM incident_management.incidents i
LEFT JOIN zabbix_alert_detail d ON d.alert_id = i.alert_id
LEFT JOIN ev oa ON oa.alert_id = i.alert_id
WHERE i.status = 'ESCALATED' AND i.alert_payload->>'message' ILIKE '%[Zabbix] [Open]%'
  AND i.created_at <= now() - make_interval(days => CAST(:min_days AS integer))
ORDER BY i.created_at
"""

KEPT_SQL = """
SELECT count(*) FROM incident_management.incidents
WHERE status = 'ESCALATED' AND alert_payload->>'message' ILIKE '%[Zabbix]%' AND created_at > now() - make_interval(days => CAST(:min_days AS integer))
"""

UPDATE_SQL = """
UPDATE incident_management.incidents
SET status = 'RESOLVED',
    resolved_at = GREATEST(CAST(:closed_at AS timestamptz), created_at),
    resolution_type = :label,
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

CSV_COLUMNS = ["incident_id", "alert_id", "created_at", "age_days", "host", "check", "problem_id", "newer_opens", "later_closed",
               "later_activity_at", "action", "label", "resolved_at_will_be", "applied", "open_message"]


def log(msg=""):
    print(msg, file=sys.stderr)


def short(msg, n=96):
    return " ".join((msg or "").split())[:n]


def plan(rows, now=None):
    """One row per candidate incident. action: close_inferred / close_stale / held_no_details."""
    now = now or datetime.now(timezone.utc)
    out, seen = [], set()
    for r in rows:
        if r.id in seen:
            continue
        seen.add(r.id)
        row = {"incident_id": r.id, "alert_id": r.alert_id, "created_at": r.created_at, "age_days": (now - r.created_at).days,
               "host": r.host, "check": r.problem_name, "problem_id": r.problem_id, "newer_opens": r.newer_opens,
               "later_closed": r.later_closed, "later_activity_at": r.later_activity_at, "applied": "",
               "open_message": " ".join((r.message or "").split())}
        if r.detail_alert is None:
            row.update(action="held_no_details", label="", resolved_at_will_be=None)
        elif r.later_activity_at is not None:
            row.update(action="close_inferred", label=LABEL_INFERRED, resolved_at_will_be=max(r.later_activity_at, r.created_at))
        else:
            row.update(action="close_stale", label=LABEL_STALE, resolved_at_will_be=r.created_at)
        out.append(row)
    return out


class _Stop(Exception):
    pass


class _Rehearsal(Exception):
    pass


async def apply(rows, commit, session_factory, text):
    """Close the rows whose action starts with close_ in ONE transaction. Rolls back unless every check passes; with
    commit=False it always rolls back (rehearsal). Returns (ok, note, stats)."""
    todo = [r for r in rows if r["action"].startswith("close_")]
    stats = {"updated": 0, "skipped": 0}
    async with session_factory() as s:
        try:
            async with s.begin():
                await s.execute(text("SET LOCAL lock_timeout = '30s'"))
                await s.execute(text("SET LOCAL statement_timeout = '120s'"))
                for r in todo:
                    iid = uuid.UUID(r["incident_id"])
                    res = await s.execute(text(UPDATE_SQL), {"id": iid, "closed_at": r["resolved_at_will_be"], "label": r["label"]})
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


def summarise(rows, commit, ok, note, stats, min_days, kept):
    log("MODE: %s | MIN_AGE_DAYS=%s" % ("COMMIT" if commit else "REHEARSAL (rolls back, changes nothing)", min_days))
    log("candidates (ESCALATED Zabbix, open %s days or more): %d | kept open because younger: %d" % (min_days, len(rows), kept))
    for action, n in sorted(Counter(r["action"] for r in rows).items(), key=lambda kv: (-kv[1], kv[0])):
        log("  %-16s %4d" % (action, n))
    log("closed in this run: %s | skipped (no longer ESCALATED): %s" % (stats["updated"], stats["skipped"]))
    log("")
    for action, title in (("close_inferred", "closed as recovery inferred (the same trigger acted again later)"),
                          ("close_stale", "closed as stale (no recovery notice, no later activity)"),
                          ("held_no_details", "NOT closed: no stored details")):
        sel = [r for r in rows if r["action"] == action]
        if sel:
            log("--- %s: %d ---" % (title, len(sel)))
            for r in sorted(sel, key=lambda r: -r["age_days"]):
                log("  %3dd %-30s %-34s problem %s%s" % (r["age_days"], str(r["host"] or "-")[:30], short(r["check"], 34), r["problem_id"],
                    (" | later activity %s" % str(r["later_activity_at"])[:16]) if r["later_activity_at"] else ""))
            log("")
    log(note)


async def main():
    commit = os.environ.get("CONFIRM") == "CLOSE"
    min_days = int(os.environ.get("MIN_AGE_DAYS", "7"))
    max_rows = int(os.environ.get("MAX_ROWS", "30"))
    from sqlalchemy import text
    from database import SessionLocal
    async with SessionLocal() as s:
        cand = (await s.execute(text(CANDIDATES_SQL), {"min_days": min_days})).fetchall()
        kept = (await s.execute(text(KEPT_SQL), {"min_days": min_days})).scalar()
    rows = plan(cand)
    todo = [r for r in rows if r["action"].startswith("close_")]
    write_csv(rows, sys.stdout)  # the decision list is written BEFORE any write to the database
    sys.stdout.flush()
    if not todo:
        summarise(rows, commit, True, "nothing to close", {"updated": 0, "skipped": 0}, min_days, kept)
        return 0
    if len(todo) > max_rows:
        summarise(rows, commit, False, "REFUSED: %d incidents to close exceeds MAX_ROWS=%d" % (len(todo), max_rows),
                  {"updated": 0, "skipped": 0}, min_days, kept)
        return 1
    ok, note, stats = await apply(rows, commit, SessionLocal, text)
    summarise(rows, commit, ok, note, stats, min_days, kept)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
