"""One-off correction of the Zabbix closures made before the old title-based closer was switched off (alert-analyser).
Decided by the owner on 2026-10-08, after the audit (scripts/audit_zabbix_ids.py) sized it: 444 closures of the last 30 days.

Looks at the incidents closed by the correlation rules (resolution_type closure_alert_correlation, detected_via message_parse or
opsgenie_live) between WINDOW_DAYS ago and CLEAN_FROM, and judges each against the recovery notice that carries its own problem ID:
  fix_time          own notice exists but resolved_at is more than 5 minutes off it: resolved_at = the notice's time (never earlier
                    than created_at), the incident_status_history row is moved to the same time, detected_via = message_parse.
  relabel_inferred  no notice of its own, but the same trigger (host + check) acted again later with another problem ID, so the
                    problem had recovered: resolution_type closure_recovery_inferred, resolved_at = that later time (an upper bound).
  reopen            no notice of its own, no later activity, younger than 7 days (14 in production): nothing shows it recovered, so
                    it goes back to ESCALATED (history row RESOLVED -> ESCALATED); the stale job closes it later if nothing happens.
  held_*            not touched: no stored details or problem ID, not exactly one history row, or no evidence and already old.
Only the groups named in GROUPS are applied (default fix_time,relabel_inferred; reopen must be asked for).
REHEARSAL by default: every write runs in one transaction that is then rolled back. CONFIRM=CORRECT commits.

Run (stdout is the full decision list as CSV with the old values, written BEFORE any database write; stderr is the summary):
  docker exec -i org-dev-alert-analyser-1 python - < agents/alert-analyser/scripts/history_correction.py > /data/backups/history_correction_$(date -u +%Y%m%d_%H%M%S).csv
  add  -e CONFIRM=CORRECT  after -i to commit, and  -e GROUPS=fix_time,relabel_inferred,reopen  to include the reopening.
Env: WINDOW_DAYS (default 30), MAX_ROWS (default 500: refuse to change more). A row is changed only while it still has the values read.
"""
import asyncio
import csv
import os
import sys
import uuid
from collections import Counter
from datetime import datetime, timezone

CLEAN_FROM = "2026-10-08 05:39:48+00"
TOLERANCE_SECONDS = 300
LABEL_INFERRED = "closure_recovery_inferred"
STALE_DAYS = 7
STALE_DAYS_PROD = 14

CANDIDATES_SQL = """
WITH ev AS MATERIALIZED (
    SELECT alert_id, CASE WHEN alert_data->>'createdAt' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN (alert_data->>'createdAt')::timestamptz END AS at
    FROM alert_sync_history WHERE alert_data->>'message' ILIKE '%[Zabbix]%'
    UNION
    SELECT alert_id, CASE WHEN alert_payload->>'createdAt' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN (alert_payload->>'createdAt')::timestamptz END
    FROM incident_management.incidents WHERE alert_payload->>'message' ILIKE '%[Zabbix]%'
), closed AS MATERIALIZED (
    SELECT d.problem_id, min(ev.at) AS closed_at
    FROM zabbix_alert_detail d JOIN ev ON ev.alert_id = d.alert_id
    WHERE d.kind = 'closed' AND d.problem_id IS NOT NULL AND ev.at IS NOT NULL
    GROUP BY d.problem_id
), det AS MATERIALIZED (
    SELECT d.alert_id, d.problem_id, d.kind, d.host, d.problem_name, ev.at
    FROM zabbix_alert_detail d JOIN ev ON ev.alert_id = d.alert_id WHERE ev.at IS NOT NULL
), hist AS (
    SELECT i.id::text AS id, i.alert_id, i.detected_via, i.created_at, i.resolved_at, i.alert_payload->>'message' AS message,
           d.alert_id AS detail_alert, d.host, d.problem_name, d.problem_id, c.closed_at, oa.at AS alert_at
    FROM incident_management.incidents i
    LEFT JOIN zabbix_alert_detail d ON d.alert_id = i.alert_id
    LEFT JOIN closed c ON c.problem_id = d.problem_id
    LEFT JOIN ev oa ON oa.alert_id = i.alert_id
    WHERE i.status = 'RESOLVED' AND i.resolution_type = 'closure_alert_correlation' AND i.detected_via IN ('message_parse', 'opsgenie_live')
      AND i.alert_payload->>'message' ILIKE '%[Zabbix] [Open]%'
      AND i.updated_at > now() - make_interval(days => CAST(:window_days AS integer)) AND i.updated_at < CAST(:clean_from AS timestamptz)
)
SELECT h.*,
       (SELECT count(*) FROM incident_management.incident_status_history s WHERE s.incident_id = CAST(h.id AS uuid) AND s.to_status = 'RESOLVED') AS hist_rows,
       (SELECT min(x.at) FROM det x WHERE x.host = h.host AND x.problem_name = h.problem_name
                                       AND x.problem_id <> h.problem_id AND x.at > h.alert_at) AS later_activity_at
FROM hist h
WHERE h.detail_alert IS NULL OR h.problem_id IS NULL OR h.closed_at IS NULL
   OR abs(extract(epoch FROM h.resolved_at - GREATEST(h.closed_at, h.created_at))) > CAST(:tolerance AS integer)
ORDER BY h.resolved_at
"""

FIX_SQL = """
UPDATE incident_management.incidents
SET resolved_at = :new_resolved, resolution_type = :label, detected_via = 'message_parse', updated_at = now()
WHERE id = :id AND status = 'RESOLVED' AND resolution_type = 'closure_alert_correlation' AND resolved_at = :old_resolved
  AND alert_payload->>'message' ILIKE '%[Zabbix]%'
"""

HISTORY_FIX_SQL = """
UPDATE incident_management.incident_status_history SET changed_at = :new_resolved
WHERE incident_id = :id AND to_status = 'RESOLVED'
"""

REOPEN_SQL = """
UPDATE incident_management.incidents
SET status = 'ESCALATED', resolved_at = NULL, resolution_type = NULL, detected_via = NULL, updated_at = now()
WHERE id = :id AND status = 'RESOLVED' AND resolution_type = 'closure_alert_correlation' AND resolved_at = :old_resolved
  AND alert_payload->>'message' ILIKE '%[Zabbix]%'
"""

HISTORY_REOPEN_SQL = """
INSERT INTO incident_management.incident_status_history (incident_id, from_status, to_status, changed_at)
VALUES (:id, 'RESOLVED', 'ESCALATED', now())
"""

STAMPED_SQL = "SELECT count(*) FROM incident_management.incidents WHERE updated_at = now()"
STAMPED_OTHER_SQL = ("SELECT count(*) FROM incident_management.incidents "
                     "WHERE updated_at = now() AND alert_payload->>'message' NOT ILIKE '%[Zabbix]%'")

CSV_COLUMNS = ["incident_id", "alert_id", "action", "selected", "old_detected_via", "old_resolved_at", "new_resolved_at", "created_at",
               "own_notice_at", "later_activity_at", "host", "check", "problem_id", "applied", "open_message"]


def log(msg=""):
    print(msg, file=sys.stderr)


def short(msg, n=60):
    return " ".join((msg or "").split())[-n:]


def is_production(message):
    s = message or ""
    i = s.find("[Env:")
    if i < 0:
        return False
    j = s.find("]", i)
    return j > i and s[i + 5:j].strip().lower().endswith("prod")


def plan(rows, groups, now=None):
    """One row per candidate. action: fix_time / relabel_inferred / reopen / held_unverifiable / held_history / held_old_no_evidence."""
    now = now or datetime.now(timezone.utc)
    out, seen = [], set()
    for r in rows:
        if r.id in seen:
            continue
        seen.add(r.id)
        row = {"incident_id": r.id, "alert_id": r.alert_id, "old_detected_via": r.detected_via, "old_resolved_at": r.resolved_at,
               "new_resolved_at": None, "created_at": r.created_at, "own_notice_at": r.closed_at, "later_activity_at": r.later_activity_at,
               "host": r.host, "check": r.problem_name, "problem_id": r.problem_id, "applied": "", "label": "closure_alert_correlation",
               "open_message": " ".join((r.message or "").split())}
        if r.detail_alert is None or r.problem_id is None or r.alert_at is None:
            row["action"] = "held_unverifiable"
        elif r.hist_rows != 1:
            row["action"] = "held_history"
        elif r.closed_at is not None:
            row.update(action="fix_time", new_resolved_at=max(r.closed_at, r.created_at))
        elif r.later_activity_at is not None:
            row.update(action="relabel_inferred", new_resolved_at=max(r.later_activity_at, r.created_at), label=LABEL_INFERRED)
        else:
            need = STALE_DAYS_PROD if is_production(r.message) else STALE_DAYS
            if (now - r.created_at).total_seconds() / 86400.0 >= need:
                row["action"] = "held_old_no_evidence"
            else:
                row["action"] = "reopen"
        row["selected"] = "yes" if row["action"] in groups else "no"
        out.append(row)
    return out


class _Stop(Exception):
    pass


class _Rehearsal(Exception):
    pass


async def apply(rows, commit, session_factory, text):
    """Apply the selected rows in ONE transaction; roll back unless every check passes. With commit=False it always rolls back."""
    todo = [r for r in rows if r["selected"] == "yes"]
    stats = {"updated": 0, "skipped": 0}
    async with session_factory() as s:
        try:
            async with s.begin():
                await s.execute(text("SET LOCAL lock_timeout = '30s'"))
                await s.execute(text("SET LOCAL statement_timeout = '300s'"))
                for r in todo:
                    iid = uuid.UUID(r["incident_id"])
                    if r["action"] == "reopen":
                        res = await s.execute(text(REOPEN_SQL), {"id": iid, "old_resolved": r["old_resolved_at"]})
                        if res.rowcount == 1:
                            h = await s.execute(text(HISTORY_REOPEN_SQL), {"id": iid})
                            ok_hist = h.rowcount == 1
                    else:
                        res = await s.execute(text(FIX_SQL), {"id": iid, "old_resolved": r["old_resolved_at"], "new_resolved": r["new_resolved_at"],
                                                              "label": r["label"]})
                        if res.rowcount == 1:
                            h = await s.execute(text(HISTORY_FIX_SQL), {"id": iid, "new_resolved": r["new_resolved_at"]})
                            ok_hist = h.rowcount == 1
                    if res.rowcount == 1:
                        if not ok_hist:
                            raise _Stop("status history change affected %s rows for incident %s" % (h.rowcount, r["incident_id"]))
                        r["applied"] = "yes" if commit else "rehearsal"
                        stats["updated"] += 1
                    else:
                        r["applied"] = "skipped_changed_meanwhile"
                        stats["skipped"] += 1
                stamped = (await s.execute(text(STAMPED_SQL))).scalar()
                stamped_other = (await s.execute(text(STAMPED_OTHER_SQL))).scalar()
                problems = []
                if stamped != stats["updated"]:
                    problems.append("rows stamped in this transaction (%s) differ from rows updated (%s)" % (stamped, stats["updated"]))
                if stamped_other != 0:
                    problems.append("%s non-Zabbix rows were stamped" % stamped_other)
                if stats["skipped"] > max(5, len(todo) // 100):
                    problems.append("%s incidents had changed meanwhile (limit %s)" % (stats["skipped"], max(5, len(todo) // 100)))
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


def summarise(rows, commit, note, stats, groups, window_days):
    log("MODE: %s | GROUPS=%s | WINDOW_DAYS=%s" % ("COMMIT" if commit else "REHEARSAL (rolls back, changes nothing)", ",".join(sorted(groups)), window_days))
    log("candidates (closed by the correlation rules before %s): %d" % (CLEAN_FROM[:16], len(rows)))
    c = Counter((r["action"], r["old_detected_via"], r["selected"]) for r in rows)
    for (action, via, selected), n in sorted(c.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        log("  %-22s %-14s %-10s %4d" % (action, via, "selected" if selected == "yes" else "NOT selected", n))
    log("changed in this run: %s | skipped (changed meanwhile): %s" % (stats["updated"], stats["skipped"]))
    log("")
    for action, title in (("reopen", "to be reopened (nothing shows they recovered)"), ("held_unverifiable", "held: no stored details or problem ID"),
                          ("held_history", "held: not exactly one status-history row"), ("held_old_no_evidence", "held: old and no evidence either way")):
        sel = [r for r in rows if r["action"] == action]
        if sel:
            log("--- %s: %d ---" % (title, len(sel)))
            for r in sel[:25]:
                log("  closed %s | created %s | %s | %s" % (str(r["old_resolved_at"])[:16], str(r["created_at"])[:16], r["old_detected_via"], short(r["open_message"])))
            log("")
    log(note)


async def main():
    commit = os.environ.get("CONFIRM") == "CORRECT"
    groups = set(g.strip() for g in os.environ.get("GROUPS", "fix_time,relabel_inferred").split(",") if g.strip())
    window_days = int(os.environ.get("WINDOW_DAYS", "30"))
    max_rows = int(os.environ.get("MAX_ROWS", "500"))
    from sqlalchemy import text
    from database import SessionLocal
    async with SessionLocal() as s:
        await s.execute(text("SET LOCAL statement_timeout = '300s'"))
        cand = (await s.execute(text(CANDIDATES_SQL), {"window_days": window_days, "clean_from": datetime.fromisoformat(CLEAN_FROM.replace("+00", "+00:00")),
                                                       "tolerance": TOLERANCE_SECONDS})).fetchall()
    rows = plan(cand, groups)
    todo = [r for r in rows if r["selected"] == "yes"]
    write_csv(rows, sys.stdout)  # the decision list is written BEFORE any write to the database
    sys.stdout.flush()
    if not todo:
        summarise(rows, commit, "nothing to change", {"updated": 0, "skipped": 0}, groups, window_days)
        return 0
    if len(todo) > max_rows:
        summarise(rows, commit, "REFUSED: %d incidents to change exceeds MAX_ROWS=%d" % (len(todo), max_rows), {"updated": 0, "skipped": 0}, groups, window_days)
        return 1
    ok, note, stats = await apply(rows, commit, SessionLocal, text)
    summarise(rows, commit, note, stats, groups, window_days)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
