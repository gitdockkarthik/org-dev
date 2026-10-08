"""Standing closure of stale Zabbix incidents (alert-analyser): the automatic form of scripts/stale_cleanup.py.

Run as a scheduled job (registered in main.py, once a day). An ESCALATED Zabbix incident that has had no recovery notice for
STALE_DAYS (7) days, or STALE_DAYS_PROD (14) days when its environment is production (an [Env:...] tag ending in "prod"),
is closed so the open list shows fresh issues. It never closes:
  - an incident that already has a recovery notice with its own problem ID (the closure job closes it, with the exact time),
  - an incident the automation agent has an outcome for (other than FAILED), the same rule as the closure job,
  - an incident without stored alert details (the trigger cannot be checked).
close_inferred: the same trigger (host + check) acted again later with another problem ID, so this problem had recovered:
  resolution_type closure_recovery_inferred, resolved_at = that later time (an upper bound of the real recovery).
close_stale: nothing is known: resolution_type stale_no_recovery, resolved_at = created_at, so the closure falls outside every
  current date window and cannot move the MTTR / MTTA cards (they count every resolution_type).
detected_via is the existing value message_parse; resolution_type carries the real label. One incident_status_history row
ESCALATED -> RESOLVED is written with the same time as resolved_at. commit=False is the dry run: every write runs inside one
transaction that is rolled back. Everything is written in ONE transaction that rolls back unless every check passes; the run is
then marked failed. At most MAX_ROWS_PER_RUN incidents are closed per run (a bigger list is refused).
"""
import asyncio
import uuid
from collections import Counter
from datetime import datetime, timezone

RUN_TIMEOUT_SECONDS = 120
MAX_ROWS_PER_RUN = 30
STALE_DAYS = 7
STALE_DAYS_PROD = 14
LABEL_INFERRED = "closure_recovery_inferred"
LABEL_STALE = "stale_no_recovery"

CANDIDATES_SQL = """
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
SELECT i.id::text AS id, i.alert_id, i.created_at, i.alert_payload->>'message' AS message,
       (i.action_outcome IS NOT NULL) AS has_outcome, i.action_outcome->'details'->>'outcome' AS outcome,
       d.alert_id AS detail_alert, d.host, d.problem_name, d.problem_id,
       (SELECT min(x.at) FROM det x WHERE x.problem_id = d.problem_id AND x.kind = 'closed') AS own_notice_at,
       (SELECT min(x.at) FROM det x WHERE x.host = d.host AND x.problem_name = d.problem_name
                                       AND x.problem_id <> d.problem_id AND x.at > oa.at) AS later_activity_at
FROM incident_management.incidents i
LEFT JOIN zabbix_alert_detail d ON d.alert_id = i.alert_id
LEFT JOIN ev oa ON oa.alert_id = i.alert_id
WHERE i.status = 'ESCALATED' AND i.alert_payload->>'message' ILIKE '%[Zabbix] [Open]%'
  AND i.created_at <= now() - make_interval(days => CAST(:min_days AS integer))
ORDER BY i.created_at
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


def is_production(message):
    """True when the title carries an [Env:...] tag whose value ends in "prod" (o1prod, nvaprod)."""
    s = message or ""
    i = s.find("[Env:")
    if i < 0:
        return False
    j = s.find("]", i)
    return j > i and s[i + 5:j].strip().lower().endswith("prod")


def plan(rows, now=None, stale_days=STALE_DAYS, prod_days=STALE_DAYS_PROD):
    """One row per candidate. action: close_inferred / close_stale / keep_younger / skip_has_notice /
    skip_agent_outcome / held_no_details."""
    now = now or datetime.now(timezone.utc)
    out, seen = [], set()
    for r in rows:
        if r.id in seen:
            continue
        seen.add(r.id)
        age = (now - r.created_at).total_seconds() / 86400.0
        need = prod_days if is_production(r.message) else stale_days
        row = {"incident_id": r.id, "alert_id": r.alert_id, "age_days": age, "label": "", "resolved_at_will_be": None, "applied": ""}
        if age < need:
            row["action"] = "keep_younger"
        elif r.detail_alert is None:
            row["action"] = "held_no_details"
        elif r.own_notice_at is not None:
            row["action"] = "skip_has_notice"
        elif r.has_outcome and (r.outcome or "").strip().upper() != "FAILED":
            row["action"] = "skip_agent_outcome"
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
    """Close the close_* rows in ONE transaction; roll back unless every check passes. With commit=False it always rolls
    back (dry run). Returns (ok, note, stats)."""
    todo = [r for r in rows if r["action"].startswith("close_")]
    stats = {"updated": 0, "skipped": 0}
    async with session_factory() as s:
        try:
            async with s.begin():
                await s.execute(text("SET LOCAL lock_timeout = '30s'"))
                await s.execute(text("SET LOCAL statement_timeout = '100s'"))
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
            return True, "dry run: everything was rolled back", stats
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
    return True, "committed", stats


def summarise(rows, commit, stats):
    c = Counter(r["action"] for r in rows)
    n = stats["updated"] if commit else c.get("close_inferred", 0) + c.get("close_stale", 0)
    return ("%s: %s %d (inferred %d, stale %d) | judged %d: kept younger %d, own notice %d, agent outcome not FAILED %d, no details %d"
            " | skipped (no longer ESCALATED) %d"
            % ("live" if commit else "dry run", "closed" if commit else "would close", n, c.get("close_inferred", 0), c.get("close_stale", 0),
               len(rows), c.get("keep_younger", 0), c.get("skip_has_notice", 0), c.get("skip_agent_outcome", 0),
               c.get("held_no_details", 0), stats["skipped"]))


async def _run(commit, session_factory, text):
    async with session_factory() as s:
        cand = (await s.execute(text(CANDIDATES_SQL), {"min_days": STALE_DAYS})).fetchall()
    rows = plan(cand)
    todo = [r for r in rows if r["action"].startswith("close_")]
    stats = {"updated": 0, "skipped": 0}
    if todo:
        if len(todo) > MAX_ROWS_PER_RUN:
            raise RuntimeError("REFUSED: %d incidents to close exceeds the limit of %d per run" % (len(todo), MAX_ROWS_PER_RUN))
        ok, note, stats = await apply(rows, commit, session_factory, text)
        if not ok:
            raise RuntimeError(note)
    return summarise(rows, commit, stats)


async def run_job(commit, session_factory=None, text=None, timeout=RUN_TIMEOUT_SECONDS):
    """Entry point for the scheduled job. Returns the one-line summary stored in the run record; raises (so the run is
    marked failed) on a rollback, a refusal or a timeout."""
    if session_factory is None:
        from database import SessionLocal as session_factory
    if text is None:
        from sqlalchemy import text
    return await asyncio.wait_for(_run(commit, session_factory, text), timeout)
