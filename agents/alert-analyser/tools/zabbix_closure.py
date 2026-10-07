"""Going-forward closure of recovered Zabbix incidents (alert-analyser).

Run as a scheduled job (registered in main.py). Uses tools/zabbix_recovery.decide() and only touches
ESCALATED Zabbix incidents that have a recovery notice: a later Zabbix Closed notice for the same
problem. There is no limit on how long after the Open the notice came: a recovery ends the problem.

Two modes share this code. commit=False is the dry run: every write is performed inside one
transaction that is rolled back, so the real SQL is checked on every run but nothing changes.
commit=True resolves the incidents. Which one runs is decided by which job is scheduled (see main.py).

A resolved incident gets (same as the one-off cleanup, decided 2026-10-07): status RESOLVED;
resolved_at = the Closed notice's own time (never earlier than created_at); resolution_type
closure_alert_correlation; detected_via message_parse; one incident_status_history row
ESCALATED -> RESOLVED with the same time. Rows are updated only while still ESCALATED and still Zabbix.

Incidents that carry another agent's action_outcome are left alone unless that outcome is FAILED
(a successful or healthy outcome is the other agent's to record). They are counted in the summary.
"""
import asyncio
import uuid
from collections import Counter

try:
    from tools.zabbix_recovery import decide, parse_ts
except ImportError:
    pass  # the tests load zabbix_recovery.py and this file into one namespace

RUN_TIMEOUT_SECONDS = 120
MAX_ROWS_PER_RUN = 1000

OPENS_SQL = """
SELECT id::text AS id, alert_id, alert_payload->>'message' AS message, alert_payload->>'createdAt' AS created_at,
       action_outcome->'details'->>'outcome' AS outcome, (action_outcome IS NOT NULL) AS has_outcome
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


def plan(opens_rows, event_rows):
    """One row per ESCALATED Zabbix incident. action: resolve / skip_agent_outcome / keep / review."""
    events_by_id = {}
    for r in event_rows:
        events_by_id.setdefault(r.id, {"id": r.id, "message": r.message, "createdAt": r.created_at})
    events = list(events_by_id.values())
    opens = [{"id": r.id, "alert_id": r.alert_id, "message": r.message, "createdAt": r.created_at} for r in opens_rows]
    meta = {r.id: r for r in opens_rows}
    rows = []
    for d in decide(opens, events):
        m = meta[d["id"]]
        action = d["decision"]
        if action == "resolve" and m.has_outcome and (m.outcome or "").strip().upper() != "FAILED":
            action = "skip_agent_outcome"
        rows.append({"incident_id": d["id"], "alert_id": m.alert_id, "closed_created_at": d["closed_at"],
                     "decision": d["decision"], "reason": d["reason"], "action": action, "applied": "",
                     "outcome": m.outcome if m.has_outcome else None})
    return rows


class _Stop(Exception):
    pass


class _Rehearsal(Exception):
    pass


async def apply(rows, commit, session_factory, text):
    """Write the 'resolve' rows in ONE transaction; roll back unless every check passes. With
    commit=False it always rolls back (dry run). Returns (ok, note, stats)."""
    todo = [r for r in rows if r["action"] == "resolve"]
    stats = {"updated": 0, "skipped": 0}
    async with session_factory() as s:
        try:
            async with s.begin():
                await s.execute(text("SET LOCAL lock_timeout = '30s'"))
                await s.execute(text("SET LOCAL statement_timeout = '100s'"))
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
    reasons = Counter(r["reason"] for r in rows if r["action"] == "resolve")
    verb = "resolved" if commit else "would resolve"
    n = stats["updated"] if commit else c.get("resolve", 0)
    return ("%s: %s %d (exact %d, prefix %d) | judged %d: keep %d, review %d, agent outcome not FAILED %d | skipped (no longer ESCALATED) %d"
            % ("live" if commit else "dry run", verb, n, reasons.get("closed_exact", 0), reasons.get("closed_prefix", 0),
               len(rows), c.get("keep", 0), c.get("review", 0), c.get("skip_agent_outcome", 0), stats["skipped"]))


async def _run(commit, session_factory, text):
    async with session_factory() as s:
        opens_rows = (await s.execute(text(OPENS_SQL))).fetchall()
        event_rows = (await s.execute(text(EVENTS_SQL))).fetchall()
    rows = plan(opens_rows, event_rows)
    todo = [r for r in rows if r["action"] == "resolve"]
    stats = {"updated": 0, "skipped": 0}
    if todo:
        if len(todo) > MAX_ROWS_PER_RUN:
            raise RuntimeError("REFUSED: %d incidents to resolve exceeds the limit of %d per run" % (len(todo), MAX_ROWS_PER_RUN))
        ok, note, stats = await apply(rows, commit, session_factory, text)
        if not ok:
            raise RuntimeError(note)
    return summarise(rows, commit, stats)


async def run_job(commit, session_factory=None, text=None, timeout=RUN_TIMEOUT_SECONDS):
    """Entry point for the scheduled job. Returns the one-line summary stored in the run record;
    raises (so the run is marked failed) on a rollback, a refusal or a timeout."""
    if session_factory is None:
        from database import SessionLocal as session_factory
    if text is None:
        from sqlalchemy import text
    return await asyncio.wait_for(_run(commit, session_factory, text), timeout)
