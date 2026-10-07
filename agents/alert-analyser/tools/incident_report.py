"""Data behind the dashboard's Incident Report tab (alert-analyser). Read-only: SELECTs only.

build_report() turns database rows into a JSON-ready dict; get_report() fetches the rows. The open-incident
reasons come from tools/zabbix_closure.plan(), so the report always agrees with what the closure job does.
Sections: sources (which sources create incidents, genuine alerts from paused sources), open incidents with a
plain-language reason and the other agent's fix attempt, the closure job (live / dry run / off, latest runs),
resolved incidents of the last 30 days by how they were resolved, and short definitions.
"""
import time
from collections import Counter
from datetime import datetime, timezone

try:
    from tools.zabbix_recovery import parse_ts
    from tools.zabbix_closure import plan, EVENTS_SQL
except ImportError:
    pass  # the tests load the three files into one namespace

LIVE_JOB = "zabbix-closure"
DRY_RUN_JOB = "zabbix-closure-dry-run"
CACHE_SECONDS = 60
MAX_RUNS = 10

OPENS_SQL = """
SELECT id::text AS id, alert_id, alert_payload->>'message' AS message, alert_payload->>'createdAt' AS created_at,
       action_outcome->'details'->>'outcome' AS outcome, (action_outcome IS NOT NULL) AS has_outcome,
       action_outcome->>'action_taken_at' AS action_taken_at, left(action_outcome->>'fix_applied', 120) AS fix_applied,
       priority, created_at AS incident_created_at
FROM incident_management.incidents
WHERE status = 'ESCALATED' AND alert_payload->>'message' ILIKE '%[Zabbix]%'
"""

SOURCES_SQL = """
SELECT alert_data->>'message' AS message, alert_data->>'source' AS source
FROM alert_sync_history
WHERE classification = 'genuine' AND alert_created_at > now() - interval '24 hours'
"""

SCHEDULES_SQL = """
SELECT job_id, cron_expression, enabled FROM alert_job_schedules
WHERE job_id IN ('zabbix-closure', 'zabbix-closure-dry-run')
"""

RUNS_SQL = """
SELECT job_id, status, triggered_by, started_at, duration_seconds, logs
FROM alert_job_runs
WHERE job_id IN ('zabbix-closure', 'zabbix-closure-dry-run')
ORDER BY started_at DESC LIMIT 20
"""

RESOLVED_SQL = """
SELECT action_outcome->'details'->>'outcome' AS outcome, resolution_type, count(*) AS n
FROM incident_management.incidents
WHERE status = 'RESOLVED' AND resolved_at >= now() - interval '30 days'
  AND resolution_type IS DISTINCT FROM 'noise_suspect_audit_record'
GROUP BY 1, 2
"""

REASON_TEXT = {
    "pending_closure": "A recovery notice was received; the closure job closes it at its next run (within 5 minutes).",
    "no_recovery": "No recovery notice has been received yet.",
    "ambiguous_prefix": "The alert title was cut off by OpsGenie and matches recovery notices for different thresholds; a person needs to confirm.",
    "older_open_superseded": "A newer alert for the same problem replaced this one; it closes when that problem recovers.",
    "unparsed_title": "The alert title could not be read (for example an unexpanded Zabbix placeholder); a person needs to review it.",
    "unparsed_time": "The alert time could not be read; a person needs to review it.",
}

RESOLVED_LABELS = [
    ("fixed_by_action", "Fixed by an automated action", "pipeline"),
    ("already_healthy", "Service was already healthy", "outside"),
    ("closed_in_opsgenie", "Alert was already closed in OpsGenie", "outside"),
    ("agent_other", "Closed by the automation agent (other outcome)", "outside"),
    ("recovered_after_failed_fix", "Recovered after an automated fix failed", "outside"),
    ("recovered_by_notice", "Recovered (recovery notice received)", "outside"),
    ("self_healed", "Self-healed (found closed in OpsGenie)", "outside"),
    ("other", "Other", "outside"),
    ("pre_launch_test", "Pre-launch test data (cleaned up before launch; not counted)", "test"),
]

# Test-era resolution types: shown separately and left out of the total so they do not pass as real resolutions.
PRE_LAUNCH_TYPES = ("pre_launch_bulk_cleanup", "pre_launch_cleanup_2026-09-16", "closure_alert_correlation_backfill")

DEFINITIONS = {
    "ESCALATED": "The incident is open: an alert that needs attention was raised and no recovery has been recorded yet.",
    "RESOLVED": "The incident is closed. The list above shows how: an automated fix succeeded, the service was already healthy, "
                "a recovery notice arrived, or the alert was already closed in OpsGenie.",
}


def _iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else v


def bucket_of(outcome, resolution_type):
    o = (outcome or "").strip().upper()
    if resolution_type in PRE_LAUNCH_TYPES:
        return "pre_launch_test"
    if resolution_type == "action_resolved":
        return {"SUCCESS": "fixed_by_action", "ALREADY_HEALTHY": "already_healthy",
                "CLOSED_IN_OPSGENIE": "closed_in_opsgenie"}.get(o, "agent_other")
    if resolution_type == "closure_alert_correlation":
        return "recovered_after_failed_fix" if o == "FAILED" else "recovered_by_notice"
    if resolution_type == "self_healed":
        return "self_healed"
    return "other"


def _reason(action, reason, outcome):
    if action == "resolve":
        code = "pending_closure"
    elif action == "keep":
        code = "no_recovery"
    elif action == "skip_agent_outcome":
        return "agent_outcome", ("The automation agent recorded an outcome (%s); its status is left to that agent." % (outcome or "unknown"))
    else:
        code = reason if reason in REASON_TEXT else "no_recovery"
    return code, REASON_TEXT[code]


def build_report(opens_rows, event_rows, source_rows, schedule_rows, run_rows, resolved_rows, enabled_sources, source_key, now):
    rows = plan(opens_rows, event_rows)
    meta = {r.id: r for r in opens_rows}
    incidents = []
    for p in rows:
        m = meta[p["incident_id"]]
        code, text = _reason(p["action"], p["reason"], p["outcome"])
        created = m.incident_created_at
        fix = None
        if m.has_outcome:
            fix = {"outcome": m.outcome or "unknown", "at": m.action_taken_at, "what": m.fix_applied}
        incidents.append({"title": " ".join((m.message or "").split()), "priority": m.priority, "created_at": _iso(created),
                          "age_hours": round((now - created).total_seconds() / 3600.0, 1) if created else None,
                          "reason_code": code, "reason": text, "fix_attempt": fix,
                          "incident_id": m.id, "alert_id": m.alert_id})
    incidents.sort(key=lambda i: i["created_at"] or "")

    counts = Counter()
    for r in source_rows:
        counts[source_key({"message": r.message, "source": r.source})] += 1
    alerts_24h = [{"source": k, "enabled": k in enabled_sources, "genuine_alerts": n}
                  for k, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]

    live_on = any(s.enabled for s in schedule_rows if s.job_id == LIVE_JOB)
    dry_on = any(s.enabled for s in schedule_rows if s.job_id == DRY_RUN_JOB)
    mode = "live" if live_on else ("dry run" if dry_on else "off")
    cron = next((s.cron_expression for s in schedule_rows if s.job_id == (LIVE_JOB if live_on else DRY_RUN_JOB)), None)
    runs = [{"job": "closure" if r.job_id == LIVE_JOB else "closure (dry run)", "status": r.status, "triggered_by": r.triggered_by,
             "started_at": _iso(r.started_at), "duration_seconds": r.duration_seconds, "note": r.logs}
            for r in sorted(run_rows, key=lambda r: r.started_at, reverse=True)[:MAX_RUNS]]

    by_bucket = Counter()
    for r in resolved_rows:
        by_bucket[bucket_of(r.outcome, r.resolution_type)] += r.n
    buckets = [{"key": k, "label": label, "count": by_bucket[k], "kind": kind} for k, label, kind in RESOLVED_LABELS if by_bucket[k]]

    return {"generated_at": _iso(now),
            "sources": {"enabled": sorted(enabled_sources), "alerts_24h": alerts_24h},
            "open": {"total": len(incidents), "by_reason": dict(Counter(i["reason_code"] for i in incidents)), "incidents": incidents},
            "closure_job": {"mode": mode, "schedule": cron, "runs": runs},
            "resolved_30d": {"total": sum(b["count"] for b in buckets if b["kind"] != "test"), "buckets": buckets},
            "definitions": DEFINITIONS}


async def get_report(session_factory, text, enabled_sources, source_key, now=None):
    async with session_factory() as s:
        opens_rows = (await s.execute(text(OPENS_SQL))).fetchall()
        event_rows = (await s.execute(text(EVENTS_SQL))).fetchall()
        source_rows = (await s.execute(text(SOURCES_SQL))).fetchall()
        schedule_rows = (await s.execute(text(SCHEDULES_SQL))).fetchall()
        run_rows = (await s.execute(text(RUNS_SQL))).fetchall()
        resolved_rows = (await s.execute(text(RESOLVED_SQL))).fetchall()
    return build_report(opens_rows, event_rows, source_rows, schedule_rows, run_rows, resolved_rows,
                        enabled_sources, source_key, now or datetime.now(timezone.utc))


_cache = {"at": None, "data": None}


async def get_report_cached(session_factory, text, enabled_sources, source_key, ttl=CACHE_SECONDS, clock=time.monotonic):
    t = clock()
    if _cache["data"] is not None and _cache["at"] is not None and t - _cache["at"] < ttl:
        return _cache["data"]
    data = await get_report(session_factory, text, enabled_sources, source_key)
    _cache["at"], _cache["data"] = t, data
    return data
