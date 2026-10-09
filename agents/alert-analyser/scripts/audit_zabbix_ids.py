"""Read-only audit of the Zabbix incident closure, built on Zabbix's own problem IDs (alert-analyser).

Independent of the closure job's code: it checks the result in the database, so it can catch the job being wrong.
Checks (the creation side, "an alert with no incident", is scripts/audit_incidents.py C1-C3: run both):
  Z1  no open incident has a recovery notice carrying its own problem ID older than 15 minutes (the job missed it)
  Z2  every open incident older than 15 minutes has its alert details stored (problem ID)
  Z3  no incident was closed since the old title-based closer was switched off (CLEAN_FROM) without a notice of its own
      (a closure's date is its resolved_at: the incident row's updated_at is also moved by corrections)
  Z4  no incident closed since then has a closing time more than 5 minutes off its own notice (never earlier than created_at)
  Z5  every Zabbix alert of the last 24 hours older than 15 minutes has its details stored
  Z6  the sync, closure and detail jobs ran in the last 24 hours without a failure and are not late
  Z7  no Zabbix incident was closed since CLEAN_FROM by the old title-based path (detected_via opsgenie_live)
Informational lines count the closures of the last 30 days made before CLEAN_FROM (by title matching or the old live-status path), split by
method; 30 days is the alert history's retention.
Run on the box:  docker exec -i org-dev-alert-analyser-1 python - < agents/alert-analyser/scripts/audit_zabbix_ids.py
Nothing is written.
"""
import asyncio
from collections import Counter
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from database import SessionLocal

CLEAN_FROM = "2026-10-08 05:39:48+00"   # the agent restart that switched the old title-based closer (opsgenie_live) off; the closure job on problem IDs went live at 03:55
GRACE = "15 minutes"
# Both helpers of CTE are MATERIALIZED (computed once): without it PostgreSQL re-ran them for every closure row and the closure
# query exceeded the role's 2 minute statement limit (8 Oct 2026).
TIME_TOLERANCE_MIN = 5
LATE = {"alert-opsgenie-sync": 10, "zabbix-closure": 15, "zabbix-detail": 15}   # minutes since the last run

CTE = """
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
)
"""

Z1_SQL = CTE + """
SELECT i.id::text AS id, right(i.alert_payload->>'message', 58) AS alert, i.created_at, c.closed_at
FROM incident_management.incidents i
JOIN zabbix_alert_detail d ON d.alert_id = i.alert_id AND d.problem_id IS NOT NULL
JOIN closed c ON c.problem_id = d.problem_id
WHERE i.status = 'ESCALATED' AND i.alert_payload->>'message' ILIKE '%[Zabbix]%' AND c.closed_at < now() - interval '__GRACE__'
ORDER BY c.closed_at
""".replace("__GRACE__", GRACE)

Z2_SQL = """
SELECT i.id::text AS id, right(i.alert_payload->>'message', 58) AS alert, i.created_at
FROM incident_management.incidents i LEFT JOIN zabbix_alert_detail d ON d.alert_id = i.alert_id
WHERE i.status = 'ESCALATED' AND i.alert_payload->>'message' ILIKE '%[Zabbix]%' AND d.alert_id IS NULL AND i.created_at < now() - interval '__GRACE__'
ORDER BY i.created_at
""".replace("__GRACE__", GRACE)

CLOSED_SQL = CTE + """
SELECT i.id::text AS id, right(i.alert_payload->>'message', 58) AS alert, i.created_at, i.resolved_at, i.updated_at, i.detected_via,
       d.alert_id AS detail_alert, d.problem_id, c.closed_at,
       (SELECT count(*) FROM zabbix_alert_detail x WHERE x.problem_id = d.problem_id AND x.kind = 'closed') AS closed_rows
FROM incident_management.incidents i
LEFT JOIN zabbix_alert_detail d ON d.alert_id = i.alert_id
LEFT JOIN closed c ON c.problem_id = d.problem_id
WHERE i.status = 'RESOLVED' AND i.resolution_type = 'closure_alert_correlation' AND i.detected_via IN ('message_parse', 'opsgenie_live')
  AND i.alert_payload->>'message' ILIKE '%[Zabbix] [Open]%' AND i.updated_at > now() - interval '30 days'
"""

Z7_SQL = """
SELECT count(*) FROM incident_management.incidents
WHERE status = 'RESOLVED' AND resolution_type = 'closure_alert_correlation' AND detected_via = 'opsgenie_live'
  AND alert_payload->>'message' ILIKE '%[Zabbix] [Open]%' AND resolved_at >= CAST(:since AS timestamptz)
"""

Z5_SQL = """
SELECT count(*) FROM alert_sync_history h LEFT JOIN zabbix_alert_detail d ON d.alert_id = h.alert_id
WHERE h.alert_data->>'message' ILIKE '%[Zabbix]%' AND h.alert_created_at > now() - interval '24 hours'
  AND h.alert_created_at < now() - interval '__GRACE__' AND d.alert_id IS NULL
""".replace("__GRACE__", GRACE)

DETAIL_STATUS_SQL = "SELECT fetch_status, count(*) AS n FROM zabbix_alert_detail GROUP BY 1 ORDER BY 2 DESC"

Z6_SQL = """
SELECT job_id, count(*) AS runs, count(*) FILTER (WHERE status <> 'success') AS not_success, max(started_at) AS last_run,
       extract(epoch FROM now() - max(started_at)) / 60.0 AS minutes_since
FROM alert_job_runs
WHERE job_id IN ('alert-opsgenie-sync', 'zabbix-closure', 'zabbix-detail') AND started_at > now() - interval '24 hours'
GROUP BY job_id
"""


def line(ok, name, detail=""):
    return "[%s] %s%s" % ("PASS" if ok else "FAIL", name, ("  " + detail) if detail else "")


async def main():
    live_at = datetime.fromisoformat(CLEAN_FROM.replace("+00", "+00:00"))
    results, out = [], []
    async with SessionLocal() as s:
        await s.execute(text("SET LOCAL statement_timeout = '300s'"))   # read-only: a limit above the role's 2 minute default is safe
        z1 = (await s.execute(text(Z1_SQL))).fetchall()
        z2 = (await s.execute(text(Z2_SQL))).fetchall()
        closed = (await s.execute(text(CLOSED_SQL))).fetchall()
        z5 = (await s.execute(text(Z5_SQL))).scalar()
        dstat = (await s.execute(text(DETAIL_STATUS_SQL))).fetchall()
        z6 = (await s.execute(text(Z6_SQL))).fetchall()
        z7 = (await s.execute(text(Z7_SQL), {"since": live_at})).scalar()

    out.append(line(not z1, "Z1 open incidents whose own recovery notice exists (the closure job missed them): %d" % len(z1)))
    results.append(not z1)
    out += ["       %s | created %s | notice %s" % (r.alert, str(r.created_at)[:16], str(r.closed_at)[:16]) for r in z1[:15]]
    out.append(line(not z2, "Z2 open incidents older than %s without stored alert details: %d" % (GRACE, len(z2))))
    results.append(not z2)
    out += ["       %s | created %s" % (r.alert, str(r.created_at)[:16]) for r in z2[:15]]

    tol = timedelta(minutes=TIME_TOLERANCE_MIN)
    no_own, wrong_time, unverifiable, ok_n = {"new": [], "old": []}, {"new": [], "old": []}, 0, 0
    aged_out = {"new": 0, "old": 0}
    for r in closed:
        era = "new" if r.resolved_at >= live_at else "old"
        if r.detail_alert is None or r.problem_id is None:
            unverifiable += 1
        elif r.closed_at is None and r.closed_rows > 0:
            aged_out[era] += 1   # a notice with their problem ID exists but is older than the alert history: its time cannot be checked
        elif r.closed_at is None:
            no_own[era].append(r)
        elif abs(r.resolved_at - max(r.closed_at, r.created_at)) > tol:
            wrong_time[era].append(r)
        else:
            ok_n += 1
    out.append(line(not no_own["new"], "Z3 incidents closed since the old closer was switched off (%s) without a notice of their own: %d" % (CLEAN_FROM[:16], len(no_own["new"]))))
    results.append(not no_own["new"])
    out += ["       %s | created %s | resolved %s" % (r.alert, str(r.created_at)[:16], str(r.resolved_at)[:16]) for r in no_own["new"][:15]]
    out.append(line(not wrong_time["new"], "Z4 incidents closed since then with a closing time more than %d min off their own notice: %d" % (TIME_TOLERANCE_MIN, len(wrong_time["new"]))))
    results.append(not wrong_time["new"])
    out += ["       %s | resolved %s | own notice %s" % (r.alert, str(r.resolved_at)[:16], str(r.closed_at)[:16]) for r in wrong_time["new"][:15]]
    out.append(line(z5 == 0, "Z5 Zabbix alerts of the last 24 h (older than %s) without stored details: %d" % (GRACE, z5)))
    results.append(z5 == 0)

    jobs = {r.job_id: r for r in z6}
    problems = []
    for job, late in LATE.items():
        r = jobs.get(job)
        if r is None:
            problems.append("%s: no run in the last 24 h" % job)
        else:
            if r.not_success:
                problems.append("%s: %d of %d runs failed" % (job, r.not_success, r.runs))
            if r.minutes_since > late:
                problems.append("%s: last run %.0f min ago (limit %d)" % (job, r.minutes_since, late))
    out.append(line(not problems, "Z6 jobs ran in the last 24 h without failures and on time", "; ".join(problems)))
    results.append(not problems)
    out.append(line(z7 == 0, "Z7 Zabbix incidents closed by the old title-based path (opsgenie_live) since %s: %d" % (CLEAN_FROM[:16], z7)))
    results.append(z7 == 0)

    out.append("")
    def by_method(rows):
        c = Counter(r.detected_via for r in rows)
        return ", ".join("%s %d" % (k, c[k]) for k in sorted(c)) or "none"

    out.append("INFO closures of the last 30 days checked against their own notice, any method: %d fully consistent, %d unverifiable (no stored problem ID)" % (ok_n, unverifiable))
    out.append("INFO closures whose own notice is older than the alert history (its time cannot be checked): %d before %s, %d since" % (aged_out["old"], CLEAN_FROM[:16], aged_out["new"]))
    out.append("INFO closed before %s with no notice of their own: %d (%s)" % (CLEAN_FROM[:16], len(no_own["old"]), by_method(no_own["old"])))
    out.append("INFO closed before %s with a closing time more than %d min off their own notice: %d (%s)"
               % (CLEAN_FROM[:16], TIME_TOLERANCE_MIN, len(wrong_time["old"]), by_method(wrong_time["old"])))
    out.append("INFO alert details by fetch status: " + ", ".join("%s %d" % (r.fetch_status, r.n) for r in dstat))
    out.append("SUMMARY: " + " ".join("Z%d=%s" % (i + 1, "PASS" if v else "FAIL") for i, v in enumerate(results)) + "  ->  " + ("PASS" if all(results) else "FAIL"))
    print("\n".join(out))


asyncio.run(main())
