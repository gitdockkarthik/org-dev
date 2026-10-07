"""Fills zabbix_alert_detail (alert-analyser): the problem ID and the full title of each Zabbix alert.

OpsGenie cuts an alert's message at 130 characters and keeps the full problem name and Zabbix's own
"Original problem ID" in the alert's description, which only Get Alert returns (see tools/zabbix_detail.py).
Every few minutes this job fetches the description of the Zabbix alerts that are not in the table yet and
stores what tools.zabbix_detail.extract() reads from it. It writes only to zabbix_alert_detail, never to an
incident, so it cannot change what the closure job does.

Order: the alerts behind the ESCALATED incidents first, then the 30-day history newest first; rows whose
earlier fetch failed ('error') are tried again after ENRICH_RETRY_MINUTES. At most ENRICH_BATCH_LIMIT alerts per run,
ENRICH_CONCURRENCY requests at a time, and the run stops at once when OpsGenie throttles (429) or the time budget is
used up; what was fetched is already stored, the rest is picked up by the next run.

source needs get_alert_description(alert_id) -> (outcome, description) with outcome 'ok', 'not_found',
'throttled' or 'error' (StandaloneOpsgenieSource has it). session_factory and text are passed in so that
the module has no import of the database layer and can be tested without one.
"""
import asyncio
import time
from collections import Counter

try:
    from tools.zabbix_detail import extract
except ImportError:
    pass  # the tests load the modules into one namespace

ENRICH_TIMEOUT_SECONDS = 110
ENRICH_BATCH_LIMIT = 200
ENRICH_BUDGET_SECONDS = 80
ENRICH_CONCURRENCY = 4
ENRICH_RETRY_MINUTES = 10

_ENRICH_FROM = """
FROM (
    SELECT DISTINCT ON (alert_id) alert_id, message, prio, ts FROM (
        SELECT alert_id::text AS alert_id, alert_payload->>'message' AS message, 0 AS prio, created_at::timestamptz AS ts
        FROM incident_management.incidents
        WHERE status = 'ESCALATED' AND alert_payload->>'message' ILIKE '%[Zabbix]%'
        UNION ALL
        SELECT alert_id::text, alert_data->>'message', 1, alert_created_at::timestamptz
        FROM alert_sync_history
        WHERE alert_data->>'message' ILIKE '%[Zabbix]%'
    ) u
    ORDER BY alert_id, prio, ts DESC
) w
LEFT JOIN zabbix_alert_detail d ON d.alert_id = w.alert_id
WHERE d.alert_id IS NULL OR (d.fetch_status = 'error' AND d.fetched_at < now() - interval '__RETRY__ minutes')
""".replace("__RETRY__", str(ENRICH_RETRY_MINUTES))

ENRICH_CANDIDATES_SQL = "SELECT w.alert_id, w.message " + _ENRICH_FROM + " ORDER BY w.prio, w.ts DESC LIMIT :n"
ENRICH_COUNT_SQL = "SELECT count(*) " + _ENRICH_FROM

ENRICH_UPSERT_SQL = """
INSERT INTO zabbix_alert_detail
    (alert_id, problem_id, problem_name, full_message, title_status, kind, host, severity, fetch_status, fetched_at)
VALUES
    (:alert_id, :problem_id, :problem_name, :full_message, :title_status, :kind, :host, :severity, :fetch_status, now())
ON CONFLICT (alert_id) DO UPDATE SET
    problem_id = EXCLUDED.problem_id, problem_name = EXCLUDED.problem_name, full_message = EXCLUDED.full_message,
    title_status = EXCLUDED.title_status, kind = EXCLUDED.kind, host = EXCLUDED.host, severity = EXCLUDED.severity,
    fetch_status = EXCLUDED.fetch_status, fetched_at = now()
"""

_ENRICH_EMPTY = {"problem_id": None, "problem_name": None, "full_message": None, "title_status": None,
          "kind": None, "host": None, "severity": None}


def build_enrich_row(alert_id, message, outcome, description):
    """The parameters stored for one fetched alert; outcome is 'ok', 'not_found' or 'error'."""
    p = dict(_ENRICH_EMPTY)
    p["alert_id"] = alert_id
    if outcome == "ok":
        if description and description.strip():
            p.update(extract(message, description))
            p["fetch_status"] = "ok"
        else:
            p["fetch_status"] = "no_description"
    else:
        p["fetch_status"] = outcome
    return p


async def _enrich_fetch(source, alert_id):
    try:
        return await source.get_alert_description(alert_id)
    except Exception:
        return "error", None


async def _enrich_run(source, session_factory, text, limit, budget_seconds, concurrency, clock):
    started = clock()
    async with session_factory() as s:
        rows = (await s.execute(text(ENRICH_CANDIDATES_SQL), {"n": limit})).fetchall()
        waiting = (await s.execute(text(ENRICH_COUNT_SQL))).scalar() or 0
    if not rows:
        return "nothing to fetch: every Zabbix alert in the history already has its details"
    fetch_status, titles = Counter(), Counter()
    stopped, stored = "none", 0
    for i in range(0, len(rows), concurrency):
        if clock() - started > budget_seconds:
            stopped = "the time budget"
            break
        chunk = rows[i:i + concurrency]
        got = await asyncio.gather(*[_enrich_fetch(source, r.alert_id) for r in chunk])
        batch = []
        for r, (outcome, description) in zip(chunk, got):
            if outcome == "throttled":
                stopped = "OpsGenie throttling"
                continue
            p = build_enrich_row(r.alert_id, r.message, outcome, description)
            batch.append(p)
            fetch_status[p["fetch_status"]] += 1
            if p["title_status"]:
                titles[p["title_status"]] += 1
        if batch:
            async with session_factory() as s:
                for p in batch:
                    await s.execute(text(ENRICH_UPSERT_SQL), p)
                await s.commit()
            stored += len(batch)
        if stopped != "none":
            break
    return ("fetched %d: ok %d, no description %d, not found %d, error %d | titles: extended %d, same %d, mismatch %d, other %d"
            " | stopped by: %s | still to fetch: %d") % (
        stored, fetch_status["ok"], fetch_status["no_description"], fetch_status["not_found"], fetch_status["error"],
        titles["extended"], titles["same"], titles["mismatch"],
        sum(titles.values()) - titles["extended"] - titles["same"] - titles["mismatch"], stopped, max(0, waiting - stored))


async def run_enrichment(source, session_factory, text, limit=ENRICH_BATCH_LIMIT, budget_seconds=ENRICH_BUDGET_SECONDS,
                  concurrency=ENRICH_CONCURRENCY, clock=time.monotonic):
    """One run. Returns the note shown in the job's run history; raises asyncio.TimeoutError after
    ENRICH_TIMEOUT_SECONDS."""
    return await asyncio.wait_for(
        _enrich_run(source, session_factory, text, limit, budget_seconds, concurrency, clock), timeout=ENRICH_TIMEOUT_SECONDS)
