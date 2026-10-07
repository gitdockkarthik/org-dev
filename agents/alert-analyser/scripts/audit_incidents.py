"""Read-only audit of Zabbix incident handling (alert-analyser).

Run (no rebuild needed; it executes from stdin inside the running container):
  docker exec -i -e HOURS=24 org-dev-alert-analyser-1 python - < agents/alert-analyser/scripts/audit_incidents.py
Env: HOURS (window, default 24), GRACE_MIN (default 5),
     SEVERITIES (comma list of Zabbix severity tags that must escalate, default critical),
     PAGE_CAP (OpsGenie pages of 100, default 190).
Writes nothing. Exit code 1 if any check FAILs.
"""
import asyncio
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import select, text

from config import settings
from database import SessionLocal
from encryption import decrypt, is_secret_key
from models import AgentConfig

HOURS = float(os.environ.get("HOURS", "24"))
GRACE_MIN = float(os.environ.get("GRACE_MIN", "5"))
SEVERITIES = [s.strip().lower() for s in os.environ.get("SEVERITIES", "critical").split(",") if s.strip()]
PAGE_CAP = int(os.environ.get("PAGE_CAP", "190"))
SAMPLES = 5
CFG = {}

# Alert state used by C4/C5: an alert counts as ESCALATED if any of its incident rows is.
CTE = r"""
WITH raw AS (
  SELECT i.alert_id, (i.status = 'ESCALATED') AS esc,
         i.alert_payload->>'message' AS msg,
         (i.alert_payload->>'createdAt')::timestamptz AS ts
  FROM incident_management.incidents i
  WHERE i.alert_payload->>'message' ILIKE '%[Zabbix]%'
    AND i.alert_payload->>'createdAt' IS NOT NULL
  UNION ALL
  SELECT h.alert_id, NULL::boolean, h.alert_data->>'message',
         (h.alert_data->>'createdAt')::timestamptz
  FROM alert_sync_history h
  WHERE h.alert_data->>'message' ILIKE '%[Zabbix]%'
    AND h.alert_data->>'createdAt' IS NOT NULL
), ev AS (
  SELECT alert_id, BOOL_OR(esc) AS esc, MIN(msg) AS msg, MIN(ts) AS ts
  FROM raw GROUP BY alert_id
), p AS (
  SELECT alert_id, esc, msg, ts,
    CASE WHEN msg ~* '\[open\]' THEN 'open' WHEN msg ~* '\[closed\]' THEN 'closed' END AS kind,
    (regexp_match(msg, '\[([^\]]+)\]\s*\[Env:'))[1] AS host,
    COALESCE((regexp_match(msg, '\[Env:([^\]]*)\]'))[1], '') AS env,
    regexp_replace(btrim(COALESCE((regexp_match(msg, '\[Env:[^\]]*\]\s*(.*)$'))[1], '')), '\s+', ' ', 'g') AS chk,
    length(msg) AS mlen
  FROM ev
), o AS (
  SELECT * FROM p WHERE kind = 'open' AND COALESCE(esc, false) AND host IS NOT NULL
), m AS (
  SELECT o.alert_id,
    BOOL_OR(c.ts > o.ts AND c.chk = o.chk) AS closed_exact,
    BOOL_OR(c.ts > o.ts AND (c.chk = o.chk OR (LEAST(o.mlen, c.mlen) >= 125
            AND (starts_with(c.chk, o.chk) OR starts_with(o.chk, c.chk))))) AS closed_any
  FROM o LEFT JOIN p c ON c.kind = 'closed' AND c.host = o.host AND c.env = o.env
  GROUP BY o.alert_id
), n AS (
  SELECT o.alert_id,
    BOOL_OR(q.ts > o.ts AND (q.chk = o.chk OR (LEAST(o.mlen, q.mlen) >= 125
            AND (starts_with(q.chk, o.chk) OR starts_with(o.chk, q.chk))))) AS newer_open
  FROM o LEFT JOIN p q ON q.kind = 'open' AND q.alert_id <> o.alert_id AND q.host = o.host AND q.env = o.env
  GROUP BY o.alert_id
), d AS (
  SELECT o.alert_id, o.host, o.chk, o.ts, m.closed_exact, m.closed_any, n.newer_open,
    CASE WHEN m.closed_any THEN 'resolve_recovered'
         WHEN n.newer_open THEN 'merge_duplicate'
         ELSE 'keep_active' END AS decision
  FROM o JOIN m USING (alert_id) JOIN n USING (alert_id)
)
"""


def tokens(msg):
    return [t.strip().lower() for t in re.findall(r"\[([^\]]+)\]", msg or "")]


def is_zabbix(msg):
    return "zabbix" in tokens(msg)


def kind_of(msg):
    t = tokens(msg)
    if "closed" in t:
        return "closed"
    if "open" in t:
        return "open"
    return None


def severity_of(msg):
    t = tokens(msg)
    if "open" in t:
        i = t.index("open")
        if i + 1 < len(t):
            return t[i + 1]
    return None


def parse_ts(s):
    s = s.replace("Z", "+00:00")
    s = re.sub(r"\.(\d+)", lambda m: "." + (m.group(1) + "000000")[:6], s, count=1)
    return datetime.fromisoformat(s)


async def load_cfg_readonly():
    # Same rows and decoding as routes_settings.load_config_from_db, minus its re-encrypt _upsert.
    async with SessionLocal() as s:
        rows = (await s.execute(
            select(AgentConfig).where(AgentConfig.agent_slug == settings.agent_slug))).scalars().all()
    for r in rows:
        secret = is_secret_key(r.key)
        try:
            CFG[r.key] = json.loads(decrypt(r.value) if secret else r.value)
        except Exception as exc:
            print("config: failed to decode key=%r (secret=%s): %s" % (r.key, secret, exc), file=sys.stderr)


async def scan_opsgenie(since_dt):
    base = CFG.get("opsgenie_base_url") or "https://api.opsgenie.com"
    hdr = {"Authorization": "GenieKey " + CFG["api_token"]}
    since_ms = int(since_dt.timestamp() * 1000)
    alerts = []
    async with httpx.AsyncClient(timeout=30.0) as c:
        for page in range(PAGE_CAP):
            r = await c.get(base + "/v2/alerts", headers=hdr,
                            params={"query": "createdAt>%d" % since_ms, "limit": 100,
                                    "offset": page * 100, "order": "asc"})
            if r.status_code != 200:
                return alerts, False, "http %s at page %d" % (r.status_code, page)
            data = r.json().get("data", [])
            alerts += data
            if len(data) < 100:
                return alerts, True, ""
    return alerts, False, "page cap %d reached" % PAGE_CAP


async def main():
    await load_cfg_readonly()
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=HOURS)
    cutoff = now - timedelta(minutes=GRACE_MIN)
    results = []

    def check(name, bad, detail, samples=()):
        status = "PASS" if bad == 0 else "FAIL"
        results.append(status)
        print("[%s] %s: %s  %s" % (status, name, bad, detail))
        for sm in list(samples)[:SAMPLES]:
            print("        sample:", sm)

    alerts, complete, note = await scan_opsgenie(since)
    zab = [a for a in alerts if is_zabbix(a.get("message"))]
    opens = [a for a in zab if kind_of(a.get("message")) == "open"]
    closeds = [a for a in zab if kind_of(a.get("message")) == "closed"]

    print("ZABBIX AUDIT | window: last %sh up to %s UTC | grace %s min | escalating severities: %s"
          % (HOURS, now.strftime("%Y-%m-%d %H:%M:%S"), GRACE_MIN, ",".join(SEVERITIES)))
    print("OpsGenie scan: %d alerts, %d Zabbix (%d Open, %d Closed) | complete: %s %s"
          % (len(alerts), len(zab), len(opens), len(closeds), complete, note))
    print("Zabbix Open by severity tag:", dict(Counter(severity_of(a.get("message")) for a in opens)))
    print("Blind spots: C4/C5 pair Open to Closed with the same host+env+check logic the recovery rule will use, "
          "so they cannot detect a flaw in that matching; Closed notices older than the 30-day history "
          "retention cannot be matched; incomplete OpsGenie scans skip C1.")
    print("")

    async with SessionLocal() as s:
        # C1: Open alerts at escalating severities with no incident after the grace period
        due = [a for a in opens
               if severity_of(a.get("message")) in SEVERITIES and parse_ts(a["createdAt"]) < cutoff]
        if not complete:
            results.append("SKIP")
            print("[SKIP] C1 missing incidents: OpsGenie scan incomplete (%s)" % note)
        else:
            have, hist = {}, {}
            if due:
                ids = [a["id"] for a in due]
                have = {r.alert_id: r.n for r in (await s.execute(
                    text("SELECT alert_id, COUNT(*) AS n FROM incident_management.incidents "
                         "WHERE alert_id = ANY(:ids) GROUP BY alert_id"), {"ids": ids})).fetchall()}
                hist = {r.alert_id: r.classification for r in (await s.execute(
                    text("SELECT alert_id, classification FROM alert_sync_history "
                         "WHERE alert_id = ANY(:ids)"), {"ids": ids})).fetchall()}
            missing = [a for a in due if a["id"] not in have]
            reasons = Counter(hist.get(a["id"], "not in sync history") for a in missing)
            check("C1 Open alerts with no incident", len(missing),
                  "(of %d due) by sync classification: %s" % (len(due), dict(reasons)),
                  ["%s | %s | %s" % (a["id"], a["createdAt"], (a.get("message") or "")[:60]) for a in missing])

        # C2: a Closed notice created an active incident
        r2 = (await s.execute(text(
            "SELECT alert_id, status, resolution_type FROM incident_management.incidents "
            "WHERE alert_payload->>'message' ILIKE '%[Zabbix]%' "
            "AND alert_payload->>'message' ~* '\\[closed\\]' AND created_at >= :since "
            "AND (status <> 'RESOLVED' OR resolution_type IS DISTINCT FROM 'noise_suspect_audit_record')"),
            {"since": since})).fetchall()
        check("C2 Closed notices that became incidents", len(r2), "",
              ["%s | %s | %s" % (r.alert_id, r.status, r.resolution_type) for r in r2])

        # C3: copies and multiple incidents per alert_id
        copies = (await s.execute(text(
            "SELECT COUNT(*) FROM incident_management.incidents WHERE related_ticket_id IS NOT NULL "
            "AND alert_payload->>'message' ILIKE '%[Zabbix]%' AND created_at >= :since"),
            {"since": since})).scalar()
        multi = (await s.execute(text(
            "SELECT alert_id, COUNT(*) AS n FROM incident_management.incidents "
            "WHERE alert_payload->>'message' ILIKE '%[Zabbix]%' AND created_at >= :since "
            "GROUP BY alert_id HAVING COUNT(*) > 1 ORDER BY n DESC"), {"since": since})).fetchall()
        check("C3 copies (related_ticket_id set)", copies,
              "| alert_ids with more than one incident: %d" % len(multi),
              ["%s | %d incidents" % (r.alert_id, r.n) for r in multi])

        # C4: ESCALATED Open that has a later Closed notice (state check, not windowed)
        rows = {r.decision: r for r in (await s.execute(text(
            CTE + " SELECT decision, COUNT(*) AS tickets, "
                  "COUNT(*) FILTER (WHERE closed_any AND NOT COALESCE(closed_exact, false)) AS prefix_dependent "
                  "FROM d GROUP BY 1"))).fetchall()}
        n_rec = rows["resolve_recovered"].tickets if "resolve_recovered" in rows else 0
        n_pre = rows["resolve_recovered"].prefix_dependent if "resolve_recovered" in rows else 0
        n_keep = rows["keep_active"].tickets if "keep_active" in rows else 0
        n_merge = rows["merge_duplicate"].tickets if "merge_duplicate" in rows else 0
        unparsed = (await s.execute(text(
            CTE + " SELECT COUNT(*) FROM p WHERE kind = 'open' AND COALESCE(esc, false) AND host IS NULL"))).scalar()
        samp = (await s.execute(text(
            CTE + " SELECT alert_id FROM d WHERE decision = 'resolve_recovered' ORDER BY ts LIMIT 5"))).fetchall()
        check("C4 recovered but still ESCALATED", n_rec,
              "(prefix-dependent: %d) | keep_active: %d | merge_duplicate: %d | host not parsed: %d"
              % (n_pre, n_keep, n_merge, unparsed), [r.alert_id for r in samp])

        # C5: more than one ESCALATED incident for the same problem
        dup = (await s.execute(text(
            CTE + " SELECT COUNT(*) AS grp, COALESCE(SUM(n - 1), 0) AS extra FROM "
                  "(SELECT COUNT(*) AS n FROM o GROUP BY host, env, chk HAVING COUNT(*) > 1) g"))).fetchone()
        check("C5 duplicate ESCALATED incidents per problem", int(dup.extra), "in %d host/check groups" % dup.grp)

    print("")
    print("SUMMARY:", " ".join(results))
    return 1 if "FAIL" in results else 0


sys.exit(asyncio.run(main()))
