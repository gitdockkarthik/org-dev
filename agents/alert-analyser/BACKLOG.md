# Alert Analyser — Backlog & Session Tracking

Live tracking doc for Alert Analyser + Incident Response System (Agentic AI). Updated per session. Mirrors the tracking pattern used for CUR Analyser and Kafka Analyser.

## Status Legend
Done | Open | Blocked | In Progress

---

## Backlog Table

| # | Item | Est. Effort | Risk/Notes | Priority Order | Plan Date | Estimate (hrs) | Status |
|---|------|------------|------------|-----------------|-----------|-----------------|--------|
| 1 | Memory audit — sawtooth spike investigation | Ad hoc | Root cause: reconciliation loop issuing ~33K individual per-row session.execute() calls per sync cycle | 1 | 03/08/26 | — | Done |
| 2 | Reconciliation loop — batch UPDATE fix | 30 min | Replaced per-row Python loop with 2 batched SQL UPDATE statements using ANY(:open_ids) | 2 | 03/08/26 | 0.5 | Done |
| 3 | Truncate incident_management.incidents (dirty test data) | 5 min | 31,855 dirty test tickets removed — safe, RCA/Action agents not yet built to consume this data | 3 | 03/08/26 | 0.1 | Done |
| 4 | Unbounded alert_reports growth — bounded lookback window fix | 45 min | Root cause: combined_alerts accumulated full alert history every cycle forever (129,922 alerts/report). Fixed via 4x noise_threshold_window_mins lookback cutoff | 4 | 03/08/26 | 0.75 | Done |
| 5 | Stuck alert_job_runs row blocking sync — manual cleanup | 15 min | Container restart during rebuild left a run stuck in status='running', silently blocking all future trigger_job() calls via _get_active_run() | 5 | 03/08/26 | 0.25 | Done |
| 6 | ALERT_BACKLOG.md — process setup | 15 min | Live tracking doc in repo, committed | 6 | 03/08/26 | 0.25 | Done |
| 7 | Startup reconciliation for orphaned "running" job rows | 1 hr | No mechanism exists today to mark stuck runs failed after a crash/restart — will recur on every future mid-sync restart | 7 | TBD | 1 | Open |
| 8 | Schedule automated purge job (currently manual-trigger only) | 30-45 min | Purge logic IS implemented (GET /dashboard/incidents/purge-preview, POST /dashboard/incidents/purge) with dry_run support and a safety gate (incident_purge_enabled). Correction from earlier assessment: NOT dead code. Gap is it's never scheduled automatically - requires manual trigger via Settings UI or API. Also only targets status='ESCALATED', does not purge stalled tickets in INVESTIGATING/RCA_COMPLETE/REMEDIATING. | 8 | TBD | 0.5-0.75 | Open |
| 9 | Purge 2 remaining oversized alert_reports rows (ids 5048–5050) | 5 min | ~130K alerts each, dead weight in Postgres — low priority, cosmetic only, no functional impact | 9 | TBD | 0.1 | Open |
| 10 | GET /dashboard/incidents/list — ticket-list endpoint | TBD | Needed to power kanban priority lanes UI — verify current status (memory vs handoff doc disagree on completion) | 10 | TBD | TBD | Open — needs audit |
| 11 | Ticket detail modal | TBD | Full popup: id, alert_id, priority, status, title, alert_payload, recurrence_count, related_ticket_id, resolved_externally, resolved_at, sla_breached — verify current status (memory vs handoff doc disagree) | 11 | TBD | TBD | Open — needs audit |
| 12 | Kanban lane enhancements — search/filter/sort/show-more | TBD | Verify current status (memory vs handoff doc disagree on completion) | 12 | TBD | TBD | Open — needs audit |
| 13 | P5 SLA threshold mapping | 30 min | P5 alerts exist in real data, silently default to 240 min (P4 threshold) — no explicit mapping | 13 | TBD | 0.5 | Open |
| 14 | Dashboard "NEW GENUINE" metric relabeling | TBD | Verify current status (memory vs handoff doc disagree on completion) | 14 | TBD | TBD | Open — needs audit |
| 15 | OpsGenie status-field unreliable — message-field parsing | TBD | Deferred pending org-wide bracket-format standardization across source tools | 15 | Blocked (org dependency) | TBD | Blocked |
| 16 | environment / affected_resource enrichment parser rewrite | TBD | Deferred pending bracket-format standardization: [SourceTool] [State] [Severity] [Product] [Host] [Env: x] message | 16 | Blocked (org dependency) | TBD | Blocked |
| 17 | Resolution ring auto vs action split — explicit resolution_type field | TBD | Currently inferred (any RESOLVED + resolved_externally=false = auto). Needs Action Agent to exist first | 17 | Blocked (depends on Action Agent) | TBD | Blocked |
| 18 | Alert Analyser v1.2.0 Bitbucket standalone delivery | TBD | Package for SRE deployment — shared/llm.py bundled as llm.py with renamed imports (pattern from v1.1.0) | 18 | TBD | TBD | Open |
| 19 | LLM provider migration — Anthropic direct to AWS Bedrock | TBD | Env var + credential switch only, no code changes expected per existing design (shared/llm.py) | 19 | TBD | TBD | Open |
| 20 | RCA Agent / Action Agent | TBD | Not yet built — incident_management schema already designed for their future consumption | 20 | TBD | TBD | Open |
| 21 | Provisioned separate DB credentials for RCA Agent + App Support Agent | 15 min | Two new Postgres roles created (rca_agent, app_support_agent), scoped to incident_management schema only, full read-write, isolated from each other and from other schemas. Credentials shared with respective teams out-of-band — not stored in this file. | 21 | 03/08/26 | 0.25 | Done |
| 22 | Postgres backup — pg_dump + systemd timer (cross-agent, see root INFRASTRUCTURE.md) | 30 min | Started from alert-analyser memory investigation session; covers entire operative_db, not alert-analyser-specific. Full details in /data/org-dev/INFRASTRUCTURE.md — backup stored on root disk (separate physical device from /data where pgdata lives), daily 02:00 UTC, 3-day retention. | 22 | 04/08/26 | 0.5 | Done |
| 23 | Restrict rca_agent/app_support_agent to incident_management only — revoke public schema visibility | 20 min | Table-level grants were always correctly scoped to incident_management only. Gap was schema-level USAGE on public, granted implicitly via Postgres's default PUBLIC pseudo-role - only exposed table names via \dt, not actual data (data access was already denied). Fixed via REVOKE USAGE ON SCHEMA public FROM PUBLIC on operative_db. Verified safe: operative is superuser (bypasses all grants), other roles on box (radar_user, rca_agent_db) own separate databases, not operative_db. Verified externally via isolated docker run psql tests for both roles post-fix. | 23 | 04/08/26 | 0.3 | Done |
| 24 | LLM Gateway migration — alert-analyser now routes through platform Gateway | N/A (done in portal session, not this repo's session) | agent.py's create_message()/stream_message() calls unchanged - shared/llm.py now transparently routes through UAP_URL/UAP_AGENT_KEY (developer key id=15), with automatic fallback to direct Anthropic/Bedrock if Gateway unreachable. uses_uap_llm=true now enforced on registry record. Two bugs fixed as part of this: agent.py line 78 hasattr(b,"text") bug broke on Claude's thinking blocks (fixed to getattr(b,"text",None)); Gateway double-tracing issue in Langfuse fixed at shared/llm.py level. No action needed on this agent's own code for LLM calls - be aware create_message() may be Gateway-routed now; future tool-use loop changes should account for _GatewayResponse shim being attribute-compatible with real Anthropic SDK object. Full detail in portal/BACKLOG.md. | 24 | 10/08/26 | — | Done (external session — portal) |

---

*Internal use only — Operative Intelligence, Incident Response System (Agentic AI). Update only the delta at the end of each session.*
