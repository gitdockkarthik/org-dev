# Session Handoff

## Engineering Priorities (Always Follow This Order)
1. **Accuracy** — critical always, non-negotiable
2. **Performance** — maximum optimisation
3. **User Experience** — no application-caused friction
4. **Operations** — no OOM, no extended CPU/mem, optimised storage
5. **Consistency** — no bugs causing misbehaviours

## Current Feature
Job Management per Agent + Remaining Issues

## Objective
Apply job management pattern to alert and kafka agents, then address remaining issues.

## Status

### Completed This Session
* CUR self-contained job management (jobs.py)
* APScheduler inside cur-analyser — hourly S3 sync
* CurJobSchedule + CurJobRun models in agent DB
* REST API: /jobs, /runs, /schedules CRUD
* Reports page sub-tabs: Reports & Data / Job Management
* Human-friendly schedule UI (not cron syntax)
* Run notes: meaningful log messages
* S3 browser cascading tree (Year → Month → Day)
* Current month expanded by default
* Session timeout → force re-login (401 redirect)
* User badge shows correct name initial
* Alert self-contained job management (jobs.py)
* Alert reports page completely redesigned — sub-tabs (Reports & Data / Job Management / Escalations)
* Active Data Source card: OpsGenie type + last sync + alert counts
* Escalation logging to alert_escalation_log (migration 0011)
* Teams: incident-based concise card (top 3 new incidents + open summary)
* Email: HTML incident report with priority table, elapsed time, NEW badges
* Email validated via Gmail SMTP ✅
* Commits: 442ab1a, 353be6b, fff0abc, e150591, b982c39, 78259dc, 5546240, 8acfb94, e349a47, 504caa0

### Pending
* Alert settings page cleanup:
  - Remove Sync Schedule card (Job Management owns it)
  - Remove Sync Status card (Reports → Job Management owns it)
  - Remove Phase lock labels from Escalation/Incident Mgmt/RAG/Autonomous tabs
* Kafka-analyser jobs.py + reports page redesign (same pattern as alert)
* Tab-level chat for alert and kafka
* must_change_password not enforced at login
* Kafka Phase 2 Prometheus — JMX exporter restart needed by CloudOps
* Email SMTP: office365 needs IT to enable SMTP AUTH or service account

## Known Constraints
* S3 bucket: attribute-cur-us-east-1-741119431024
* Report 6: FoAIAnalysis (6 parts), 2.8M rows, $295,754 (Parquet, manual)
* Report 7: auto-sync, 4M rows (Parquet, latest July)
* CACHE_VERSION = "v2" in routes_dashboard.py
* LLM_PROVIDER=bedrock, LLM_MODEL=us.anthropic.claude-sonnet-5 in .env
* CUR job schedule: hourly (0 * * * *)
* Alert job schedule: every 15 min (*/15 * * * *)
* Kafka Phase 2 fail_count reset SQL: UPDATE agent_config SET value = '{"phase2_fail_count": 0, "throughput_available": true}' WHERE key LIKE 'phase2_%';

## Architecture Decisions
* Job management: self-contained per agent (jobs.py per agent)
* Job Server (port 8020): platform-level view only, not shipped with agents
* Parquet as storage: 81-83% smaller, 266x faster
* Escalation: Teams (incident-based Adaptive Cards) + Email (HTML report)
* Escalation triggers: new ESCALATED incidents since last cycle
* Open incidents: all ESCALATED status shown in email report
* alert_escalation_log: tracks all sends, visible in Reports → Escalations tab

## Next Checkpoint
1. Alert settings page cleanup
2. Kafka jobs.py + reports page redesign
3. Tab-level chat for alert and kafka

## Blocked
None — all work unblocked.
