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
* Commits: 442ab1a, 353be6b, fff0abc

### Pending
* Apply jobs.py pattern to alert-analyser (OpsGenie polling job)
* Apply jobs.py pattern to kafka-analyser (metrics collection job)
* Audit what schedules alert and kafka currently have
* Tab-level chat for alert and kafka
* must_change_password not enforced at login
* Kafka Phase 2 Prometheus — JMX exporter restart needed by CloudOps
* Job Server UI fixes (lower priority — per-agent jobs.py is the approach)
* Email/Teams notification for failed jobs (future)

## Known Constraints
* S3 bucket: attribute-cur-us-east-1-741119431024
* Report 6: FoAIAnalysis (6 parts), 2.8M rows, $295,754 (Parquet, manual)
* Report 7: 2026-07-21 (8 parts), 4M rows (Parquet, auto-sync)
* CACHE_VERSION = "v2" in routes_dashboard.py
* LLM_PROVIDER=bedrock, LLM_MODEL=us.anthropic.claude-sonnet-5 in .env
* CUR job schedule: hourly (0 * * * *) in cur_job_schedules table
* Kafka Phase 2 fail_count reset SQL: UPDATE agent_config SET value = '{"phase2_fail_count": 0, "throughput_available": true}' WHERE key LIKE 'phase2_%';

## Architecture Decisions
* Job management: self-contained per agent (jobs.py per agent)
* No shared job service dependency
* Job tables in agent's own DB
* Job Server (port 8020): platform-level view only, not shipped with agents
* Parquet as storage: 81-83% smaller, 266x faster
* Auto-sync: hourly check, replace existing auto report
* S3 browser: Year → Month → Day cascading tree

## Next Checkpoint
1. Audit alert-analyser current sync/schedule mechanism
2. Create alert-analyser/jobs.py with OpsGenie polling job
3. Add Job Management sub-tab to alert-analyser reports page
4. Same for kafka-analyser
5. Tab-level chat for alert and kafka

## Blocked
None — all work unblocked.

## Additional Next Session Items (prepend to Next Checkpoint)
* Human-friendly schedule UI for CUR jobs (frequency picker, no cron syntax exposed)
* Hide global filter bar in CUR dashboard (causing data inconsistencies with pre-aggregated tabs)
* Then: alert-analyser and kafka-analyser jobs.py
