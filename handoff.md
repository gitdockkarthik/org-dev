# Session Handoff

## Engineering Priorities (Always Follow This Order)
1. **Accuracy** — critical always, non-negotiable
2. **Performance** — maximum optimisation
3. **User Experience** — no application-caused friction
4. **Operations** — no OOM, no extended CPU/mem, optimised storage
5. **Consistency** — no bugs causing misbehaviours

## Current Feature
Job Server + CUR S3 Sync via Job Server

## Objective
Complete Job Server standalone UI, register alert and kafka jobs, clean up redundant sync UI from agent settings/reports pages.

## Status

### Completed This Session
* CUR Settings page redesign — removed Phase 2/3/4/5 locked tabs, data source selector, S3 config, inventory file nested in enrichment card
* CUR Reports page redesign — file upload section, S3 sync section
* CUR S3 sync implemented — GET /data-sources/s3/status, POST /data-sources/s3/sync
* S3 sync: file-modification-time based deduplication (not folder name)
* S3 sync: file count change detection  
* Migration 0009: cur_report.file_size BIGINT
* All agents: Chat navigation fixed — opens inside portal (not new tab)
* All agents: Model display dynamic from settings endpoint
* All agents: config.py model field uses LLM_MODEL env var via Pydantic alias
* portal.js: window.portalReady exposed for window scope
* CUR tab-level chat — floating panel, per-tab context, pre-defined suggestions
* CUR main chat — pre-aggregated context, no tool calls for common questions
* Job Server — new standalone service (port 8020)
  - Job definitions, schedules (cron via APScheduler), run history
  - Overlap prevention with timeout detection
  - Retry logic with 30s backoff
  - CUR S3 sync job registered (30 6,13,22 * * *)
  - cur-analyser /internal/sync endpoint working
  - End-to-end validated
* Job Server standalone UI — three tabs: Job Definitions, Schedules, Monitor
* Commits: 7ba27b3, 0236ff2, 9ed4f43, 9ef4ce5, 58d60ab

### Pending
* Register alert-analyser and kafka-analyser jobs in Job Server
* Clean up CUR Reports page — remove S3 sync section (Job Server owns it)
* Clean up CUR Settings page — remove auto-sync config (Job Server owns it)
* Same cleanup for alert and kafka settings pages
* Tab-level chat for alert-analyser and kafka-analyser
* S3 sync memory optimisation (3.3GB peak — architectural fix in EKS via dedicated Job Server pod)
* must_change_password not enforced at login
* Kafka Phase 2 Prometheus — JMX exporter restart needed by CloudOps

## Known Constraints
* S3 bucket: attribute-cur-us-east-1-741119431024
* Prefix: AIAnalysis/FoAIAnalysis/data/BILLING_PERIOD=2026-07/
* Report 3: FoAIAnalysis 6 parts, 2.8M rows, $295,754 (uploaded)
* Report 4: 2026-07-21 (8 parts), 3.9M rows, $382,608 (S3 sync)
* CACHE_VERSION = "v2" in routes_dashboard.py
* LLM_PROVIDER=bedrock, LLM_MODEL=us.anthropic.claude-sonnet-5 in .env
* Job Server port: 8020
* Kafka Phase 2 fail_count reset SQL: UPDATE agent_config SET value = '{"phase2_fail_count": 0, "throughput_available": true}' WHERE key LIKE 'phase2_%';

## Job Server Architecture
* Separate standalone service — own container, own DB schema (js_jobs, js_job_schedules, js_job_runs)
* Agents register jobs via POST /jobs
* Job Server calls agent endpoints via HTTP
* No manual sync in agent UI — Job Server owns scheduling
* Portal Admin tab embeds Job Server UI via iframe
* Future: Teams webhook for failed/timeout job escalation
* EKS: Job Server runs as dedicated pod with higher memory limits

## Next Checkpoint
1. Register alert-analyser and kafka-analyser sync jobs in Job Server
2. Clean up agent settings/reports pages (remove sync UI now owned by Job Server)
3. Tab-level chat for alert and kafka

## Blocked
None — all work unblocked.
