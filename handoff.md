# Session Handoff

## Engineering Priorities (Always Follow This Order)
1. **Accuracy** — critical always, non-negotiable
2. **Performance** — maximum optimisation
3. **User Experience** — no application-caused friction
4. **Operations** — no OOM, no extended CPU/mem, optimised storage
5. **Consistency** — no bugs causing misbehaviours

## Current Feature
S3 Browser + Job Management per Agent

## Objective
1. S3 browser UI — browse billing periods, select folders for sync
2. Deprecate file upload from laptop (replace with S3 browser)
3. Job management per agent (jobs.py per agent, self-contained)

## Status

### Completed This Session
* Parquet pipeline for file upload — replaces DuckDB ingest
* Single file upload: CSV.gz → Parquet (part-00001.parquet)
* Folder upload: each part converted sequentially → parquet_dir
* file_size reflects actual Parquet size (not original CSV size)
* Data accuracy validated: report 6 = 2,820,685 rows, $295,754.49 ✅
* Storage: 282MB Parquet vs 1.5GB DuckDB (81% smaller) ✅
* Commits: 6293315, 7746f02

### Pending
* S3 browser UI — browse billing periods, select folders for sync
  - GET /data-sources/s3/browse — list billing periods and folders
  - POST /data-sources/s3/sync — accept specific folder path
  - Multi-period selection for comparison
* Deprecate file upload UI (replace with S3 browser for production)
* User badge shows "A" (Admin) instead of logged-in user's initial — cosmetic fix
* Job management per agent (jobs.py per agent, not shared)
  - Audit current schedules: CUR (S3 sync), Alert (OpsGenie), Kafka (metrics)
  - Implement self-contained jobs.py per agent
  - Jobs sub-tab in Reports page
* Tab-level chat for alert and kafka
* must_change_password not enforced at login
* Kafka Phase 2 Prometheus — JMX exporter restart needed by CloudOps

## Known Constraints
* S3 bucket: attribute-cur-us-east-1-741119431024
* Prefix: AIAnalysis/FoAIAnalysis/data/
* Report 5: 2026-07-21 (8 parts), 3.9M rows, $382,608 (Parquet, S3 sync) ✅
* Report 6: FoAIAnalysis (6 parts), 2.8M rows, $295,754 (Parquet, uploaded) ✅
* CACHE_VERSION = "v2" in routes_dashboard.py
* LLM_PROVIDER=bedrock, LLM_MODEL=us.anthropic.claude-sonnet-5 in .env
* Job Server port: 8020 (platform-level, not shipped with agents)
* Kafka Phase 2 fail_count reset SQL: UPDATE agent_config SET value = '{"phase2_fail_count": 0, "throughput_available": true}' WHERE key LIKE 'phase2_%';

## Architecture Decisions
* File upload from laptop → deprecated for production (security risk, slow)
* All CUR data lives in S3 → agent reads from S3 directly (no laptop transfers)
* Parquet as storage format → 81-83% smaller, 266x faster queries
* Job management → self-contained per agent (jobs.py per agent, no shared service)
* Job Server (port 8020) → platform-level monitoring only, not shipped with agents

## Next Checkpoint
1. S3 browser backend: GET /data-sources/s3/browse endpoint
2. S3 browser UI in Reports page (replace S3 sync section)
3. Multi-period selection and sync
4. User badge cosmetic fix (show user initial, not role)
5. Audit current job schedules per agent

## Blocked
None — all work unblocked.
