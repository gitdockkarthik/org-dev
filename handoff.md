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
1. S3 browser — cascading tree (Year → Month → Day)
2. Job management per agent (jobs.py per agent, self-contained)
3. Job Server UI fixes

## Status

### Completed This Session
* Parquet pipeline for file upload — replaces DuckDB ingest
* Single file upload: CSV.gz → Parquet (part-00001.parquet)
* Folder upload: each part converted sequentially → parquet_dir
* file_size reflects actual Parquet size (not original CSV size)
* Data accuracy validated: report 6 = 2,820,685 rows, $295,754.49 ✅
* Storage: 282MB Parquet vs 1.5GB DuckDB (81% smaller) ✅
* sync_type field: auto/manual — auto-sync report replaced not accumulated
* Auto-sync report: separate table, no delete button
* Manual reports: separate table with delete
* S3 browser: latest export per day only
* Single folder selection guard (one at a time, sequential)
* Upload section hidden when source_type=s3
* Selection bar inside browse section
* All timestamps in UTC
* Job Server schedule: hourly (0 * * * *)
* Migration 0010: sync_type column
* Commits: 6293315, 7746f02, c3c0ae4

### Pending
* S3 browser cascading tree: Year → Month → Day → latest export
  - Year/Month dropdown search filters
  - Backend: browse endpoint with year/month filtering
  - Scales to multi-year without page length issues
* User badge shows role initial "A" instead of username initial — cosmetic fix
* Session timeout → force re-login (security — 401 should redirect to login)
* Job management per agent (jobs.py per agent, not shared)
  - Audit current schedules: CUR (S3 sync), Alert (OpsGenie), Kafka (metrics)
  - Implement self-contained jobs.py per agent
  - Jobs sub-tab in Reports page
* Tab-level chat for alert and kafka
* must_change_password not enforced at login
* Kafka Phase 2 Prometheus — JMX exporter restart needed by CloudOps

## Job Server UI Fixes (Next Session)
* All timestamps → UTC consistently (run history shows browser timezone)
* Schedule UI: human-friendly (not cron syntax) for FinOps users
  - "Run every: [Hour] At: [06:00] [UTC]" instead of cron expression
* Schedule: Add Edit + Enable/Disable toggle with confirmation dialog
* Job Definition: Add Edit button
* Run button → rename "Trigger Now", move to Monitor tab
* Delete job: cascade warning ("will delete N schedules")
* Timezone label: "All times in UTC" shown prominently

## Known Constraints
* S3 bucket: attribute-cur-us-east-1-741119431024
* Prefix: AIAnalysis/FoAIAnalysis/data/
* Report 6: FoAIAnalysis (6 parts), 2.8M rows, $295,754 (Parquet, manual) ✅
* Report 7: 2026-07-21 (8 parts), 4M rows, $265,512 (Parquet, auto-sync) ✅
* CACHE_VERSION = "v2" in routes_dashboard.py
* LLM_PROVIDER=bedrock, LLM_MODEL=us.anthropic.claude-sonnet-5 in .env
* Job Server port: 8020, schedule: hourly (0 * * * *)
* Kafka Phase 2 fail_count reset SQL: UPDATE agent_config SET value = '{"phase2_fail_count": 0, "throughput_available": true}' WHERE key LIKE 'phase2_%';

## Architecture Decisions
* File upload from laptop → deprecated for production (security risk)
* All CUR data → S3 browser (server-to-server, no laptop transfers)
* Parquet as storage format → 81-83% smaller, 266x faster queries
* Job management → self-contained per agent (jobs.py per agent)
* Job Server (port 8020) → platform-level monitoring, not shipped with agents
* Auto-sync: hourly check → sync only if S3 files newer than last sync
* sync_type=auto → one rotating report, replaced on each sync
* sync_type=manual → permanent, user-managed, for comparison

## Next Checkpoint
1. S3 browser cascading tree UI
2. Job Server UI fixes (timestamps UTC, human-friendly scheduler, edit/disable)
3. Session timeout → force re-login
4. User badge cosmetic fix
5. Job management per agent design + implementation

## Blocked
None — all work unblocked.
