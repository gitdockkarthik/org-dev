# Session Handoff

## Engineering Priorities (Always Follow This Order)
1. **Accuracy** — critical always, non-negotiable
2. **Performance** — maximum optimisation
3. **User Experience** — no application-caused friction
4. **Operations** — no OOM, no extended CPU/mem, optimised storage
5. **Consistency** — no bugs causing misbehaviours

## Current Feature
Parquet Pipeline + Job Management

## Objective
1. File upload → Parquet pipeline (replace DuckDB ingest)
2. E2E validation of both upload and S3 sync with Parquet
3. Job management design and implementation per agent

## Status

### Completed
* S3 sync → Parquet pipeline (sequential, 1.2GB peak, 266x faster queries)
* _open_con helper: unified connection for .duckdb/.parquet/parquet_dir/csv
* Job Server standalone service (port 8020) — UI + API working
* CUR S3 sync job registered and validated end-to-end
* All agents: Chat navigation fixed, model display dynamic
* CUR dashboard complete redesign — all tabs from pre-aggregated cache
* CUR tab-level chat — per-tab context, pre-defined suggestions
* CUR main chat — pre-aggregated context, fast responses
* Migration 0009: cur_report.file_size BIGINT
* Commits up to: eeafd55

### Pending
* File upload pipeline → Parquet (replace ingest_to_duckdb)
* E2E validation: upload + S3 sync both with Parquet
* Job management per agent (jobs.py per agent, not shared)
* Clean up CUR Reports/Settings pages (remove sync UI owned by Job Server)
* Tab-level chat for alert and kafka
* Register alert + kafka jobs in Job Server (or per-agent jobs.py)
* must_change_password not enforced at login
* Kafka Phase 2 Prometheus — JMX exporter restart needed by CloudOps

## Known Constraints
* S3 bucket: attribute-cur-us-east-1-741119431024
* Prefix: AIAnalysis/FoAIAnalysis/data/BILLING_PERIOD=2026-07/
* Report 3: FoAIAnalysis 6 parts, 2.8M rows, $295,754 (DuckDB — to be migrated)
* Report 5: 2026-07-21 (8 parts), 3.9M rows, $382,608 (Parquet, S3 sync) ✅
* CACHE_VERSION = "v2" in routes_dashboard.py
* LLM_PROVIDER=bedrock, LLM_MODEL=us.anthropic.claude-sonnet-5 in .env
* Job Server port: 8020
* Kafka Phase 2 fail_count reset SQL: UPDATE agent_config SET value = '{"phase2_fail_count": 0, "throughput_available": true}' WHERE key LIKE 'phase2_%';

## Job Management — Current State (to audit next session)
* CUR: S3 sync triggered via /data-sources/s3/sync endpoint + background task in main.py
* Alert: OpsGenie polling — need to audit where schedule is managed
* Kafka: metrics collection — need to audit where schedule is managed
* Target: jobs.py per agent (self-contained, no shared dependency)
* Job Server (port 8020) — platform-level view, not shipped with agents

## Next Checkpoint
1. Replace ingest_to_duckdb with Parquet conversion in file upload pipeline
2. Validate E2E: upload → Parquet → dashboard → chat
3. Validate E2E: S3 sync → Parquet → dashboard → chat  
4. Audit current job schedules per agent
5. Design and implement jobs.py per agent

## Blocked
None — all work unblocked.
