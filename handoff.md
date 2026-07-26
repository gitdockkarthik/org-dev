# Session Handoff

## Engineering Priorities
1. Accuracy 2. Performance 3. UX 4. Operations 5. Consistency

## Current State
Kafka Analyser — SLI/SLO feature complete with full compliance tracking.

## Next Session Agenda
1. **SLI/SLO enhancements**:
   - Monthly comparison section (grouped bar chart)
   - Consumer lag top offenders in SLO dashboard (already has popup)
   - SLO trend chart for overall compliance (currently shows connector only)
2. **Prod cluster onboarding** — add Prod Kafka cluster, jobs auto-register
3. **AI Insights kafka_store removal** — critical before Prod (backlog item)

## Active Job Schedules (cluster 3 — DevQA)
* kafka-broker-health-3: */2 * * * * (60s timeout)
* kafka-consumer-lag-3: */3 * * * * (90s timeout, ~47s runtime)
* kafka-topic-sizes-3: */15 * * * * (30s timeout)
* kafka-msg-rate-3: */2 * * * * (60s timeout)
* kafka-topic-structure-3: 0 */2 * * * (90s timeout)
* kafka-connector-snapshots-3: */2 * * * * (30s timeout)
* kafka-slo-compliance-3: 5 * * * * (60s timeout, hourly)

## SLI/SLO Architecture
* kafka_slo_targets: per-cluster targets (7 configurable SLOs)
* kafka_connector_snapshots: every 2min, 291 connectors
* kafka_slo_compliance: hourly computed (6 metrics → overall %)
* SLI definitions:
  - Connector Avail = RUNNING/(RUNNING+FAILED) — excludes PAUSED
  - Task Health = connectors with 0 failed tasks / total active
  - Broker Avail = actual/expected brokers (dynamic)
  - CPU/Heap = linear scale compliance vs target
  - Lag = lag ≤ target → 100%, else 0%
  - URP = URP ≤ target → 100%, else 0%

## Backlog (Must Not Miss Before Prod)
* AI Insights: remove kafka_store dependency
* Schema Registry pagination
* Message Rate Trends 1h resolution
* Monthly SLO comparison chart

## Known Issues
* Prometheus JMX exporter port 7071: deadlocked (CloudOps needed)
* worker01 + worker10 Kafka Connect: connection refused (CloudOps needed)

## Session Update — 2026-07-24
### Completed This Session
* SLI/SLO full feature:
  - 7 jobs per cluster (including connector-snapshots + slo-compliance)
  - 7 configurable SLO targets (connector avail, task health, lag, URP, CPU, heap, failed tasks)
  - SLO Status table showing all SLOs with target/current/status
  - Connector trend chart from snapshots (5min resolution)
  - Monthly comparison (graceful single/multi month)
  - Connector detail popup (search + filter, sorted by anomaly)
  - Consumer lag popup + Topics popup
  - Broker availability: dynamic expected count
  - Lag trend: negative values filtered out

### Remaining SLI/SLO enhancements (next session)
* Overall compliance trend chart (currently shows connector trend only)
* Connector task-level SLI per connector in popup (task health %)
* SLO breach alerts via Teams (when compliance drops below threshold)

## Fix Applied
* SLO task health compliance: was 2838% due to counting multiple snapshots per connector
* Fixed with DISTINCT ON per connector — now correct at 97.6%
* Cleared corrupt compliance rows

## Session Update — 2026-07-24 (late morning)
### Fixed
* CUR dashboard: now defaults to latest auto-synced report (was showing manual report)
* CUR active-report-id endpoint: reads from cur_report table (latest ready)

### Monday Agenda (unchanged)
1. Prod Kafka cluster onboarding
2. AI Insights kafka_store removal (must before Prod)
3. Teams SLO breach alerts
4. SLI/SLO: lag compliance improvement (currently binary 0/100%)

## Audit Tab — Architecture Frozen
### Phase 1 Scope
1. Langfuse (self-hosted) — LLM call tracing per agent/user
2. API Key rotation audit — logged to audit_logs table in backend

### Langfuse Setup
* Docker compose service on port 3001 (verify no conflict)
* Uses org-dev postgres, separate database: langfuse_db
* shared/llm.py wraps every LLM call with Langfuse tracing

### Audit Tab (portal platform feature, like Admin)
* portal/admin/audit.html
* backend/routes/audit.py
* Sub-sections: LLM Usage (Langfuse), API Key Events (audit_logs)

### audit_logs table
* id, timestamp, event_type, agent_slug, user_email, user_role
* resource_type, resource_id, action, outcome, details (JSONB), ip_address
* Retention: 30 days default, configurable via platform settings

### RAG + Platform Governance (this week)
* RAG for incident response
* Platform governance framework
* These are separate from Audit tab — plan separately

## Backlog — Infrastructure
* **Langfuse v3 upgrade**: Requires ClickHouse (analytics DB). Raise Jira for:
  1. ClickHouse service provisioned on KPI box or separate node
  2. DNS entry for langfuse.kpi-internal.cloud.operative.com (port 443)
  3. Firewall rule to expose port 3001 externally
  Once infra ready: upgrade langfuse image to v3, configure LANGFUSE_BASEPATH,
  remove nginx sub_filter workarounds, enable native basepath support.

## Session Update — 2026-07-26
### Completed
* Audit tab — Phase 1 complete:
  - Langfuse v3 (self-hosted): ClickHouse + Redis + MinIO + langfuse-worker
  - LLM tracing: model, input/output tokens captured per LLM call
  - shared/llm.py: @observe decorator pattern (v4 SDK)
  - audit_logs table: API key create/rotate/delete events
  - Portal: Admin → Audit tab (API Key Events + LLM Usage)
  - Backend proxy: /api/audit/llm-usage → Langfuse API
  - Security: AUTH_DISABLE_SIGNUP=true, admin credentials only
  - GitGuardian: placeholder secrets removed from .env.example

### Langfuse Infrastructure
* langfuse:3000 (internal) → nginx proxy at /langfuse/
* langfuse-worker: processes Redis queue → ClickHouse
* MinIO: S3-compatible storage for OTEL events
* Redis: BullMQ job queue
* ClickHouse: analytics DB (single node, no cluster)

### Backlog — Infrastructure
* Langfuse v3 DNS: langfuse.kpi-internal.cloud.operative.com
* Instance upgrade: t3.xlarge → r6i family (memory optimized)

### Next
* Audit tab UI improvements (LLM Usage charts, token trends)
* Portal Langfuse proxy (nginx sub_filter) refinement
* Monday: hardcoded mappings audit + security audit

## Session Update — 2026-07-26 (continued)
### Audit Tab Complete
* Langfuse v3 + ClickHouse + Redis + MinIO + langfuse-worker
* LLM tracing: all 3 agents, both create_message and stream_message
* Token capture: model, input/output tokens, user email, session_id
* Audit Events tab: all platform events (llm.invoke, apikey.*)
* LLM Usage tab: date range filter (24h/7d/30d/all), server-side filtering
* Audit as top-level nav alongside Agents and Admin
* Security: AUTH_DISABLE_SIGNUP=true, admin credentials only
* GitGuardian: placeholder secrets removed from .env.example

### Monday Agenda (unchanged — first priority)
1. ⚠️ Hardcoded mappings audit (cur-analyser/routes_reports.py lines 46-78)
2. ⚠️ Security audit (no real customer data in source)
3. Prod Kafka cluster onboarding
4. AI Insights kafka_store removal
