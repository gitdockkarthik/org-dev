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
