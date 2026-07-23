# Session Handoff

## Engineering Priorities
1. Accuracy 2. Performance 3. UX 4. Operations 5. Consistency

## Current State
Kafka Analyser — postgres migration complete for core data, per-cluster jobs working.

## Next Session Agenda (in order)
1. **SLI/SLO** — key feature, richest/most actionable monitoring capability
   - Connector availability SLO (% RUNNING, target >99%)
   - Consumer lag SLO (per group threshold)
   - Broker availability SLO
   - Under-replicated partition SLO
   - Bytes throughput baseline

## Completed This Session
* Per-cluster job architecture: kafka-{type}-{cluster_id}
* Consumer lag: real lag calculation (47s, batched, single consumer session)
* kafka_consumer_group_lag table: upsert, dashboard reads from postgres
* kafka_broker_distribution table: leader/replica partitions per broker
* Broker tab charts replaced: Leader + Replica partition distribution
* kafka_topic_metrics: partition_count + RF updated by topic-structure job
* Topics tab: pagination from postgres, no streaming
* Consumer Groups tab: from postgres, no streaming
* Search endpoints: topics/groups/connectors from postgres/live
* Anomalies section removed from overview
* Job Management UI: per-cluster registry + run monitor
* Reports tab: all counts from postgres
* topic-structure: enabled, 2-hourly schedule

## Active Job Schedules
* kafka-broker-health-3: */2 * * * * (60s timeout)
* kafka-consumer-lag-3: */3 * * * * (90s timeout, ~47s runtime)
* kafka-topic-sizes-3: */15 * * * * (30s timeout)
* kafka-msg-rate-3: */2 * * * * (60s timeout)
* kafka-topic-structure-3: 0 */2 * * * (90s timeout, ~82s runtime)

## Backlog (Must Not Miss Before Prod)
* AI Insights: remove kafka_store dependency → _build_ai_context from postgres
  (routes_dashboard.py lines: 516, 689, 710, 748, 1139, 1308, 1309)
* Mirrormaker tab: reads from kafka_store (low priority, not configured)
* Schema Registry pagination: server-side /dashboard/schema-registry/subjects?offset=N
* Message Rate Trends 1h resolution: need kafka_topic_metrics_snapshots table (per-run, 2min)
* Topics tab governance KPIs: Stale Topics filter, Dead/Empty consumer groups count

## Known Issues
* kafka-topic-structure-3 runtime 82s — bulk partition update adds 30s
* Prometheus JMX exporter port 7071: deadlocked (CloudOps needed)
* worker01 + worker10 Kafka Connect: connection refused (CloudOps needed)

## Architecture (Frozen)
* Jobs: per-cluster (kafka-{type}-{cluster_id})
* Storage: postgres is authoritative, kafka_store is write-through cache only
* No kafka_store for dashboard reads (except AI insights — backlogged)
* Topic sizes: bulk upsert all topics (describe_log_dirs, 0.45s)
* Consumer lag: batched 100 groups + single consumer session
* Kafka Connect: live REST API, multi-worker parallel, fingerprint dedup
* Schema Registry + ZooKeeper: live REST API on tab click
