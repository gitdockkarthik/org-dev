# Session Handoff

## Engineering Priorities
1. Accuracy 2. Performance 3. UX 4. Operations 5. Consistency

## Current State
Kafka Analyser — postgres migration complete for core data, per-cluster jobs working.

## Next Session Agenda (in order)
1. **Topic Details popup** — re-enable row click, fix endpoint to read from postgres, change expanded row to floating popup
2. **ZooKeeper tab revamp** — rename to Governance, show useful Kafka admin metrics from postgres
3. **Schema Registry slowness** — investigate and fix performance

## Following Session
4. **SLI/SLO** — key feature, richest/most actionable monitoring capability
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

## Known Issues
* kafka-topic-structure-3 runtime 82s — bulk partition update adds 30s
  Consider: separate partition update job or optimize bulk UPDATE
* Prometheus JMX exporter port 7071: deadlocked (TCP ok, HTTP hangs)
  CloudOps action needed to restart exporter process
* worker01 + worker10 Kafka Connect: connection refused (CloudOps needed)

## Architecture (Frozen)
* Jobs: per-cluster (kafka-{type}-{cluster_id})
* Storage: postgres is authoritative, kafka_store is write-through cache only
* No kafka_store for dashboard reads (except AI insights — backlogged)
* Topic sizes: bulk upsert all topics (describe_log_dirs, 0.45s)
* Consumer lag: batched 100 groups + single consumer session
* Kafka Connect: live REST API, multi-worker parallel, fingerprint dedup
* Schema Registry + ZooKeeper: live REST API on tab click

## Session Update — 2026-07-23 (continued)
### Completed
* Job Management UI fully working:
  - Human-readable cron display (Every 2 min, Every 2h, etc.)
  - Edit Schedule button per job (frequency + timeout modal)
  - Run Monitor filters: job type + status (server-side)
  - Reports & Data: shows correct cluster info from postgres
* Lag snapshot insert in consumer lag collector (overview chart working)
* Job resilience: warm-up connections + retry on timeout
* All counts in /dashboard/counts from postgres

### Next
1. Topic Details popup (balloon instead of expanded row)
2. ZooKeeper tab revamp → Governance
3. Schema Registry performance
4. SLI/SLO feature


## Backlog additions
* **Schema Registry pagination**: Currently loads 50 subject samples (3.5s). Need server-side 
  pagination — fetch subjects in pages of 50 on "Show More". Requires new endpoint 
  `/dashboard/schema-registry/subjects?offset=N`. Label should be honest about loaded vs total.
* **Message Rate Trends 1h resolution**: Chart shows flat lines for 1h range due to hourly 
  bucket aggregation. Need finer-grained `kafka_topic_metrics_snapshots` table (per-run, 2min).
* **Topics tab governance KPIs**: Add Stale Topics filter in table, Dead/Empty consumer groups 
  count in Consumer Groups tab.
