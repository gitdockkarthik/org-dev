# Session Handoff

## Engineering Priorities (Always Follow This Order)
1. **Accuracy** — critical always, non-negotiable
2. **Performance** — maximum optimisation
3. **User Experience** — no application-caused friction
4. **Operations** — no OOM, no extended CPU/mem, optimised storage
5. **Consistency** — no bugs causing misbehaviours

## Current Feature
Kafka Analyser — Multi-Job Architecture + Dashboard Revamp

## Objective
Complete remaining Kafka dashboard tabs and job management UI.

## Status

### Completed This Session
* Multi-job architecture replacing monolithic _collection_loop
* collect_broker_health: Prometheus Phase 1 → upsert kafka_broker_metrics (6s)
* collect_topic_sizes: describe_log_dirs → bulk upsert all 16k topics (8s, 0.45s collection)
* collect_consumer_lag_active: AdminClient per-group offsets (54s, 663 groups)
* collect_msg_rate: describe_log_dirs delta → bytes_in_per_sec (14s per cycle)
* kafka_topic_metrics: unique constraint + last_seen + bulk upsert (0.65s for 16k rows)
* kafka_broker_metrics: unique constraint + upsert per broker
* /dashboard/topics: reads from postgres with pagination + search (16,352 topics)
* /dashboard/counts: top_topics_by_size + large_topics_count + top_topics_by_msg_rate from postgres
* /dashboard/brokers: reads from postgres (broker_id fixed)
* Message Rate Trends chart: working from kafka_topic_metrics_hourly
* Kafka Connect: multi-worker parallel (0.6s for 291 connectors across 10 workers)
* Kafka Connect: fingerprint deduplication (worker05-08 same cluster)
* Kafka Connect: worker node status section (up/down per node)
* Kafka Connect: IP→hostname resolution for worker_id
* Kafka Connect: Paused KPI card + Failed Tasks filter
* Kafka Connect: single expand API call (replaces per-connector calls)
* HA failover: comma-separated URLs for Connect/Schema Registry/ZooKeeper
* Migrations 0012 (broker unique) + 0013 (topic unique + last_seen)
* Scheduled 4 jobs for overnight: broker-health, topic-sizes, msg-rate, consumer-lag-active

### Pending
* Large Topics KPI card fix (Topics tab — shows 0, data is correct in API)
* Job Management UI — show all 6 jobs (remove connectors + schema registry jobs)
* Schema Registry — validate data collection (REST API, confirm no job needed)
* Tooltip for UNASSIGNED task pills in Kafka Connect table
* kafka-topic-structure job: test + enable (2 min cycle, 90s timeout)
* kafka-consumer-lag-full job: test + enable (daily schedule, 180s timeout)
* Consumer Groups tab — validate data showing correctly
* ZooKeeper tab → rename to Governance tab
* Tab-level chat for alert and kafka
* Alert settings page cleanup (remove Sync Schedule/Status cards, Phase lock labels)
* must_change_password not enforced at login

## Architecture Decisions (Frozen)
* Jobs → kafka_store (write-through cache) → PostgreSQL → dashboard reads postgres
* No kafka_store as source of truth — postgres is authoritative
* Topic sizes: bulk upsert all 16k topics (not top N)
* Topic cleanup: last_seen < 35 min → deleted (stale topic detection)
* Msg rate: describe_log_dirs delta (bytes/sec, not msgs/sec — Kafka platform metric)
* Hot Topics threshold: >100 KB/s bytes_in_per_sec
* Kafka Connect: live REST API (no job) — 0.6s response
* Schema Registry: live REST API (no job) — instant
* kafka-connectors + kafka-schema-registry jobs: removed
* Connect cluster deduplication: fingerprint by connector name set

## Job Schedules (Enabled for Overnight)
* kafka-broker-health: */2 * * * * (60s timeout, ~6s runtime)
* kafka-consumer-lag-active: 1 */2 * * * (90s timeout, ~54s runtime)
* kafka-topic-sizes: */15 * * * * (30s timeout, ~8s runtime)
* kafka-msg-rate: */2 * * * * (30s timeout, ~14s runtime)

## Disabled Jobs (Test Tomorrow)
* kafka-topic-structure: 2 min cycle, 90s timeout (35s runtime)
* kafka-consumer-lag-full: daily, 180s timeout (governance)

## Known Constraints
* DevQA Kafka: 16,593 topics, 27,327 partitions, 663 consumer groups
* Kafka Connect: 10 workers, worker01 down, 291 unique connectors across 5 clusters
* Broker CPU: 83-89% (causes slow requests)
* Prometheus JMX exporter: deadlocked on port 7071 (HTTP hangs, TCP works)
* LLM_PROVIDER=bedrock, LLM_MODEL=us.anthropic.claude-sonnet-5
* CACHE_VERSION = "v2" in routes_dashboard.py

## Next Checkpoint
1. Large Topics KPI card fix
2. Job Management UI for 6 jobs
3. Schema Registry validation
4. kafka-topic-structure job test + enable
5. Consumer Groups tab validation
6. ZooKeeper → Governance tab rename

## Blocked
None — overnight jobs running.

## Additional Fix This Session
* Kafka Connect search: now searches both connector name AND connector_class (e.g. "elasticsearch", "debezium")
