# Backlog

**Rule: this file is the only source of truth for pending work.** If an item isn't here,
committed to git, it doesn't exist — regardless of what was said in any chat session.
Update this file in the SAME commit as the code change that creates, resolves, or modifies
an item. Never treat "I'll add it to the backlog" as done until it's in this file and
`git log` shows it committed.

Each item: short description, why it matters, status, date added.

---

## Open

### Startup sync burst competes with job scheduler on every restart
Found 2026-08-01 while investigating consumer-lag job slowness after a routine rebuild.
`main.py`'s `_startup_sync()` (the `"Startup: found N enabled cluster(s) — syncing in
background"` path) runs UNCONDITIONALLY on every container restart — calling
`collect_summary()` (list_consumer_groups, describe_group_states, topic listing, etc.)
for EVERY enabled cluster immediately on boot. This is separate from and NOT gated by
`collection_interval_secs=0` (which only disables that legacy loop's PERIODIC re-runs,
not this one-time startup call). It fires at the exact same moment the job scheduler is
also starting up and beginning to fire its own freshly-registered cron jobs — two
independent systems competing for broker connections right as the connection pool is
being freshly re-established.
User confirmed this pattern happens after EVERY rebuild specifically (not just tonight,
not explained by DevQA's separately-known chronically-high baseline broker load) — this
is the more likely root cause of "first job after any restart is disproportionately slow
or times out" than ambient broker load alone.
Its own code comment ("Do NOT overwrite restored cache with partial collect_summary data
— Parallel scans will enrich data and update cache incrementally") suggests its own
output is already largely superseded by the DB-restored snapshot and the job pipeline's
own incremental updates — worth checking whether this startup sync can be significantly
simplified, delayed until job-scheduler steady-state, or removed entirely now that the
job-pipeline architecture has fully superseded its original purpose.
NOT investigated further tonight — flagging precisely rather than guessing at a fix.
*Added: 2026-08-01*



### Mini message In/Out charts on Consumer Groups / Kafka Connect topic-lag popups
User request (2026-08-01): extend the same per-topic mini-chart pattern just built and
validated in the Topic Details popup (Topics tab) to the existing topic-lag drill-down
popups on Consumer Groups and Kafka Connect tabs (_showGroupTopicsPopup). Same endpoint
(GET /dashboard/topics/message-rate?topic=...), same compact chart design — should be a
quick reuse, not a new design, since the pattern is already proven. Explicitly for demo
impact with a wider audience — user's words: "these kind of magics will impress people."
*Added: 2026-08-01*



### Lag-based filter and sortable columns (Consumer Groups / Kafka Connect)
Quickly isolate critical groups/connectors via a lag threshold filter, plus ascending/
descending sort on the Lag column. Explicitly requested by user, queued after other items.
*Added: 2026-07-31 (original session, recovered from re-uploaded handoff.md)*

### N/A status UX review for PAUSED connectors
Currently a PAUSED connector and a source connector (architecturally no offsets) show
identical grey "N/A" status — user wants these distinguished, since N/A appears against
most PAUSED connectors and conflates two different meanings ("paused by someone" vs.
"not measurable by design").
*Added: 2026-07-31 (original session, recovered from re-uploaded handoff.md)*

### Source connector -> topic correlation
Use the all-topics inflow data (once the message in-flow feature ships) to assess source
connector health — i.e. is it actually producing. Explicitly agreed as its own separate
future cycle, not bundled into the message-rate chart work.
*Added: 2026-07-31 (original session, recovered from re-uploaded handoff.md)*

### SLO tab snapshot timestamp
Kafka Connect tab reads live REST (up-to-the-second); SLO tab reads the last
kafka_connector_snapshots row (up to 2 min old) — occasional small count discrepancies
between the two are a timing difference, not a bug, but look contradictory without
context. Fix: surface "as of HH:MM" on the SLO tab. Low priority, cosmetic.
*Added: 2026-07-31 (original session, recovered from re-uploaded handoff.md)*

### Existing "State: Active/Empty" filter on Consumer Groups tab likely a no-op
Checks `gState === 'empty'`, but `g.state` in this data source (from
`collect_consumer_lag_active`) only ever holds `"consumer"`/`"connect"` — never
`"empty"`. Found while adding the Type filter (2026-07-30), not fixed. Low priority,
cosmetic/misleading rather than data-incorrect.
*Added: 2026-07-30*

### kafka-consumer-lag-3 job duration — monitor
Timeout increased 150s → 300s after partition-level lag upserts added real write volume.
Currently stable at 35-62s/run (6,008 partition rows for cluster 3), but worth a one-time
check that duration isn't still trending upward as data grows.
*Added: 2026-07-30*

### Request-handler idle % — one-character metric name bug (safe to fix)
Our code requests `kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent`;
real exported metric is `..._requesthandleravgidle_percent` (underscore before "percent").
Confirmed via live JMX dump — real value present and sensible (90.9% idle). Unlike the
JMX cardinality issue below, this is entirely fixable in our own filter list, zero
broker-side risk.
*Added: 2026-07-30*

### Message-rate/Activity chart shows 0 at the top of every UTC hour
`kafka_topic_metrics_hourly` has no rows for a new hour bucket until `collect_msg_rate`'s
first cycle of that hour lands (~2-4 min gap). `get_topics_history`'s bucket-alignment
logic zero-fills missing buckets rather than omitting or carrying the previous value
forward — misleading (looks like "no traffic" when it's actually "data still landing").
Confirmed live, self-corrects within minutes. Low urgency, but flagged as user-facing
misleading behavior, not just cosmetic. Fix direction: skip-render the newest incomplete
bucket, or carry the previous value forward as placeholder.
*Added: 2026-07-31 (found during pre-demo check)*

### Legacy `_collection_loop` — decommission decision
Currently disabled via `collection_interval_secs=0` (config only, not code removal).
Decide: fully remove the code, or formalize the disabled state as permanent. Blocks a
clean AI Insights fix (AI Insights partially depends on `kafka_metrics_history`, which
only this loop populates).
*Added: 2026-07-29*

### AI Insights — full dedicated pass
Not audited in recent sessions. Depends partly on the disabled legacy loop's stale
`kafka_metrics_history`. Needs a full trace of what data AI Insights actually receives
today before deciding what needs a job-pipeline replacement vs. what can be simplified away.
*Added: 2026-07-29*

### Cluster-switch race condition (Overview tab)
No request cancellation on cluster switch — an in-flight request from the previously
selected cluster can resolve after switching and silently render onto the wrong cluster's
view. Confirmed via live Network tab capture. Overview tab (message-rate chart
specifically) is the one confirmed-risky surface; all other tabs have self-evident
identifying content (broker IDs, hostnames, topic/group names) that makes a mix-up
obvious. Needs a dedicated, carefully validated session (AbortController on switch, or
tag+validate responses against current cluster ID before rendering) — not a quick fix.
*Added: 2026-07-29*

### `describe_consumer_groups` — other dependent features to check
Root cause fixed (2026-07-30, consumer-protocol filtering before the call). Worth
checking whether anything else in the codebase depended on the old (broken) behavior in
a way that needs re-validation now that the call succeeds where it previously failed.
*Added: 2026-07-30*

---

## Established Patterns (Reference — do not re-litigate, cite this instead)

### Safe pattern for bulk blocking Kafka work (large partition/group counts)
Established 2026-08-01 during the message-inflow redesign. Applies whenever a collector
needs to do blocking Kafka client work (KafkaConsumer/KafkaAdminClient calls) across a
large number of items (partitions, groups, topics) that could exceed a single call's safe
bounds:

1. **Bound the blast radius of any single blocking call** — cap how many items one
   KafkaConsumer/thread touches at once (e.g. a per-cluster tunable like
   `max_inflow_partitions_per_cycle`, default sane for a small cluster, adjustable per
   cluster as bigger environments are onboarded). This is what actually prevents a repeat
   of the original incident (one unbounded 27,746-partition sweep hanging at 880s against
   a 450s timeout) — NOT avoiding parallelism.
2. **Use a DEDICATED ThreadPoolExecutor for this job**, not the shared default
   `run_in_executor(None, ...)` pool used by every other collector — isolates any hang's
   damage to just this one job, so a stuck thread here can't eventually starve unrelated
   collectors sharing the app-wide default pool. Create it ONCE at module level, reuse
   across every job run.
3. **Process bounded chunks IN PARALLEL via that dedicated pool** (chunk work + dispatch
   each chunk via `loop.run_in_executor(dedicated_executor, sync_fn, chunk)` + gather) —
   this is safe and already proven elsewhere in this codebase (RealKafkaCollector's
   group-lag fetching in `real_kafka.py`, running in production throughout this entire
   session with zero hang incidents). Parallelism does NOT add risk beyond what already
   exists in sequential execution — the underlying `asyncio.wait_for`-cannot-cancel-a-
   thread limitation is identical either way; what actually matters is #1 and #2 above.
4. **Give each parallel worker its own client instance** (KafkaConsumer/KafkaAdminClient
   are not thread-safe — never share one instance across threads).
5. **Honest, accepted residual risk**: this does NOT eliminate the underlying "can't
   forcibly kill a hung thread" limitation — it CONTAINS the damage (isolated pool) and
   REDUCES the likelihood (bounded chunk size), it doesn't remove the possibility. A true
   hard-kill would need a separate OS process (`multiprocessing`, where `.terminate()`
   genuinely works) — a materially bigger architectural change, not undertaken here since
   we have no evidence the contained/bounded approach is insufficient. Revisit only if a
   dedicated-pool job is ever observed to actually exhaust its pool from repeated hangs.

### Data-layer documentation for Docs Hub (ERD + table/job/functionality mapping)
User request (2026-08-01). Deliverable for the Docs Hub, distinct from a raw ERD (user
already has one) — needs three things:
1. Visual ERD-style diagram, ideally close to what native DB tools (e.g. MS SQL Server's
   built-in diagramming) produce — proper entity/relationship visualization, not just a
   text table list.
2. Table -> job mapping: which collector/job writes to (and reads from) each table, so
   someone can trace "where does this data come from" without reading collectors.py
   line by line.
3. Table -> functionality mapping: which dashboard features/tabs/endpoints actually use
   each table, so the data layer's purpose is traceable to real product functionality,
   not just structurally documented.
Scope note: this agent's schema has grown substantially and organically across many
sessions (kafka_broker_metrics, kafka_consumer_group_lag, kafka_consumer_group_topic_lag,
kafka_consumer_group_partition_lag, kafka_topic_message_rate_snapshots,
kafka_topic_partition_inflow_baseline, kafka_connector_snapshots, kafka_slo_targets/
compliance, kafka_job_schedules/runs, kafka_clusters, and more) — this is a genuinely
sizeable documentation effort, not a quick add-on. Good candidate for its own dedicated
session rather than squeezing in alongside feature work.
*Added: 2026-08-01*

## Value-Add Ideas (not scoped as concrete backlog items yet — discuss before building)
- Orphaned/dead-write topic detection — DevQA has only ~2,557 (group, topic) pairs with
  any committed offset, out of ~17-18k total topics. Cross-reference
  `kafka_topic_metrics.bytes_in_per_sec > 0` against consumer-group topic coverage.
- RF=1 topic list (not just count), cross-referenced with active bytes_in_per_sec.
- Automatic partition-leader-imbalance alerting.
- Dead/empty consumer group hygiene listing.
- Schema Registry compatibility-mode risk flags (subjects set to `NONE`).
- Read-only SSH RAM/disk-mount metrics — parked, check with Kafka team whether they want
  this consolidated here given CloudWatch/NR already cover generic infra metrics.

## Explicitly Parked (not being worked, revisit only when trigger condition below is met)
- **JMX exporter per-topic cardinality fix** (would unlock GC/ISR/latency/bytes_out
  filtering) — a prior attempt at a JMX exporter config change caused a broker to fail to
  come back up, required rollback. **Trigger to revisit: only during a scheduled, reviewed
  Kafka upgrade maintenance window**, tested by senior Kafka team review of the exact
  exclude pattern first, never as a standalone change.
- Kafka team's response on the ~38GB unaccounted mount-space gap on external staging.
- Internal Staging Kafka onboarding — waiting on soak monitoring of clusters 3/4.

---

### In-memory-state + run_in_executor cancellation audit — completed 2026-08-01
Full findings: 9 module-level state dicts total, classified by restart consequence.
`_jobs` (jobs.py) and `_lag_trend_cache` (routes_dashboard.py) — zero risk, rebuilt from
postgres / TTL cache in front of postgres. `_cooldown_cache` (escalation_notifier.py),
`_broker_state`/`_topic_state` (prometheus_collector.py), `_prev_offsets` (collect_msg_rate)
— low risk, self-healing within one short (~2 min) cycle. `_prev_end_offsets`/
`_prev_end_offset_time` (collect_topic_message_inflow) — the real one, ~200-430s cycle
means a restart wastes a genuinely expensive sweep, not a cheap one.
Separately: of 9 scheduled jobs, 6 use `run_in_executor` (directly or via real_kafka.py)
and are technically exposed to the same "timeout can't cancel a thread" limitation, but
only `kafka-topic-inflow` has ever actually hit it (880s vs 450s timeout) — it's the only
one sweeping tens of thousands of items per cycle; the rest operate on single-digit-to-
low-hundreds of items. Decision: fix `collect_topic_message_inflow` specifically rather
than a blanket fix across all 6, to avoid solving a problem with no evidence elsewhere.
If any other job ever shows the same symptom (run time wildly exceeding its timeout),
revisit this decision for that specific job.

### Message In/Out — persisted baseline redesign, sharded + parallelized — shipped 2026-08-01
Full rewrite of `collect_topic_message_inflow`. Removed in-memory `_prev_end_offsets`/
`_prev_end_offset_time` entirely. New per-cluster tunable `max_inflow_partitions_per_cycle`
(migration 0033, default 5000) determines shard count via ceiling division. Partitions
assigned to shards deterministically via `zlib.crc32` (NOT Python's `hash()`, which is
per-process randomized and would silently reshuffle shard membership on every restart).
Baseline persisted in `kafka_topic_partition_inflow_baseline` (migration 0032, finally
committed after sitting untracked since the original rollback). All shards processed
CONCURRENTLY every cycle via a dedicated `ThreadPoolExecutor(max_workers=10)` — isolated
from the shared default executor pool used by every other collector, so a hang here can't
starve unrelated jobs. See "Established Patterns" section above for the reusable version
of this approach.
**Validated with real production data**: single-shard test 185.4s (~4,711 partitions);
full-parallel test 189.5s for the ENTIRE cluster (all 27,746 partitions, all 6 shards) —
essentially the same wall-clock time as one shard alone, confirming the parallel design
works as intended. Three natural cron-triggered cycles at the new 5-min/300s-timeout
schedule: 176.9s, 129.6s, 172.3s — all successful, comfortable margin, no skipped or
overlapping runs (confirmed APScheduler's default max_instances=1 protects against
overlap even though not explicitly configured). Self-healing from ~23,035 stale rows left
over from the original incident (crc32 shard assignment doesn't care about row history —
stale partitions simply get overwritten on their next natural cycle, no manual cleanup
needed). Delta computation uses each partition's own baseline `updated_at` for interval
(not a single global interval), correctly handling the transitional catch-up period.
Job schedule tightened from 10min/450s to 5min/300s after validation.

### Message In/Out chart — Chunk 5, fully complete — shipped 2026-08-01
All three UI surfaces built and validated on both static and portal dashboards: (1)
Overview tab — new cluster-wide two-line chart (Message Volume — Real In/Out, msgs/sec)
alongside the existing bytes/sec throughput chart; (2) Topics tab — restructured into a
two-column layout, Throughput Trends (bytes) next to the new cluster-wide Message Volume
chart; (3) Topic Details popup — new per-topic mini-chart using the endpoint's
topic= single-topic mode (the endpoint's own docstring indicated this was its intended
use). Added inflow_rate/outflow_rate fields (messages/bucket-duration-seconds) to
GET /dashboard/topics/message-rate for correct cross-zoom-level comparability (raw
per-bucket counts aren't comparable across different bucket sizes as the time range
changes). Separately fixed a real, pre-existing mislabeling: the ORIGINAL "Message Rate"
chart titles on Overview and Topics tab were actually bytes/sec throughput
(kafka_topic_metrics_hourly), not real message counts — renamed to "Throughput
(Bytes/sec)" to avoid confusion now that a genuinely accurate message-count feature
exists alongside it.
This closes the full message-inflow feature end to end, started as Chunk 1 in the
2026-07-31 session: schema -> sharded/parallelized collector -> endpoint -> three UI
surfaces, all built and validated with real production data.

## Resolved (kept for reference — move here, don't delete, when an item closes)

### total_connectors hardcoded to 0 — fixed 2026-07-30, commit c284a0a
### describe_consumer_groups crash — root-caused and fixed 2026-07-30, commit a9dd92f
### Broker-timestamp correlation hardening — fixed 2026-07-30, commit 99e85e3
### Consumer vs. Connector filter (Consumer Groups tab) — shipped 2026-07-30, commit d73f4eb
### Source vs. sink Connect-group lag distinction — superseded by real per-connector lag
feature (2026-07-30, commits fd2fadb/6c46d30) — the honest "N/A" badge for zero-offset
groups is the final answer; no further work needed.
### Real per-connector lag on Kafka Connect tab — shipped 2026-07-30, commits fd2fadb, 6c46d30
### Per-partition lag drill-down (Connector -> Consumer Group -> Topics -> Partitions -> Lag)
— shipped 2026-07-30, commits 0ee1590, 423553e, 2e4694c
