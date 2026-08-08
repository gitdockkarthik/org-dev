# Session Reconciliation Log

**Purpose:** A single, project-wide record of what changed in every session, across
every part of the platform (any agent, portal, backend, database, infrastructure) --
not just one agent's own BACKLOG.md. Every session must check this file at the
start of work and append a new entry at the end.

**Why this exists:** A documented "fix" from a prior session (handoff.md's claim
that a risky collector redesign had been rolled back) turned out not to match the
actual deployed code -- the rollback was a working-tree discard that was never
committed, and a later, undocumented commit re-introduced the same risky pattern.
This went undetected for days until a live memory/CPU incident forced investigation
on 2026-08-08. This file exists so that claim and reality never drift apart
silently again.

**Rules for every session:**
1. Read the most recent entry before starting any work.
2. Before ending a session (or when explicitly asked to reconcile), verify that
   every item listed as "working before" in the most recent entry is still
   genuinely working -- not assumed, checked.
3. Append a new entry (never edit or delete a prior one) covering: state before,
   every change made (with file paths and commit hashes where applicable), state
   after (re-verified, not assumed), anything deferred, and any known risks.
4. Proper permanent fixes only -- no workarounds or shortcuts taken to hit a time
   target. If a genuine fix cannot be completed properly in the available time, it
   is deferred explicitly (stated here, with the user's sign-off) rather than
   patched partially and presented as done.

---

## 2026-08-08 -- Kafka job stability incident, monitoring stack, ClickHouse resource leak

### State before this session
- kafka-analyser container at 1.09GiB/2GiB memory, CPU up to 172%, after only ~14h
  uptime.
- `collect_topic_message_inflow` failing every cycle for over an hour (~600s
  timeouts, double its own limit) -- the exact `run_in_executor`-cannot-be-cancelled
  issue `handoff.md` had already documented and warned against retrying, which a
  later, undocumented commit (5b69bde) retried anyway and got committed.
- `collect_topic_structure` had two additional, never-migrated thread-based Kafka
  call sites (list_all_topics, broker-leader-distribution), the same class of risk.
- Kafka job schedules for broker-health/connector-snapshots/topic-inflow/topic-sizes
  had multiple clusters firing at the identical minute mark, causing real,
  confirmed contention on a 4-worker process pool.
- No resource or job-health monitoring existed for the platform at all.
- ClickHouse (Langfuse's mandatory backing store, tracks LLM token usage/cost
  platform-wide) was consuming up to 445% CPU continuously, driving host load
  average to 13+ on an 8-core box -- root cause: its own internal diagnostic
  tables (system.text_log, system.metric_log, system.asynchronous_metric_log) had
  accumulated ~7GB / 145M+ rows over 3-4 weeks with no retention policy, requiring
  constant background merging. Confirmed unrelated to Langfuse's actual
  application data (traces/observations tables were tiny, ~159 rows).

### Changes made
1. `collect_topic_message_inflow` (collectors.py) -- reverted to last proven-stable
   logic (bcf60ca) and migrated to genuine process isolation (new
   `seek_to_end_isolated` in kafka_process_pool.py). Commit a6e043f.
2. `collect_topic_structure` (collectors.py) -- migrated its two remaining
   thread-based call sites to new process-isolated functions
   (`list_topics_isolated`, `describe_broker_distribution_isolated` in
   kafka_process_pool.py). Same commit a6e043f.
3. Job schedule staggering -- applied directly to `kafka_job_schedules` in
   postgres: broker-health/connector-snapshots/topic-inflow/topic-sizes across
   clusters 3/4/8 spread across different minute offsets instead of colliding at
   :00 seconds. Not a code commit (database configuration change).
4. `kafka_process_pool`'s `max_workers` increased 4 -> 6 as an initial safety
   margin (commit b074e05), then reverted back to 4 later in the same session once
   the real root cause of the remaining consumer-lag-8 failures was found to be
   unrelated ClickHouse CPU starvation, not pool contention -- the increase was
   never actually needed. Reverted per the principle of minimal,
   evidence-justified changes.
5. `collect_consumer_lag_active`'s timeout increased 60s -> 90s (collectors.py).
   Commit (bundled with above).
6. Monitoring stack added: Prometheus, Grafana, node-exporter, cadvisor
   (docker-compose.yml, monitoring/) -- two dashboards (Platform Resource
   Overview, Kafka Job Health) and four alert rules (host memory/CPU, container
   memory, job-missed-schedule). Commits eae4e5c, b074e05. One alert rule
   (`relativeTimeRange: {0,0}`) briefly crashed Grafana entirely on first deploy --
   caught and fixed within the same session (`{from: 60, to: 0}`).
7. ClickHouse internal log tables (system.text_log, system.metric_log,
   system.asynchronous_metric_log) truncated (~7GB reclaimed) and given a 1-day
   TTL to prevent reaccumulation. Applied directly via clickhouse-client, not a
   file-tracked change -- documented here as the record of this change. This was
   the actual root cause of consumer-lag-8's intermittent failures (host load
   average 13+ on an 8-core box from ClickHouse alone consuming up to 445% CPU),
   confirmed by the failures fully stopping once this was fixed and not
   recurring after the process-pool size was reverted back to 4.
8. Both Grafana dashboards initially showed "No Data" in the UI despite the
   underlying queries working correctly when tested directly -- two separate,
   real bugs found via live browser console debugging: (a) each panel's
   individual `target` needs its own `datasource` field, not just the panel-level
   one (fixed in both dashboard JSON files), and (b) this Grafana version
   (13.1.3) additionally requires `database` inside the datasource's `jsonData`
   block, not just the top-level `database` field (fixed in datasources.yml).
   Both confirmed resolved with real data rendering in all panels, verified via
   screenshot from the user's own incognito browser session.

### State after this session (re-verified, not assumed)
- kafka-analyser: zero job failures across broker-health, topic-sizes,
  topic-structure, topic-inflow, consumer-lag, both active clusters, sustained
  over multiple real observation windows post-fix. Memory settled ~800-950MB
  (was 1.09GiB and climbing), CPU normal.
- ClickHouse: CPU dropped 445% -> ~2-3%, stable. Host load average dropped from
  13+ to under 1 (1-min average; 5/15-min averages confirmed trending down in the
  same session).
- Langfuse tracking validated end-to-end post-ClickHouse-fix: a real, live LLM
  call through kafka-analyser correctly appeared in `default.traces` and
  `default.observations` with accurate token usage (1236 input / 6 output / 1242
  total, matching the agent's own reported count exactly) and calculated cost
  ($0.002532) -- confirms the cleanup did not affect Langfuse's actual application
  data or cost-tracking accuracy.
- Monitoring stack confirmed live: both dashboards render real data (verified via
  Grafana API, not just file-valid JSON); all 4 alert rules loaded and evaluating
  (one, "Kafka Job Missed Schedule", correctly fired on a genuinely stale,
  deliberately-retired job `kafka-urp-status-4` -- a known false positive to
  refine, not a bug in the alert logic itself).
- Prior sessions' other confirmed-working items (broker reachability, URP
  accuracy, Topics/Consumer Groups/Kafka Connect tab fixes from 2026-08-06/07) --
  not re-touched this session; no changes made to any of that code.

### Deferred (explicit, with reason)
- Teams alerting contact point for the 4 alert rules -- user's own instruction:
  attempt a fresh Teams channel/webhook first, fall back to the existing
  alert-analyser webhook if blocked. Not started this session.
- "Kafka Job Missed Schedule" alert's false positive on retired job IDs
  (kafka-urp-status-4) -- needs a filter for jobs with no active schedule entry.
- SLI/SLO, ZooKeeper, MirrorMaker, Schema Registry tab audits -- explicitly
  deprioritized by the user given today's incident; a lighter safety-pass (not
  full audit) still owed.
- Design (not implementation) of running Kafka's collection jobs in a separate
  worker process/deployment, decoupled from the API-serving runtime, for
  resource-optimized HA -- explicitly scoped as "design today, implement
  tomorrow," tightly coupled to kafka-analyser specifically (not a generic,
  reusable job runner, per user's explicit instruction referencing a prior,
  decommissioned attempt at that pattern).

### Known risks / caveats carried forward
- `ProcessPoolExecutor`'s `future.cancel()` on a timeout does not guarantee the
  underlying OS process stops instantly if the task has already started
  executing -- it kills and replaces the worker, which is a genuine, meaningful
  improvement over a thread (which cannot be forcibly stopped at all), but is not
  a perfect, instantaneous guarantee. Documented here rather than overstated as
  a complete fix.
- ClickHouse's 1-day TTL on its own internal logs is a first pass; worth
  revisiting whether even 1 day is more than needed, per the user's own instinct.
