# Backlog

**Rule: this file is the only source of truth for pending work.** If an item isn't here,
committed to git, it doesn't exist — regardless of what was said in any chat session.
Update this file in the SAME commit as the code change that creates, resolves, or modifies
an item. Never treat "I'll add it to the backlog" as done until it's in this file and
`git log` shows it committed.

**Rule: always run `git status` immediately before `git commit`, not just before
`git add`.** Found 2026-08-01: a docs-only commit for one fix left the actual code
change unstaged and forgotten, caught only because `git status` was checked before the
NEXT commit. A separate incident the same day: two unrelated changes got bundled into
one commit because files staged earlier were still staged when a later `git add` ran —
harmless (nothing lost, both changes correctly committed) but produced an inaccurate
commit message. Always check the full staged set right before committing, not just
right after the most recent `git add`.

**Primary validation surface: portal** (`http://kpi-internal.cloud.operative.com:3000/...`),
not the static/direct dashboard — user's team will use portal in production. Static-first
validation is still the right approach (isolated test bed catches bugs before they can
affect the portal surface other users depend on), but portal is the one that must actually
work before considering an item done. Always test BOTH before committing, but portal is
the one that matters most if forced to choose.

Each item: short description, why it matters, status, date added.

---

## Open

## Prioritized Plan (finalized 2026-08-01 — this table is the execution order; full
detail for each item lives in its own entry below under ## Open. User also tracks a
copy of this table in Excel for human-readable/team visibility — this file remains the
authoritative source if the two ever diverge)

### Day 1 — Today (2026-08-01), 9.1 hrs
| Order | Item | Est. |
|---|---|---|
| 1 | Monitor kafka-consumer-lag-3 duration | 0.1 hr |
| 2 | Request-handler idle % metric name fix | 0.25 hr |
| 3 | Message-rate chart hour-boundary zero-fill | 1.5 hrs |
| 4 | Lag-based filter + sortable columns | 1.5 hrs |
| 5 | N/A status UX for PAUSED connectors | 0.75 hr |
| 6 | SLO tab snapshot timestamp | 0.5 hr |
| 7 | Fix "State: Active/Empty" filter no-op | 0.5 hr |
| 8 | **Cluster-switch race condition (CRITICAL)** | 4 hrs |

### Day 2 — Tomorrow (2026-08-02), 7.75 hrs
| Order | Item | Est. |
|---|---|---|
| 9 | Startup sync burst vs. job scheduler contention | 2 hrs |
| 10 | Source connector -> topic correlation | 2 hrs |
| 11 | describe_consumer_groups dependent-feature check | 0.75 hr |
| 12 | Legacy _collection_loop decommission decision | 3 hrs |

### Day 3 — Monday (2026-08-03), 8 hrs
| Order | Item | Est. |
|---|---|---|
| 13 | Data-layer documentation (ERD + mappings) | 6 hrs |
| 17 | Throughput (Bytes/sec) chart — raw snapshots for 1-hour granularity | 2 hrs |

### Day 4 — Tuesday (2026-08-04), 6 hrs
| Order | Item | Est. |
|---|---|---|
| 14 | AI Insights full dedicated pass | 5 hrs |
| 15 | CSV Export for Topics, Consumer Groups, Connectors | 1 hr |

### TBD — blocked, not scheduled
| Order | Item | Est. |
|---|---|---|
| 16 | Product/Service tag mapping from SharePoint CSV | TBD — pending tag schema finalization + SharePoint read-mechanism decision |

**Grand total: 30.85 hrs across 4 scheduled days (excludes TBD item).**

Rule: at the start of each day's session, re-check this plan against actual progress —
if a prior day's items slipped, re-confirm order rather than assuming this table is still
accurate. Update this table (not just individual item entries) whenever priorities change.



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





### kafka-consumer-lag-3 job duration — checked 2026-08-01, stable
Timeout is 300s. Checked last 9 runs (2026-08-01 08:30-09:03): range 32-208s, no
failures, no upward trend — variance matches DevQA's known chronic broker load
fluctuation, not a growing-data problem. One cycle skipped (08:33) confirming
APScheduler's max_instances=1 overlap protection works correctly in practice (skip
rather than stack). No action needed.
*Added: 2026-07-30, checked and closed: 2026-08-01*

### Request-handler idle % — name fixed 2026-08-01, but does NOT restore real data
CORRECTION to prior assessment: name was fixed (underscore added before "percent",
matching the real exported metric), but live-testing the CORRECTED name against the
filtered scrape endpoint (?name[]=...) still returns EMPTY (0 bytes) — this metric has
the SAME filtering limitation as GC/ISR/latency (parked JMX cardinality issue), not a
simple typo issue as previously assumed. The prior assessment ("entirely fixable in our
own filter list") was based only on confirming the value existed in the FULL unfiltered
dump — that test was incomplete; it did not verify the filtered endpoint specifically.
Current state: dashboard still correctly shows 100.0 (the existing, unchanged fallback
behavior) rather than a wrong or crashing value — no regression, but no improvement
either. Name correction itself is harmless/more accurate and was kept. Real fix requires
the same JMX exporter reconfiguration as the parked GC/ISR/latency issue — merge into
that item rather than treating as separately fixable. Commit: (pending, being added this
session).
*Added: 2026-07-30, corrected: 2026-08-01*

### Message-rate/Activity chart hour-boundary zero-fill — FIXED 2026-08-01
`get_topics_history` now drops the newest bucket from the response if it has zero data
across every series (only applies to the single trailing/current bucket, not historical
gaps, which remain legitimate zeros). Validated: cluster 3 correctly trimmed 24 buckets
to 23 right at a fresh UTC hour boundary while genuine data existed elsewhere; a fresh
page load with no prior cluster switches showed a correct two-point chart with a real
trend line. (A confusing single-dot render was separately investigated and traced to the
cluster-switch race condition, not this fix — see that item.)
*Added: 2026-07-31 (found during pre-demo check), fixed: 2026-08-01*

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

### PARTIALLY DONE — Cluster-switch race condition, main tab-load path complete,
### popup/interaction functions remain (2026-08-02)
Main tab-load path DONE and validated live (static + portal, committed): (1) global
window._loadGeneration counter, incremented on cluster switch; (2) Overview tab's 3
charts (activity, messages, lag-trend); (3) loadTab's 9 shared-pattern cases (all main
tabs except SLO); (4) SLO tab's loadSLODashboard -- all 5 async render points (main
content, broker gauges, connector-trend, overall-trend, lag-trend). This covers
everything a user sees immediately/constantly on tab load.
REMAINING (found during the final audit that was always part of this item's scope): 9
more functions that fetch-and-render but sit OUTSIDE the main tab-load path -- popups
and user-triggered detail views, not always-visible content: _showGroupTopicsPopup,
_showTopicPopup, _openSLOPopup (detail popups); _loadTopicTable, _topicCompareSearch,
_renderTopicTimeSeries (topic table interactions); _expandTopicRow (expandable
partition rows); _streamTopicDetails, _streamGroupLags (streaming describe flows).
Lower risk than main content (user-triggered, modal/focused, not constantly visible),
but the same stale-render risk applies if a cluster switch happens mid-load. Needs its
own pass: same generation-guard pattern, static + portal, validated live.
**Keep this item's status as Pending until this remaining piece is closed** -- user's
explicit instruction, not marking done prematurely.
Original CRITICAL status/detail preserved below — still valid.

### CRITICAL — Cluster-switch race condition, scope broader than previously tracked
No request cancellation on cluster switch — an in-flight request from the previously
selected cluster can resolve after switching and silently render onto the wrong
cluster's view. Originally scoped (2026-07-29) as "Overview tab only, other tabs have
self-evident identifying content that makes a mix-up obvious" — that assumption is now
KNOWN WRONG, with TWO separate independent live reproductions on 2026-08-01:
1. After adding External Staging (cluster 4) alongside DevQA (cluster 3), switching to
   cluster 4 on the Kafka Connect tab showed connector data — cluster 4 legitimately
   has none.
2. Overview tab's Throughput chart showed a single, stale, wrong-looking data point
   after switching between clusters 3 and 4 within the same session — confirmed via a
   completely fresh page load (no prior tab/cluster switches) that the SAME data,
   fetched correctly, renders correctly as two real points with a genuine trend line.
   The broken render ONLY appeared after switching clusters, not on first load —
   directly confirms stale-response-overwrites-fresh-render as the mechanism, exactly
   matching the originally suspected AbortController/response-validation root cause.
A new user unfamiliar with which cluster has what would not find either symptom
"self-evident" — both look like real data.
**Elevated to critical — team handover for validation/monitoring starts next week,**
this cannot ship to a wider audience unfixed. Needs: (1) a full re-audit of EVERY tab,
not just Overview, to find every place stale cross-cluster data can render, (2) the
actual fix (AbortController on switch, or tag+validate every response against the
current cluster ID before rendering — decide which during the fix session, not before).
Dedicated, carefully validated session required — this is a correctness bug affecting
trust in the whole dashboard, not a UX polish item.
*Added: 2026-07-29, rescoped and elevated to critical: 2026-08-01, second live
reproduction confirmed: 2026-08-01*

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

### Throughput (Bytes/sec) chart — 1-hour view too coarse, always looks flat
User finding (2026-08-01): the 1-hour zoom on Throughput Trends (Overview + Topics tab)
only ever shows 2 HOURLY buckets even at its tightest zoom, because
kafka_topic_metrics_hourly is a genuine hourly-rollup table — the raw ~2-min readings
that feed it get folded into a running average and discarded, never persisted
individually. Getting real 5/10-min granularity for the 1-hour view needs the same
scope of work as the message-rate redesign: new raw-snapshot table, collector change to
persist individual readings (not just the hourly rollup), endpoint change to bucket off
raw data for short zoom levels. NOT a quick fix — comparable effort to the message-rate
Chunk 1-3 work. Note: the newer Message Volume (real msgs/sec) chart already has this —
kafka_topic_message_rate_snapshots stores raw individual readings, so its 1-hour view
already shows real shape, not a flat line. That's the right tool for short-term trend
inspection today; this item is specifically about closing the same gap for the
bytes/sec chart.
*Added: 2026-08-01, scheduled: Monday 2026-08-03 alongside data-layer documentation*



### Static-vs-portal UI parity audit
User found "a few discrepancies" between static and portal dashboards while validating
today's SLO fix — not detailed/urgent (no plans to ship this agent to another platform
soon), but worth a full side-by-side audit at some point given the two files are
maintained in parallel and drift has happened before (caught multiple times today via
diff-symmetry checks). Low priority.
*Added: 2026-08-01*

### CRITICAL (elevated) — Dashboard 24h query performance, confirmed real DB bottleneck
Investigated properly with real evidence (2026-08-01), NOT just timing/race-condition
related — confirmed via EXPLAIN ANALYZE this is a genuine, separate data-layer problem.
`kafka_topic_message_rate_snapshots` has grown to 6,288,569 rows (started ~2026-07-31,
i.e. accumulated in about a day) via raw, un-aggregated per-topic-per-cycle snapshots
(deliberate design choice from the message-inflow redesign, to avoid the flat-line/
zero-fill problems the old hourly-rollup table had). The table HAS correct indexes
(cluster_id, collected_at) and (cluster_id, topic, collected_at) — but Postgres's query
planner chooses a Parallel Seq Scan over them for the 24h window query, because a 24h
window already covers ~64% of the table's total (very short) lifetime, making a seq
scan genuinely cheaper by cost-based estimation than an index scan. Confirmed:
EXPLAIN ANALYZE showed 2.99s execution time, ~4M rows scanned via Parallel Seq Scan,
Rows Removed by Filter: 756,506 per worker.
This will get WORSE as the table keeps growing (~6M rows/day at current collection
rates) — not a one-time blip, a compounding problem.
Separately confirmed (same investigation): Overview tab's three charts (activity,
messages, lag-trend) load SEQUENTIALLY (three chained `await` calls in one function),
not concurrently — a real, independent inefficiency worth fixing alongside this
(should be three parallel fetches, total time = slowest one, not the sum of all three).
Real fix needed (NOT just an index tweak — indexes are already correct): likely a
retention/rollup policy — keep raw 5-min granularity for a short recent window (e.g.
last 24-48h), roll up older data into coarser pre-aggregated buckets (matching the
pattern kafka_topic_metrics_hourly already uses, but WITHOUT reintroducing that table's
zero-fill problem — needs careful design, likely combining raw-snapshot querying for
very recent data with a rolled-up table for anything older).
**User explicitly wants this elevated to be fixed immediately after item #8 (cluster-
switch race condition), same session, not deferred further** — this is a real,
worsening architectural gap in the data layer, not a nice-to-have.
*Added: 2026-08-01, elevated with real evidence same day*

### Job schedule staggering to avoid same-tick collisions -- FIXED 2026-08-02
Found live: multiple job groups shared identical cron schedules, meaning they fired at
the exact same second every cycle -- guaranteed collision, not occasional bad luck.
This was the direct cause of the remaining ~1-2% failure rate even after all of
tonight's connection/thread-pool fixes. Audited ALL 19 registered schedules, staggered
the slow/resource-intensive collisions (left fast jobs like connector-snapshots and
slo-compliance alone -- their collision window is negligible given sub-5s duration):
- kafka-msg-rate-3/4: */2 -> 1-59/2 (separates from broker-health-3/4, same */2)
- kafka-consumer-lag-4: */3 -> 1-59/3 (separates from consumer-lag-3)
- kafka-topic-structure-4: */5 -> 2-59/5 (separates from topic-inflow-3/4, same */5)
Applied via direct kafka_job_schedules UPDATEs (no migration/audit trail exists for
this table -- see the separate backlog item about that gap).
**Validated live**: 10 consecutive runs immediately after, spanning every job type
(broker-health, connector-snapshots, msg-rate, sr-sync, topic-inflow, consumer-lag,
topic-sizes), zero failures, all fast (1.2-80.2s) -- combined effect of this staggering
plus tonight's logging-suppression and shared connection/thread-pool fixes.
*Added: 2026-08-02*

### Disable job schedule/cron edit in UI (protect tonight's careful tuning) -- DONE
User's decision (2026-08-02): given how carefully the job schedules (cron + staggering)
were tuned to eliminate real collection failures, an accidental UI edit could silently
undo this. Removed the Edit Schedule button from reports.html (static + portal) --
future changes go through backend scripts, not casual UI editing. Run and Enable/
Disable buttons unaffected; the underlying /jobs/{id}/schedules/{id} PUT endpoint and
the dormant modal JS functions were left fully intact for scripted use.
*Added: 2026-08-02, completed same day*

### Shared thread pool + schedule staggering need to scale with cluster count
User is onboarding more clusters soon (internal staging by Tuesday, then internal prod
and external prod). Two things from tonight's fixes need revisiting as cluster count
grows:
1. **Shared thread pool (_kafka_io_executor, currently 12 workers)** is one pool for
   the WHOLE application, not per-cluster -- confirmed: the AdminClient connection
   itself is fully isolated per cluster (keyed by bootstrap_servers), but all clusters'
   jobs compete for the same 12 worker threads. More clusters running jobs
   simultaneously = more total demand on this one shared resource. Revisit pool size
   when the next cluster is onboarded, based on real measured contention, not
   speculatively now.
2. **Schedule staggering (done tonight for clusters 3/4) does not automatically apply
   to new clusters** -- a newly onboarded cluster's jobs will use the same default cron
   patterns as existing clusters, recreating the same same-tick collisions we just
   fixed, just between different clusters. Worth making this automatic (e.g. offset
   each cluster's schedule based on cluster ID at registration time) rather than
   manually re-staggering every time a cluster is added.
*Added: 2026-08-02*

### Host-level disk/resource investigation and cleanup (2026-08-02)
Investigated shared t3.xlarge host (16GB RAM, 4 vCPU, 30GB root disk + separate 60GB
/data volume for Docker) for scaling planning. Findings:
1. **Load average 10.95/10.91/11.33 on 4 vCPUs** (~2.75x capacity) -- NOT caused by
   our own agent (kafka-analyser confirmed using 0.13% CPU, 414MB/2GB RAM at time of
   check). Dominant consumer: org-dev-clickhouse-1 at 211.93% CPU (used for Langfuse
   LLM token/cost tracking across onboarded teams), plus several other containers
   (langfuse-worker, alert-analyser heavy I/O, redis). This is a genuine, evidence-based
   case for a capacity request -- the box is shared and oversubscribed by combined
   team workload, independent of our own agent's efficiency.
2. **38 dangling Docker images + up to 1.7GB build cache** reclaimed via `docker image
   prune -f` and `docker builder prune -f` -- safe, zero-risk (orphaned layers from
   repeated rebuilds). Freed ~1GB+ images, all reclaimable build cache.
3. **No Docker daemon-level log rotation was configured at all** (`daemon.json` only
   set data-root) -- genuine unbounded-growth risk for container stdout/stderr over
   time. Fixed: added `"log-driver": "json-file", "log-opts": {"max-size": "10m",
   "max-file": "3"}` to /etc/docker/daemon.json (backed up first), restarted Docker
   daemon (confirmed all 25 previously-running containers came back -- 4 needed a
   manual `docker start` after the daemon restart: mongodb, radar-agent, rca-backend,
   rca-frontend, since they lack an auto-restart policy -- worth flagging to their
   owners), then `docker compose up -d --force-recreate` for org-dev's own services
   specifically to pick up the new log config (daemon restart alone doesn't retroactively
   apply new defaults to already-existing containers). Validated: new config confirmed
   active via docker inspect, all 14 org-dev containers back up cleanly, kafka-analyser
   job scheduler and dashboard confirmed working normally post-recreate.
4. **CUR-analyser disk cleanup bug found, NOT fixed here** -- handed off to the
   dedicated CUR-analyser session per user's explicit request (avoiding cross-agent
   context pollution). Summary: cleanup_old_report_files(keep_last=3) reads an
   in-memory _reports list that resets to empty on every restart and is never
   repopulated from the database -- so it can only ever see reports added since the
   last restart, permanently losing visibility into older ones. Confirmed live: 8
   parquet_dir folders exist (should be 3), ~4.5GB reclaimable. Fix direction: query
   CurReport via SessionLocal directly instead of the in-memory list (pattern already
   exists in this file's delete_report function). Low urgency -- /data at 37% (39GB
   free), growth ~600MB/sync, not a near-term risk.
5. **Other teams' agents not yet live** (onboarded yesterday except appsupport, a few
   weeks old) -- made the daemon restart low-risk to do now rather than needing
   off-hours coordination.
6. **Automated housekeeping via systemd timer** (this host uses systemd timers, not
   crontab -- crontab isn't even installed). Created docker-housekeeping.service +
   .timer, running twice daily (06:00 and 18:00 UTC): `docker image prune -f` (dangling
   images) + `docker builder prune -f --filter until=24h` (build cache older than 24h,
   leaving same-day cache intact for active rebuild sessions). Enabled and validated
   with a manual test run (both commands exited successfully). Files: /etc/systemd/
   system/docker-housekeeping.service and .timer.
*Added: 2026-08-02*

### Missing retention/purge on 4 large Postgres tables -- DONE (2026-08-04)
Cross-session finding (governance session, 2026-08-02): 5 tables with no retention,
~3.35GB. kafka_topic_message_rate_snapshots already had working rollup+delete logic;
fixed the other 4, one at a time, with retention windows sized against REAL verified UI
usage (not defaulted to a uniform "long" window) -- checked exactly what each table's
consumers actually query before picking a number, per user's explicit ask ("we have our
time filter max available for 30 days, so worth storing 90 days retention?").

- **kafka_topic_message_rate_hourly_rollup**: matches UI's own "Last 30 days" max time
  filter (was going to size for 90 days, corrected down after checking the real UI
  ceiling). Added Step 3 to the existing hourly rollup job. ~4.8GB estimated
  steady-state (2 clusters).
- **kafka_connector_snapshots**: matches SLO tab's own 30-day max filter. Had zero
  cleanup mechanism at all -- added to collect_connector_snapshots, guarded to run
  ~once/hour (job runs every 2 min). ~822MB estimated steady-state.
- **kafka_consumer_group_rate_snapshots**: only ever queried with minutes=60 (popup
  chart) -- used 24h as a generous safety margin, not 30 days. Added to
  collect_consumer_lag_active, guarded to ~once/hour. First run purged 450,127
  genuinely old rows (652K->202K); VACUUM FULL reclaimed 177MB->43MB.
- **kafka_metrics_history**: audited every query against this table -- ALL are
  `ORDER BY collected_at DESC LIMIT 1`, meaning only the single latest row per
  (cluster_id, scan_type) is ever read anywhere. Not a date-window case at all --
  fixed to keep only the latest row, in the shared _insert_metric() write path (covers
  every save_* caller). One-time backlog cleanup: deleted 11,239 stale rows, VACUUM
  FULL reclaimed 891MB->400KB (only 15 rows were ever genuinely needed).

**Total real steady-state across all 4 tables: ~5.7GB** (vs. the original ~3.35GB
already-accumulated figure, which was climbing unbounded) -- properly bounded now, not
just delayed. Every retention window was checked against actual UI/query usage before
being chosen, not assumed.
*Added: 2026-08-02, completed: 2026-08-04*

### Internal Staging Kafka onboarded, DevQA temporarily disabled for capacity relief (2026-08-04)
Onboarded cluster 8 (Internal Staging Kafka, 17 Connect workers, 24,508 topics -- larger
than DevQA). Cleanup: removed unused test clusters 1/2 (verified zero dependent rows
across all tables first). Registered cluster 8, staggered its job schedules against
clusters 3/4 (topic-structure, topic-inflow, consumer-lag offset to unique ticks),
validated each job live. Found and raised broker-health-8/msg-rate-8 timeouts to 300s
to match cluster 4's pattern (same real, measured Prometheus-scrape characteristic, not
a bug). Found + fixed a real bug along the way: trigger_job()'s stale-run check used
the hardcoded registration-time default timeout instead of the real DB-configured one
(_execute_job, the scheduler's own path, was already correct) -- caused manual triggers
to incorrectly clear legitimately-still-running jobs as "stale". Fixed, validated, committed.

Host capacity: confirmed live that with 3 clusters active, host load average reached
12.40-13.51 on 4 cores, and kafka-analyser's own CPU climbed to 70% during concurrent
onboarding validation (previously ~0.1% at idle) -- genuine host-level contention,
compounded by ClickHouse's persistent 226%+ CPU (separate service, Langfuse token/cost
tracking, not ours to fix). Tried a hard Docker CPU limit (cpus: 1.5) as a containment
measure -- caused real failures (topic-structure-8 timed out completely under concurrent
load that succeeded cleanly minutes earlier without the limit) -- REVERTED, this specific
number was too aggressive for our actual 12-worker-pool/3-cluster concurrent workload.

**User's decision**: temporarily disable DevQA (cluster 3, id=3, enabled=false) --
lower priority than External/Internal Staging (dev-only regression cluster, not
production-facing), while a scaling/capacity request is raised with CloudOps separately.
Validated immediately after: kafka-analyser CPU dropped from 70% to 4.6%, cluster 4's
jobs returned to fast/consistent (1-17s), host load average improved (10.47-12.38, down
from 12.40-13.51). Re-enable DevQA once capacity is confirmed available.
Alternative considered but explicitly deferred by user: a dedicated Kafka-analyser box
-- higher cost/maintenance, not preferred unless the shared-host approach proves
insufficient even after scaling.
*Added: 2026-08-04*

### KPI Box scaling approved: t3.xlarge -> m5.2xlarge (2026-08-04)
User got approval to scale the shared host. Recommendation given: m5.2xlarge (8 vCPU,
32GB RAM, general-purpose, fixed performance) over staying in the T-series -- t3.xlarge
is burstable (CPU credits), and measured load was SUSTAINED (10-13 load average on 4
cores for extended periods, not occasional bursts), so a burstable family would either
throttle hard on credit exhaustion or incur unlimited-mode surcharges. General-purpose
(not compute-optimized) chosen because the box runs a genuinely mixed workload
(Postgres, ClickHouse, multiple agent containers) where RAM-to-CPU ratio matters as
much as raw cores.

Confirmed for the user: this is a stop -> change instance type -> start operation, NOT a
rebuild. EBS volumes (root + the separate /data volume Docker uses) persist untouched.
Brief downtime during the type change (not live/zero-downtime). All Docker containers
restart as part of this (same category of event as today's full-host restart for the
log-rotation fix -- already validated that recovery works cleanly, including the 4
containers needing a manual start due to missing restart policies). m5.2xlarge is
x86_64, same architecture as t3.xlarge, so no image rebuilds or compatibility work
needed (unlike Graviton/ARM64, which the user has separately in mind as a longer-term,
cost-saving option but explicitly wants evaluated properly in the dedicated infra
session later, not bundled into this urgent scaling fix).

Recommended ticket timing: low-traffic window; good timing since other teams' agents
aren't live yet.
*Added: 2026-08-04*

### Minor UX: SLO tab briefly shows red "0/3 online" right after a container restart
Found live (2026-08-04) while validating cluster 8 onboarding: SLO tab briefly showed
"0/3 online" in red for brokers that were actually all healthy -- self-corrected without
a manual refresh within a normal poll cycle. Root cause understood, not a defect: any
container restart briefly empties the in-memory cache the Brokers/SLO tabs read from,
until the first post-restart collector cycle completes and repopulates it. Confirmed via
job history: broker-health-8 succeeded from its very first run (04:00), so this wasn't a
missing-data issue -- just the normal brief empty-state window after one of today's many
onboarding-related restarts. Low priority, cosmetic-only, only visible right after a
restart. Possible improvement: show "collecting data..." instead of a red/alarming 0/3
during this specific transient window, distinguishing "genuinely just restarted" from
"actually degraded." To be validated by the team in parallel.
*Added: 2026-08-04*

### Message-rate chart bug: totally empty for any cluster with no rollup data yet -- FIXED
Found live (2026-08-04) while UI-testing cluster 8: message-rate chart (topic-level
in/out) showed completely empty despite 707K real raw rows existing (2+ hours of data).
Root cause: get_topic_message_rate() set cutoff=now whenever a cluster had zero rows in
kafka_topic_message_rate_hourly_rollup (e.g. any brand-new cluster, before the hourly
rollup job has processed it) -- backwards, since it caused recent-window queries to
incorrectly route to the blended raw+rollup path (which then found nothing in either
half: no rollup data by definition, and no raw data "after now" since nothing can be
collected in the future) instead of the simple raw-only path. Fixed: cutoff set to
range_start - 1 day when no rollup exists, correctly triggering the raw-only path for
the full requested range. Validated live: cluster 8 now returns real data; cluster 4
(already working, has rollup history) shows no regression. get_group_message_rate
checked separately -- doesn't blend with rollup at all, unaffected by this bug. This
would have recurred identically for every future newly-onboarded cluster if not fixed
now.
*Added: 2026-08-04*

## Value-Add Ideas (not scoped as concrete backlog items yet — discuss before building)
- **CSV Export for dashboard tables** (Topics, Consumer Groups, Connectors) — quick win,
  unblocked, no dependencies. User's team will use exported data to manually tag
  entries (product, service, etc.) before uploading a completed tag mapping to
  SharePoint. Added 2026-08-01.
- **Product/Service tag mapping display** — read the team's tag mapping (product,
  service, etc.) from a SharePoint location once the tag schema is finalized, and
  correlate it against our existing tables (topics, consumer groups, connectors) to
  show tagged ownership info in the dashboard — directly helps incident correlation
  ("what product/service does this topic belong to"). BLOCKED on: (1) CSV Export
  shipping first (so the team can actually populate tags), (2) the team finalizing
  their tag schema, (3) deciding how we read from SharePoint (API? scheduled pull?
  manual upload to our own storage?) — do not start design until those are settled.
  Added 2026-08-01.
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

### Group-level Message In/Out mini-chart on Consumer Groups / Kafka Connect popups — shipped 2026-08-01
Scope evolved during design from the original request (per-topic charts, reusing the
Topics tab pattern) to a genuinely more correct group-level aggregate, after discussion
clarified that the existing topic-level outflow table aggregates consumption ACROSS ALL
GROUPS reading a topic — not safely attributable to a single group. Built properly
instead: new table kafka_consumer_group_rate_snapshots (migration 0034, accumulating
history, unlike kafka_consumer_group_partition_lag which is upsert-in-place/latest-only).
collect_consumer_lag_active extended to aggregate its already-computed per-partition
inflow_since_last/consumed_since_last BY GROUP (no new Kafka calls). New endpoint
GET /dashboard/consumer-groups/{group_id}/message-rate, mirroring the topic-level one
exactly. Wired into the single shared _showGroupTopicsPopup function, so both Consumer
Groups and Kafka Connect tabs get it automatically with zero duplicated code.
**Validated with exact cross-check**: connect-aosqa_resolution_scheduler_connector showed
group-level inflow=94, matching the exact sum of its 3 non-zero partitions (32+24+38)
from kafka_consumer_group_partition_lag for the same collection cycle — interval and
outflow also verified exact.
**Separately found and fixed during this work**: a genuinely idle partition (lag=0,
inflow=0, outflow=0) was rendering as a blank line in the partition expand view —
indistinguishable from a rendering failure. Now shows explicit "idle (no lag, no
movement)" label.
**Also found (documented, not fixed)**: `_startup_sync()` in main.py runs unconditionally
on every container restart, competing with the job scheduler for broker connections —
likely root cause of "first job after any rebuild is disproportionately slow," separate
from DevQA's known chronically-high baseline broker load. See its own backlog entry.

### Connector-to-group naming assumption — FIXED 2026-08-01, generalizes to any cluster
Root cause: 5 of 290 connectors on cluster 3 use a custom `consumer.override.group.id`
that doesn't follow Kafka Connect's default `connect-{name}` pattern. Investigated
before building anything (user's explicit ask, given upcoming internal staging
onboarding): tested config-based discovery against ALL 290 connectors — 100% success,
zero errors, exactly 5 real mismatches, all cross-validated against genuine lag data
already sitting under their true group names.
**Fix**: `consumer.override.group.id` is already present in the config data
`KafkaConnectCollector.collect()` fetches for `connector_class` — extracting it required
ZERO new HTTP calls. `get_kafka_connect()` now uses each connector's real discovered
group ID when present, falling back to `connect-{name}` only when no override is
configured. This reads Kafka Connect's own source of truth per connector rather than
guessing a naming pattern — correctly generalizes to any cluster's engineering practices,
not just this one's specific `group-` prefix convention.
**Real mistake caught during implementation**: first attempt added the extraction to
`_get_connector_detail()`, which turned out to be DEAD CODE for this live path —
`collect()` actually uses a separate, more efficient bulk `?expand=status&expand=info`
call and never invokes that method. Caught via live validation (override still showed
None), traced to the actual code path, fixed correctly, and the dead-code edit was
reverted rather than left behind.
**Also decided, scope-narrowed by user during design**: hid the Consumer Groups tab's
"State: Consumer/Connector" classification from the UI entirely (column + filter) — not
core to the primary Connector->Group->Topic->Partition tracking feature, confusing to
explain to a wider audience, and not the user's responsibility to police non-standard
connector naming practices across teams. Logic preserved underneath, only display
removed.
Validated on both static and portal dashboards: all 5 connectors now show real lag
(previously null/N/A) with correct discovered group IDs.

### SLO tab connector data made live — FIXED 2026-08-01 (far exceeded original 0.5hr scope)
Started as a small "add as of HH:MM label" cosmetic fix. User correctly rejected that as
insufficient after reporting a PERSISTENT (not occasional) 1-count discrepancy observed
over a week. Full investigation: verified no code bug in either side's query/aggregation
logic (live and DB snapshot agreed exactly when checked simultaneously, multiple times);
verified the connector-snapshots collector job itself is healthy (real timing, real
success, matches manual validation); verified the Kafka Connect tab's own KPI computation
is internally consistent with its rendered table (no frontend display bug there either).
User's own architectural instinct was the right fix, not mine: a "current state" KPI
should never depend on a periodic snapshot when the live call is fast and cheap
(validated: full 290-connector collect() ~3s). Implemented: SLO tab's current-state
connector stats and connector list now call get_kafka_connect() directly — the exact
same live source as the Connect tab — eliminating disagreement by construction rather
than explaining it with a label. Trend chart's historical queries correctly remain
snapshot-based (genuinely need accumulated history). Removed the now-obsolete "Snapshot
as of HH:MM" label. Validated: both tabs now show byte-identical connector counts.

### State: Active/Empty filter (Consumer Groups tab) — removed 2026-08-01
Confirmed non-functional: "Active" was a silent no-op (g.state never held the value
being checked against, so the filter matched everything, same as "All"); "Empty" always
matched zero groups (misleading, looked like a bug). No active collector captures
genuine Kafka lifecycle state for this table (a legacy one existed, now disabled) —
building one was far beyond scope for this item. Removed the non-functional chips,
kept the search input (genuinely functional, unrelated). Validated on both static and
portal dashboards.

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
