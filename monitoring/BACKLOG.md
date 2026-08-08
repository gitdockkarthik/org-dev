# Monitoring Stack Backlog

## Pending

### Postgres memory -- further optimization pass
Session close (2026-08-08): after tonight's fixes (kafka_topic_metrics VACUUM FULL
185MB->15MB, kafka_topic_message_rate_snapshots retention 6h->2h + VACUUM FULL,
per-role resource governance for all roles sharing this instance -- see git log
for full detail), Postgres container memory settled at 2.5-3GB, down from 85%+ of
its 4GB limit. Confirmed stable, not still climbing.

Tomorrow, after completing the Kafka audit: revisit whether further optimization
is worthwhile at current data volume (e.g., reviewing kafka_connector_snapshots'
3.36M rows for its own bounded retention, currently unbounded) -- not urgent
given tonight's fixes hold, but worth a proper look while already in this area.

*Added: 2026-08-08*

### Custom tabbed page wiring all dashboards into a single, in-place view
User's actual expectation, clarified after building the current home dashboard: not
page-navigation links (Grafana's own dashboard-links feature, which reloads the page
for each switch), but a true tabbed experience where switching between dashboards
happens in-place, no reload -- similar to how the Kafka agent's own dashboard
(portal/agents/kafka-analyser/dashboard.html) has custom-built tabs.

Grafana has no native feature for this (dashboard links are page-navigation only).
Would require a custom, standalone HTML/JS page (not a Grafana dashboard itself)
embedding each Grafana dashboard via iframe, with real tab-switching logic --
similar effort/pattern to the Kafka agent's own dashboard, but for Grafana content
instead of our own API. Deliberately scoped as separate, future work rather than
attempted as a quick addition to the current Grafana-native home dashboard, which
is kept as the interim navigation solution.

*Added: 2026-08-08*

### Container Restart Count panel -- removed, needs a dedicated exporter to do properly
Removed from UAP Platform - Resource Overview (2026-08-08) after live investigation
found no reliable way to build this with currently-deployed tools.

What was tried and why each failed:
1. `changes(container_last_seen[24h])` (original) -- fundamentally broken, not just
   mislabeled. container_last_seen updates on every 15s scrape, so this counted
   every scrape as a "restart," producing values in the thousands.
2. `count by (name) (count by (name, id) (container_last_seen)) - 1` -- catches a
   restart only if a sample point happens to land in the brief window where both
   the old and new container's series coexist before the old one goes stale.
   Confirmed unreliable live: worked once by luck, then showed 0/blank for
   several genuine, known Grafana restarts.
3. `changes(container_start_time_seconds[24h:])` -- confirmed via direct
   comparison against `docker inspect`: this metric tracks container *creation*
   time, not process restart time. A `docker compose restart` (same container,
   process restarts) does not change it at all -- confirmed exactly matching the
   container's `Created` timestamp, not its most recent start.
4. Checked whether cadvisor exposes Docker's own `RestartCount` field (the
   correct, meaningful signal -- only increments on automatic, policy-driven
   restarts from crashes/OOM-kills, not manual maintenance restarts) -- confirmed
   cadvisor does not expose this metric at all (no restart-related metric of any
   kind in its output).

Real fix requires a small, dedicated exporter that polls the Docker API directly
(via the Docker socket, mounted read-only) for each container's `RestartCount`
and exposes it as a Prometheus gauge. This is new, custom code (no standard,
off-the-shelf exporter does this), estimated 30-60+ minutes of focused work to
build and validate properly -- deliberately deferred rather than rushed.

Until fixed: genuine crashes/OOM-kills remain indirectly visible via the
Per-Container Memory/CPU panels (a sudden drop-to-zero-then-recovery pattern),
just without a dedicated, precise counter.

*Added: 2026-08-08*
