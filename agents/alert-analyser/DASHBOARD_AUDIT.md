# Alert Analyser — Full Dashboard Audit

Systematic, per-tab, per-chart audit of every metric/chart/table on the live dashboard. Goal: verify each one's data source is correct, time-scoped sensibly, actually populated (not silently dead), and clearly labeled — following the same discipline that made the Kafka Analyser audit successful.

## Methodology
For each chart/table/KPI, verify:
1. **Data source** — which query/field feeds it
2. **Time correctness** — snapshot vs. delta; does it use the right column for its purpose (avoiding the alert_report_summary snapshot-discontinuity bug found 2026-08-17)
3. **Actually populated** — confirm the underlying field isn't silently always-zero/empty (like the p1_count-p5_count bug found 2026-08-17)
4. **Labeling clarity** — does the title/subtitle honestly describe what's shown and its time scope
5. **Edge cases** — empty state, zero data, single-data-point handling

## Status Legend
Not Audited | In Progress | Confirmed OK | Bug Found | Fixed

---

## Known Issues Found So Far (2026-08-17)

### BUG 1 — alert_report_summary snapshot discontinuity
`total_alerts`, `genuine_count`, `noise_count`, `suspect_count`, `noise_pct` columns reflect the CURRENT bounded-window snapshot size at each sync, not a stable incremental measure. Before the 2026-08-03 bounded-window fix, these grew unbounded (up to 129,922); after the fix, they dropped to ~400-500. Any trend chart built directly on these columns shows a false discontinuity at the fix boundary (e.g. noise_pct jumped 43.61% -> 49.88% at the exact fix moment, with no real behavior change). `new_alerts`/`new_genuine`/`new_noise`/`new_suspect` (delta columns) are NOT affected and are the correct source for all historical trend analysis.

### BUG 2 — p1_count through p5_count always zero
report_store.py reads `stats.get("priority_counts", {})` when building the alert_report_summary insert, but compute_dashboard_stats() (tools/dashboard_builder.py) never returns a key called `priority_counts` — the real data lives under `data_quality.priority.distribution`. Result: `.get()` always silently returns `{}`, so p1_count-p5_count have been zero in every row since this table's inception. The live dashboard's own Priority Distribution panel is unaffected (reads from the correct live `data_quality` path), but no historical priority trend has ever been possible from this table.

### CRITICAL BUG 3 — False auto-resolution of incidents (fixed 2026-08-17)

**Severity: Critical.** Reconciliation logic compared open tickets against `deduped_alerts`, itself derived from the 4-hour bounded classification window (2026-08-03 fix). Any ticket whose triggering alert aged past that window was auto-resolved regardless of real OpsGenie state - the pipeline's core promise (accurate incident tracking) was broken for any incident open longer than ~4 hours.

**Real-world impact, measured before fix:** 51,541 tickets had been auto-resolved under this logic. A live audit against real OpsGenie (see resolution-audit endpoint below) found only 3,290 (6.4%) were genuinely closed - 48,251 (93.6%) were still actually open in production, incorrectly showing as RESOLVED on the dashboard.

**Fix (routes_settings.py, migration 0044):**
- Replaced snapshot comparison with live per-ticket OpsGenie status checks via new `AlertSource.get_alert_status()` method (added to JSMSource, StandaloneOpsgenieSource, FileSource in tools/source.py)
- Bounded, fair rotation: 100 oldest-unchecked tickets per sync cycle, 10 concurrent lookups, fail-safe on lookup failure (leaves ticket untouched rather than guessing)
- New `last_reconciliation_check_at` and `resolution_type` columns track verification state per ticket
- This also sets up the future capability to have the pipeline close OpsGenie alerts automatically once RCA/Action agents can apply real fixes

**Historical correction (migration 0045, resolution-audit endpoints):**
- New GET /dashboard/incidents/resolution-audit (read-only, samples unverified resolutions, reports live accuracy) and POST /dashboard/incidents/resolution-audit/correct (batched, deterministic FIFO correction with dry_run support)
- Ran full correction across all 51,541 unverified tickets (512 batches): 48,251 reopened to ESCALATED with `reopen_reason='false_positive_reconciliation'`, 3,290 verified correct and marked `resolution_type='self_healed'`
- Final verified state: 48,614 genuinely open ESCALATED tickets, 3,290 verified-correct RESOLVED tickets

**Separate finding during this work:** discovered `RCA_INELIGIBLE` (72 tickets) and `RCA_COMPLETED` (23 tickets) statuses in the table, from an early RCA agent implementation that called Claude directly for investigation rather than building real intelligence into the agent - user has already directed this agent be rejected and rebuilt from scratch. These statuses predate this session's work and are unaffected by the reconciliation fix (which only touches RESOLVED tickets). INCIDENT_SCHEMA.md needs updating to reflect these statuses exist in historical data, or note the RCA agent is being rebuilt.

---

## Tab-by-Tab Audit

**Overview tab: fully audited and fixed, 2026-08-17.**

### Overview Tab
| Item | Status | Notes |
|---|---|---|
| Total Alerts / Noise / Suspect / Genuine KPI cards (all-time) | Confirmed OK | Fixed 2026-08-10 (lifetime counter work), raw/dedup split verified |
| Alert Volume trend chart | Fixed | New /dashboard/history endpoint aggregates delta columns (immune to snapshot discontinuity), auto-granularity by span. Decoupled from top date filter - always shows trailing 30-day view in its own "Historical Trend" section, since a trend chart's purpose doesn't match a narrow KPI snapshot window |
| Alert Breakdown donut | Fixed | Was bound to lifetime totals (sAllTime) despite sitting in the period-filtered section - now correctly bound to period-filtered stats (s). Also expanded from 2 categories (Genuine/Noise) to 3 (Genuine/Noise/Suspect) - was silently hiding suspect count |
| Default date range | Fixed | Was 1 hour (caused near-empty views on every chart) - changed to 24 hours |
| Data Quality Insights panels (Lifecycle Health, Source Health, Priority Distribution, Acknowledgement Rate) | Confirmed OK | Verified against live data - all four panels populated correctly with sensible values |

### Noise Analysis Tab
| Item | Status | Notes |
|---|---|---|
| Top Noisy Sources chart | Bug Found | Single-report snapshot (~4hr window) - not true "top sources," needs historical aggregation |
| Noise Score by Service chart | Bug Found | Same issue |
| Repeat Offenders table | Bug Found | Same issue - "repeat" over 4hrs is not meaningful |
| Suppression Recommendations table | Bug Found | Same issue |

### Genuine Alerts Tab
| Item | Status | Notes |
|---|---|---|
| Total Genuine / Open Alerts / P1-P2 KPI cards | Not Audited | |
| Team Alert Breakdown chart | Bug Found | Single-report snapshot, same issue |
| High Severity Genuine table | Not Audited | |
| Unresolved Genuine table | Not Audited | |

### Trends Tab
| Item | Status | Notes |
|---|---|---|
| Noise % Trend Over Time chart | Bug Found | Uses alert_report_summary but plots raw noise_pct column directly with no bucketing (5800 raw points) and inherits BUG 1's discontinuity |
| Alert Volume by Hour heatmap | Bug Found | Single-report snapshot (~4hr window) - not a real hour-of-day pattern |
| Peak Alert Hours chart | Bug Found | Same issue |
| Daily Alert Classification chart | Bug Found | Single-report stats.daily_trend, same as Overview's Alert Volume by Day |
| Hourly Alert Distribution chart | Bug Found | Same issue |

### Escalated Incidents Tab
| Item | Status | Notes |
|---|---|---|
| Pipeline Flow panel | Not Audited | |
| Ticket Aging panel | Not Audited | |
| Resolution Method panel | Not Audited | |
| Recurrence Signal panel | Not Audited | |
| Kanban lanes | Not Audited | |
| Ticket detail modal | Not Audited | |

### AI Insights Tab
| Item | Status | Notes |
|---|---|---|
| Full tab | Not Audited | |

---

## Fix Plan (in priority order)
1. New `/dashboard/history` endpoint - bucketed deltas (hourly/daily/weekly auto-granularity), immune to BUG 1
2. Migrate Alert Volume by Day, Daily Alert Classification, Noise % Trend, Peak Alert Hours/heatmap to new endpoint
3. Fix BUG 2 (priority_counts key mismatch) so future rows populate real P1-P5 deltas
4. Extend alert_report_summary (or new table) with per-source/per-team delta tracking for Top Noisy Sources, Noise Score by Service, Team Breakdown, Repeat Offenders
5. Complete audit of Escalated Incidents and AI Insights tabs
6. Full labeling pass - every chart title states its actual time scope

---
*Operative Intelligence — Incident Response System (Agentic AI). Started 2026-08-17.*
