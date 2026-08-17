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

---

## Tab-by-Tab Audit

### Overview Tab
| Item | Status | Notes |
|---|---|---|
| Total Alerts / Noise / Suspect / Genuine KPI cards (all-time) | Confirmed OK | Fixed 2026-08-10 (lifetime counter work) |
| Alert Volume by Day chart | Bug Found | Sourced from single-report stats.daily_trend, bounded to ~4hr window - shows single dot regardless of date filter. Needs migration to alert_report_summary deltas |
| Genuine vs Noise donut | Not Audited | Appears to be current-snapshot by design - verify this is the intended scope |
| Data Quality Insights panels (Lifecycle Health, Source Health, Priority Distribution, Acknowledgement Rate) | Not Audited | |

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
