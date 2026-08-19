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

## New Feature — Alert Category Breakdown (heuristic, 2026-08-17)

Added categorize_alert() in tools/dashboard_builder.py - best-effort keyword-based categorization into Data Pipeline/Database/Infrastructure/Application/Uncategorized, checked in that priority order to avoid false matches (e.g. a connector hostname containing "mongo" correctly matches Data Pipeline, not Database). Explicitly NOT authoritative - architected as an interim heuristic until org-wide bracket-format standardization (see INCIDENT_SCHEMA.md) provides real structured fields to categorize from directly, at which point this function can be swapped without redesigning callers. New category_breakdown field added to compute_dashboard_stats() return, mirroring the existing team_breakdown pattern. Verified with live data: sensible distribution across all 5 categories, no single category (including Uncategorized) dominating. UI integration pending - user will share specific requirements before frontend work begins.

## Recurring Issue — Orphaned alert_job_runs rows after container restart (2026-08-17, third occurrence)

Same class of bug manually fixed once already on 2026-08-03 (documented in cur-analyser's handoff docs as a known gap, and separately flagged as alert-analyser backlog item #7 "startup reconciliation for orphaned running job rows"). Recurred today: a container rebuild during active reconciliation work killed an in-flight sync job mid-execution, leaving its alert_job_runs row stuck at status='running' forever (DB-based check in jobs.py's trigger_job(), separate from the in-memory _sync_lock which correctly resets on restart). This silently blocked all sync triggers for ~20 minutes and wasted 493 trigger attempts from an unrelated background backlog-clearing loop before being diagnosed. Manually fixed again (UPDATE alert_job_runs SET status='failed' WHERE id=2776). Permanent fix (auto-mark orphaned running rows as failed on every app startup, since a fresh process start means nothing could genuinely still be running) deferred until after the category breakdown feature ships, per user decision - but this is now a proven, recurring, real gap worth prioritizing soon given how disruptive it was this time.

**Permanent fix shipped (2026-08-17):** added startup reconciliation to main.py's lifespan() - marks any status='running' alert_job_runs rows as 'failed' on every app startup, since a fresh process start means nothing could genuinely still be running. Verified end-to-end: manually inserted a fake orphaned row, confirmed it was correctly cleaned up (status='failed', error_message set) on the next container restart, with zero remaining 'running' rows afterward. This should be the last time this bug needs manual intervention.

---

## CRITICAL BUG 4 — Ticket-creation loop had zero exception handling (fixed 2026-08-18)

**Severity: Critical.** Found via live incident report: a genuine, correctly-classified P2 alert ("Active mq service is down", awo1-sup-alerts02, alert_id 44788bf2-ef7b-4ccf-85a0-ea527b28ff5a-1787023677139, created 2026-08-18T03:27:57Z) was confirmed fetched from OpsGenie by the 03:30 sync cycle and confirmed classified as genuine (verified by re-running classify_alerts() against the real OpsGenie payload), but never created an incident ticket - no error was logged anywhere.

**Root cause:** the per-alert ticket creation/update loop in routes_settings.py had no exception handling at all, unlike reconciliation (already hardened earlier in this session). Any single alert's DB error during that loop would silently abort processing for the REST of that cycle's entire batch, with zero trace in logs - meaning other alerts in the same affected cycle could also have been silently dropped, not just this one.

**Investigation method (for future reference):** confirmed step by step - (1) searched incidents table for the ticket, not found; (2) confirmed sync cycles ran on schedule with no gap; (3) decrypted real OpsGenie credentials from agent_config using ENCRYPTION_KEY + Fernet to query OpsGenie directly (bypassing the earlier known issue where a fresh Python process can't access the live server's decrypted in-memory config); (4) confirmed via direct OpsGenie API query that the alert is genuinely new (not a reopened/reused alert - ruled out the createdAt-based incremental sync gap as the cause); (5) replicated the exact sync query with the exact cutoff timestamp used at 03:30, confirmed OpsGenie's raw response DID include this alert; (6) ran the alert through the real classify_alerts() function, confirmed genuine classification; (7) reviewed the ticket-creation loop code, found no try/except; (8) confirmed via logs that no error/exception/traceback was logged for that sync run, consistent with a silent, unlogged failure.

**Fix:** wrapped the per-alert ticket creation/update logic in try/except, logs alert_id + error on any failure, loop continues to the next alert instead of silently aborting the batch. Matches the same defensive pattern already used in reconciliation (see BUG 3 above).

---

## MAJOR FEATURE — Escalated Incidents tab rebuild + message-based resolution detection (2026-08-19)

Complete rebuild of the Escalated Incidents tab per CTO demo requirements. Summary of what shipped:

**New schema (migrations 0046, 0047):**
- incident_status_history - every status transition logged (creation, auto-resolve, reopen-correction)
- incident_creation_failures - persists ticket-creation failures (was log-only before today)
- incidents.detected_via - tracks whether a resolution was detected via message-text parsing or live OpsGenie check
- incidents.opsgenie_sync_status - reserved placeholder for a future feature (writing agent decisions back to OpsGenie automatically) - not implemented yet, per user decision

**New backend endpoints:** mttx-summary (real MTTD/MTTA/MTTR min/max/avg), open-list, resolved-list (both with search/status/hours filters, per-row MTTD/MTTA/MTTR, opsgenie_status snapshot), failures + retrigger, incident flow-map ({ticket_id}/flow - full status history + stage map with skipped-stage detection), trend (weekly/monthly by priority).

**Critical business fix - message-based resolution detection:** OpsGenie's own status field only updates when a human manually closes it in the UI - confirmed by the user as the actual operational gap the agent should eliminate, not the source of truth to trust. Standardized bracket-tagged messages (Zabbix, New Relic, LightStep) are auto-updated by the source tool itself the instant it detects real resolution, with zero human latency. New parse_message_status() (tools/dashboard_builder.py) checks bracketed tokens against closed/resolved keywords first, open/firing keywords second, case-insensitive, tolerant of inconsistent formatting. Reconciliation checks this FIRST, falls back to live OpsGenie check only when the message gives no clear signal - verified live, first message-parse-detected resolution occurred within one sync cycle of deployment.

**Known gap, not yet fixed:** LightStep's actual format is "[Light step]Resolved Resolved: system filesystem utilization" - the word "Resolved" appears OUTSIDE brackets, not matching our bracket-only parser. Found via live testing 2026-08-19. User is gathering more real inconsistent-format examples from the source-tool owning team to expand parse_message_status() - this is an active, ongoing refinement, not a one-time fix, given the team's confirmed lack of uniform configuration compliance.

**Sync interval reduced 15min -> 2min (2026-08-19):** directly addresses the "OpsGenie Status (at creation)" snapshot staleness - user's reasoning: "5 mins is also too long for an incident management system to detect the issue." Verified job completes in ~6.4s, comfortable headroom under the 2-minute interval. Verified schedule change required an explicit `docker compose restart` (not just `up -d --build`, which no-ops if the image is unchanged) - worth remembering for future schedule changes.

**Frontend:** new renderMttxKpis(), renderPipelineSummary(), renderOpenIncidentsSection(), renderResolvedIncidentsSection(), renderFailuresSection(), renderIncidentsTrendSection(), openFlowModal() - full pagination (20/page) across all three tables, shared fmtDuration() helper (seconds/minutes/hours/days as appropriate, not everything in raw minutes). Old Pipeline Flow/Aging/Resolution Method/Recurrence Signal panels removed per user decision; kanban lanes kept for now, removal decision deferred.

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
| Data Quality Insights panels | Fixed | Found 2 real bugs: (1) "All Time" column was silently showing period data - regression from the 2026-08-10 lifetime-counter fix changing window._allTimeData's shape (was .stats.data_quality, became flat lifetime totals). (2) Acknowledgement Rate panel duplicated Lifecycle Health's Acknowledged/Unacknowledged numbers exactly - backend computed the same data twice under different keys. Decided (per user): historical/lifetime data quality doesn't serve the operational mission (timely escalation/resolution, MTTR reduction) - simplified Lifecycle Health to one "Selected Period" column, replaced duplicate Acknowledgement panel with real "Resolution Time (MTTR by Priority)" panel using new mttr_by_priority calculation in /dashboard/incidents (created_at to resolved_at for verified resolutions only, compared against existing resolution SLA thresholds). Also fixed Resolution Method ring to use resolution_type (new, accurate) instead of resolved_externally (old flag that would have missed all future App Support Agent contributions since their contract doesn't set it) - added rca_assisted as a 4th category. Source Health and Priority Distribution panels verified correct as-is. |

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
