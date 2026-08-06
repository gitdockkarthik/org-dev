# CUR Analyser — Backlog & Working Notes

Live tracking document for CUR Analyser work. Update after every validated chunk of work, then commit.
Do not rely on chat memory for continuity — this file is the source of truth.

---

## Status Legend
- `Open` — identified, not started
- `In Progress` — actively being worked
- `Done` — implemented and validated

---

## Architecture

### Billing-period retention vs keep_last=3
- **Status:** Open
- **Added:** 2026-08-01
- **Why:** `cleanup_old_report_files(keep_last=3)` retains the last 3 *report syncs* (intra-day drops), not last 3 *billing periods*. Going back to a prior month (e.g. July) currently requires a manual re-sync (~8 min) since historical parquet files are already deleted.
- **Need:** Retention model keyed off `bill_billing_period_start_date`, keeping N distinct billing periods on disk rather than N most recent report IDs.
- **Scope:** Dedicated session — review `report_store.py` / `cleanup_old_report_files` / S3 sync cadence, freeze retention design, implement, validate against the 10GB disk cap (same constraint that caused the original 97%-full incident — fix must stay disk-aware, not revert to unbounded retention).
- **Open questions:** How many billing periods to retain by default? Should older periods re-sync on-demand vs stay dormant on disk? How does this interact with the existing 10GB cap?
- **Blocks:** Showback Milestone 2+ multi-period/MoM comparison work (deck requires combining daily files across periods).

---

## Bugs

### Inventory coverage endpoint — IOException on parquet_dir glob
- **Status:** Open (low priority)
- **Added:** 2026-08-01
- **Why:** `GET /data-sources/inventory/coverage` throws `duckdb.duckdb.IOException: IO Error: No files found that match the pattern "/app/data/cur/{id}.parquet_dir"` — code treats the parquet_dir as a literal file path instead of globbing `{id}.parquet_dir/*.parquet`.
- **Found during:** Showback Phase 1 discovery session, while trying to locate a previously-uploaded inventory file.

---

## Data / Attribution (Showback Phase 1 feed-in)

### CUR 2.0 schema & tag findings — Milestone 1 discovery (complete)
- **Status:** Done
- **Added:** 2026-08-01
- **Findings (apply to CUR 2.0 format generally, not tied to a specific report ID — AWS auto-generates consistently):**
  - Join keys present and format-aligned: `line_item_resource_id`, `line_item_usage_account_id` (verified against inventory `Instance ID`/`Account` on EC2; other resource types not yet individually verified)
  - `resource_tags` / `tags` (JSON blobs) carry rich resource-level tags, but `user_customer` conflates Product / end-Customer-Tenant / internal-shared-bucket concepts — not usable as a direct Product-axis source
  - Inventory's `Application` field is the best Product-axis candidate found so far (AOS, O1, OnAir, OnTarget, Connect, Medea, Shared all appear) — cleaner than CUR's `user_customer`, but casing inconsistent (O1/o1) and IBMS missing from both datasets
  - No clean Team-axis field found in either CUR or inventory — `user_business` (CUR) only populated on 0.14% of rows; inventory `Role`/`ManagedBy` are 67-90% null and describe infra roles, not org teams. Account-map expected to be the primary Team source, not a fallback.
  - Tag coverage: 82.1% of rows tagged, but only 55.1% of cost sits on tagged rows (cost-weighted coverage is the number to track against the deck's 80% target, not row-count)
  - 17 distinct AWS accounts identified in inventory; several account names hint at Product ownership (OPOnTarget, OP-AOS-Integrations, OP-LinearProducts-Internal)
- **Reference doc:** `Showback_Phase1_Milestone1_StatusReview.docx` (shared with Amrithanshu for review, Monday follow-up pending)
- **Open questions for CloudOps/Finance:** Is Linear a tracked product (shows ~$33K real spend, absent from product list)? Is IBMS still active? Cost metric choice (blended/unblended/net-unblended)? Approve/revise 80% tag-coverage target?

### Milestone 2 — not started
- **Status:** Open
- **Depends on:** Monday review outcome with Amrithanshu
- **Planned:** Verify resource_id format match across remaining resource types (EBS, S3, RDS, Lambda), draft `account_map_v1.json` using the 17-account seed list with `Application` as primary Product signal, circulate for sign-off before building attribution logic.

---

## Operational Notes
- **Always confirm current latest report_id before querying** — CUR syncs ~3x/day; `keep_last=3` only retains 3 most recent report directories on disk. Never assume a previously-used report_id still resolves.
- Report 65 (used during Milestone 1 discovery) is expected to be cleaned up now that later reports (72+) exist — this is expected behavior, not a bug.

---

## Tracker (manual, human-readable)

| # | Item | Est. Effort | Risk/Notes | Priority Order | Plan | Estimate | Status |
|---|---|---|---|---|---|---|---|
| 1 | Milestone 1 — CUR + inventory schema discovery | Ad hoc | Discovery only, no code — confirmed CUR 2.0 join keys, tag structure, Product/Team gaps | 1 | 01/08/26 | — | Done |
| 2 | Showback gap-analysis doc (v1, formal) | 1 hr | Full technical writeup for CloudOps | 2 | 01/08/26 | 1 | Done |
| 3 | Showback status-review doc (v2, casual/visual) | 45 min | Status-matrix format, revised per feedback | 3 | 01/08/26 | 0.75 | Done |
| 4 | Parquet cleanup fix (keep_last=3) | — | Already applied prior to this thread — logged for context only | 4 | 01/08/26 | — | Done |
| 5 | CUR BACKLOG.md — process setup | 15 min | Live tracking doc in repo, committed | 5 | 01/08/26 | 0.25 | Done |
| 6 | Inventory coverage endpoint IOException fix | 20–30 min | Low priority — glob pattern bug on `.parquet_dir` | 6 | TBD | 0.5 | Open |
| 7 | Billing-period retention architecture (vs keep_last=3) | 3–4 hrs | Needs dedicated session — freeze design before implementing; blocks multi-period Showback work | 7 | TBD | 4 | Open |
| 8 | Resource_id format verification (EBS, S3, RDS, Lambda) | 1–1.5 hrs | Milestone 2 precursor — EC2 already confirmed | 8 | TBD | 1.5 | Blocked (Monday review) |
| 9 | account_map_v1.json draft | 1.5–2 hrs | Uses 17-account seed list, Application field as primary Product signal | 9 | TBD | 2 | Blocked (Monday review) |
| 10 | Attribution engine — Layer 1/2 (Product + Team) | TBD | Depends on #8, #9, and CloudOps sign-off on open questions (Linear, IBMS, cost metric) | 10 | TBD | TBD | Open |

Update this table after each work chunk — status changes, new rows appended, estimates refined once actuals are known.

### RESOLVED — Missing synced CUR reports, restored via forced re-sync
- **Status:** Done
- **Added/Resolved:** 2026-08-01
- **What happened:** `/reports` showed only the stale manual upload (report 6, 2026-07-21) — all previously synced auto reports (7 through 72 from earlier sessions) were gone from both disk and the `cur_report` DB table.
- **Root cause of the "stuck" state:** `s3_last_synced_at` in `agent_config` (05:14:43) matched the latest S3 folder's timestamp, so `_run_s3_sync`'s unchanged-file check (`files_unchanged and count_unchanged`) short-circuited every sync attempt with "Already up to date," even though no valid report actually existed. This check only compares S3 file timestamps/counts — it has no awareness of whether a corresponding report record still exists.
- **Fix applied:** Manually reset `s3_last_synced_at` to an old value directly in `agent_config` via psql, then restarted the `cur-analyser` container (required because `_config` is an in-memory write-through cache populated from DB only at startup — updating the DB row alone did not affect the already-running process). Re-triggered sync afterward; it correctly detected the change and re-ingested successfully (new report id=7, 5,764,986 rows, persisted to DB correctly, confirmed via direct psql check immediately after).
- **Old stale manual report (id=6) removed** per request — only the live auto-synced report remains.
- **Unresolved / lower priority follow-up:** the original mechanism by which reports 7-72 (from prior sessions) disappeared from the DB in the first place is not fully root-caused. `persist_report()` behaved correctly in this session's live test, so it may have been a one-off (e.g. a restart during an earlier session before this DB row existed) rather than a recurring bug — not chasing further unless it recurs.
- **Process gap worth fixing:** `_run_s3_sync`'s "unchanged" check should ideally also verify a valid report record still exists (not just compare S3 timestamps) before skipping — otherwise this exact stuck state can recur any time a report is lost/deleted between sync cycles without S3 data itself having changed. Candidate for a small follow-up fix.

### Additional evidence — billing periods accumulating, not separated (2026-08-01)
- **Observed:** Dashboard "Viewing report" shows date range 2026-07-01 → 2026-09-01 (33 days,
  5,764,986 rows) — spanning a month boundary in a single report, meaning August data is being
  merged into the same "latest" report rather than split into its own period.
- **S3 Browser confirms the gap:** Historical Data browser only lists "July 2026 (31 days)" and
  "June 2026 (2 days)" as distinct periods — no separate August 2026 entry yet, consistent with
  new data being accumulated into the existing latest report instead of starting a fresh
  period-scoped one.
- **Confirms:** this is the same root design gap as the billing-period retention item above — the
  sync/report model is "always latest, single blob," with no concept of period boundaries at all.
  Not just a retention/cleanup problem — the accumulation itself needs to be period-aware before
  retention rules can even be meaningful.
- **Fold into:** Item 1 (Billing-period retention redesign) — the fix needs to (a) detect and split
  by billing period on ingest, not just (b) decide how many periods to keep afterward.

### CRITICAL — cleanup_old_report_files() loses visibility into old reports after every restart
- **Status:** Open — scheduled for Monday (priority, ahead of mid-week foundation session)
- **Found:** 2026-08-02, during a Kafka-analyser session, while investigating host disk usage on the
  shared t3.xlarge box.
- **Symptom:** 8 `parquet_dir` folders on disk (IDs 6-13, ~614MB each, ~4.5GB total) when only 3
  should exist per `keep_last=3`. Active report is ID 13.
- **Root cause:** `cleanup_old_report_files(keep_last=3)` in `report_store.py` computes which reports
  to delete by reading the in-memory `_reports` list — but that list starts empty on every container
  restart, and nothing repopulates it from the database on startup. Only `add_report()` appends to it
  going forward. So after any restart, cleanup can only "see" reports added since that restart — it
  silently and permanently loses visibility into older reports, and their files are never cleaned up.
- **Distinct from the Aug 1 fix:** that fix correctly wired the cleanup *call* into the sync path
  (`main.py`). This bug is in the cleanup *function's own data source* — a deeper, separate issue.
- **Fix direction:** Make `cleanup_old_report_files()` async; have it query `CurReport` via
  `SessionLocal` directly for the real, persistent list of report IDs (ordered by `id desc`, filtered
  by `agent_slug`) instead of reading `_reports`. DB-query pattern to copy from already exists in this
  same file, in `delete_report()`. All 3 call sites in `main.py` (~lines 550, 725, 1617) need `await`
  added once the function becomes async.
- **Urgency:** Low technically (`/data` at 37%, 39GB free, ~600MB growth per sync, few times/day —
  not a near-term disk risk) — but prioritizing for Monday anyway to close the gap before it compounds
  further and to avoid a repeat of the Aug 1 stuck-sync incident pattern.

### RESOLVED — cleanup_old_report_files() restart-visibility bug (#11)
- **Status:** Done
- **Resolved:** 2026-08-02 (Sunday, ahead of Monday target)
- **Fix applied:** `cleanup_old_report_files()` converted to async, now queries `CurReport` via
  `SessionLocal` directly (filtered by `agent_slug`, ordered by `id.desc()`) instead of reading the
  in-memory `_reports` list — same pattern as `delete_report()`. Returns 0 if `SessionLocal` is None
  or on query failure (logged via `logger.exception`). 3 call sites in `main.py` updated to `await`
  (`_process_upload_job()` line ~551, `_process_folder_upload_job()` line ~726, `_run_s3_sync()`
  line ~1618).
- **Validated:** Direct invocation of the fixed function against live DB/disk confirmed it now
  correctly reads report state from the DB (found only report 13, the sole valid row) rather than an
  empty in-memory list.
- **One-time manual cleanup also performed:** 7 orphaned `parquet_dir` folders (IDs 6-12, ~4.5GB)
  existed on disk with **no corresponding DB row at all** — these predate the fix and were never
  going to be swept by `cleanup_old_report_files()` even when working correctly, since the function
  only manages files for reports it can see in the DB beyond `keep_last`. Manually removed via `rm -rf`
  after confirming via `/reports` and direct DB query that report 13 was the only valid report.
  Disk now shows exactly 1 folder, matching the 1 DB row.
- **Confirms going forward:** cleanup will now survive container restarts correctly, since it no
  longer depends on in-memory state that resets on restart.

### Process note — code fix deployed but not committed (caught via cross-session diff)
- **Found:** 2026-08-03, while working in a Kafka-analyser session, via `git diff` scoped to
  `agents/cur-analyser/main.py` and `report_store.py` before starting unrelated work.
- **What happened:** The #11 fix (async `cleanup_old_report_files()`) was applied to the working
  tree earlier and validated live, but the actual code change was never `git commit`ed — only a
  follow-up docs commit (`f86e57b`) went through, updating BACKLOG.md but not the code itself.
  The real fix sat live-but-uncommitted in the working tree until caught.
- **Resolved:** Verified the diff was this thread's own known, validated work (not abandoned or
  superseded), then committed properly (`d7c6546`).
- **Practice going forward:** Before starting work in any session, check `git diff`/`git status`
  scoped to the files that session is about to touch. If uncommitted changes belong to a different
  agent/session, do not act on them — flag them and let the respective session's owner decide
  (finish + commit, or discard) in that session, not from an unrelated one. This mirrors how this
  exact gap was caught safely today.

### RESOLVED — delete_report() fails on directories + depends on stale in-memory state (#12)
- **Status:** Done (code fix applied and committed); real-world validation pending next natural
  S3 sync cycle
- **Found:** 2026-08-04, during real-world validation of the #11 fix — the first genuine S3 sync
  after deployment (replacing report 13 with report 14, following the S3-prefix billing-period fix
  applied the same day) surfaced this immediately.
- **Symptom:** `13.parquet_dir` remained on disk after its DB row was correctly deleted —
  `IsADirectoryError: [Errno 21] Is a directory: '/app/data/cur/13.parquet_dir'` in logs, from
  `delete_report()`'s use of `os.unlink()`.
- **Root cause — two bugs, same function:**
  1. `os.unlink()` only works on files, not directories — `.parquet_dir` reports are always
     directories, so this always failed silently (caught, logged, swallowed).
  2. The function determined the file path to delete, and its `removed` return value, from the
     in-memory `_reports` list — same root problem class as #11. Post-restart, this list may not
     reflect reality.
- **Fix applied:** Convention-based file lookup via `report_file_path()` (same pattern as
  `cleanup_old_report_files()`), `shutil.rmtree()` for directories / `os.remove()` for files,
  wrapped in try/except. Return value now based on actual DB delete result
  (`result.rowcount > 0`), not in-memory state.
- **Manual cleanup performed:** Removed orphaned `13.parquet_dir` directly.
- **Validation status:** Syntax-checked, rebuilt, confirmed no regression (report 14 still serving
  correctly). Full end-to-end validation (confirming a *future* replace-in-place delete correctly
  removes its directory) still pending the next natural S3 sync — expected within the normal
  ~8-hour cadence once August data accumulates further.
- **Relationship to #11:** Same underlying lesson — any code path that decides "does this file/report
  exist" must query the DB or filesystem directly, never trust the in-memory `_reports` list, which
  resets on every restart. Worth treating as a design principle for this file going forward, not just
  two isolated bugs.

### #12 — Real-world validation CONFIRMED (2026-08-05)
- **Status:** Done — fully validated, no caveats remaining
- Observed 7 successful `delete_report: removed /app/data/cur/{id}.parquet_dir` log lines across
  multiple natural sync cycles today (reports 19-25 each correctly cleaned up on replacement).
  Zero `IsADirectoryError` or `delete_report: failed` occurrences since the fix was deployed.
- Disk confirmed clean: exactly 1 folder (currently 26.parquet_dir) matching the 1 active DB report,
  no orphans accumulated across ~12 replace-in-place cycles (14 through 26).
- Both #11 and #12 are now considered fully closed — the "in-memory state is not source of truth"
  lesson has held up under real, repeated production cycles.
