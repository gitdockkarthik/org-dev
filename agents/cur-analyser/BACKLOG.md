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
