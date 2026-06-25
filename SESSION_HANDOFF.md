# Session Handoff — CUR Analyser Large File Support
## Date: 2026-06-25

## What Was Completed Today

### 1. Column Detection Fix (commit 4fbb998)
- Added `line_item_UsageAccountId` to ACCOUNT_COL_CANDIDATES
- Added `line_item_ResourceId` to RESOURCE_COL_CANDIDATES
- DBR format: `LinkedAccountId` correctly detected
- CUR 2.0 format: `line_item_UsageAccountId` correctly detected

### 2. DuckDB Native CSV Reading (commits 35e7c60, step1)
- Replaced `pd.read_csv(file_path)` with DuckDB native view
- `_load_df()` returns `SimpleNamespace(columns=[...])` for file-path mode
- All aggregate queries run against DuckDB view — no pandas materialisation
- csv_text path (legacy reports) unchanged

### 3. OOM Guards (commits 0cb214f, 9100c80, security fix)
- `_is_large_file()` helper — True if file > 200MB
- `/data-sources/inventory/coverage` — DuckDB DISTINCT accounts, no full load
- `/data-sources/enriched-summary` — safe response for large files
- `/data-sources/enriched-rows` — skip stream for large files
- `/data-sources/enriched-values` — DuckDB distinct accounts for filter dropdown
- `/invoke` and `/invoke/stream` — skip cache load for large files, degrade gracefully
- `_quote_ident()` — SQL injection guard for user-supplied column names

### 4. Validated Results
- 1.81GB DBR file (5,141,614 rows) loads in dashboard at 4.7GB RAM
- Total cost: $1,209,040.82 from real AWS billing data
- 24 real AWS accounts detected and matched to inventory (100%)
- Memory stable throughout — no OOM

## Known Issues to Fix Next Session

### Issue 1 — Null service name (Priority: High)
Top service shows $965,318.73 with no name.
DBR rows with empty ProductName grouped together.
Fix: filter ProductName IS NULL → label "Unallocated"
Files: agents/cur-analyser/tools/duckdb_engine.py
Function: get_cost_by_service()

### Issue 2 — Null date entries (Priority: High)
Date range shows "2026-06-01 → None"
Monthly trend shows "None" bucket with $861K cost
Fix: filter UsageStartDate IS NOT NULL in trend queries
Files: agents/cur-analyser/tools/duckdb_engine.py
Function: get_daily_trend(), get_mom_comparison()

### Issue 3 — Account names missing (Priority: High)
By Account tab shows IDs but no names from inventory
Fix: account-level enrichment via DuckDB JOIN
Files: agents/cur-analyser/tools/duckdb_engine.py
Function: get_cost_by_account()

### Issue 4 — Environment/Customer panels empty (Priority: High)
By Environment, Cost Centres & Tags all empty for DBR file
Root cause: no enrichment applied to large file queries
Fix: account-level enrichment via DuckDB JOIN (Step C below)
Files: agents/cur-analyser/tools/duckdb_engine.py
Functions: get_cost_by_environment(), get_cost_by_tag()

### Issue 5 — Environment × Service Category cross-tab $0 (Priority: Medium)
Pre-existing bug — server-side enrichment not applied to cross-tab
Files: agents/cur-analyser/tools/duckdb_engine.py
Function: get_cost_by_service_category()

### Issue 6 — Pandas cache cleanup (Priority: Low)
_df_cache, _get_cached_df, _cache_df still in code but unused
Remove after enrichment is working

## Next Session Plan — Strict Order

### Step A — Read before writing anything
agents/cur-analyser/tools/duckdb_engine.py
  - _load_df()
  - get_cost_by_account()
  - get_cost_by_environment()
  - get_cost_by_service()
  - get_daily_trend()
agents/cur-analyser/tools/inventory_enricher.py
  - enrich_dataframe()
  - _account_lookup structure
agents/cur-analyser/main.py
  - _is_large_file()
  - ds_inventory_coverage()

### Step B — Fix null service and null date (safe, isolated)
No enrichment changes. Only filter null values in queries.
Validate with both DBR file AND rich-matched-cur-2026.csv

### Step C — Account-level enrichment via DuckDB JOIN
Design the join before writing any code:
1. Get distinct LinkedAccountId from DBR via DuckDB
2. Look up inventory _account_lookup for those IDs
3. Build small pandas lookup DataFrame
4. Register as DuckDB table
5. JOIN with cur_data view in each query function
6. Validate all tabs populate with Customer/Environment data

### Step D — Remove pandas cache
Only after Step C validated.

### Step E — Environment × Service Category fix
Investigate and fix cross-tab showing $0.

## Session Discipline Rules
- Read all relevant code before designing
- Design and confirm before writing any code
- One step at a time — validate completely before moving on
- Watch memory on every test with the 1.81GB file
- Validate rich-matched-cur-2026.csv still works after every change
- No back and forth fixes — understand root cause first

## Key Files
agents/cur-analyser/tools/duckdb_engine.py  — main query engine
agents/cur-analyser/tools/inventory_enricher.py — enrichment logic
agents/cur-analyser/main.py — FastAPI endpoints with OOM guards
agents/cur-analyser/tools/dashboard_builder.py — parallel query runner
agents/cur-analyser/report_store.py — get_report_path(), get_report_csv()
agents/cur-analyser/routes_dashboard.py — dashboard route with cache

## Test Commands for Next Session

### Check stack health
docker compose ps
git log --oneline -5

### Validate detection after any duckdb_engine change
docker compose exec -T cur-analyser python3 << 'PYEOF'
import sys, asyncio
sys.path.insert(0, '/app')
from report_store import load_from_db, list_reports, get_report_rows
from tools.duckdb_engine import _detect_account_col, _detect_resource_col
async def main():
    await load_from_db()
    for r in list_reports():
        rows = get_report_rows(r['id'])
        if rows:
            cols = list(rows[0].keys())
            print(f"ID:{r['id']} | {r['filename']}")
            print(f"  account: {_detect_account_col(cols)}")
            print(f"  resource: {_detect_resource_col(cols)}")
asyncio.run(main())
PYEOF

### Test dashboard API for DBR file (report 19)
API_KEY=$(grep BACKEND_API_KEY /data/org-dev/.env | cut -d= -f2 | tr -d '  \n\r')
curl -s "http://localhost:8002/dashboard?report_id=19" \
  -H "X-API-Key: $API_KEY" | python3 -c "
import sys, json
d = json.load(sys.stdin)
s = d.get('stats', {})
print('total_cost:', s.get('total_cost'))
print('row_count:', s.get('row_count'))
print('accounts:', len(s.get('account_breakdown', [])))
print('services:', len(s.get('service_breakdown', [])))
"

### Watch memory during tests
watch -n 1 'free -h | grep Mem'
