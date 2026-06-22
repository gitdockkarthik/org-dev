"""DuckDB-backed CUR query engine.

All public query functions accept raw CSV text and return plain Python structures.
The CurQueryTool wraps them as an Anthropic-callable ToolExecutor that reads from
the per-session CUR cache populated by main.py.
"""
import io
import json
from typing import Any, ClassVar, Literal

import duckdb
import pandas as pd

from tools.base import ToolExecutor

# ── Column detection helpers ──────────────────────────────────────────────────

QueryType = Literal["total_cost", "cost_by_service", "daily_trend", "cost_by_region"]


# Ordered column-name candidates per logical column. Real AWS CUR 2.0 uses the
# slash format (lineItem/..., product/...); normalised exports use underscores;
# legacy CUR uses bare CamelCase names; synthetic/test data uses bare snake_case.
# Tried in order, case-insensitively.
COST_COL_CANDIDATES = [
    "lineItem/UnblendedCost",   # real CUR 2.0
    "line_item_unblended_cost", # normalised
    "UnBlendedCost",            # legacy CUR
    "BlendedCost",              # fallback
    "unblended_cost",           # synthetic
]
SERVICE_COL_CANDIDATES = [
    "lineItem/ProductCode",     # real CUR 2.0
    "line_item_product_code",   # normalised
    "ProductName",              # legacy CUR
    "product/ProductName",      # real CUR 2.0 product name
    "product_name",             # synthetic
]
DATE_COL_CANDIDATES = [
    "lineItem/UsageStartDate",    # real CUR 2.0
    "line_item_usage_start_date", # normalised
    "UsageStartDate",             # legacy CUR
    "usage_start_date",           # synthetic
]
REGION_COL_CANDIDATES = [
    "product/region",   # real CUR 2.0
    "product_region",   # normalised
    "AvailabilityZone", # legacy CUR fallback (AZ not region but closest)
    "region",           # synthetic
]
ACCOUNT_COL_CANDIDATES = [
    "lineItem/UsageAccountId",      # real AWS CUR (current format)
    "line_item_usage_account_id",   # normalised format
    "LinkedAccountId",              # legacy CUR (per-resource / usage account)
    "account_id",                   # synthetic / test data
    "PayerAccountId",               # legacy CUR payer (master) — lowest priority
]
RESOURCE_COL_CANDIDATES = [
    "lineItem/ResourceId",          # real AWS CUR (current format)
    "line_item_resource_id",        # normalised format
    "ResourceId",                   # legacy CUR (no lineItem/ prefix)
    "resource_id",                  # synthetic / test data
]


def _match_candidate(columns: list[str], candidates: list[str]) -> str | None:
    """Return the actual column matching the first candidate present
    (case-insensitive), or ``None`` when no candidate matches."""
    lower = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def resolve_col(df, candidates: list[str], required: bool = True) -> str | None:
    """Central column resolver. Tries each candidate against ``df``'s columns
    case-insensitively, in order.

    When ``required`` is True and no candidate matches, raises ``ValueError``
    naming what was tried versus the columns that actually exist (to make a
    misconfigured / unexpected CUR format easy to diagnose). When ``required``
    is False, returns ``None`` instead.
    """
    col = _match_candidate(list(df.columns), candidates)
    if col is None and required:
        raise ValueError(
            f"Required column not found. Tried candidates {candidates}; "
            f"available CUR columns: {list(df.columns)}"
        )
    return col


def _detect_cost_col(columns: list[str]) -> str | None:
    """Prefer the unblended-cost candidates; fall back to any cost column."""
    col = _match_candidate(columns, COST_COL_CANDIDATES)
    if col:
        return col
    for c in columns:
        if "unblended" in c.lower() and "cost" in c.lower():
            return c
    for c in columns:
        if "cost" in c.lower():
            return c
    return None


def _detect_service_col(columns: list[str]) -> str | None:
    """Prefer the product/service candidates; accept productname / servicename / service."""
    col = _match_candidate(columns, SERVICE_COL_CANDIDATES)
    if col:
        return col
    for c in columns:
        cl = c.lower()
        if any(k in cl for k in ("productname", "product_code", "servicename")) or cl == "service":
            return c
    return None


def _detect_date_col(columns: list[str]) -> str | None:
    """Detect the usage start-date column across slash / underscore / legacy /
    bare formats, then fall back to any usage-date-like column."""
    col = _match_candidate(columns, DATE_COL_CANDIDATES)
    if col:
        return col
    for c in columns:
        if "usagestart" in c.lower():
            return c
    for c in columns:
        if "usagedate" in c.lower():
            return c
    return None


def _detect_region_col(columns: list[str]) -> str | None:
    """Detect the region column; legacy CUR has no region so AvailabilityZone is
    used as the closest fallback (handled via the candidate list)."""
    col = _match_candidate(columns, REGION_COL_CANDIDATES)
    if col:
        return col
    for c in columns:
        if "region" in c.lower():
            return c
    return None


def _detect_account_col(columns: list[str]) -> str | None:
    """Detect the usage-account-id column across formats. Tries the explicit
    candidates in order, then falls back to any column containing ``account``."""
    col = _match_candidate(columns, ACCOUNT_COL_CANDIDATES)
    if col:
        return col
    for c in columns:
        if "account" in c.lower():
            return c
    return None


def _detect_resource_col(columns: list[str]) -> str | None:
    """Detect the resource-id column across formats by trying the explicit
    candidates in order, case-insensitively."""
    return _match_candidate(columns, RESOURCE_COL_CANDIDATES)


# ── Tag column detection ──────────────────────────────────────────────────────
# Real AWS CUR exposes user tags as ``resourceTags/user:<Name>`` columns, while
# synthetic / normalised data uses a ``tag_<Name>`` prefix.
TAG_COL_PREFIXES = ["tag_", "resourceTags/user:"]

# Display-name aliases so a requested tag resolves across naming variants
# (e.g. synthetic "CostCentre" vs real CUR "CostCenter").
_TAG_DISPLAY_ALIASES = {
    "costcentre": {"costcentre", "costcenter"},
    "costcenter": {"costcentre", "costcenter"},
}


def detect_tag_columns(df) -> dict[str, str]:
    """Returns ``{display_name: actual_column_name}`` for every tag column,
    across both the ``tag_`` and ``resourceTags/user:`` prefixes."""
    found: dict[str, str] = {}
    for col in df.columns:
        for prefix in TAG_COL_PREFIXES:
            if col.startswith(prefix):
                display = col[len(prefix):]  # strip prefix for display
                found[display] = col
    return found


def _resolve_tag_col(df, requested: str) -> str | None:
    """Resolve a requested tag column (e.g. ``tag_Environment``) to the actual
    column present, tolerating prefix and display-name variants (incl. the
    CostCentre/CostCenter alias). Returns ``None`` when the tag is absent."""
    if requested in df.columns:
        return requested  # exact native match (synthetic / normalised)
    display = requested
    for p in TAG_COL_PREFIXES:
        if requested.startswith(p):
            display = requested[len(p):]
            break
    target = display.lower()
    targets = _TAG_DISPLAY_ALIASES.get(target, {target})
    for disp, actual in detect_tag_columns(df).items():
        if disp.lower() in targets:
            return actual
    return None


# ── Core query functions (migrated from engine.py) ────────────────────────────

def _load_df(
    csv_text: str, enricher=None
) -> tuple[pd.DataFrame, duckdb.DuckDBPyConnection]:
    """Load CUR CSV into a DataFrame + DuckDB connection.

    ``enricher`` is optional. When ``None`` (the default, and the path every
    existing caller takes) behaviour is unchanged. When supplied, the inventory
    enricher adds ``inv_*`` virtual columns *before* the frame is registered, so
    downstream queries can group/filter on them. An inactive enricher (no
    inventory loaded) is a safe no-op.
    """
    con = duckdb.connect(database=":memory:")
    df = pd.read_csv(io.StringIO(csv_text))
    if enricher is not None:
        df = enricher.enrich_dataframe(df)
    con.register("cur_data", df)
    return df, con


def get_total_cost(csv_text: str) -> dict:
    df, con = _load_df(csv_text)
    try:
        cost_col = _detect_cost_col(list(df.columns))
        if not cost_col:
            return {"error": "No cost column found in CUR data."}
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        return {
            "total_cost": round(total, 4),
            "row_count": len(df),
            "cost_column": cost_col,
        }
    finally:
        con.close()


def get_cost_by_service(csv_text: str, limit: int = 15) -> list[dict]:
    df, con = _load_df(csv_text)
    try:
        cost_col = _detect_cost_col(list(df.columns))
        svc_col = _detect_service_col(list(df.columns))
        if not cost_col or not svc_col:
            return []
        rows = con.execute(
            f'SELECT "{svc_col}", SUM("{cost_col}") AS cost '
            f'FROM cur_data GROUP BY "{svc_col}" ORDER BY cost DESC LIMIT {limit}'
        ).fetchall()
        return [{"service": r[0], "cost": round(float(r[1] or 0), 4)} for r in rows]
    finally:
        con.close()


def get_daily_trend(csv_text: str) -> list[dict]:
    df, con = _load_df(csv_text)
    try:
        cost_col = _detect_cost_col(list(df.columns))
        date_col = _detect_date_col(list(df.columns))
        if not cost_col or not date_col:
            return []
        rows = con.execute(
            f'SELECT CAST("{date_col}" AS DATE) AS day, SUM("{cost_col}") AS cost '
            f'FROM cur_data GROUP BY day ORDER BY day'
        ).fetchall()
        return [{"date": str(r[0]), "cost": round(float(r[1] or 0), 4)} for r in rows]
    finally:
        con.close()


def get_cost_by_region(csv_text: str) -> list[dict]:
    df, con = _load_df(csv_text)
    try:
        cost_col = _detect_cost_col(list(df.columns))
        region_col = _detect_region_col(list(df.columns))
        if not cost_col or not region_col:
            return []
        rows = con.execute(
            f'SELECT "{region_col}", SUM("{cost_col}") AS cost '
            f'FROM cur_data GROUP BY "{region_col}" ORDER BY cost DESC'
        ).fetchall()
        return [{"region": str(r[0]), "cost": round(float(r[1] or 0), 4)} for r in rows]
    finally:
        con.close()


def get_cost_by_account(csv_text: str) -> list[dict]:
    df, con = _load_df(csv_text)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        acct_col = _detect_account_col(cols)
        if not cost_col or not acct_col:
            return []
        name_col = "line_item_usage_account_name" if "line_item_usage_account_name" in cols else None
        if name_col:
            rows = con.execute(
                f'SELECT "{acct_col}", "{name_col}", SUM("{cost_col}") AS cost, COUNT(*) AS rc '
                f'FROM cur_data GROUP BY "{acct_col}", "{name_col}" ORDER BY cost DESC'
            ).fetchall()
            return [
                {
                    "account_id": str(r[0]),
                    "account_name": str(r[1]) if r[1] is not None else "",
                    "cost": round(float(r[2] or 0), 4),
                    "row_count": int(r[3] or 0),
                }
                for r in rows
            ]
        rows = con.execute(
            f'SELECT "{acct_col}", SUM("{cost_col}") AS cost, COUNT(*) AS rc '
            f'FROM cur_data GROUP BY "{acct_col}" ORDER BY cost DESC'
        ).fetchall()
        return [
            {
                "account_id": str(r[0]),
                "account_name": "",
                "cost": round(float(r[1] or 0), 4),
                "row_count": int(r[2] or 0),
            }
            for r in rows
        ]
    except Exception:
        return []
    finally:
        con.close()


def get_cost_by_org_unit(csv_text: str) -> list[dict]:
    df, con = _load_df(csv_text)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        if not cost_col or "org_unit_name" not in cols:
            return []
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        rows = con.execute(
            f'SELECT "org_unit_name", SUM("{cost_col}") AS cost '
            f'FROM cur_data GROUP BY "org_unit_name" ORDER BY cost DESC'
        ).fetchall()
        return [
            {
                "org_unit": str(r[0]) if r[0] is not None else "",
                "cost": round(float(r[1] or 0), 4),
                "pct_of_total": round(float(r[1] or 0) / total * 100, 2) if total else 0.0,
            }
            for r in rows
        ]
    except Exception:
        return []
    finally:
        con.close()


def get_cost_by_environment(csv_text: str) -> list[dict]:
    df, con = _load_df(csv_text)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        env_col = _resolve_tag_col(df, "tag_Environment")
        if not cost_col or not env_col:
            return []
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        has_team = "env_owner_team" in cols
        has_email = "env_owner_email" in cols
        select_extra = ""
        group_extra = ""
        if has_team:
            select_extra += ', MAX("env_owner_team")'
        if has_email:
            select_extra += ', MAX("env_owner_email")'
        rows = con.execute(
            f'SELECT "{env_col}", SUM("{cost_col}") AS cost{select_extra} '
            f'FROM cur_data GROUP BY "{env_col}" ORDER BY cost DESC{group_extra}'
        ).fetchall()
        result = []
        for r in rows:
            env = r[0]
            env_label = str(env) if env is not None and str(env) != "" else "Untagged"
            idx = 2
            team = ""
            email = ""
            if has_team:
                team = str(r[idx]) if r[idx] is not None else ""
                idx += 1
            if has_email:
                email = str(r[idx]) if r[idx] is not None else ""
                idx += 1
            result.append({
                "environment": env_label,
                "cost": round(float(r[1] or 0), 4),
                "pct_of_total": round(float(r[1] or 0) / total * 100, 2) if total else 0.0,
                "env_owner_team": team,
                "env_owner_email": email,
            })
        return result
    except Exception:
        return []
    finally:
        con.close()


def get_cost_by_service_category(csv_text: str) -> list[dict]:
    df, con = _load_df(csv_text)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        if not cost_col or "service_category" not in cols:
            return []
        has_team = "service_owner_team" in cols
        has_email = "service_owner_email" in cols
        group_cols = ['"service_category"']
        if has_team:
            group_cols.append('"service_owner_team"')
        if has_email:
            group_cols.append('"service_owner_email"')
        select_clause = ", ".join(group_cols)
        group_clause = ", ".join(group_cols)
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        rows = con.execute(
            f'SELECT {select_clause}, SUM("{cost_col}") AS cost '
            f'FROM cur_data GROUP BY {group_clause} ORDER BY cost DESC'
        ).fetchall()
        result = []
        for r in rows:
            idx = 1
            team = ""
            email = ""
            if has_team:
                team = str(r[idx]) if r[idx] is not None else ""
                idx += 1
            if has_email:
                email = str(r[idx]) if r[idx] is not None else ""
                idx += 1
            cost = float(r[idx] or 0)
            result.append({
                "category": str(r[0]) if r[0] is not None else "",
                "owner_team": team,
                "owner_email": email,
                "cost": round(cost, 4),
                "pct_of_total": round(cost / total * 100, 2) if total else 0.0,
            })
        return result
    except Exception:
        return []
    finally:
        con.close()


def get_cost_by_tag(csv_text: str, tag_col: str) -> list[dict]:
    df, con = _load_df(csv_text)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        actual_tag = _resolve_tag_col(df, tag_col)
        if not cost_col or not actual_tag:
            return []
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        rows = con.execute(
            f'SELECT "{actual_tag}", SUM("{cost_col}") AS cost '
            f'FROM cur_data GROUP BY "{actual_tag}" ORDER BY cost DESC'
        ).fetchall()
        result = []
        for r in rows:
            val = r[0]
            label = str(val) if val is not None and str(val) != "" else "Untagged"
            cost = float(r[1] or 0)
            result.append({
                "tag_value": label,
                "cost": round(cost, 4),
                "pct_of_total": round(cost / total * 100, 2) if total else 0.0,
            })
        return result
    except Exception:
        return []
    finally:
        con.close()


def get_untagged_resources(csv_text: str) -> dict:
    df, con = _load_df(csv_text)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        total_rows = len(df)
        # Detect tag columns across tag_ and resourceTags/user: prefixes.
        coverage = []
        for tag in detect_tag_columns(df).values():
            untagged_count = int(con.execute(
                f"SELECT COUNT(*) FROM cur_data "
                f"WHERE \"{tag}\" IS NULL OR CAST(\"{tag}\" AS VARCHAR) = ''"
            ).fetchone()[0] or 0)
            if cost_col:
                untagged_cost = float(con.execute(
                    f'SELECT SUM("{cost_col}") FROM cur_data '
                    f"WHERE \"{tag}\" IS NULL OR CAST(\"{tag}\" AS VARCHAR) = ''"
                ).fetchone()[0] or 0)
            else:
                untagged_cost = 0.0
            coverage_pct = (
                round((total_rows - untagged_count) / total_rows * 100, 1)
                if total_rows else 0.0
            )
            coverage.append({
                "tag": tag,
                "coverage_pct": coverage_pct,
                "untagged_count": untagged_count,
                "untagged_cost": round(untagged_cost, 4),
            })
        overall = (
            round(sum(c["coverage_pct"] for c in coverage) / len(coverage), 1)
            if coverage else 0.0
        )
        return {
            "total_rows": total_rows,
            "tag_coverage": coverage,
            "overall_coverage_pct": overall,
        }
    except Exception:
        return {"total_rows": 0, "tag_coverage": [], "overall_coverage_pct": 0.0}
    finally:
        con.close()


def get_cost_by_pricing_term(csv_text: str) -> list[dict]:
    df, con = _load_df(csv_text)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        if not cost_col or "pricing_term" not in cols:
            return []
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        rows = con.execute(
            f'SELECT "pricing_term", SUM("{cost_col}") AS cost, COUNT(*) AS rc '
            f'FROM cur_data GROUP BY "pricing_term" ORDER BY cost DESC'
        ).fetchall()
        return [
            {
                "pricing_term": str(r[0]) if r[0] is not None else "",
                "cost": round(float(r[1] or 0), 4),
                "pct_of_total": round(float(r[1] or 0) / total * 100, 2) if total else 0.0,
                "row_count": int(r[2] or 0),
            }
            for r in rows
        ]
    except Exception:
        return []
    finally:
        con.close()


def get_mom_comparison(csv_text: str) -> list[dict]:
    df, con = _load_df(csv_text)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        date_col = _detect_date_col(cols)
        svc_col = _detect_service_col(cols)
        if not cost_col or not date_col or not svc_col:
            return []
        rows = con.execute(
            f'SELECT strftime(CAST("{date_col}" AS DATE), \'%Y-%m\') AS month, '
            f'"{svc_col}" AS service, SUM("{cost_col}") AS cost '
            f'FROM cur_data GROUP BY month, service ORDER BY month, cost DESC'
        ).fetchall()
        return [
            {
                "month": str(r[0]),
                "service": str(r[1]) if r[1] is not None else "",
                "cost": round(float(r[2] or 0), 4),
            }
            for r in rows
        ]
    except Exception:
        return []
    finally:
        con.close()


def get_top_resources(csv_text: str, limit: int = 10) -> list[dict]:
    df, con = _load_df(csv_text)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        res_col = _detect_resource_col(cols)
        if not cost_col or not res_col:
            return []
        svc_col = _detect_service_col(cols)
        region_col = _detect_region_col(cols)
        env_col = _resolve_tag_col(df, "tag_Environment")
        team_col = _resolve_tag_col(df, "tag_Team")
        svc_sel = f'MAX("{svc_col}")' if svc_col else "''"
        region_sel = f'MAX("{region_col}")' if region_col else "''"
        env_sel = f'MAX("{env_col}")' if env_col else "''"
        team_sel = f'MAX("{team_col}")' if team_col else "''"
        rows = con.execute(
            f'SELECT "{res_col}", {svc_sel}, {region_sel}, {env_sel}, {team_sel}, '
            f'SUM("{cost_col}") AS cost '
            f'FROM cur_data '
            f"WHERE \"{res_col}\" IS NOT NULL AND CAST(\"{res_col}\" AS VARCHAR) <> '' "
            f'GROUP BY "{res_col}" ORDER BY cost DESC LIMIT {limit}'
        ).fetchall()
        return [
            {
                "resource_id": str(r[0]),
                "service": str(r[1]) if r[1] is not None else "",
                "region": str(r[2]) if r[2] is not None else "",
                "environment": str(r[3]) if r[3] is not None else "",
                "team": str(r[4]) if r[4] is not None else "",
                "cost": round(float(r[5] or 0), 4),
            }
            for r in rows
        ]
    except Exception:
        return []
    finally:
        con.close()


def get_savings_opportunities(csv_text: str) -> dict:
    df, con = _load_df(csv_text)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        if not cost_col:
            return {
                "on_demand_cost": 0.0,
                "on_demand_pct": 0.0,
                "top_on_demand_services": [],
                "untagged_cost": 0.0,
                "untagged_pct": 0.0,
                "savings_signals": [],
            }
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        svc_col = _detect_service_col(cols)

        # a) On-Demand cost
        on_demand_cost = 0.0
        top_on_demand_services: list[dict] = []
        if "pricing_term" in cols:
            on_demand_cost = float(con.execute(
                f'SELECT SUM("{cost_col}") FROM cur_data '
                f"WHERE \"pricing_term\" = 'OnDemand'"
            ).fetchone()[0] or 0)
            # b) Top 5 OnDemand services
            if svc_col:
                rows = con.execute(
                    f'SELECT "{svc_col}", SUM("{cost_col}") AS cost FROM cur_data '
                    f"WHERE \"pricing_term\" = 'OnDemand' "
                    f'GROUP BY "{svc_col}" ORDER BY cost DESC LIMIT 5'
                ).fetchall()
                top_on_demand_services = [
                    {"service": str(r[0]) if r[0] is not None else "",
                     "cost": round(float(r[1] or 0), 4)}
                    for r in rows
                ]
        on_demand_pct = round(on_demand_cost / total * 100, 1) if total else 0.0

        # d) Untagged (no owner) cost
        untagged_cost = 0.0
        owner_col = _resolve_tag_col(df, "tag_Owner")
        if owner_col:
            untagged_cost = float(con.execute(
                f'SELECT SUM("{cost_col}") FROM cur_data '
                f"WHERE \"{owner_col}\" IS NULL OR CAST(\"{owner_col}\" AS VARCHAR) = ''"
            ).fetchone()[0] or 0)
        untagged_pct = round(untagged_cost / total * 100, 1) if total else 0.0

        # c) Single-AZ heuristic — region with no trailing -[a-c] AZ suffix
        single_az_cost = 0.0
        region_col = _detect_region_col(cols)
        if region_col:
            single_az_cost = float(con.execute(
                f'SELECT SUM("{cost_col}") FROM cur_data '
                f"WHERE CAST(\"{region_col}\" AS VARCHAR) NOT SIMILAR TO '%-[abc]'"
            ).fetchone()[0] or 0)

        # Human-readable signals
        signals: list[str] = []
        if on_demand_pct > 0:
            signals.append(
                f"{on_demand_pct:.0f}% of spend is On-Demand — "
                f"consider Reserved Instances or Savings Plans"
            )
        if untagged_cost > 0:
            signals.append(
                f"${untagged_cost:,.0f} spend has no owner tag — "
                f"cost allocation at risk"
            )
        if single_az_cost > 0:
            signals.append(
                f"${single_az_cost:,.0f} spend lacks a clear AZ-level region — "
                f"review for resiliency/placement"
            )

        return {
            "on_demand_cost": round(on_demand_cost, 4),
            "on_demand_pct": on_demand_pct,
            "top_on_demand_services": top_on_demand_services,
            "untagged_cost": round(untagged_cost, 4),
            "untagged_pct": untagged_pct,
            "savings_signals": signals,
        }
    except Exception:
        return {
            "on_demand_cost": 0.0,
            "on_demand_pct": 0.0,
            "top_on_demand_services": [],
            "untagged_cost": 0.0,
            "untagged_pct": 0.0,
            "savings_signals": [],
        }
    finally:
        con.close()


def run_context_query(csv_text: str) -> dict:
    """Return the pre-computed data context Claude uses for cost Q&A.

    Mirrors the original run_natural_language_query() from engine.py.
    """
    summary = get_total_cost(csv_text)
    services = get_cost_by_service(csv_text, limit=10)
    trend = get_daily_trend(csv_text)
    return {
        "summary": summary,
        "top_services": services,
        "daily_trend": trend[-14:],
    }


def get_enrichment_summary(csv_text: str, enricher) -> dict:
    """Compute inventory-enrichment coverage for the CUR data.

    Returns ``{"active": False}`` when no enricher / inventory is in play, so the
    existing flow is never affected. When active, returns match counts/rates,
    cost-weighted coverage, per-service match rates, the top unmatched resources
    by cost (with a reason), and a before/after tag-coverage comparison.
    """
    if enricher is None or not getattr(enricher, "active", False):
        return {"active": False}

    df, con = _load_df(csv_text, enricher=enricher)
    try:
        cols = list(df.columns)
        cost_col = _detect_cost_col(cols)
        account_col = _detect_account_col(cols)
        resource_col = _detect_resource_col(cols)
        if not account_col:
            return {
                "active": True,
                "joinable": False,
                "reason": "CUR data lacks an account id column",
            }
        # No resource column => account-level enrichment fallback. The match
        # rate then reflects account-level coverage, not resource-level.
        enrichment_level = "resource" if resource_col else "account"

        stats = enricher.get_match_stats()
        matched_mask = getattr(enricher, "_last_matched", [])
        reasons = getattr(enricher, "_last_reason", [])

        # Cost-weighted coverage.
        total_spend = 0.0
        matched_spend = 0.0
        if cost_col:
            costs = pd.to_numeric(df[cost_col], errors="coerce").fillna(0.0).tolist()
            total_spend = float(sum(costs))
            for i, c in enumerate(costs):
                if i < len(matched_mask) and matched_mask[i]:
                    matched_spend += float(c)
        spend_rate = round(matched_spend / total_spend * 100, 1) if total_spend else 0.0

        # Top unmatched resources by cost.
        unmatched_top: list[dict] = []
        if cost_col and resource_col:
            svc_col = _detect_service_col(cols)
            tmp = df.copy()
            tmp["__cost"] = pd.to_numeric(tmp[cost_col], errors="coerce").fillna(0.0)
            tmp["__matched"] = (matched_mask + [False] * len(tmp))[: len(tmp)]
            tmp["__reason"] = (reasons + [""] * len(tmp))[: len(tmp)]
            un = tmp[~tmp["__matched"]]
            grp = (
                un.groupby(resource_col)
                .agg(cost=("__cost", "sum"))
                .reset_index()
                .sort_values("cost", ascending=False)
                .head(20)
            )
            reason_by_res = (
                un.groupby(resource_col)["__reason"].first().to_dict()
            )
            svc_by_res = (
                un.groupby(resource_col)[svc_col].first().to_dict() if svc_col else {}
            )
            for _, r in grp.iterrows():
                rid = str(r[resource_col])
                unmatched_top.append({
                    "resource_id": rid,
                    "service": str(svc_by_res.get(rid, "")) if svc_by_res else "",
                    "cost": round(float(r["cost"]), 4),
                    "reason": str(reason_by_res.get(rid, REASON_LABEL_DEFAULT)),
                })

        # Before/after tag coverage (native CUR tag vs inventory-filled).
        total_rows = len(df)
        before_after = {}
        comparisons = [
            ("Customer", "tag_Customer", "inv_customer"),
            ("Application", "tag_Product", "inv_application"),
            ("Budget_Code", "tag_CostCentre", "inv_budget_code"),
        ]
        for label, native_col, inv_col in comparisons:
            before_pct = 0.0
            after_pct = 0.0
            if total_rows:
                native_nonblank = pd.Series([False] * total_rows)
                resolved_native = _resolve_tag_col(df, native_col)
                if resolved_native:
                    native_nonblank = df[resolved_native].astype(str).str.strip().ne("") & df[resolved_native].notna()
                before_pct = round(float(native_nonblank.mean()) * 100, 1)
                inv_nonblank = pd.Series([False] * total_rows)
                if inv_col in df.columns:
                    inv_nonblank = df[inv_col].astype(str).str.strip().ne("")
                after_nonblank = native_nonblank | inv_nonblank
                after_pct = round(float(after_nonblank.mean()) * 100, 1)
            before_after[label] = {
                "before_pct": before_pct,
                "after_pct": after_pct,
                "improvement_pct": round(after_pct - before_pct, 1),
            }

        return {
            "active": True,
            "joinable": True,
            "enrichment_level": enrichment_level,
            "matched_count": stats["matched_count"],
            "unmatched_count": stats["unmatched_count"],
            "match_rate_pct": stats["match_rate_pct"],
            "total_spend": round(total_spend, 4),
            "matched_spend": round(matched_spend, 4),
            "spend_match_rate_pct": spend_rate,
            "per_service": stats["per_service"],
            "unmatched_top": unmatched_top,
            "before_after": before_after,
        }
    except Exception:
        return {"active": True, "joinable": False, "reason": "enrichment summary failed"}
    finally:
        con.close()


# Default unmatched reason label (kept local to avoid importing enricher here).
REASON_LABEL_DEFAULT = "Not in inventory"


# ── ToolExecutor wrapper ──────────────────────────────────────────────────────

class CurQueryTool(ToolExecutor):
    """Run a targeted DuckDB query against cached CUR data."""

    name: ClassVar[str] = "query_cur"
    description: ClassVar[str] = (
        "Run a targeted cost analysis query against the loaded AWS CUR data. "
        "query_type options: "
        "'total_cost' — total spend and row count; "
        "'cost_by_service' — spend breakdown by AWS service; "
        "'daily_trend' — day-by-day cost totals sorted by date; "
        "'cost_by_region' — spend breakdown by AWS region."
    )
    input_schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {
            "session_id": {
                "type": "string",
                "description": "Session whose cached CUR CSV to query.",
            },
            "query_type": {
                "type": "string",
                "enum": ["total_cost", "cost_by_service", "daily_trend", "cost_by_region"],
                "description": "Which analysis to run.",
            },
        },
        "required": ["session_id", "query_type"],
    }

    def __init__(self, cache: dict[str, str]) -> None:
        self._cache = cache  # session_id → raw CSV text

    async def execute(self, session_id: str, query_type: QueryType) -> str:  # type: ignore[override]
        csv_text = self._cache.get(session_id)
        if not csv_text:
            return json.dumps({"error": "No CUR data loaded for this session."})

        if query_type == "total_cost":
            return json.dumps(get_total_cost(csv_text))
        if query_type == "cost_by_service":
            return json.dumps(get_cost_by_service(csv_text))
        if query_type == "daily_trend":
            return json.dumps(get_daily_trend(csv_text))
        if query_type == "cost_by_region":
            return json.dumps(get_cost_by_region(csv_text))
        return json.dumps({"error": f"Unknown query_type '{query_type}'."})
