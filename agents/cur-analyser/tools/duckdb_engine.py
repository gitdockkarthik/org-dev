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


def _detect_cost_col(columns: list[str]) -> str | None:
    """Prefer line_item_unblended_cost; fall back to any cost column."""
    for col in columns:
        if "unblended" in col.lower() and "cost" in col.lower():
            return col
    for col in columns:
        if "cost" in col.lower():
            return col
    return None


def _detect_service_col(columns: list[str]) -> str | None:
    """Prefer line_item_product_code; accept productname / servicename / service."""
    for col in columns:
        cl = col.lower()
        if cl == "line_item_product_code":
            return col
    for col in columns:
        cl = col.lower()
        if any(k in cl for k in ("productname", "product_code", "servicename")) or cl == "service":
            return col
    return None


def _detect_date_col(columns: list[str]) -> str | None:
    """Detect usage start date column — supports both
    slash format (lineItem/UsageStartDate) and
    underscore format (line_item_usage_start_date)."""
    # Priority 1 — exact underscore match
    for col in columns:
        if col.lower() == "line_item_usage_start_date":
            return col
    # Priority 2 — exact slash match (standard CUR export)
    for col in columns:
        if col.lower() == "lineitem/usagestartdate":
            return col
    # Priority 3 — partial match on usagestart
    for col in columns:
        if "usagestart" in col.lower():
            return col
    # Priority 4 — any usage date
    for col in columns:
        if "usagedate" in col.lower():
            return col
    return None


def _detect_region_col(columns: list[str]) -> str | None:
    for col in columns:
        if "region" in col.lower():
            return col
    return None


def _detect_account_col(columns: list[str]) -> str | None:
    for col in columns:
        if "account" in col.lower():
            return col
    return None


# ── Core query functions (migrated from engine.py) ────────────────────────────

def _load_df(csv_text: str) -> tuple[pd.DataFrame, duckdb.DuckDBPyConnection]:
    con = duckdb.connect(database=":memory:")
    df = pd.read_csv(io.StringIO(csv_text))
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
        if not cost_col:
            return []
        if "line_item_usage_account_id" in cols:
            acct_col = "line_item_usage_account_id"
        elif "bill_payer_account_id" in cols:
            acct_col = "bill_payer_account_id"
        else:
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
        if not cost_col or "tag_Environment" not in cols:
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
            f'SELECT "tag_Environment", SUM("{cost_col}") AS cost{select_extra} '
            f'FROM cur_data GROUP BY "tag_Environment" ORDER BY cost DESC{group_extra}'
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
        if not cost_col or tag_col not in cols:
            return []
        total = float(con.execute(f'SELECT SUM("{cost_col}") FROM cur_data').fetchone()[0] or 0)
        rows = con.execute(
            f'SELECT "{tag_col}", SUM("{cost_col}") AS cost '
            f'FROM cur_data GROUP BY "{tag_col}" ORDER BY cost DESC'
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
        tag_cols = [
            "tag_Product", "tag_Environment", "tag_Team",
            "tag_Customer", "tag_Owner", "tag_CostCentre",
        ]
        coverage = []
        for tag in tag_cols:
            if tag not in cols:
                continue
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
        if not cost_col or "resource_id" not in cols:
            return []
        svc_col = _detect_service_col(cols)
        region_col = _detect_region_col(cols)
        svc_sel = f'MAX("{svc_col}")' if svc_col else "''"
        region_sel = f'MAX("{region_col}")' if region_col else "''"
        env_sel = 'MAX("tag_Environment")' if "tag_Environment" in cols else "''"
        team_sel = 'MAX("tag_Team")' if "tag_Team" in cols else "''"
        rows = con.execute(
            f'SELECT "resource_id", {svc_sel}, {region_sel}, {env_sel}, {team_sel}, '
            f'SUM("{cost_col}") AS cost '
            f'FROM cur_data '
            f"WHERE \"resource_id\" IS NOT NULL AND CAST(\"resource_id\" AS VARCHAR) <> '' "
            f'GROUP BY "resource_id" ORDER BY cost DESC LIMIT {limit}'
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
        if "tag_Owner" in cols:
            untagged_cost = float(con.execute(
                f'SELECT SUM("{cost_col}") FROM cur_data '
                f"WHERE \"tag_Owner\" IS NULL OR CAST(\"tag_Owner\" AS VARCHAR) = ''"
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
