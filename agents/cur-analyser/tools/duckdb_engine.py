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
