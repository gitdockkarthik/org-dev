"""Dashboard builder — aggregates all CUR query results into a single stats dict.

Computes: total_cost, top_service, service_breakdown, daily_trend, region_breakdown.
"""
from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any, ClassVar

from tools.base import ToolExecutor
from tools.duckdb_engine import (
    get_cost_by_account,
    get_cost_by_environment,
    get_cost_by_org_unit,
    get_cost_by_pricing_term,
    get_cost_by_region,
    get_cost_by_service,
    get_cost_by_service_category,
    get_cost_by_tag,
    get_daily_trend,
    get_mom_comparison,
    get_savings_opportunities,
    get_top_resources,
    get_total_cost,
    get_untagged_resources,
)

# Thread pool for running the (blocking, DuckDB-backed) query functions
# concurrently. Each query opens and closes its OWN DuckDB connection inside
# _load_df, and the cached DataFrame is shared read-only, so they are safe to
# run in parallel across threads.
_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="dash-query")


# ── Core aggregation ──────────────────────────────────────────────────────────

def _assemble_dashboard(results: list) -> dict:
    """Build the dashboard payload from the 17 query results (same order whether
    they were produced sequentially or concurrently)."""
    (summary, service_breakdown, daily_trend, region_breakdown,
     account_breakdown, org_unit_breakdown, environment_breakdown,
     service_category_breakdown, tag_product_breakdown, tag_team_breakdown,
     tag_customer_breakdown, tag_costcentre_breakdown, untagged_resources,
     pricing_term_breakdown, mom_comparison, top_resources,
     savings_opportunities) = results

    top_service = service_breakdown[0] if service_breakdown else None

    # Month-over-month summary derived from daily trend
    monthly: dict[str, float] = {}
    for row in daily_trend:
        month = row["date"][:7]  # YYYY-MM
        monthly[month] = round(monthly.get(month, 0.0) + row["cost"], 4)
    monthly_trend = [{"month": m, "cost": c} for m, c in sorted(monthly.items())]

    return {
        "total_cost": summary.get("total_cost", 0),
        "row_count": summary.get("row_count", 0),
        "top_service": top_service,
        "service_breakdown": service_breakdown,
        "daily_trend": daily_trend,
        "monthly_trend": monthly_trend,
        "region_breakdown": region_breakdown,
        "account_breakdown": account_breakdown,
        "org_unit_breakdown": org_unit_breakdown,
        "environment_breakdown": environment_breakdown,
        "service_category_breakdown": service_category_breakdown,
        "tag_product_breakdown": tag_product_breakdown,
        "tag_team_breakdown": tag_team_breakdown,
        "tag_customer_breakdown": tag_customer_breakdown,
        "tag_costcentre_breakdown": tag_costcentre_breakdown,
        "untagged_resources": untagged_resources,
        "pricing_term_breakdown": pricing_term_breakdown,
        "mom_comparison": mom_comparison,
        "top_resources": top_resources,
        "savings_opportunities": savings_opportunities,
    }


def _run_all_queries(csv_text, file_path, filters) -> list:
    """Run every dashboard query sequentially (in the caller's thread)."""
    return [
        get_total_cost(csv_text, file_path=file_path, filters=filters),
        get_cost_by_service(csv_text, limit=15, file_path=file_path, filters=filters),
        get_daily_trend(csv_text, file_path=file_path, filters=filters),
        get_cost_by_region(csv_text, file_path=file_path, filters=filters),
        get_cost_by_account(csv_text, file_path=file_path, filters=filters),
        get_cost_by_org_unit(csv_text, file_path=file_path, filters=filters),
        get_cost_by_environment(csv_text, file_path=file_path, filters=filters),
        get_cost_by_service_category(csv_text, file_path=file_path, filters=filters),
        get_cost_by_tag(csv_text, "tag_Product", file_path=file_path, filters=filters),
        get_cost_by_tag(csv_text, "tag_Team", file_path=file_path, filters=filters),
        get_cost_by_tag(csv_text, "tag_Customer", file_path=file_path, filters=filters),
        get_cost_by_tag(csv_text, "tag_CostCentre", file_path=file_path, filters=filters),
        get_untagged_resources(csv_text, file_path=file_path, filters=filters),
        get_cost_by_pricing_term(csv_text, file_path=file_path, filters=filters),
        get_mom_comparison(csv_text, file_path=file_path, filters=filters),
        get_top_resources(csv_text, limit=10, file_path=file_path, filters=filters),
        get_savings_opportunities(csv_text, file_path=file_path, filters=filters),
    ]


def compute_dashboard(
    csv_text: str | None = None, *, file_path: str | None = None,
    filters: dict | None = None,
) -> dict:
    """Synchronous dashboard build (queries run sequentially).

    Kept for the Anthropic tool (``DashboardBuilderTool``) and any non-async
    caller. The HTTP route uses :func:`compute_dashboard_async` for parallelism.

    The source is either ``csv_text`` (legacy in-memory pipeline) or
    ``file_path`` (file-path pipeline for large CUR files). ``filters`` is
    forwarded to every query so the whole dashboard is computed over the
    filtered subset server-side.
    """
    return _assemble_dashboard(_run_all_queries(csv_text, file_path, filters))


async def compute_dashboard_async(
    csv_text: str | None = None, *, file_path: str | None = None,
    filters: dict | None = None,
) -> dict:
    """Parallel dashboard build: run all queries concurrently in a thread pool.

    Identical output to :func:`compute_dashboard`. Each query opens its own
    DuckDB connection (see ``_load_df``) and the cached DataFrame is shared
    read-only, so concurrent execution is safe. This collapses ~17 × ~700ms
    sequential queries into a few concurrent waves.
    """
    loop = asyncio.get_running_loop()

    def run(fn, *args, **kwargs):
        return loop.run_in_executor(_executor, lambda: fn(*args, **kwargs))

    results = await asyncio.gather(
        run(get_total_cost, csv_text, file_path=file_path, filters=filters),
        run(get_cost_by_service, csv_text, limit=15, file_path=file_path, filters=filters),
        run(get_daily_trend, csv_text, file_path=file_path, filters=filters),
        run(get_cost_by_region, csv_text, file_path=file_path, filters=filters),
        run(get_cost_by_account, csv_text, file_path=file_path, filters=filters),
        run(get_cost_by_org_unit, csv_text, file_path=file_path, filters=filters),
        run(get_cost_by_environment, csv_text, file_path=file_path, filters=filters),
        run(get_cost_by_service_category, csv_text, file_path=file_path, filters=filters),
        run(get_cost_by_tag, csv_text, "tag_Product", file_path=file_path, filters=filters),
        run(get_cost_by_tag, csv_text, "tag_Team", file_path=file_path, filters=filters),
        run(get_cost_by_tag, csv_text, "tag_Customer", file_path=file_path, filters=filters),
        run(get_cost_by_tag, csv_text, "tag_CostCentre", file_path=file_path, filters=filters),
        run(get_untagged_resources, csv_text, file_path=file_path, filters=filters),
        run(get_cost_by_pricing_term, csv_text, file_path=file_path, filters=filters),
        run(get_mom_comparison, csv_text, file_path=file_path, filters=filters),
        run(get_top_resources, csv_text, limit=10, file_path=file_path, filters=filters),
        run(get_savings_opportunities, csv_text, file_path=file_path, filters=filters),
    )
    return _assemble_dashboard(list(results))


# ── ToolExecutor wrapper ──────────────────────────────────────────────────────

class DashboardBuilderTool(ToolExecutor):
    """Compute a full CUR dashboard: total cost, service breakdown, trends, and regions."""

    name: ClassVar[str] = "build_dashboard"
    description: ClassVar[str] = (
        "Compute a full AWS cost dashboard from cached CUR data. Returns total_cost, "
        "top_service, service_breakdown (up to 15 services), daily_trend, monthly_trend, "
        "and region_breakdown. Call this for an overview or before answering summary questions."
    )
    input_schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {
            "session_id": {
                "type": "string",
                "description": "Session whose cached CUR data to analyse.",
            }
        },
        "required": ["session_id"],
    }

    def __init__(self, cache: dict[str, str]) -> None:
        self._cache = cache

    async def execute(self, session_id: str) -> str:  # type: ignore[override]
        csv_text = self._cache.get(session_id)
        if not csv_text:
            return json.dumps({"error": "No CUR data loaded for this session."})
        return json.dumps(compute_dashboard(csv_text), default=str)
