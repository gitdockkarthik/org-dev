"""Dashboard builder — aggregates all CUR query results into a single stats dict.

Computes: total_cost, top_service, service_breakdown, daily_trend, region_breakdown.
"""
from __future__ import annotations

import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, ClassVar

from tools.base import ToolExecutor
from tools.duckdb_engine import (
    get_cost_by_account,
    get_cost_by_env_category,
    get_cost_by_env_month,
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
from tools.data_sources.registry import get_registry
from tools.inventory_enricher import build_enricher

logger = logging.getLogger(__name__)

# Thread pool for running the (blocking, DuckDB-backed) query functions
# concurrently. Each query opens and closes its OWN DuckDB connection inside
# _load_df, and the cached DataFrame is shared read-only, so they are safe to
# run in parallel across threads.
_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="dash-query")


# ── Core aggregation ──────────────────────────────────────────────────────────

def _assemble_dashboard(results: list) -> dict:
    """Build the dashboard payload from the sequential runner's query results."""
    if len(results) == 19:
        (summary, service_breakdown, daily_trend, region_breakdown,
         account_breakdown, org_unit_breakdown, environment_breakdown,
         env_month_breakdown, env_category_breakdown,
         service_category_breakdown, tag_product_breakdown, tag_team_breakdown,
         tag_customer_breakdown, tag_costcentre_breakdown, untagged_resources,
         pricing_term_breakdown, mom_comparison, top_resources,
         savings_opportunities) = results
    else:
        (summary, service_breakdown, daily_trend, region_breakdown,
         account_breakdown, org_unit_breakdown, environment_breakdown,
         service_category_breakdown, tag_product_breakdown, tag_team_breakdown,
         tag_customer_breakdown, tag_costcentre_breakdown, untagged_resources,
         pricing_term_breakdown, mom_comparison, top_resources,
         savings_opportunities) = results
        env_month_breakdown = []
        env_category_breakdown = []

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
        "env_month_breakdown": env_month_breakdown,
        "env_category_breakdown": env_category_breakdown,
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


def _run_all_queries(csv_text, file_path, filters, enricher=None) -> list:
    """Run every dashboard query sequentially (in the caller's thread)."""
    return [
        get_total_cost(csv_text, file_path=file_path, filters=filters),
        get_cost_by_service(csv_text, limit=15, file_path=file_path, filters=filters),
        get_daily_trend(csv_text, file_path=file_path, filters=filters),
        get_cost_by_region(csv_text, file_path=file_path, filters=filters),
        get_cost_by_account(csv_text, file_path=file_path, filters=filters, enricher=enricher),
        get_cost_by_org_unit(csv_text, file_path=file_path, filters=filters),
        get_cost_by_environment(csv_text, file_path=file_path, filters=filters, enricher=enricher),
        get_cost_by_env_month(csv_text, file_path=file_path, filters=filters, enricher=enricher),
        get_cost_by_env_category(csv_text, file_path=file_path, filters=filters, enricher=enricher),
        get_cost_by_service_category(csv_text, file_path=file_path, filters=filters),
        get_cost_by_tag(csv_text, "tag_Product", file_path=file_path, filters=filters, enricher=enricher),
        get_cost_by_tag(csv_text, "tag_Team", file_path=file_path, filters=filters, enricher=enricher),
        get_cost_by_tag(csv_text, "tag_Customer", file_path=file_path, filters=filters, enricher=enricher),
        get_cost_by_tag(csv_text, "tag_CostCentre", file_path=file_path, filters=filters, enricher=enricher),
        get_untagged_resources(csv_text, file_path=file_path, filters=filters, enricher=enricher),
        get_cost_by_pricing_term(csv_text, file_path=file_path, filters=filters),
        get_mom_comparison(csv_text, file_path=file_path, filters=filters),
        get_top_resources(csv_text, limit=10, file_path=file_path, filters=filters, enricher=enricher),
        get_savings_opportunities(csv_text, file_path=file_path, filters=filters),
    ]


def compute_dashboard(
    csv_text: str | None = None, *, file_path: str | None = None,
    filters: dict | None = None,
    enricher=None,
) -> dict:
    """Synchronous dashboard build (queries run sequentially).

    Kept for the Anthropic tool (``DashboardBuilderTool``) and any non-async
    caller. The HTTP route uses :func:`compute_dashboard_async` for parallelism.
    """
    return _assemble_dashboard(_run_all_queries(csv_text, file_path, filters, enricher=enricher))


async def compute_dashboard_async(
    csv_text: str | None = None, *, file_path: str | None = None,
    filters: dict | None = None,
    enricher=None,
) -> dict:
    """Parallel dashboard build: run all queries concurrently in a thread pool."""
    loop = asyncio.get_running_loop()

    def run(fn, *args, **kwargs):
        return loop.run_in_executor(_executor, lambda: fn(*args, **kwargs))

    results = list(await asyncio.gather(
        run(get_total_cost, csv_text, file_path=file_path, filters=filters),
        run(get_cost_by_service, csv_text, limit=15, file_path=file_path, filters=filters),
        run(get_daily_trend, csv_text, file_path=file_path, filters=filters),
        run(get_cost_by_region, csv_text, file_path=file_path, filters=filters),
        run(get_cost_by_account, csv_text, file_path=file_path, filters=filters, enricher=enricher),
        run(get_cost_by_org_unit, csv_text, file_path=file_path, filters=filters),
        run(get_cost_by_environment, csv_text, file_path=file_path, filters=filters, enricher=enricher),
        run(get_cost_by_service_category, csv_text, file_path=file_path, filters=filters),
        run(get_cost_by_tag, csv_text, "tag_Product", file_path=file_path, filters=filters, enricher=enricher),
        run(get_cost_by_tag, csv_text, "tag_Team", file_path=file_path, filters=filters, enricher=enricher),
        run(get_cost_by_tag, csv_text, "tag_Customer", file_path=file_path, filters=filters, enricher=enricher),
        run(get_cost_by_tag, csv_text, "tag_CostCentre", file_path=file_path, filters=filters, enricher=enricher),
        run(get_untagged_resources, csv_text, file_path=file_path, filters=filters, enricher=enricher),
        run(get_cost_by_pricing_term, csv_text, file_path=file_path, filters=filters),
        run(get_mom_comparison, csv_text, file_path=file_path, filters=filters),
        run(get_top_resources, csv_text, limit=10, file_path=file_path, filters=filters, enricher=enricher),
        run(get_savings_opportunities, csv_text, file_path=file_path, filters=filters),
    ))
    env_month = await loop.run_in_executor(_executor, lambda: get_cost_by_env_month(csv_text, file_path=file_path, filters=filters, enricher=enricher))
    env_category = await loop.run_in_executor(_executor, lambda: get_cost_by_env_category(csv_text, file_path=file_path, filters=filters, enricher=enricher))
    full_results = list(results)
    full_results.insert(7, env_month)
    full_results.insert(8, env_category)
    return _assemble_dashboard(full_results)


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

    def __init__(self, cache: dict[str, str], report_map: dict[str, int] | None = None) -> None:
        self._cache = cache
        self._report_map = report_map if report_map is not None else {}

    async def execute(self, session_id: str) -> str:  # type: ignore[override]
        csv_text = self._cache.get(session_id)
        if csv_text:
            return json.dumps(compute_dashboard(csv_text), default=str)
        report_id = self._report_map.get(session_id)
        if report_id:
            from report_store import get_report_path
            file_path = get_report_path(report_id)
            if file_path:
                from tools.dashboard_builder import compute_dashboard_async
                enricher = await build_enricher(get_registry())
                result = await compute_dashboard_async(None, file_path=file_path, filters=None, enricher=enricher)
                return json.dumps(result, default=str)
        return json.dumps({"error": "No CUR data loaded for this session."})
