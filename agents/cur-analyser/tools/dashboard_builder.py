"""Dashboard builder — aggregates all CUR query results into a single stats dict.

Computes: total_cost, top_service, service_breakdown, daily_trend, region_breakdown.
"""
import json
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


# ── Core aggregation ──────────────────────────────────────────────────────────

def compute_dashboard(csv_text: str) -> dict:
    """Return a unified dashboard payload from a CUR CSV string."""
    summary = get_total_cost(csv_text)
    service_breakdown = get_cost_by_service(csv_text, limit=15)
    daily_trend = get_daily_trend(csv_text)
    region_breakdown = get_cost_by_region(csv_text)

    top_service = service_breakdown[0] if service_breakdown else None

    # Month-over-month summary derived from daily trend
    monthly: dict[str, float] = {}
    for row in daily_trend:
        month = row["date"][:7]  # YYYY-MM
        monthly[month] = round(monthly.get(month, 0.0) + row["cost"], 4)
    monthly_trend = [{"month": m, "cost": c} for m, c in sorted(monthly.items())]

    account_breakdown          = get_cost_by_account(csv_text)
    org_unit_breakdown         = get_cost_by_org_unit(csv_text)
    environment_breakdown      = get_cost_by_environment(csv_text)
    service_category_breakdown = get_cost_by_service_category(csv_text)
    tag_product_breakdown      = get_cost_by_tag(csv_text, "tag_Product")
    tag_team_breakdown         = get_cost_by_tag(csv_text, "tag_Team")
    tag_customer_breakdown     = get_cost_by_tag(csv_text, "tag_Customer")
    tag_costcentre_breakdown   = get_cost_by_tag(csv_text, "tag_CostCentre")
    untagged_resources         = get_untagged_resources(csv_text)
    pricing_term_breakdown     = get_cost_by_pricing_term(csv_text)
    mom_comparison             = get_mom_comparison(csv_text)
    top_resources              = get_top_resources(csv_text, limit=10)
    savings_opportunities      = get_savings_opportunities(csv_text)

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
