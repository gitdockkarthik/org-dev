"""CUR Analyser — dashboard route."""
from __future__ import annotations
import time
from fastapi import APIRouter, Query
from report_store import (
    get_latest_csv,
    get_latest_meta,
    get_latest_path,
    get_report_csv,
    get_report_path,
)
from tools.dashboard_builder import compute_dashboard_async
from tools.data_sources.registry import get_registry
from tools.inventory_enricher import build_enricher

router = APIRouter(tags=["dashboard"])

_dashboard_cache: dict = {}
_DASHBOARD_CACHE_TTL_SECS = 600

_tab_cache: dict = {}  # keyed by (report_id, tab_name)

def _get_cached_tab(report_id, tab: str):
    entry = _tab_cache.get((report_id, tab))
    if entry is None:
        return None
    data, cached_at = entry
    if (time.time() - cached_at) > _DASHBOARD_CACHE_TTL_SECS:
        del _tab_cache[(report_id, tab)]
        return None
    return data

def _set_cached_tab(report_id, tab: str, data: dict):
    _tab_cache[(report_id, tab)] = (data, time.time())

def invalidate_tab_cache(report_id: int):
    for key in list(_tab_cache.keys()):
        if key[0] == report_id:
            del _tab_cache[key]


def _get_cached_dashboard(report_id: int):
    entry = _dashboard_cache.get(report_id)
    if entry is None:
        return None
    data, cached_at = entry
    if (time.time() - cached_at) > _DASHBOARD_CACHE_TTL_SECS:
        del _dashboard_cache[report_id]
        return None
    return data


def _set_cached_dashboard(report_id: int, data: dict):
    _dashboard_cache[report_id] = (data, time.time())


def invalidate_dashboard_cache(report_id: int):
    _dashboard_cache.pop(report_id, None)
    invalidate_tab_cache(report_id)


def _csv_list(value: str | None) -> list[str]:
    """Parse a comma-separated query param into a list of non-empty values."""
    if not value:
        return []
    return [v for v in (s.strip() for s in value.split(",")) if v]


async def _resolve_report(report_id: int | None):
    """Resolve file_path and csv_text for a given report_id (or latest if None).
    Returns (file_path, csv_text, enricher) or raises HTTPException."""
    from fastapi import HTTPException
    if report_id is not None:
        file_path = get_report_path(report_id)
        if file_path:
            csv_text = None
        else:
            csv_text = get_report_csv(report_id)
            if csv_text is None:
                raise HTTPException(status_code=404, detail=f"Report {report_id} not found")
    else:
        file_path = get_latest_path()
        if file_path:
            csv_text = None
        else:
            csv_text = get_latest_csv()
            if csv_text is None:
                return None, None, None
    reg = get_registry()
    enricher = await build_enricher(reg)
    return file_path, csv_text, enricher


@router.get("/dashboard")
async def get_dashboard(
    report_id: int = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    accounts: str | None = Query(default=None),
    environments: str | None = Query(default=None),
    services: str | None = Query(default=None),
    regions: str | None = Query(default=None),
    pricing_terms: str | None = Query(default=None),
    tag_products: str | None = Query(default=None),
    tag_teams: str | None = Query(default=None),
) -> dict:
    # Build the server-side filter set from the optional query params.
    filters: dict = {}
    if date_from:
        filters["date_from"] = date_from
    if date_to:
        filters["date_to"] = date_to
    for key, raw in (
        ("accounts", accounts), ("environments", environments),
        ("services", services), ("regions", regions),
        ("pricing_terms", pricing_terms), ("tag_products", tag_products),
        ("tag_teams", tag_teams),
    ):
        vals = _csv_list(raw)
        if vals:
            filters[key] = vals

    # The cache holds the *unfiltered* dashboard per report; never serve or store
    # a filtered result against that key.
    if not filters:
        cached = _get_cached_dashboard(report_id)
        if cached:
            return cached

    # Prefer the on-disk file (file-path pipeline — avoids materialising a
    # multi-GB CSV string); fall back to stored csv_text for legacy reports.
    if report_id is not None:
        file_path = get_report_path(report_id)
        if file_path:
            csv_text = None
        else:
            csv_text = get_report_csv(report_id)
            if csv_text is None:
                return {"empty": True, "reason": f"Report {report_id} not found"}
    else:
        file_path = get_latest_path()
        if file_path:
            csv_text = None
        else:
            csv_text = get_latest_csv()
            if csv_text is None:
                return {"empty": True}

    import logging as _logging
    _log = _logging.getLogger(__name__)
    _reg = get_registry()
    enricher = await build_enricher(_reg)
    dashboard = await compute_dashboard_async(csv_text, file_path=file_path, filters=filters or None, enricher=enricher)
    report = get_latest_meta() if report_id is None else None
    dashboard["report"] = report
    if not filters:
        _set_cached_dashboard(report_id, dashboard)
    return dashboard


@router.get("/dashboard/tab/overview")
async def get_tab_overview(report_id: int = Query(default=None)) -> dict:
    cached = _get_cached_tab(report_id, "overview")
    if cached:
        return cached
    file_path, csv_text, enricher = await _resolve_report(report_id)
    if file_path is None and csv_text is None:
        return {"empty": True}
    from tools.duckdb_engine import (
        get_total_cost, get_cost_by_service, get_daily_trend, get_savings_opportunities
    )
    import asyncio
    loop = asyncio.get_running_loop()
    from tools.dashboard_builder import _executor
    def run(fn, *args, **kwargs):
        return loop.run_in_executor(_executor, lambda: fn(*args, **kwargs))
    summary, service_breakdown, daily_trend, savings_opportunities = await asyncio.gather(
        run(get_total_cost, csv_text, file_path=file_path),
        run(get_cost_by_service, csv_text, limit=15, file_path=file_path),
        run(get_daily_trend, csv_text, file_path=file_path),
        run(get_savings_opportunities, csv_text, file_path=file_path),
    )
    monthly: dict = {}
    for row in daily_trend:
        month = row["date"][:7]
        monthly[month] = round(monthly.get(month, 0.0) + row["cost"], 4)
    monthly_trend = [{"month": m, "cost": c} for m, c in sorted(monthly.items())]
    result = {
        "total_cost": summary.get("total_cost", 0),
        "row_count": summary.get("row_count", 0),
        "top_service": service_breakdown[0] if service_breakdown else None,
        "service_breakdown": service_breakdown,
        "daily_trend": daily_trend,
        "monthly_trend": monthly_trend,
        "savings_opportunities": savings_opportunities,
    }
    _set_cached_tab(report_id, "overview", result)
    return result


@router.get("/dashboard/tab/accounts")
async def get_tab_accounts(report_id: int = Query(default=None)) -> dict:
    cached = _get_cached_tab(report_id, "accounts")
    if cached:
        return cached
    file_path, csv_text, enricher = await _resolve_report(report_id)
    if file_path is None and csv_text is None:
        return {"empty": True}
    from tools.duckdb_engine import get_cost_by_account, get_cost_by_org_unit
    import asyncio
    loop = asyncio.get_running_loop()
    from tools.dashboard_builder import _executor
    def run(fn, *args, **kwargs):
        return loop.run_in_executor(_executor, lambda: fn(*args, **kwargs))
    account_breakdown, org_unit_breakdown = await asyncio.gather(
        run(get_cost_by_account, csv_text, file_path=file_path, enricher=enricher),
        run(get_cost_by_org_unit, csv_text, file_path=file_path),
    )
    result = {
        "account_breakdown": account_breakdown,
        "org_unit_breakdown": org_unit_breakdown,
    }
    _set_cached_tab(report_id, "accounts", result)
    return result


@router.get("/dashboard/tab/environments")
async def get_tab_environments(report_id: int = Query(default=None)) -> dict:
    cached = _get_cached_tab(report_id, "environments")
    if cached:
        return cached
    file_path, csv_text, enricher = await _resolve_report(report_id)
    if file_path is None and csv_text is None:
        return {"empty": True}
    from tools.duckdb_engine import get_cost_by_environment, get_cost_by_env_month, get_cost_by_env_category
    from tools.dashboard_builder import _executor
    import asyncio
    loop = asyncio.get_running_loop()
    def run(fn, *args, **kwargs):
        return loop.run_in_executor(_executor, lambda: fn(*args, **kwargs))
    environment_breakdown, env_month_breakdown, env_category_breakdown = await asyncio.gather(
        run(get_cost_by_environment, csv_text, file_path=file_path, enricher=enricher),
        run(get_cost_by_env_month, csv_text, file_path=file_path, enricher=enricher),
        run(get_cost_by_env_category, csv_text, file_path=file_path, enricher=enricher),
    )
    result = {
        "environment_breakdown": environment_breakdown,
        "env_month_breakdown": env_month_breakdown,
        "env_category_breakdown": env_category_breakdown,
    }
    _set_cached_tab(report_id, "environments", result)
    return result


@router.get("/dashboard/tab/services")
async def get_tab_services(report_id: int = Query(default=None)) -> dict:
    cached = _get_cached_tab(report_id, "services")
    if cached:
        return cached
    file_path, csv_text, enricher = await _resolve_report(report_id)
    if file_path is None and csv_text is None:
        return {"empty": True}
    from tools.duckdb_engine import get_cost_by_service_category, get_top_resources
    from tools.dashboard_builder import _executor
    import asyncio
    loop = asyncio.get_running_loop()
    def run(fn, *args, **kwargs):
        return loop.run_in_executor(_executor, lambda: fn(*args, **kwargs))
    service_category_breakdown, top_resources = await asyncio.gather(
        run(get_cost_by_service_category, csv_text, file_path=file_path),
        run(get_top_resources, csv_text, limit=10, file_path=file_path, enricher=enricher),
    )
    result = {
        "service_category_breakdown": service_category_breakdown,
        "top_resources": top_resources,
    }
    _set_cached_tab(report_id, "services", result)
    return result


@router.get("/dashboard/tab/tags")
async def get_tab_tags(report_id: int = Query(default=None)) -> dict:
    cached = _get_cached_tab(report_id, "tags")
    if cached:
        return cached
    file_path, csv_text, enricher = await _resolve_report(report_id)
    if file_path is None and csv_text is None:
        return {"empty": True}
    from tools.duckdb_engine import get_cost_by_tag, get_untagged_resources
    from tools.dashboard_builder import _executor
    import asyncio
    loop = asyncio.get_running_loop()
    def run(fn, *args, **kwargs):
        return loop.run_in_executor(_executor, lambda: fn(*args, **kwargs))
    tag_product, tag_team, tag_customer, tag_costcentre, tag_team_native, untagged_resources = await asyncio.gather(
        run(get_cost_by_tag, csv_text, "tag_Product", file_path=file_path, enricher=enricher),
        run(get_cost_by_tag, csv_text, "tag_Team", file_path=file_path, enricher=enricher),
        run(get_cost_by_tag, csv_text, "tag_Customer", file_path=file_path, enricher=enricher),
        run(get_cost_by_tag, csv_text, "tag_CostCentre", file_path=file_path, enricher=enricher),
        run(get_cost_by_tag, csv_text, "tag_Team", file_path=file_path, enricher=enricher, native_only=True),
        run(get_untagged_resources, csv_text, file_path=file_path, enricher=enricher),
    )
    result = {
        "tag_product_breakdown": tag_product,
        "tag_team_breakdown": tag_team,
        "tag_team_native_breakdown": tag_team_native,
        "tag_customer_breakdown": tag_customer,
        "tag_costcentre_breakdown": tag_costcentre,
        "untagged_resources": untagged_resources,
    }
    _set_cached_tab(report_id, "tags", result)
    return result


@router.get("/dashboard/tab/trends")
async def get_tab_trends(report_id: int = Query(default=None)) -> dict:
    cached = _get_cached_tab(report_id, "trends")
    if cached:
        return cached
    file_path, csv_text, enricher = await _resolve_report(report_id)
    if file_path is None and csv_text is None:
        return {"empty": True}
    from tools.duckdb_engine import get_cost_by_pricing_term, get_mom_comparison, get_cost_by_region
    from tools.dashboard_builder import _executor
    import asyncio
    loop = asyncio.get_running_loop()
    def run(fn, *args, **kwargs):
        return loop.run_in_executor(_executor, lambda: fn(*args, **kwargs))
    pricing_term_breakdown, mom_comparison, region_breakdown = await asyncio.gather(
        run(get_cost_by_pricing_term, csv_text, file_path=file_path),
        run(get_mom_comparison, csv_text, file_path=file_path),
        run(get_cost_by_region, csv_text, file_path=file_path),
    )
    result = {
        "pricing_term_breakdown": pricing_term_breakdown,
        "mom_comparison": mom_comparison,
        "region_breakdown": region_breakdown,
    }
    _set_cached_tab(report_id, "trends", result)
    return result
