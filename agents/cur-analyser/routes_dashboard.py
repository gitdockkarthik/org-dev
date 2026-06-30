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


def _csv_list(value: str | None) -> list[str]:
    """Parse a comma-separated query param into a list of non-empty values."""
    if not value:
        return []
    return [v for v in (s.strip() for s in value.split(",")) if v]


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
