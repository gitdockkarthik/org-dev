"""CUR Analyser — dashboard route."""
from __future__ import annotations
import time
from fastapi import APIRouter, Query
from report_store import get_latest_csv, get_latest_meta, get_report_csv
from tools.dashboard_builder import compute_dashboard

router = APIRouter(tags=["dashboard"])

_dashboard_cache: dict = {}
_DASHBOARD_CACHE_TTL_SECS = 120


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


@router.get("/dashboard")
async def get_dashboard(report_id: int = Query(default=None)) -> dict:
    cached = _get_cached_dashboard(report_id)
    if cached:
        return cached

    if report_id is not None:
        csv_text = get_report_csv(report_id)
        if csv_text is None:
            return {"empty": True, "reason": f"Report {report_id} not found"}
    else:
        csv_text = get_latest_csv()
        if csv_text is None:
            return {"empty": True}

    dashboard = compute_dashboard(csv_text)
    report = get_latest_meta() if report_id is None else None
    dashboard["report"] = report
    _set_cached_dashboard(report_id, dashboard)
    return dashboard
