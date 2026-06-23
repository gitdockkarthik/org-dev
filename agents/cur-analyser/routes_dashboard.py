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
from tools.dashboard_builder import compute_dashboard

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


@router.get("/dashboard")
async def get_dashboard(report_id: int = Query(default=None)) -> dict:
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

    dashboard = compute_dashboard(csv_text, file_path=file_path)
    report = get_latest_meta() if report_id is None else None
    dashboard["report"] = report
    _set_cached_dashboard(report_id, dashboard)
    return dashboard
