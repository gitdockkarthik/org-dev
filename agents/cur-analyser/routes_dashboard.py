"""CUR Analyser — dashboard route."""
from __future__ import annotations
from fastapi import APIRouter, Query
from report_store import get_latest_csv, get_latest_meta, get_report_csv
from tools.dashboard_builder import compute_dashboard

router = APIRouter(tags=["dashboard"])

@router.get("/dashboard")
async def get_dashboard(report_id: int = Query(default=None)) -> dict:
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
    return dashboard
