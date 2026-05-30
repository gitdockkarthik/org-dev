"""CUR Analyser — dashboard route."""
from __future__ import annotations

from fastapi import APIRouter

from report_store import get_latest_csv, get_latest_meta
from tools.duckdb_engine import get_cost_by_service, get_daily_trend, get_total_cost

router = APIRouter(tags=["dashboard"])


@router.get("/dashboard")
async def get_dashboard() -> dict:
    csv_text = get_latest_csv()
    if csv_text is None:
        return {"empty": True}

    summary = get_total_cost(csv_text)
    services = get_cost_by_service(csv_text, limit=10)
    trend = get_daily_trend(csv_text)
    report = get_latest_meta()

    return {
        "summary": summary,
        "services": services,
        "trend": trend,
        "report": report,
    }
