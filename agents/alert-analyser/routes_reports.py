import json
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, UploadFile, File

from report_store import add_report, list_reports, get_report_classified
from tools.noise_detector import classify_alerts
from tools.sample_generator import generate_synthetic_alerts

router = APIRouter(prefix="/reports", tags=["reports"])


@router.post("/generate-sample")
async def generate_sample() -> dict:
    """Generate 200 synthetic OpsGenie alerts, classify them, and store in-memory."""
    alerts = generate_synthetic_alerts(200)
    classified = classify_alerts(alerts)
    filename = f"synthetic-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}.json"
    return add_report(filename, alerts, classified)


@router.post("/upload")
async def upload_alerts(file: UploadFile = File(...)) -> dict:
    """Accept a JSON file of OpsGenie alerts, classify, and store in-memory."""
    content = (await file.read()).decode("utf-8")
    try:
        alerts = json.loads(content)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid JSON: {exc}")
    if not isinstance(alerts, list):
        raise HTTPException(status_code=422, detail="Expected a JSON array of alert objects")
    classified = classify_alerts(alerts)
    return add_report(file.filename or "upload.json", alerts, classified)


@router.get("")
async def list_reports_endpoint() -> list[dict]:
    """List all stored reports (most recent first), without raw alert data."""
    return list_reports()


@router.get("/{report_id}/data")
async def get_report_data(report_id: int) -> list[dict]:
    """Return the full classified alert list for a specific report."""
    classified = get_report_classified(report_id)
    if classified is None:
        raise HTTPException(status_code=404, detail=f"Report {report_id} not found")
    return classified
