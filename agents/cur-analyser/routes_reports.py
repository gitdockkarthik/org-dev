"""CUR Analyser — reports routes: generate sample, upload, list."""
from __future__ import annotations

import csv
import io
import random
from datetime import date, timedelta

from fastapi import APIRouter, HTTPException, UploadFile

from report_store import add_report, list_reports, get_report_rows, persist_report
from tools.duckdb_engine import get_total_cost

router = APIRouter(prefix="/reports", tags=["reports"])

_SERVICES = [
    "Amazon EC2",
    "Amazon RDS",
    "Amazon S3",
    "AWS Lambda",
    "Amazon CloudFront",
    "Amazon DynamoDB",
    "AWS Glue",
    "Amazon Redshift",
    "Amazon EKS",
    "Amazon SQS",
]
_REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
_COLUMNS = [
    "line_item_product_code",
    "line_item_unblended_cost",
    "line_item_usage_start_date",
    "product_region",
]

_COST_WEIGHTS = {
    "Amazon EC2": 8.0,
    "Amazon RDS": 4.0,
    "Amazon Redshift": 3.5,
    "Amazon EKS": 3.0,
    "Amazon S3": 1.5,
    "Amazon CloudFront": 1.2,
    "Amazon DynamoDB": 1.0,
    "AWS Glue": 0.8,
    "AWS Lambda": 0.4,
    "Amazon SQS": 0.2,
}


def _generate_csv(n: int = 300) -> str:
    start_date = date(2024, 1, 1)
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(_COLUMNS)
    for _ in range(n):
        service = random.choice(_SERVICES)
        weight = _COST_WEIGHTS.get(service, 1.0)
        cost = round(random.uniform(0.01, weight * 50), 6)
        day_offset = random.randint(0, 89)  # Jan–Mar 2024
        usage_date = (start_date + timedelta(days=day_offset)).isoformat()
        region = random.choice(_REGIONS)
        writer.writerow([service, cost, usage_date, region])
    return out.getvalue()


@router.post("/generate-sample")
async def generate_sample() -> dict:
    csv_text = _generate_csv(300)
    summary = get_total_cost(csv_text)
    file_size = len(csv_text.encode())
    report = add_report(
        filename="sample-cur-export.csv",
        csv_text=csv_text,
        row_count=summary.get("row_count", 300),
        total_cost=summary.get("total_cost", 0.0),
        file_size=file_size,
    )
    await persist_report(report["id"])
    return report


@router.post("/upload")
async def upload_report(file: UploadFile) -> dict:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")
    raw = await file.read()
    if len(raw) > 50 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File too large (max 50 MB)")
    csv_text = raw.decode("utf-8-sig", errors="replace")
    summary = get_total_cost(csv_text)
    if "error" in summary:
        raise HTTPException(status_code=422, detail=summary["error"])
    report = add_report(
        filename=file.filename,
        csv_text=csv_text,
        row_count=summary.get("row_count", 0),
        total_cost=summary.get("total_cost", 0.0),
        file_size=len(raw),
    )
    await persist_report(report["id"])
    return report


@router.get("")
async def get_reports() -> list[dict]:
    return list_reports()


@router.get("/{report_id}/data")
async def get_report_data(report_id: int) -> list[dict]:
    """Return the parsed CSV rows for a specific report."""
    rows = get_report_rows(report_id)
    if rows is None:
        raise HTTPException(status_code=404, detail=f"Report {report_id} not found")
    return rows
