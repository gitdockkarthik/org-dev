"""CUR Analyser — reports routes: generate sample, upload, list."""
from __future__ import annotations

import calendar
import csv
import io
import random
import zipfile
from datetime import date, timedelta
from uuid import uuid4

from fastapi import APIRouter, HTTPException, UploadFile
from fastapi import Query as FastAPIQuery

from report_store import add_report, list_reports, get_report_rows, get_report_csv, persist_report
from routes_dashboard import invalidate_dashboard_cache
from tools.duckdb_engine import (
    get_total_cost, get_cost_by_service, get_daily_trend,
    get_cost_by_region, get_cost_by_account, get_cost_by_environment,
    get_cost_by_service_category, get_cost_by_tag,
    get_untagged_resources, get_cost_by_pricing_term,
    get_mom_comparison, get_savings_opportunities
)

router = APIRouter(prefix="/reports", tags=["reports"])

_ACCOUNTS = {
    "111122223333": "prod-core",
    "222233334444": "prod-tenants",
    "333344445555": "nonprod",
    "444455556666": "data-platform",
    "555566667777": "security",
}

_ORG_UNITS = {
    "111122223333": "Production",
    "222233334444": "Production",
    "333344445555": "NonProduction",
    "444455556666": "DataPlatform",
    "555566667777": "Security",
}

_ENVIRONMENTS = ["prod", "staging", "nonprod", "dev"]

_ENV_OWNERS = {
    "prod":    {"team": "AppSupport",    "owner": "app-support@operative.com"},
    "staging": {"team": "AppSupport",    "owner": "app-support@operative.com"},
    "nonprod": {"team": "Engineering",   "owner": "engineering@operative.com"},
    "dev":     {"team": "Engineering",   "owner": "engineering@operative.com"},
}

_SERVICE_CATEGORIES = {
    "Amazon EC2":                  "Compute",
    "Amazon ECS":                  "Compute",
    "AWS Lambda":                  "Compute",
    "Amazon RDS":                  "Database",
    "Amazon DynamoDB":             "Database",
    "Amazon ElastiCache":          "Database",
    "Amazon S3":                   "Storage",
    "Amazon ECR":                  "Storage",
    "Amazon CloudFront":           "Network",
    "Amazon Route 53":             "Network",
    "AWS WAF":                     "Security",
    "AWS Secrets Manager":         "Security",
    "Amazon CloudWatch":           "Observability",
    "Amazon Kinesis":              "Streaming",
    "Amazon OpenSearch Service":   "Analytics",
}

_SERVICE_OWNERS = {
    "Compute":      {"team": "Engineering",   "owner": "engineering@operative.com"},
    "Database":     {"team": "DBAdmin",       "owner": "dba@operative.com"},
    "Storage":      {"team": "Platform",      "owner": "platform@operative.com"},
    "Network":      {"team": "Platform",      "owner": "platform@operative.com"},
    "Security":     {"team": "Security",      "owner": "security@operative.com"},
    "Observability":{"team": "Platform",      "owner": "platform@operative.com"},
    "Streaming":    {"team": "Data",          "owner": "data@operative.com"},
    "Analytics":    {"team": "Data",          "owner": "data@operative.com"},
}

_COST_WEIGHTS = {
    "Amazon EC2": 8.0, "Amazon RDS": 4.0, "Amazon ECS": 3.5,
    "Amazon ElastiCache": 3.0, "Amazon S3": 2.0, "Amazon CloudFront": 1.8,
    "AWS WAF": 1.5, "Amazon DynamoDB": 1.2, "Amazon CloudWatch": 1.0,
    "Amazon Kinesis": 0.9, "AWS Lambda": 0.8, "Amazon OpenSearch Service": 0.7,
    "AWS Secrets Manager": 0.4, "Amazon ECR": 0.3, "Amazon Route 53": 0.2,
}

_PRODUCTS = ["ProductA", "ProductB", "ProductC", "Internal", "Shared"]
_TEAMS = ["platform", "data", "security", "finops", "engineering", "appdev", "dba"]
_REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "eu-central-1",
            "ap-southeast-1", "ap-northeast-1"]
_USAGE_TYPES = ["BoxUsage:t3.large", "BoxUsage:m5.xlarge",
                "DataTransfer-Out-Bytes", "Requests", "TimedStorage-ByteHrs",
                "LoadBalancerUsage", "DNS-Queries", "QueueUsage"]
_LINE_ITEM_TYPES = ["Usage", "Tax", "Credit", "Fee",
                    "SavingsPlanCoveredUsage"]
_PRICING_TERMS = ["OnDemand"] * 80 + ["Reserved"] * 15 + ["Spot"] * 5

_COLUMNS = [
    "identity_line_item_id", "identity_time_interval",
    "bill_billing_period_start_date", "bill_billing_period_end_date",
    "bill_payer_account_id",
    "line_item_usage_account_id", "line_item_usage_account_name",
    "line_item_line_item_type", "line_item_usage_start_date",
    "line_item_usage_end_date", "line_item_product_code",
    "line_item_usage_type", "line_item_operation",
    "line_item_unblended_cost", "line_item_blended_cost",
    "product_region", "product_instance_type", "resource_id",
    "org_unit_name", "service_category", "service_owner_team",
    "service_owner_email",
    "tag_Product", "tag_Environment", "tag_Team", "tag_Customer",
    "tag_Owner", "tag_CostCentre",
    "env_owner_team", "env_owner_email",
    "pricing_term",
]

_PROD_ACCOUNTS = ["111122223333", "222233334444"]
_TENANT_CUSTOMERS = ["CustomerA", "CustomerB", "CustomerC", "CustomerD"]
_COST_CENTRES = ["CC-1001", "CC-1002", "CC-1003", "CC-2001", "CC-2002"]


def _generate_csv(n: int = 2000) -> str:
    today = date.today()
    start_date = today - timedelta(days=90)
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(_COLUMNS)
    for _ in range(n):
        # Service and its owning category/team
        service = random.choice(list(_SERVICE_CATEGORIES.keys()))
        service_category = _SERVICE_CATEGORIES[service]
        service_owner = _SERVICE_OWNERS[service_category]
        service_owner_team = service_owner["team"]
        service_owner_email = service_owner["owner"]

        # Environment, with account bias
        environment = random.choice(_ENVIRONMENTS)
        if environment in ("prod", "staging"):
            if random.random() < 0.8:
                account_id = random.choice(_PROD_ACCOUNTS)
            else:
                account_id = random.choice(list(_ACCOUNTS.keys()))
        elif environment == "nonprod":
            if random.random() < 0.8:
                account_id = "333344445555"
            else:
                account_id = random.choice(list(_ACCOUNTS.keys()))
        else:  # dev
            if random.random() < 0.6:
                account_id = "333344445555"
            else:
                account_id = random.choice(list(_ACCOUNTS.keys()))
        account_name = _ACCOUNTS[account_id]
        org_unit_name = _ORG_UNITS[account_id]

        env_owner = _ENV_OWNERS[environment]
        env_owner_team = env_owner["team"]
        env_owner_email = env_owner["owner"]

        # Cost
        weight = _COST_WEIGHTS.get(service, 1.0)
        cost = round(random.uniform(0.01, weight * 50), 6)
        blended_cost = round(cost * random.uniform(0.85, 1.0), 6)
        pricing_term = random.choice(_PRICING_TERMS)

        # Dates
        day_offset = random.randint(0, 90)
        usage_day = start_date + timedelta(days=day_offset)
        usage_date = usage_day.isoformat()
        usage_end_date = (usage_day + timedelta(days=1)).isoformat()
        period_start = usage_day.replace(day=1).isoformat()
        last_day = calendar.monthrange(usage_day.year, usage_day.month)[1]
        period_end = usage_day.replace(day=last_day).isoformat()

        region = random.choice(_REGIONS)
        resource_id = f"arn:aws:{account_id}:{region}:resource/{uuid4().hex[:8]}"
        line_item_id = uuid4().hex
        time_interval = f"{usage_date}T00:00:00Z/{usage_date}T23:59:59Z"
        line_item_type = random.choice(_LINE_ITEM_TYPES)
        usage_type = random.choice(_USAGE_TYPES)

        # Compute-specific fields
        if service_category == "Compute":
            product_instance_type = random.choice(_USAGE_TYPES)
            operation = "RunInstances"
        elif service_category == "Database":
            product_instance_type = ""
            operation = "CreateDBInstance"
        else:
            product_instance_type = ""
            operation = "StandardStorage"

        # Tags
        tag_product = random.choice(_PRODUCTS) if random.random() < 0.75 else ""
        tag_environment = environment if random.random() < 0.85 else ""
        tag_team = service_owner_team if random.random() < 0.75 else ""
        if account_name == "prod-tenants" and random.random() < 0.8:
            tag_customer = random.choice(_TENANT_CUSTOMERS)
        else:
            tag_customer = ""
        tag_owner = service_owner_email if random.random() < 0.7 else ""
        tag_cost_centre = random.choice(_COST_CENTRES) if random.random() < 0.6 else ""

        writer.writerow([
            line_item_id, time_interval,
            period_start, period_end,
            "000011112222",
            account_id, account_name,
            line_item_type, usage_date,
            usage_end_date, service,
            usage_type, operation,
            cost, blended_cost,
            region, product_instance_type, resource_id,
            org_unit_name, service_category, service_owner_team,
            service_owner_email,
            tag_product, tag_environment, tag_team, tag_customer,
            tag_owner, tag_cost_centre,
            env_owner_team, env_owner_email,
            pricing_term,
        ])
    return out.getvalue()


@router.post("/generate-sample")
async def generate_sample() -> dict:
    csv_text = _generate_csv(2000)
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
    import os

    from tools.data_sources.file_providers import (
        UploadTooLarge,
        _select_inner_csv,
        stream_upload_to_temp,
    )

    filename = file.filename or ""
    fname_lower = filename.lower()

    # Stream the upload to a temp file on disk (bounded memory) instead of
    # loading the whole CUR into memory via ``await file.read()``.
    # File size check — 2GB limit (large CUR exports) is enforced while streaming.
    try:
        tmp_path, file_size = await stream_upload_to_temp(
            file, max_bytes=2048 * 1024 * 1024
        )
    except UploadTooLarge:
        raise HTTPException(status_code=413,
            detail="File too large (max 2 GB)")

    try:
        # ── CSV ──────────────────────────────────────────────────────
        if fname_lower.endswith(".csv"):
            # Read in text mode so only the decoded string is held — never the
            # raw bytes alongside it.
            with open(tmp_path, encoding="utf-8-sig", errors="replace") as f:
                csv_text = f.read()

        # ── ZIP containing CSV ───────────────────────────────────────
        elif fname_lower.endswith(".zip"):
            try:
                with zipfile.ZipFile(tmp_path) as zf:
                    # Find first CSV file inside the zip
                    csv_names = [n for n in zf.namelist()
                                 if n.lower().endswith(".csv")
                                 and not n.startswith("__MACOSX")]
                    if not csv_names:
                        raise HTTPException(status_code=422,
                            detail="No CSV file found inside the ZIP archive")
                    # Pick the richest inner CSV — AWS legacy DBR zips bundle a
                    # tag-less detailed-line-items CSV alongside the
                    # "-with-resources-and-tags" variant; csv_names[0] can grab
                    # the tag-less one and drop every resourceTags/user:* column.
                    chosen = _select_inner_csv(zf, csv_names)
                    with zf.open(chosen) as f:
                        csv_text = io.TextIOWrapper(
                            f, encoding="utf-8-sig", errors="replace"
                        ).read()
                    # Use the inner CSV filename
                    filename = chosen.split("/")[-1]
            except zipfile.BadZipFile:
                raise HTTPException(status_code=422,
                    detail="Invalid ZIP file")

        # ── PARQUET ──────────────────────────────────────────────────
        elif fname_lower.endswith(".parquet"):
            try:
                import duckdb
                con = duckdb.connect()
                # Read parquet from the temp file path and convert to CSV string
                csv_text = con.execute(
                    f"SELECT * FROM read_parquet('{tmp_path}')"
                ).df().to_csv(index=False)
                con.close()
            except Exception as e:
                raise HTTPException(status_code=422,
                    detail=f"Failed to read Parquet file: {str(e)}")

        else:
            raise HTTPException(status_code=400,
                detail="Unsupported file format. "
                       "Supported: .csv, .zip (containing CSV), .parquet")
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    # ── Common processing ─────────────────────────────────────────
    summary = get_total_cost(csv_text)
    if "error" in summary:
        raise HTTPException(status_code=422, detail=summary["error"])

    report = add_report(
        filename=filename,
        csv_text=csv_text,
        row_count=summary.get("row_count", 0),
        total_cost=summary.get("total_cost", 0.0),
        file_size=file_size,
    )
    await persist_report(report["id"])
    invalidate_dashboard_cache(report["id"])
    invalidate_dashboard_cache(None)
    return report


def _detect_columns(csv_text: str) -> set[str]:
    import io, csv
    reader = csv.reader(io.StringIO(csv_text))
    headers = next(reader, [])
    return {h.lower().strip() for h in headers}

def _report_capabilities(cols: set[str]) -> dict:
    return {
        "has_service":      any(c in cols for c in
                                ["line_item_product_code",
                                 "productname", "servicename"]),
        "has_date":         any(c in cols for c in
                                ["line_item_usage_start_date",
                                 "usagestartdate"]),
        "has_region":       any(c in cols for c in
                                ["product_region", "region"]),
        "has_account":      any(c in cols for c in
                                ["line_item_usage_account_id",
                                 "bill_payer_account_id"]),
        "has_environment":  "tag_environment" in cols,
        "has_tags":         any(c.startswith("tag_") for c in cols),
        "has_org_unit":     "org_unit_name" in cols,
        "has_pricing_term": "pricing_term" in cols,
        "has_service_category": "service_category" in cols,
        "has_cost_centre":  "tag_costcentre" in cols,
        "has_customer":     "tag_customer" in cols,
        "column_count":     len(cols),
    }


@router.get("/compare")
async def compare_reports(
    ids: str = FastAPIQuery(..., description="Comma-separated report IDs")
) -> dict:
    from report_store import list_reports, get_report_csv
    all_reports = {r["id"]: r for r in list_reports()}

    # Parse report IDs
    try:
        report_ids = [int(i.strip()) for i in ids.split(",") if i.strip()]
    except ValueError:
        raise HTTPException(status_code=400,
            detail="ids must be comma-separated integers")

    if len(report_ids) < 2:
        raise HTTPException(status_code=400,
            detail="At least 2 report IDs required for comparison")

    # Load reports
    reports_data = []
    for rid in report_ids:
        report = all_reports.get(rid)
        if not report:
            raise HTTPException(status_code=404,
                detail=f"Report {rid} not found")
        csv_text = get_report_csv(rid)
        if not csv_text:
            raise HTTPException(status_code=404,
                detail=f"Report {rid} has no data — please re-upload")
        cols = _detect_columns(csv_text)
        caps = _report_capabilities(cols)
        reports_data.append({
            "id": rid,
            "filename": report["filename"],
            "row_count": report["row_count"],
            "total_cost": report["total_cost"],
            "created_at": str(report.get("created_at", "")),
            "capabilities": caps,
            "csv_text": csv_text,
        })

    # Build compatibility matrix
    # A comparison is possible if ALL selected reports have the capability
    all_caps = [r["capabilities"] for r in reports_data]
    compatibility = {
        cap: all(c.get(cap, False) for c in all_caps)
        for cap in all_caps[0].keys()
        if cap != "column_count"
    }

    # Build per-report summaries
    summaries = []
    for r in reports_data:
        csv_text = r.pop("csv_text")
        summary = {
            "id": r["id"],
            "filename": r["filename"],
            "row_count": r["row_count"],
            "total_cost": r["total_cost"],
            "created_at": r["created_at"],
            "capabilities": r["capabilities"],
        }

        # Always include
        summary["service_breakdown"] = get_cost_by_service(
            csv_text, limit=15) if r["capabilities"]["has_service"] else []
        summary["daily_trend"] = get_daily_trend(
            csv_text) if r["capabilities"]["has_date"] else []
        summary["monthly_trend"] = []
        if r["capabilities"]["has_date"]:
            from tools.dashboard_builder import compute_dashboard
            dd = compute_dashboard(csv_text)
            summary["monthly_trend"] = dd.get("monthly_trend", [])
        summary["region_breakdown"] = get_cost_by_region(
            csv_text) if r["capabilities"]["has_region"] else []

        # Conditional on capabilities
        summary["account_breakdown"] = get_cost_by_account(
            csv_text) if r["capabilities"]["has_account"] else []
        summary["environment_breakdown"] = get_cost_by_environment(
            csv_text) if r["capabilities"]["has_environment"] else []
        summary["service_category_breakdown"] = get_cost_by_service_category(
            csv_text) if r["capabilities"]["has_service_category"] else []
        summary["tag_product"] = get_cost_by_tag(
            csv_text, "tag_Product") if r["capabilities"]["has_tags"] else []
        summary["tag_team"] = get_cost_by_tag(
            csv_text, "tag_Team") if r["capabilities"]["has_tags"] else []
        summary["tag_customer"] = get_cost_by_tag(
            csv_text, "tag_Customer") if r["capabilities"]["has_customer"] else []
        summary["tag_costcentre"] = get_cost_by_tag(
            csv_text, "tag_CostCentre") if r["capabilities"]["has_cost_centre"] else []
        summary["pricing_term_breakdown"] = get_cost_by_pricing_term(
            csv_text) if r["capabilities"]["has_pricing_term"] else []
        summary["untagged_resources"] = get_untagged_resources(
            csv_text) if r["capabilities"]["has_tags"] else {}
        summary["savings_opportunities"] = get_savings_opportunities(
            csv_text) if r["capabilities"]["has_service"] else {}

        summaries.append(summary)

    # Unavailability notes
    unavailable = []
    cap_labels = {
        "has_account":          "Account breakdown",
        "has_environment":      "Environment breakdown",
        "has_service_category": "Service category breakdown",
        "has_tags":             "Tag coverage & breakdown",
        "has_org_unit":         "Org unit breakdown",
        "has_pricing_term":     "Pricing term breakdown",
        "has_cost_centre":      "Cost centre breakdown",
        "has_customer":         "Customer breakdown",
    }
    for cap, label in cap_labels.items():
        missing = [r["filename"] for r in summaries
                   if not r["capabilities"].get(cap, False)]
        if missing:
            unavailable.append({
                "feature": label,
                "missing_in": missing,
                "available": compatibility.get(cap, False),
            })

    return {
        "report_count": len(summaries),
        "report_ids": report_ids,
        "compatibility": compatibility,
        "unavailable_features": unavailable,
        "reports": summaries,
    }


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


@router.get("/{report_id}/stream")
async def stream_report_data(report_id: int):
    from fastapi.responses import StreamingResponse
    import json

    csv_text = get_report_csv(report_id)
    if csv_text is None:
        raise HTTPException(status_code=404,
            detail=f"Report {report_id} not found")

    rows = get_report_rows(report_id)
    if not rows:
        raise HTTPException(status_code=404,
            detail=f"Report {report_id} has no data")

    async def generate():
        # Send total count first
        yield json.dumps({"total": len(rows)}) + "\n"
        # Stream rows in chunks of 50
        chunk_size = 50
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i+chunk_size]
            yield json.dumps({"rows": chunk,
                            "offset": i,
                            "count": len(chunk)}) + "\n"
            import asyncio
            await asyncio.sleep(0)  # yield control

    return StreamingResponse(
        generate(),
        media_type="application/x-ndjson",
        headers={"X-Accel-Buffering": "no",
                 "Cache-Control": "no-cache"}
    )
