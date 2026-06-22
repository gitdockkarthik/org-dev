"""CUR report store — in-memory write-through cache backed by PostgreSQL.

In-memory functions (add_report, list_reports, get_report_rows, …) are
synchronous for simplicity.  DB operations (persist_report, load_from_db)
are async and called from route handlers and the lifespan hook respectively.
"""
from __future__ import annotations

import csv
import io
import logging
import re
import threading
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_reports: list[dict[str, Any]] = []
_counter = 0


def _needs_normalisation(columns: list[str]) -> bool:
    """Check if columns have slash format (external CUR export) needing normalisation."""
    return any("/" in col for col in columns)

def _normalise_key(k: str) -> str:
    """Normalise slash-format CUR column names to a safe snake_case-ish key.
    lineItem/UsageAccountId -> line_item_UsageAccountId
    product/region -> product_region
    resourceTags/user:Environment -> resource_tags_user_Environment

    The ':' in ``resourceTags/user:<Name>`` tag columns is replaced with '_' so
    the resulting key is safe everywhere (DuckDB identifiers, JSON keys, JS
    property accessors) and is never silently dropped by downstream consumers.
    Case after the slash is preserved (Environment, Team, CostCenter, …).
    """
    if "/" not in k:
        return k  # Already normalised — return as-is
    prefix, _, rest = k.partition("/")
    # Convert camelCase prefix to snake_case
    prefix = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', prefix).lower()
    # Replace ':' (resourceTags/user:<Name>) with '_' for a safe key; the rest's
    # case is otherwise preserved so tag display names stay intact.
    rest = rest.replace(":", "_")
    return f"{prefix}_{rest}"

def _parse_rows(csv_text: str) -> list[dict[str, str]]:
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = [dict(row) for row in reader]
    if rows and _needs_normalisation(list(rows[0].keys())):
        rows = [{_normalise_key(k): v for k, v in row.items()} for row in rows]
    # DEBUG: surface the actual (post-normalisation) column names so unexpected
    # CUR header formats are visible in the logs while diagnosing real data.
    if rows:
        logger.info("_parse_rows: %d columns -> %s", len(rows[0]), list(rows[0].keys()))
    return rows


# ── In-memory (sync) ──────────────────────────────────────────────────────────

def add_report(filename: str, csv_text: str, row_count: int, total_cost: float, file_size: int) -> dict[str, Any]:
    global _counter
    rows = _parse_rows(csv_text)
    with _lock:
        _counter += 1
        report: dict[str, Any] = {
            "id": _counter,
            "filename": filename,
            "_csv": csv_text,
            "_rows": rows,
            "row_count": row_count,
            "total_cost": round(total_cost, 4),
            "file_size": file_size,
            "status": "ready",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        _reports.insert(0, report)
    return _public(report)


def list_reports() -> list[dict[str, Any]]:
    with _lock:
        return [_public(r) for r in _reports]


def get_report_rows(report_id: int) -> list[dict[str, str]] | None:
    with _lock:
        for r in _reports:
            if r["id"] == report_id:
                return r["_rows"]
        return None


def get_latest_csv() -> str | None:
    with _lock:
        return _reports[0]["_csv"] if _reports else None


def get_report_csv(report_id: int) -> str | None:
    """Return the raw CSV text for a specific report_id, or None if not found."""
    for r in _reports:
        if r["id"] == report_id:
            return r.get("_csv")
    return None


def get_latest_meta() -> dict[str, Any] | None:
    with _lock:
        return _public(_reports[0]) if _reports else None


def _public(r: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in r.items() if not k.startswith("_")}


def _get_internal(report_id: int) -> dict[str, Any] | None:
    with _lock:
        for r in _reports:
            if r["id"] == report_id:
                return r
    return None


# ── DB persistence (async) ────────────────────────────────────────────────────

async def persist_report(report_id: int) -> None:
    """Upsert a report (identified by its in-memory id) to the database."""
    from config import settings
    from database import SessionLocal
    from models import CurReport
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    if SessionLocal is None:
        return

    report = _get_internal(report_id)
    if report is None:
        logger.warning("persist_report: report %d not found in memory", report_id)
        return

    created_at = datetime.fromisoformat(report["created_at"])
    try:
        async with SessionLocal() as session:
            stmt = (
                pg_insert(CurReport)
                .values(
                    id=report["id"],
                    agent_slug=settings.agent_slug,
                    filename=report["filename"],
                    csv_data=report["_csv"],
                    row_count=report["row_count"],
                    total_cost=report["total_cost"],
                    file_size=report["file_size"],
                    status=report["status"],
                    created_at=created_at,
                )
                .on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "agent_slug": settings.agent_slug,
                        "filename": report["filename"],
                        "csv_data": report["_csv"],
                        "row_count": report["row_count"],
                        "total_cost": report["total_cost"],
                        "file_size": report["file_size"],
                        "status": report["status"],
                    },
                )
            )
            await session.execute(stmt)
            await session.commit()
    except Exception:
        logger.exception("persist_report: failed to save report %d to DB", report_id)


async def load_from_db() -> int:
    """Populate the in-memory store from the database on startup. Returns count restored."""
    global _counter
    from config import settings
    from database import SessionLocal
    from models import CurReport
    from sqlalchemy import select

    if SessionLocal is None:
        return 0

    try:
        async with SessionLocal() as session:
            rows = (
                await session.execute(
                    select(CurReport)
                    .where(CurReport.agent_slug == settings.agent_slug)
                    .order_by(CurReport.created_at.desc())
                )
            ).scalars().all()

        if not rows:
            return 0

        loaded: list[dict[str, Any]] = []
        for r in rows:
            try:
                parsed_rows = _parse_rows(r.csv_data)
            except Exception:
                parsed_rows = []
            loaded.append({
                "id": r.id,
                "filename": r.filename,
                "_csv": r.csv_data,
                "_rows": parsed_rows,
                "row_count": r.row_count,
                "total_cost": r.total_cost,
                "file_size": r.file_size,
                "status": r.status,
                "created_at": r.created_at.isoformat(),
            })

        with _lock:
            _reports.clear()
            _reports.extend(loaded)
            _counter = max(r["id"] for r in loaded)

        logger.info("load_from_db: restored %d report(s) from DB", len(loaded))
        return len(loaded)
    except Exception:
        logger.exception("load_from_db: failed to load reports from DB")
        return 0
