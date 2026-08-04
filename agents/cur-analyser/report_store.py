"""CUR report store — in-memory write-through cache backed by PostgreSQL.

In-memory functions (add_report, list_reports, get_report_rows, …) are
synchronous for simplicity.  DB operations (persist_report, load_from_db)
are async and called from route handlers and the lifespan hook respectively.
"""
from __future__ import annotations

import csv
import io
import logging
import os
import re
import threading
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

def get_cur_storage_usage() -> dict:
    """Return current CUR data directory disk usage."""
    import shutil
    try:
        total_disk, used_disk, free_disk = shutil.disk_usage(CUR_DATA_DIR)
        cur_used = sum(
            os.path.getsize(os.path.join(CUR_DATA_DIR, f))
            for f in os.listdir(CUR_DATA_DIR)
            if os.path.isfile(os.path.join(CUR_DATA_DIR, f))
        ) if os.path.exists(CUR_DATA_DIR) else 0
        return {
            "cur_used_bytes": cur_used,
            "disk_free_bytes": free_disk,
            "disk_total_bytes": total_disk,
            "cur_used_gb": round(cur_used / 1024**3, 2),
            "disk_free_gb": round(free_disk / 1024**3, 2),
        }
    except Exception:
        return {"cur_used_bytes": 0, "disk_free_bytes": 0, "disk_total_bytes": 0, "cur_used_gb": 0.0, "disk_free_gb": 0.0}

def _compute_date_range_for_report(report: dict) -> tuple[str | None, str | None]:
    """Compute date range from the report's DuckDB file. Returns (start, end) ISO strings."""
    path = report.get("_file_path")
    if not path or not os.path.exists(path) or not path.endswith(".duckdb"):
        return None, None
    try:
        import duckdb as _duckdb
        from tools.duckdb_engine import _detect_date_col
        con = _duckdb.connect(":memory:")
        safe = path.replace("'", "''")
        con.execute(f"ATTACH '{safe}' AS src (READ_ONLY)")
        con.execute("CREATE VIEW cur_data AS SELECT * FROM src.cur_data")
        cols = [r[0] for r in con.execute("DESCRIBE cur_data").fetchall()]
        date_col = _detect_date_col(cols)
        if not date_col:
            con.close()
            return None, None
        row = con.execute(
            f'SELECT MIN("{date_col}"), MAX("{date_col}") FROM cur_data '
            f'WHERE "{date_col}" IS NOT NULL'
        ).fetchone()
        con.close()
        if row and row[0] and row[1]:
            return str(row[0])[:10], str(row[1])[:10]
    except Exception:
        pass
    return None, None

_lock = threading.Lock()
_reports: list[dict[str, Any]] = []
_counter = 0

# ── Permanent CUR file storage ────────────────────────────────────────────────
# Large CUR files (1.8 GB+) are stored on disk and read directly by DuckDB /
# pandas via their path, instead of being held as a giant ``csv_text`` string in
# memory and in the database. Override the location with ``CUR_DATA_DIR``.
# NOTE: for files to survive a container rebuild this directory must be backed by
# a persistent volume (see docker-compose.yml → cur_analyser_data).
CUR_DATA_DIR = os.environ.get("CUR_DATA_DIR", "/app/data/cur")


def _ensure_data_dir() -> str:
    os.makedirs(CUR_DATA_DIR, exist_ok=True)
    return CUR_DATA_DIR


def report_file_path(report_id: int, ext: str = ".csv") -> str:
    """Conventional permanent path for a report's CUR file."""
    return os.path.join(CUR_DATA_DIR, f"{report_id}{ext}")


def _discover_file_path(report_id: int) -> str | None:
    """Recover a report's on-disk CUR file by convention (used on startup, since
    the path itself is not persisted in the DB)."""
    for ext in (".parquet_dir", ".duckdb", ".csv.gz", ".csv", ".parquet"):
        p = report_file_path(report_id, ext)
        if os.path.exists(p):
            return p
    return None


def _file_to_csv_text(path: str) -> str:
    """Read an on-disk CUR file into raw CSV text. Parquet is converted via
    DuckDB; everything else is read as UTF-8 text."""
    if path.lower().endswith(".parquet"):
        import duckdb

        con = duckdb.connect()
        try:
            return con.execute(
                "SELECT * FROM read_parquet(?)", [path]
            ).df().to_csv(index=False)
        finally:
            con.close()
    with open(path, encoding="utf-8-sig", errors="replace") as f:
        return f.read()


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

def add_report(
    filename: str,
    csv_text: str,
    row_count: int,
    total_cost: float,
    file_size: int,
    file_path: str | None = None,
    sync_type: str = "manual",
) -> dict[str, Any]:
    """Register a report.

    Pass ``csv_text`` for the legacy in-memory pipeline, or ``file_path`` (with
    ``csv_text=""``) for the file-path pipeline used by large CUR files. When a
    file path is used, rows are parsed lazily from disk on demand rather than
    held in memory.
    """
    global _counter
    rows = _parse_rows(csv_text) if csv_text else None
    with _lock:
        _counter += 1
        report: dict[str, Any] = {
            "id": _counter,
            "filename": filename,
            "_csv": csv_text or "",
            "_rows": rows,
            "_file_path": file_path,
            "row_count": row_count,
            "total_cost": round(total_cost, 4),
            "file_size": file_size,
            "status": "ready",
            "sync_type": sync_type,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        _reports.insert(0, report)
    return _public(report)


async def cleanup_old_report_files(keep_last: int = 3) -> int:
    """Delete on-disk files for reports beyond the most recent `keep_last`.
    Returns count of report file sets removed. Safe to call after each new
    report is persisted — prevents unbounded parquet_dir/csv accumulation."""
    import shutil
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
                    .order_by(CurReport.id.desc())
                )
            ).scalars().all()

        report_ids = [r.id for r in rows]
    except Exception:
        logger.exception("cleanup_old_report_files: failed to query reports from DB")
        return 0

    to_remove = report_ids[keep_last:]
    removed = 0
    for rid in to_remove:
        for ext in (".parquet_dir", ".duckdb", ".csv.gz", ".csv", ".parquet"):
            p = report_file_path(rid, ext)
            if os.path.exists(p):
                try:
                    if os.path.isdir(p):
                        shutil.rmtree(p)
                    else:
                        os.remove(p)
                    removed += 1
                    logger.info("cleanup_old_report_files: removed %s", p)
                except Exception as e:
                    logger.warning("cleanup_old_report_files: failed to remove %s: %s", p, e)
    return removed


def set_report_path(report_id: int, file_path: str) -> bool:
    """Attach a permanent on-disk CUR file path to an existing report."""
    with _lock:
        for r in _reports:
            if r["id"] == report_id:
                r["_file_path"] = file_path
                return True
    return False


def get_report_path(report_id: int) -> str | None:
    """Return the on-disk CUR file path for a report, if one exists."""
    with _lock:
        path = next((r.get("_file_path") for r in _reports if r["id"] == report_id), None)
    return path if path and os.path.exists(path) else None


def get_latest_path() -> str | None:
    """Return the on-disk CUR file path for the most recent report, if any."""
    with _lock:
        path = _reports[0].get("_file_path") if _reports else None
    return path if path and os.path.exists(path) else None


def list_reports() -> list[dict[str, Any]]:
    with _lock:
        reports_copy = list(_reports)
    # Compute date ranges outside the lock to avoid blocking other threads
    for r in reports_copy:
        if "date_range_start" not in r:
            start, end = _compute_date_range_for_report(r)
            with _lock:
                r["date_range_start"] = start
                r["date_range_end"] = end
    with _lock:
        return [_public(r) for r in _reports]


def get_report_rows(report_id: int) -> list[dict[str, str]] | None:
    with _lock:
        match = next((r for r in _reports if r["id"] == report_id), None)
        if match is None:
            return None
        cached = match.get("_rows")
        path = match.get("_file_path")
        csv_text = match.get("_csv")
    if cached is not None:
        return cached
    # File-path report: parse rows lazily from disk (not cached, to keep large
    # files out of memory).
    if path and os.path.exists(path):
        try:
            file_size = os.path.getsize(path)
            if file_size > 200 * 1024 * 1024:
                # Large file — read header only for column detection; never load full file
                with open(path, encoding="utf-8-sig", errors="replace") as f:
                    header = f.readline()
                    first_row = f.readline()
                return _parse_rows(header + first_row)
            return _parse_rows(_file_to_csv_text(path))
        except Exception:
            logger.exception("get_report_rows: failed to read %s", path)
            return None
    return _parse_rows(csv_text) if csv_text else None


def get_latest_csv() -> str | None:
    with _lock:
        match = _reports[0] if _reports else None
        path = match.get("_file_path") if match else None
        csv_text = match.get("_csv") if match else None
    if match is None:
        return None
    if path and os.path.exists(path):
        try:
            return _file_to_csv_text(path)
        except Exception:
            logger.exception("get_latest_csv: failed to read %s", path)
    return csv_text or None


def get_report_csv(report_id: int) -> str | None:
    """Return the raw CSV text for a specific report_id, or None if not found.

    Reads from the on-disk file when one exists; otherwise falls back to the
    stored ``csv_text`` (legacy reports persisted before the file-path pipeline).
    """
    with _lock:
        match = next((r for r in _reports if r["id"] == report_id), None)
        path = match.get("_file_path") if match else None
        csv_text = match.get("_csv") if match else None
    if match is None:
        return None
    if path and os.path.exists(path):
        try:
            return _file_to_csv_text(path)
        except Exception:
            logger.exception("get_report_csv: failed to read %s", path)
    return csv_text or None


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
                    sync_type=report["sync_type"],
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
                        "sync_type": report["sync_type"],
                    },
                )
            )
            await session.execute(stmt)
            await session.commit()
    except Exception:
        logger.exception("persist_report: failed to save report %d to DB", report_id)


async def delete_report(report_id: int) -> bool:
    """Remove a report from the in-memory store and the database.

    Returns True if a report with ``report_id`` was found and removed from the DB.
    """
    import shutil
    from config import settings
    from database import SessionLocal
    from models import CurReport
    from sqlalchemy import delete as sa_delete

    # Remove from in-memory list if present (for consistency).
    with _lock:
        _reports[:] = [r for r in _reports if r["id"] != report_id]

    # Remove on-disk files by convention, regardless of in-memory state.
    # After container restart, _reports is empty, so we can't rely on it.
    for ext in (".parquet_dir", ".duckdb", ".csv.gz", ".csv", ".parquet"):
        p = report_file_path(report_id, ext)
        if os.path.exists(p):
            try:
                if os.path.isdir(p):
                    shutil.rmtree(p)
                else:
                    os.remove(p)
                logger.info("delete_report: removed %s", p)
            except Exception:
                logger.exception("delete_report: failed to remove %s", p)

    # Remove from database and use the actual delete result.
    if SessionLocal is None:
        return False

    try:
        async with SessionLocal() as session:
            result = await session.execute(
                sa_delete(CurReport).where(
                    CurReport.id == report_id,
                    CurReport.agent_slug == settings.agent_slug,
                )
            )
            await session.commit()
            return result.rowcount > 0
    except Exception:
        logger.exception("delete_report: failed to delete report %d from DB", report_id)
        return False


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
            # File-path reports persist an empty csv_data blob; recover their
            # on-disk file by convention and parse rows lazily. Legacy reports
            # keep their csv_data and parse rows eagerly as before.
            disk_path = _discover_file_path(r.id)
            if r.csv_data:
                try:
                    parsed_rows: list[dict[str, str]] | None = _parse_rows(r.csv_data)
                except Exception:
                    parsed_rows = []
            else:
                parsed_rows = None
            loaded.append({
                "id": r.id,
                "filename": r.filename,
                "_csv": r.csv_data or "",
                "_rows": parsed_rows,
                "_file_path": disk_path,
                "row_count": r.row_count,
                "total_cost": r.total_cost,
                "file_size": r.file_size,
                "status": r.status,
                "sync_type": getattr(r, "sync_type", "manual"),
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
