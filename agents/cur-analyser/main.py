import asyncio
import csv
import io
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Header, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from agent import AgentRunner
from config import settings
import jobs as _jobs_module
from routes_dashboard import router as dashboard_router
from routes_reports import router as reports_router
from report_store import load_from_db as load_reports_from_db
from routes_settings import load_config_from_db, router as settings_router
from tools.dashboard_builder import DashboardBuilderTool
from tools.duckdb_engine import CurQueryTool
from tools.source import FileSource

from shared.llm import stream_message as _llm_stream

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── CUR CSV cache ─────────────────────────────────────────────────────────────
_cur_cache: dict[str, str] = {}
_session_report_map: dict[str, int] = {}  # session_id -> report_id, for tools to resolve file_path when csv_text cache is empty (large files)

# --- async upload jobs ------------------------------------------------------
import uuid as _uuid_mod
from enum import Enum as _Enum

class UploadStatus(_Enum):
    UPLOADING     = "uploading"
    DECOMPRESSING = "decompressing"
    PROCESSING    = "processing"
    INGESTING     = "ingesting"
    READY         = "ready"
    FAILED        = "failed"
    CANCELLED     = "cancelled"

_upload_jobs: dict[str, dict] = {}  # job_id -> job record

def _make_job(filename: str) -> dict:
    job_id = _uuid_mod.uuid4().hex
    _upload_jobs[job_id] = {
        "job_id":    job_id,
        "filename":  filename,
        "status":    UploadStatus.UPLOADING.value,
        "report_id": None,
        "error":     None,
        "cancel":    False,
    }
    return _upload_jobs[job_id]

def _job_update(job: dict, status: UploadStatus, **kw) -> None:
    job["status"] = status.value
    job.update(kw)

# ── Agent setup ───────────────────────────────────────────────────────────────
_runner = AgentRunner(
    tools=[
        CurQueryTool(_cur_cache, _session_report_map),
        DashboardBuilderTool(_cur_cache, _session_report_map),
    ]
)

# ── Schemas ───────────────────────────────────────────────────────────────────


class InvokeRequest(BaseModel):
    session_id: str
    user_message: str
    context: dict[str, Any] = Field(default_factory=dict)
    history: list[dict[str, Any]] = Field(default_factory=list)


class InvokeResponse(BaseModel):
    session_id: str
    response: str
    metadata: dict[str, Any] = Field(default_factory=dict)


# ── Self-registration ─────────────────────────────────────────────────────────


async def _register_self() -> None:
    if not settings.registry_url:
        logger.info("Self-registration skipped: REGISTRY_URL not set")
        return

    manifest = json.loads((Path(__file__).parent / "manifest.json").read_text())
    base = settings.registry_url.rstrip("/")

    async with httpx.AsyncClient(timeout=10.0) as client:
        # Fetch API key dynamically — agents don't need BACKEND_API_KEY in env.
        api_key = ""
        try:
            token_resp = await client.get(f"{base}/api/platform/agent-token")
            token_resp.raise_for_status()
            api_key = token_resp.json().get("registration_token", "")
        except Exception as exc:
            logger.warning("Self-registration: could not fetch agent-token: %s", exc)

        if not api_key:
            api_key = settings.backend_api_key  # legacy env-var fallback

        if not api_key:
            logger.error("Self-registration skipped: no registration token available")
            return

        headers = {"X-API-Key": api_key}
        reg_resp = await client.post(
            f"{base}/api/registry/agents",
            json={
                "name": manifest["name"],
                "slug": manifest["slug"],
                "description": manifest.get("description", ""),
                "version": manifest.get("version", "0.1.0"),
                "invoke_url": manifest.get("invoke_url"),
                "tools": manifest.get("tools", []),
            },
            headers=headers,
        )

        if reg_resp.status_code == 201:
            agent_id = reg_resp.json()["id"]
            logger.info("Self-registration: registered as %s", agent_id)
        elif reg_resp.status_code == 409:
            list_resp = await client.get(f"{base}/api/registry/agents", headers=headers)
            list_resp.raise_for_status()
            match = next((a for a in list_resp.json() if a["slug"] == manifest["slug"]), None)
            if not match:
                logger.error("Self-registration: 409 conflict but slug not found in agent list")
                return
            agent_id = match["id"]
            logger.info("Self-registration: already registered as %s", agent_id)
        else:
            logger.error("Self-registration failed: %s — %s", reg_resp.status_code, reg_resp.text)
            return

        pub_resp = await client.post(f"{base}/api/registry/agents/{agent_id}/publish", headers=headers)
        if pub_resp.status_code == 200:
            logger.info("Self-registration: published successfully")
        else:
            logger.error("Self-registration publish failed: %s — %s", pub_resp.status_code, pub_resp.text)


# ── App ───────────────────────────────────────────────────────────────────────


async def _init_config() -> None:
    from database import engine
    from models import Base

    if engine is not None:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("_init_config: DB tables ensured")

    db_cfg = await load_config_from_db()
    if not db_cfg:
        logger.info("_init_config: no saved config found — waiting for user setup")
    else:
        logger.info("_init_config: config loaded from DB — source_type: %s", db_cfg.get("source_type", "file"))

    report_count = await load_reports_from_db()
    if report_count:
        logger.info("_init_config: restored %d report(s) from DB", report_count)
        # Trigger background pre-aggregation for active report on startup
        # This ensures tabs load instantly from DB cache after restart
        try:
            from report_store import list_reports
            from routes_settings import _config as _sc
            reports = list_reports()
            if reports:
                active_id = reports[0]["id"]
                # Only pre-aggregate if DB cache is missing for current enrichment state
                from database import SessionLocal
                from models import CurTabCache
                from sqlalchemy import select as _sel
                enrichment_enabled = bool(_sc.get("inventory_enrichment_enabled", False))
                needs_compute = True
                if SessionLocal is not None:
                    async with SessionLocal() as _sess:
                        from routes_dashboard import CACHE_VERSION
                        _r = await _sess.execute(
                            _sel(CurTabCache).where(
                                CurTabCache.report_id == active_id,
                                CurTabCache.tab_name == "overview",
                                CurTabCache.enrichment_enabled == enrichment_enabled,
                                CurTabCache.cache_version == CACHE_VERSION,
                            )
                        )
                        needs_compute = _r.scalar_one_or_none() is None
                if needs_compute:
                    asyncio.create_task(_precompute_all_tabs(active_id))
                    logger.info("_init_config: triggered pre-aggregation for report_id=%d (enrichment=%s)", active_id, enrichment_enabled)
                else:
                    logger.info("_init_config: DB cache exists for report_id=%d (enrichment=%s) — skipping pre-aggregation", active_id, enrichment_enabled)
        except Exception as _pre_exc:
            logger.warning("_init_config: pre-aggregation trigger failed: %s", _pre_exc)
    else:
        logger.info("_init_config: no reports found in DB")

    # ── Data-source registry (pluggable CUR/inventory sources) ──────────────
    try:
        from storage import init_storage
        from tools.data_sources.registry import init_registry

        storage = init_storage(settings.agent_slug)
        registry = await init_registry(storage)
        # report_store is the source of truth for which CUR files exist — make the
        # registry's CUR sources exactly match the reports restored from the DB.
        await registry.sync_registry_from_reports()
        logger.info(
            "_init_config: data-source registry ready — %d CUR source(s), inventory=%s",
            len(registry.list_cur()),
            registry.get_inventory() is not None,
        )
    except Exception:
        logger.exception("_init_config: data-source registry init failed (agent still starts)")


async def _sync_loop() -> None:
    """Background task: placeholder for future scheduled CUR source sync."""
    interval = settings.sync_interval_minutes
    logger.info("Auto-sync loop started: interval=%d minutes", interval)
    while True:
        await asyncio.sleep(interval * 60)
        logger.debug("Auto-sync: no remote CUR source configured — skipping")


async def _ensure_database() -> None:
    """Create the agent's database if it does not exist."""
    import asyncpg
    if not settings.database_url:
        return
    try:
        db_name = settings.database_url.split('/')[-1]
        postgres_url = settings.database_url.rsplit('/', 1)[0] + '/postgres'
        postgres_url = postgres_url.replace('postgresql+asyncpg://', 'postgresql://')
        conn = await asyncpg.connect(postgres_url)
        try:
            exists = await conn.fetchval(
                "SELECT 1 FROM pg_database WHERE datname=$1", db_name
            )
            if not exists:
                await conn.execute(f'CREATE DATABASE "{db_name}"')
                logger.info("_ensure_database: created database %s", db_name)
            else:
                logger.info("_ensure_database: database %s already exists", db_name)
        finally:
            await conn.close()
    except Exception as e:
        logger.warning("_ensure_database: failed: %s", e)


async def _precompute_all_tabs(report_id: int) -> None:
    """Background task: pre-compute all dashboard tabs and persist to PostgreSQL cache."""
    import logging
    _log = logging.getLogger(__name__)
    from routes_settings import _config as _sc
    _log.info("Pre-aggregation: starting for report_id=%d, enrichment_enabled=%s", report_id, _sc.get("inventory_enrichment_enabled", False))
    tabs = ["overview", "accounts", "environments", "services", "tags", "trends"]
    from routes_dashboard import (
        get_tab_overview, get_tab_accounts, get_tab_environments,
        get_tab_services, get_tab_tags, get_tab_trends,
    )
    tab_fns = {
        "overview": get_tab_overview,
        "accounts": get_tab_accounts,
        "environments": get_tab_environments,
        "services": get_tab_services,
        "tags": get_tab_tags,
        "trends": get_tab_trends,
    }
    for tab in tabs:
        try:
            _log.info("Pre-aggregation: computing tab '%s' for report_id=%d", tab, report_id)
            await tab_fns[tab](report_id=report_id)
            _log.info("Pre-aggregation: tab '%s' done for report_id=%d", tab, report_id)
        except Exception as e:
            _log.warning("Pre-aggregation: tab '%s' failed for report_id=%d: %s", tab, report_id, e)
    _log.info("Pre-aggregation: completed for report_id=%d", report_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await _ensure_database()
    except Exception:
        logger.exception("_ensure_database failed")
    try:
        await _register_self()
    except Exception:
        logger.exception("Self-registration raised an unexpected exception (agent will still start)")
    try:
        await _init_config()
    except Exception:
        logger.exception("Config initialisation raised an unexpected exception (agent will still start)")

    # Register and start self-contained job scheduler
    from database import engine as _db_engine, SessionLocal
    from models import Base, CurJobSchedule, CurJobRun
    # Ensure job tables exist
    if _db_engine is not None:
        async with _db_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    # Register CUR S3 sync job
    async def _s3_sync_job():
        from routes_settings import _config as _sc
        bucket = _sc.get("s3_bucket", "")
        prefix = _sc.get("s3_prefix", "")
        region = _sc.get("s3_region", "us-east-1")
        if not bucket or not prefix:
            raise ValueError("S3 not configured")
        import uuid
        job_id = str(uuid.uuid4())
        _s3_sync_jobs[job_id] = {"status": "started", "progress": "Initialising..."}
        await _run_s3_sync(job_id, bucket, prefix, region)
        result = _s3_sync_jobs.get(job_id, {})
        progress = result.get("progress", "Sync complete")
        if result.get("status") == "failed":
            raise Exception(progress)
        # Store result message so job runner can log it
        _s3_sync_job._last_result = progress
    _jobs_module.register_job(
        "cur-s3-sync",
        "CUR S3 Sync",
        "Sync latest CUR data from S3, convert to Parquet, pre-aggregate dashboard tabs",
        _s3_sync_job,
    )
    # Register default hourly schedule if none exists
    from sqlalchemy import select as _select
    async with SessionLocal() as _sess:
        existing = await _sess.execute(_select(CurJobSchedule).where(CurJobSchedule.job_id == "cur-s3-sync"))
        if not existing.scalar_one_or_none():
            await _jobs_module.create_schedule("cur-s3-sync", "0 * * * *", enabled=True)
            logger.info("Created default hourly schedule for cur-s3-sync")
    count = await _jobs_module.load_schedules()
    logger.info("Job scheduler: loaded %d schedule(s)", count)
    _jobs_module.start_scheduler()
    # Clear stuck running jobs from previous session
    try:
        from sqlalchemy import text as _ct
        async with SessionLocal() as _sess:
            result = await _sess.execute(_ct(
                "UPDATE cur_job_runs SET status='failed', ended_at=now(), "
                "error_message='Cleared on restart' WHERE status='running'"
            ))
            if result.rowcount > 0:
                logger.info("Cleared %d stuck CUR job run(s) on startup", result.rowcount)
            await _sess.commit()
    except Exception as _e:
        logger.warning("Failed to clear stuck CUR runs: %s", _e)

    yield

    _jobs_module.stop_scheduler()


app = FastAPI(title=settings.agent_name, version="0.1.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")

from fastapi.responses import RedirectResponse

@app.get("/")
async def root():
    return RedirectResponse(url="/static/dashboard.html")
app.include_router(dashboard_router)
app.include_router(reports_router)
app.include_router(settings_router)


@app.get("/health")
async def health():
    from database import SessionLocal
    from sqlalchemy import text
    from fastapi.responses import JSONResponse
    if SessionLocal is None:
        return JSONResponse(status_code=503, content={"status": "error", "reason": "database not configured", "agent": settings.agent_slug})
    try:
        async with SessionLocal() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ok", "agent": settings.agent_slug}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "error", "reason": "database unavailable", "agent": settings.agent_slug})


# ── Data-source abstraction endpoints ───────────────────────────────────────────
# Additive only — none of these modify the existing /reports or /dashboard routes.

_MAX_CUR_BYTES = 2048 * 1024 * 1024      # 2 GB (large CUR exports; matches /reports/upload)
_MAX_INVENTORY_BYTES = 12 * 1024 * 1024  # ~12 MB (10 MB inventory + headroom)


def _require_registry():
    from tools.data_sources.registry import get_registry

    reg = get_registry()
    if reg is None:
        raise HTTPException(status_code=503, detail="Data-source registry not initialised")
    return reg


async def _active_cur_csv(reg) -> str | None:
    """Resolve the CSV text for the active CUR source (registry first, then the
    most-recent report as a fallback so this works even before any source is
    explicitly registered)."""
    try:
        providers = await reg.get_active_cur_providers()
        if providers:
            return await providers[0].fetch()
    except Exception:
        logger.exception("_active_cur_csv: provider fetch failed")
    from report_store import get_latest_csv
    return get_latest_csv()


# Files above this size are never materialised into memory by the enrichment
# endpoints (a 5.4M-row CUR is ~14 GB once parsed). They fall back to DuckDB
# account-level sampling / server-side aggregation instead. Matches the
# file-path pipeline threshold used by the dashboard.
_LARGE_FILE_BYTES = 200 * 1024 * 1024   # 200 MB
_CUR_STORAGE_CAP_BYTES = 10 * 1024 ** 3  # 10 GB default cap for CUR data directory


def _is_large_file(report_id: int) -> bool:
    """True when the report's row count exceeds the large-file threshold.
    Row count is used instead of file size since gz files are stored compressed
    (281MB gz vs 2.2GB decompressed) — file size is no longer a reliable signal."""
    from report_store import list_reports

    meta = next((r for r in list_reports() if r["id"] == report_id), None)
    if meta is None:
        return False
    return (meta.get("row_count") or 0) > 200_000


def _quote_ident(name: str) -> str:
    """Quote a SQL identifier (column name) for safe interpolation, escaping any
    embedded double quotes per SQL rules. Column names here come from a
    user-uploaded CUR header, so they must never be trusted raw in a query."""
    return '"' + str(name).replace('"', '""') + '"'


async def _process_upload_job(job: dict, tmp_path: str, filename: str, file_size: int) -> None:
    """Background task: decompress, register, and mark job ready or failed."""
    import os
    import shutil
    from report_store import (
        _ensure_data_dir,
        add_report,
        persist_report,
        report_file_path,
        set_report_path,
    )
    from routes_dashboard import invalidate_dashboard_cache
    from tools.data_sources.file_providers import FileUploadCURProvider, materialize_cur
    from tools.duckdb_engine import get_total_cost
    reg = _require_registry()
    mat_path: str | None = None
    try:
        _job_update(job, UploadStatus.DECOMPRESSING)
        if job["cancel"]:
            _job_update(job, UploadStatus.CANCELLED)
            return
        try:
            mat_path, resolved, ext = materialize_cur(filename, tmp_path)
        except ValueError as exc:
            _job_update(job, UploadStatus.FAILED, error=str(exc))
            return

        _job_update(job, UploadStatus.PROCESSING)
        if job["cancel"]:
            _job_update(job, UploadStatus.CANCELLED)
            return

        summary = get_total_cost(file_path=mat_path)
        if "error" in summary:
            _job_update(job, UploadStatus.FAILED, error=summary["error"])
            return

        report = add_report(
            filename=resolved,
            csv_text="",
            row_count=summary.get("row_count", 0),
            total_cost=summary.get("total_cost", 0.0),
            file_size=file_size,
            file_path=mat_path,
        )
        _ensure_data_dir()
        # Ingest into persistent DuckDB file for instant queries.
        # Storage cap check before ingestion
        from report_store import get_cur_storage_usage
        _storage = get_cur_storage_usage()
        if _storage["cur_used_bytes"] + os.path.getsize(mat_path) * 4 > _CUR_STORAGE_CAP_BYTES:
            _job_update(job, UploadStatus.FAILED, error=f"Storage cap of 10 GB reached ({_storage['cur_used_gb']:.1f} GB used). Remove older reports to free space.")
            return
        _job_update(job, UploadStatus.INGESTING)
        parquet_dir = report_file_path(report["id"], ".parquet_dir")
        os.makedirs(parquet_dir, exist_ok=True)
        try:
            import duckdb as _ddb
            con = _ddb.connect(":memory:")
            try:
                con.execute("SET threads=2")
                con.execute("SET memory_limit='1.2GB'")
                src_safe = mat_path.replace("'", "''")
                dst = os.path.join(parquet_dir, "part-00001.parquet")
                dst_safe = dst.replace("'", "''")
                con.execute(f"""
                    COPY (SELECT * FROM read_csv_auto('{src_safe}', ignore_errors=true))
                    TO '{dst_safe}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """)
            finally:
                con.close()
            perm_path = parquet_dir
            # Update file_size to reflect actual Parquet size
            actual_parquet_size = sum(
                os.path.getsize(os.path.join(parquet_dir, f))
                for f in os.listdir(parquet_dir)
                if f.endswith(".parquet")
            )
            from report_store import _get_internal as _gri
            _r = _gri(report["id"])
            if _r:
                _r["file_size"] = actual_parquet_size
        except Exception as exc:
            logger.warning("Parquet conversion failed, storing gz instead: %s", exc)
            import shutil as _shutil
            perm_path = report_file_path(report["id"], ext)
            _shutil.move(mat_path, perm_path)
            mat_path = None
        set_report_path(report["id"], perm_path)

        await persist_report(report["id"])
        try:
            from report_store import cleanup_old_report_files
            await cleanup_old_report_files(keep_last=3)
        except Exception as _ce:
            logger.warning("cleanup_old_report_files failed: %s", _ce)
        invalidate_dashboard_cache(report["id"])
        invalidate_dashboard_cache(None)

        provider = FileUploadCURProvider(
            source_id=f"cur-{report['id']}",
            filename=resolved,
            csv_text="",
            record_count=summary.get("row_count", 0),
            total_cost=summary.get("total_cost", 0.0),
            file_size=file_size,
            report_id=report["id"],
            file_path=perm_path,
        )
        await reg.register_cur(provider)
        # Compute date range in background after registration — safe, scalar DuckDB query only
        try:
            provider.get_date_ranges()
            await reg.register_cur(provider)
        except Exception:
            pass
        _job_update(job, UploadStatus.READY, report_id=report["id"])
        asyncio.create_task(_precompute_all_tabs(report["id"]))

    except Exception as exc:
        _job_update(job, UploadStatus.FAILED, error=f"Unexpected error: {exc}")
    finally:
        for p in (tmp_path, mat_path):
            if p and os.path.exists(p):
                try:
                    os.unlink(p)
                except OSError:
                    pass


async def _process_folder_upload_job(job: dict, tmp_paths: list[str], filenames: list[str], total_size: int) -> None:
    """Background task: validate schemas, ingest all parts into one DuckDB file."""
    import os
    import shutil
    from report_store import _ensure_data_dir, add_report, persist_report, report_file_path, set_report_path
    from routes_dashboard import invalidate_dashboard_cache
    from tools.data_sources.file_providers import FileUploadCURProvider, materialize_cur
    from tools.duckdb_engine import get_total_cost, ingest_to_duckdb
    import duckdb
    reg = _require_registry()
    mat_paths: list[str] = []
    try:
        _job_update(job, UploadStatus.DECOMPRESSING)
        if job["cancel"]:
            _job_update(job, UploadStatus.CANCELLED)
            return
        # Materialize each part (validate gz, copy to temp)
        for i, (tmp_path, filename) in enumerate(zip(tmp_paths, filenames)):
            if job["cancel"]:
                _job_update(job, UploadStatus.CANCELLED)
                return
            try:
                mat_path, resolved, ext = materialize_cur(filename, tmp_path)
                mat_paths.append(mat_path)
            except ValueError as exc:
                _job_update(job, UploadStatus.FAILED, error=f"Part {filename}: {exc}")
                return

        _job_update(job, UploadStatus.PROCESSING)
        # Validate all parts have identical schema
        headers = []
        for mat_path in mat_paths:
            con = duckdb.connect(":memory:")
            try:
                safe = mat_path.replace("'", "''")
                cols = [r[0] for r in con.execute(
                    f"DESCRIBE SELECT * FROM read_csv_auto('{safe}', ignore_errors=true)"
                ).fetchall()]
                headers.append(cols)
            finally:
                con.close()
        if len(set(tuple(h) for h in headers)) > 1:
            _job_update(job, UploadStatus.FAILED, error="Part files have mismatched schemas — all parts must be from the same S3 export folder.")
            return
        # Validate all parts are from the same billing period
        billing_periods = set()
        for mat_path in mat_paths:
            con = duckdb.connect(":memory:")
            try:
                safe = mat_path.replace("'", "''")
                cols = [r[0] for r in con.execute(
                    f"DESCRIBE SELECT * FROM read_csv_auto('{safe}', ignore_errors=true)"
                ).fetchall()]
                if "bill_billing_period_start_date" in cols:
                    row = con.execute(
                        f"SELECT DISTINCT bill_billing_period_start_date FROM read_csv_auto('{safe}', ignore_errors=true) LIMIT 1"
                    ).fetchone()
                    if row and row[0]:
                        billing_periods.add(str(row[0]))
            finally:
                con.close()
        if len(billing_periods) > 1:
            _job_update(job, UploadStatus.FAILED, error=f"Part files span multiple billing periods ({', '.join(sorted(billing_periods))}) — all parts must be from the same S3 export folder.")
            return

        # Get summary from first part
        summary = get_total_cost(file_path=mat_paths[0])
        if "error" in summary:
            _job_update(job, UploadStatus.FAILED, error=summary["error"])
            return

        # Compute combined row count from all parts
        total_rows = 0
        total_cost = 0.0
        for mat_path in mat_paths:
            s = get_total_cost(file_path=mat_path)
            total_rows += s.get("row_count", 0)
            total_cost += s.get("total_cost", 0.0)

        folder_name = job["filename"]
        report = add_report(
            filename=folder_name,
            csv_text="",
            row_count=total_rows,
            total_cost=round(total_cost, 4),
            file_size=total_size,
            file_path=mat_paths[0],
        )
        _ensure_data_dir()

        # Storage cap check before ingestion
        from report_store import get_cur_storage_usage
        _storage = get_cur_storage_usage()
        _est_size = sum(os.path.getsize(p) for p in mat_paths) * 4
        if _storage["cur_used_bytes"] + _est_size > _CUR_STORAGE_CAP_BYTES:
            _job_update(job, UploadStatus.FAILED, error=f"Storage cap of 10 GB reached ({_storage['cur_used_gb']:.1f} GB used). Remove older reports to free space.")
            return
        _job_update(job, UploadStatus.INGESTING)
        parquet_dir = report_file_path(report["id"], ".parquet_dir")
        os.makedirs(parquet_dir, exist_ok=True)
        parquet_paths = []
        try:
            for i, mat_path in enumerate(mat_paths):
                _job_update(job, UploadStatus.INGESTING)
                dst = os.path.join(parquet_dir, f"part-{i+1:05d}.parquet")
                con = duckdb.connect(":memory:")
                try:
                    con.execute("SET threads=2")
                    con.execute("SET memory_limit='1.2GB'")
                    src_safe = mat_path.replace("'", "''")
                    dst_safe = dst.replace("'", "''")
                    con.execute(f"""
                        COPY (SELECT * FROM read_csv_auto('{src_safe}', ignore_errors=true))
                        TO '{dst_safe}' (FORMAT PARQUET, COMPRESSION ZSTD)
                    """)
                finally:
                    con.close()
                parquet_paths.append(dst)
        except Exception as exc:
            logger.warning("folder Parquet conversion failed: %s", exc)
            _job_update(job, UploadStatus.FAILED, error=f"Failed to convert parts to Parquet: {exc}")
            return
        perm_path = parquet_dir
        # Update file_size to reflect actual Parquet size (not original upload size)
        actual_parquet_size = sum(
            os.path.getsize(os.path.join(parquet_dir, f))
            for f in os.listdir(parquet_dir)
            if f.endswith(".parquet")
        )
        from report_store import _get_internal as _gri
        _r = _gri(report["id"])
        if _r:
            _r["file_size"] = actual_parquet_size

        set_report_path(report["id"], perm_path)
        await persist_report(report["id"])
        try:
            from report_store import cleanup_old_report_files
            await cleanup_old_report_files(keep_last=3)
        except Exception as _ce:
            logger.warning("cleanup_old_report_files failed: %s", _ce)
        invalidate_dashboard_cache(report["id"])
        invalidate_dashboard_cache(None)

        provider = FileUploadCURProvider(
            source_id=f"cur-{report['id']}",
            filename=folder_name,
            csv_text="",
            record_count=total_rows,
            total_cost=round(total_cost, 4),
            file_size=total_size,
            report_id=report["id"],
            file_path=perm_path,
        )
        await reg.register_cur(provider)
        _job_update(job, UploadStatus.READY, report_id=report["id"])
        asyncio.create_task(_precompute_all_tabs(report["id"]))

    except Exception as exc:
        _job_update(job, UploadStatus.FAILED, error=f"Unexpected error: {exc}")
    finally:
        for p in tmp_paths + mat_paths:
            if p and os.path.exists(p):
                try:
                    os.unlink(p)
                except OSError:
                    pass


@app.post("/data-sources/cur/upload")
async def ds_cur_upload(file: UploadFile) -> dict:
    """Upload a new CUR file (CSV / CSV.zip / Parquet) and register it as a
    data source. Reuses the existing report store so the dashboard picks it up."""
    from tools.data_sources.file_providers import UploadTooLarge, stream_upload_to_temp
    from starlette.background import BackgroundTask
    from fastapi.responses import JSONResponse

    filename = file.filename or "upload.csv"
    _fname_lower = filename.lower()
    _MAX_FLAT_CSV_BYTES = 200 * 1024 * 1024  # 200 MB — flat CSV only; gz has no cap
    if _fname_lower.endswith(".csv") and not _fname_lower.endswith(".csv.gz"):
        if file.size and file.size > _MAX_FLAT_CSV_BYTES:
            raise HTTPException(
                status_code=413,
                detail=(
                    f"Flat CSV files are limited to 200 MB. Your file is "
                    f"{round(file.size / 1024 / 1024, 1)} MB. "
                    f"Please compress it first using PowerShell: "
                    f"Compress-Archive -Path '<filename>.csv' -DestinationPath '<filename>.csv.zip' "
                    f"or on Linux: gzip -k '<filename>.csv' — then upload the compressed file."
                ),
            )
    try:
        tmp_path, file_size = await stream_upload_to_temp(
            file, max_bytes=_MAX_CUR_BYTES
        )
    except UploadTooLarge:
        raise HTTPException(status_code=413, detail="File too large (max 2 GB)")

    job = _make_job(filename)
    return JSONResponse(
        status_code=202,
        content={"job_id": job["job_id"], "filename": filename},
        background=BackgroundTask(
            _process_upload_job, job, tmp_path, filename, file_size
        ),
    )


@app.post("/data-sources/cur/upload-folder")
async def ds_cur_upload_folder(files: list[UploadFile]) -> dict:
    """Upload multiple CUR part files from one S3 export folder and ingest into one DuckDB report."""
    from tools.data_sources.file_providers import UploadTooLarge, stream_upload_to_temp
    from starlette.background import BackgroundTask
    from fastapi.responses import JSONResponse
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")
    _MAX_FOLDER_BYTES = 10 * 1024 * 1024 * 1024  # 10 GB total
    tmp_paths = []
    filenames = []
    total_size = 0
    for file in files:
        filename = file.filename or "upload.csv.gz"
        try:
            tmp_path, file_size = await stream_upload_to_temp(file, max_bytes=_MAX_CUR_BYTES)
            tmp_paths.append(tmp_path)
            filenames.append(filename)
            total_size += file_size
        except UploadTooLarge:
            for p in tmp_paths:
                import os
                if os.path.exists(p): os.unlink(p)
            raise HTTPException(status_code=413, detail=f"Part file {filename} too large (max 2 GB per part)")
        if total_size > _MAX_FOLDER_BYTES:
            for p in tmp_paths:
                import os
                if os.path.exists(p): os.unlink(p)
            raise HTTPException(status_code=413, detail="Total folder size exceeds 10 GB limit")
    # Use common filename prefix as the report name
    import re, os as _os
    base = _os.path.basename(filenames[0])
    prefix = base.replace('.csv.gz', '').replace('.gz', '').replace('.csv', '')
    prefix = re.sub(r'-\d+$', '', prefix)
    folder_name = f"{prefix} ({len(files)} parts)"
    job = _make_job(folder_name)
    return JSONResponse(
        status_code=202,
        content={"job_id": job["job_id"], "filename": folder_name},
        background=BackgroundTask(
            _process_folder_upload_job, job, tmp_paths, filenames, total_size
        ),
    )


@app.get("/data-sources/cur/upload-status/{job_id}")
async def ds_cur_upload_status(job_id: str) -> dict:
    """Poll the status of an async CUR upload job."""
    job = _upload_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Upload job not found")
    return job


@app.delete("/data-sources/cur/upload-status/{job_id}")
async def ds_cur_upload_cancel(job_id: str) -> dict:
    """Cancel an in-progress CUR upload job and clean up any partial files."""
    import os

    job = _upload_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Upload job not found")
    if job["status"] in (UploadStatus.READY.value, UploadStatus.FAILED.value, UploadStatus.CANCELLED.value):
        _upload_jobs.pop(job_id, None)
        return {"ok": True, "cancelled": False, "reason": "Job already completed"}
    job["cancel"] = True
    _upload_jobs.pop(job_id, None)
    return {"ok": True, "cancelled": True}


@app.post("/data-sources/inventory/upload")
async def ds_inventory_upload(file: UploadFile) -> dict:
    """Upload an inventory XLSX and set it as the active inventory source."""
    from uuid import uuid4

    from tools.data_sources.file_providers import FileUploadInventoryProvider

    reg = _require_registry()
    filename = file.filename or ""
    if not filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Inventory must be an .xlsx file")

    raw = await file.read()
    if len(raw) > _MAX_INVENTORY_BYTES:
        raise HTTPException(status_code=413, detail="Inventory file too large (max 10 MB)")

    try:
        provider = FileUploadInventoryProvider.from_upload(
            f"inv-{uuid4().hex[:8]}", filename, raw
        )
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Failed to read inventory: {exc}")

    if provider.get_resource_count() == 0:
        raise HTTPException(
            status_code=422,
            detail="No recognised inventory sheets/resources found. Expected sheets "
                   "like EC2, EBS, RDS, S3, Lambda, ALB/ELB, Redis, DynamoDB, EKS "
                   "with a join-key column and an account id column.",
        )

    meta = await reg.set_inventory(provider, raw)
    return {
        "ok": True,
        "source": meta.to_dict(),
        "resource_count": provider.get_resource_count(),
        "per_sheet_counts": provider.per_sheet_counts,
        "unmatched_count": provider.unmatched_count,
        "enrichment_enabled": settings.enable_inventory_enrichment,
    }


@app.get("/data-sources/storage-usage")
async def ds_storage_usage() -> dict:
    """Return CUR data storage usage and cap."""
    from report_store import get_cur_storage_usage
    usage = get_cur_storage_usage()
    usage["cap_bytes"] = _CUR_STORAGE_CAP_BYTES
    usage["cap_gb"] = round(_CUR_STORAGE_CAP_BYTES / 1024**3, 1)
    usage["used_pct"] = round(usage["cur_used_bytes"] / _CUR_STORAGE_CAP_BYTES * 100, 1) if _CUR_STORAGE_CAP_BYTES else 0.0
    return usage


@app.get("/data-sources/active-report-id")
async def ds_active_report_id() -> dict:
    """Return the report_id of the latest ready CUR report."""
    try:
        from database import SessionLocal
        from sqlalchemy import text as _t
        if SessionLocal:
            async with SessionLocal() as sess:
                row = await sess.execute(_t(
                    "SELECT id FROM cur_report WHERE status='ready' ORDER BY created_at DESC LIMIT 1"
                ))
                r = row.fetchone()
                if r:
                    return {"report_id": r.id}
        # Fallback to registry
        reg = _require_registry()
        active = reg.get_active_cur()
        if not active:
            return {"report_id": None}
        meta = active[0]
        report_id = (meta.extra or {}).get("report_id")
        return {"report_id": report_id}
    except Exception:
        return {"report_id": None}


@app.get("/data-sources/status")
async def ds_status() -> dict:
    """Registry status — sources, active selection, staleness, archives."""
    reg = _require_registry()
    status = reg.status()
    from routes_settings import _config as _sc
    status["enabled"] = bool(_sc.get("inventory_enrichment_enabled", False))
    return status


@app.get("/data-sources/inventory/coverage")
async def ds_inventory_coverage(report_id: int = Query(default=None)) -> dict:
    """Per-service match rates and cost coverage after enriching a CUR file with
    the loaded inventory. Defaults to the active CUR; pass ``report_id`` to score
    a specific uploaded file (used by the per-file match rate in the UI grid)."""
    from report_store import get_report_csv
    from tools.duckdb_engine import get_enrichment_summary
    from tools.inventory_enricher import build_enricher

    reg = _require_registry()
    if reg.get_inventory() is None:
        return {"active": False, "inventory_loaded": False,
                "enabled": settings.enable_inventory_enrichment}

    enricher = await build_enricher(reg)
    if not enricher.active:
        # Inventory present but enrichment disabled by feature flag.
        return {"active": False, "inventory_loaded": True,
                "enabled": settings.enable_inventory_enrichment}

    # Large-file guard: never load a multi-GB CUR into memory just to score
    # coverage. Sample the distinct account ids via DuckDB and do account-level
    # matching against the inventory (large/DBR exports have no resource id
    # column, so resource-level enrichment isn't possible anyway).
    _effective_report_id = report_id
    if _effective_report_id is None:
        from report_store import list_reports
        _rpts = list_reports()
        if _rpts:
            _effective_report_id = _rpts[0]["id"]
    if _effective_report_id is not None and _is_large_file(_effective_report_id):
        import duckdb

        from report_store import get_report_path
        from tools.duckdb_engine import _detect_account_col, _detect_resource_col

        path = get_report_path(_effective_report_id)
        con = duckdb.connect(":memory:")
        try:
            safe_path = str(path).replace("'", "''")
            if str(path).lower().endswith(".duckdb"):
                con.execute(f"ATTACH '{safe_path}' AS src (READ_ONLY)")
                con.execute("CREATE VIEW f AS SELECT * FROM src.cur_data")
            else:
                con.execute(
                    f"CREATE VIEW f AS SELECT * FROM read_csv_auto('{safe_path}', ignore_errors=true)"
                )
            cols = [r[0] for r in con.execute("DESCRIBE f").fetchall()]
            acct_col = _detect_account_col(cols)
            has_resource_col = _detect_resource_col(cols) is not None
            if not acct_col:
                return {"report_id": report_id, "active": True, "joinable": False,
                        "inventory_loaded": True, "enabled": True,
                        "enrichment_level": "none", "has_resource_column": False,
                        "reason": "No account id column found in CUR data"}
            qcol = _quote_ident(acct_col)
            cur_accounts = {
                str(r[0]) for r in
                con.execute(f'SELECT DISTINCT {qcol} FROM f').fetchall()
                if r[0] is not None and str(r[0]).strip() != ""
            }
        finally:
            con.close()
        # Account-level inventory accounts == those the enricher can resolve
        # via match_account (its per-account aggregated lookup).
        inv_accounts = set(enricher._account_lookup.keys())
        matched = cur_accounts & inv_accounts
        match_rate = len(matched) / len(cur_accounts) if cur_accounts else 0.0
        return {
            "report_id": report_id,
            "active": True,
            "joinable": True,
            "inventory_loaded": True,
            "enabled": True,
            "enrichment_level": "resource" if (has_resource_col and bool(getattr(enricher, '_lookup', {}))) else "account",
            "has_resource_column": has_resource_col,
            "matched_accounts": len(matched),
            "total_accounts": len(cur_accounts),
            "account_match_rate": round(match_rate * 100, 1),
            "spend_match_rate_pct": round(match_rate * 100, 1),
            "matched_count": len(matched),
            "unmatched_count": len(cur_accounts) - len(matched),
        }

    csv_text = get_report_csv(report_id) if report_id is not None else await _active_cur_csv(reg)
    if not csv_text:
        return {"active": True, "joinable": False, "inventory_loaded": True,
                "enabled": True, "reason": "No CUR data loaded yet"}

    summary = get_enrichment_summary(csv_text, enricher)
    summary["inventory_loaded"] = True
    summary["enabled"] = True
    return summary


@app.get("/data-sources/enriched-rows")
async def ds_enriched_rows(report_id: int):
    """Stream a report's rows with ``inv_*`` enrichment columns added, in the
    same NDJSON format as ``/reports/{id}/stream`` so the dashboard can consume
    it interchangeably. Falls back to plain rows when enrichment is inactive."""
    import json as _json

    # Large-file guard: streaming millions of enriched rows would materialise the
    # whole CUR. The dashboard's enriched panels use server-side aggregation
    # (/data-sources/enriched-summary) for these, so emit an explicit skip.
    if _is_large_file(report_id):
        async def empty_stream():
            yield _json.dumps({
                "total": 0,
                "skipped": True,
                "reason": "Large file — enriched panels use server-side aggregation",
            }) + "\n"

        return StreamingResponse(empty_stream(), media_type="application/x-ndjson")

    from report_store import get_report_rows
    from tools.data_sources.registry import get_registry
    from tools.duckdb_engine import _detect_account_col, _detect_resource_col
    from tools.inventory_enricher import build_enricher

    src_rows = get_report_rows(report_id)
    if src_rows is None:
        raise HTTPException(status_code=404, detail=f"Report {report_id} not found")

    # Copy so we never mutate the shared report-store rows.
    rows = [dict(r) for r in src_rows]

    enricher = await build_enricher(get_registry())
    if enricher.active and rows:
        cols = list(rows[0].keys())
        account_col = _detect_account_col(cols)
        resource_col = _detect_resource_col(cols)
        # resource_col may be None (legacy CUR without a resource id) — the
        # enricher then falls back to account-level enrichment.
        if account_col:
            enricher.enrich_query_result(rows, account_col, resource_col)

    async def generate():
        yield _json.dumps({"total": len(rows)}) + "\n"
        for i in range(0, len(rows), 50):
            chunk = rows[i:i + 50]
            yield _json.dumps({"rows": chunk, "offset": i, "count": len(chunk)}) + "\n"
            await asyncio.sleep(0)

    return StreamingResponse(
        generate(),
        media_type="application/x-ndjson",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )


@app.get("/data-sources/enriched-values")
async def ds_enriched_values(report_id: int) -> dict:
    """Distinct values for each ``inv_*`` enrichment field, computed in DuckDB.

    Lets the dashboard populate enriched filter dropdowns without streaming the
    full (potentially 200k+) row set. Returns ``{"report_id", "values"}`` where
    ``values`` maps each present ``inv_*`` column to its sorted distinct values
    (empty ``values`` when enrichment is inactive)."""
    # Large-file guard: never read the whole CUR into memory as csv_text. Read
    # distinct account ids straight off disk via a DuckDB native view (large/DBR
    # exports carry no inv_* columns, so accounts is the only usable dropdown).
    if _is_large_file(report_id):
        import duckdb

        from report_store import get_report_path
        from tools.duckdb_engine import _detect_account_col

        path = get_report_path(report_id)
        import tempfile, os
        _tmp_db = tempfile.mktemp(suffix=".duckdb")
        con = duckdb.connect(_tmp_db)
        values: dict[str, list] = {}
        try:
            safe_path = str(path).replace("'", "''")
            con.execute(
                f"CREATE VIEW f AS SELECT * FROM read_csv_auto('{safe_path}', ignore_errors=true)"
            )
            cols = [r[0] for r in con.execute("DESCRIBE f").fetchall()]
            acct_col = _detect_account_col(cols)
            if acct_col:
                qcol = _quote_ident(acct_col)
                rows = con.execute(
                    f'SELECT DISTINCT CAST({qcol} AS VARCHAR) AS a FROM f '
                    f'WHERE {qcol} IS NOT NULL AND CAST({qcol} AS VARCHAR) <> \'\' '
                    f"ORDER BY a"
                ).fetchall()
                values["accounts"] = [r[0] for r in rows]
        finally:
            con.close()
            try:
                os.unlink(_tmp_db)
            except Exception:
                pass
        return {"report_id": report_id, "values": values}

    from report_store import get_report_csv
    from tools.data_sources.registry import get_registry
    from tools.duckdb_engine import _load_df
    from tools.inventory_enricher import INV_COLUMNS, build_enricher

    csv_text = get_report_csv(report_id)
    if csv_text is None:
        raise HTTPException(status_code=404, detail=f"Report {report_id} not found")

    values: dict[str, list] = {}
    enricher = await build_enricher(get_registry())
    if enricher.active:
        df, con = _load_df(csv_text, enricher=enricher)
        try:
            present = set(df.columns)
            for inv_col in INV_COLUMNS:
                if inv_col not in present:
                    continue
                rows = con.execute(
                    f'SELECT DISTINCT "{inv_col}" FROM cur_data '
                    f"WHERE \"{inv_col}\" IS NOT NULL "
                    f"AND CAST(\"{inv_col}\" AS VARCHAR) <> '' ORDER BY 1"
                ).fetchall()
                values[inv_col] = [str(r[0]) for r in rows]
        finally:
            con.close()

    return {"report_id": report_id, "values": values}


@app.get("/data-sources/enriched-summary")
async def ds_enriched_summary(report_id: int) -> dict:
    """Server-side enriched aggregations for the dashboard.

    Computes the Environment / Customer / Application / Budget-Code spend
    breakdowns, per-attribute tag coverage and a native-vs-enriched before/after
    using ``inv_*`` columns (inventory primary, native CUR tag fallback), all via
    DuckDB GROUP BY over the enriched frame. Returns a single small JSON payload
    so the dashboard never has to stream 200k+ rows to the browser to aggregate
    them client-side."""
    # Large-file guard: skip the full enriched aggregation. These exports are
    # account-level only (no resource id column), so report that honestly
    # instead of attempting a resource-level enrichment over a multi-GB file.
    # Guard against large files — check both file size (on-disk path) and row
    # count (in-memory fallback). Either exceeding the threshold blocks the
    # full enriched aggregation which would materialise a multi-GB CSV.
    from report_store import get_latest_meta
    _meta = next((r for r in __import__('report_store').list_reports() if r["id"] == report_id), None)
    _row_count_large = (_meta["row_count"] > 500_000) if _meta else False
    if _is_large_file(report_id) or _row_count_large:
        from report_store import get_report_path
        from tools.data_sources.registry import get_registry
        from tools.inventory_enricher import build_enricher
        import duckdb as _duckdb
        from tools.duckdb_engine import _detect_account_col, _detect_cost_col
        _reg = get_registry()
        _enricher = await build_enricher(_reg)
        _inv_accounts = set(_enricher._account_lookup.keys()) if _enricher and _enricher.active else set()
        _path = get_report_path(report_id)
        _spend_match_pct = None
        _inv_cost = 0.0
        _total_cost = 0.0
        if _path and _inv_accounts:
            try:
                _con = _duckdb.connect(":memory:")
                _safe = str(_path).replace("'", "''")
                if str(_path).lower().endswith(".duckdb"):
                    _con.execute(f"ATTACH '{_safe}' AS src (READ_ONLY)")
                    _con.execute("CREATE VIEW f AS SELECT * FROM src.cur_data")
                else:
                    _con.execute(f"CREATE VIEW f AS SELECT * FROM read_csv_auto('{_safe}', ignore_errors=true)")
                _cols = [r[0] for r in _con.execute("DESCRIBE f").fetchall()]
                _acct_col = _detect_account_col(_cols)
                _cost_col = _detect_cost_col(_cols)
                if _acct_col and _cost_col:
                    _qa = _quote_ident(_acct_col)
                    _qc = _quote_ident(_cost_col)
                    _total_cost = float(_con.execute(f"SELECT SUM({_qc}) FROM f").fetchone()[0] or 0)
                    _placeholders = ",".join(["?"] * len(_inv_accounts))
                    _inv_cost = float(_con.execute(
                        f"SELECT SUM({_qc}) FROM f WHERE CAST({_qa} AS VARCHAR) IN ({_placeholders})",
                        list(_inv_accounts)
                    ).fetchone()[0] or 0)
                    _spend_match_pct = round(_inv_cost / _total_cost * 100, 1) if _total_cost else 0.0
                _con.close()
            except Exception:
                pass
        from tools.duckdb_engine import _detect_resource_col, get_per_service_match_rate, get_before_after_coverage
        _has_res_col = _detect_resource_col(_cols) is not None if '_cols' in dir() else False
        _has_res_inv = bool(_enricher and _enricher.active and getattr(_enricher, '_lookup', {}))
        _enrichment_level = "resource" if (_has_res_col and _has_res_inv) else "account"
        _path = get_report_path(report_id)
        _per_service, _unmatched_top = get_per_service_match_rate(_path, _enricher) if _has_res_inv else ([], [])
        _before_after = get_before_after_coverage(_path, _enricher) if _enricher and _enricher.active else {}
        return {
            "report_id": report_id,
            "active": True,
            "joinable": True,
            "enrichment_level": _enrichment_level,
            "has_resource_column": _has_res_col,
            "spend_match_rate_pct": _spend_match_pct,
            "matched_count": round(_inv_cost, 2),
            "unmatched_count": round(_total_cost - _inv_cost, 2),
            "per_service": _per_service,
            "unmatched_top": _unmatched_top,
            "before_after": _before_after,
        }

    from report_store import get_report_csv, get_report_path
    from tools.data_sources.registry import get_registry
    from tools.duckdb_engine import get_enriched_summary
    from tools.inventory_enricher import build_enricher

    # Prefer the on-disk file (file-path pipeline); fall back to stored csv_text.
    file_path = get_report_path(report_id)
    csv_text = None
    if file_path is None:
        csv_text = get_report_csv(report_id)
        if csv_text is None:
            raise HTTPException(status_code=404, detail=f"Report {report_id} not found")

    enricher = await build_enricher(get_registry())
    summary = get_enriched_summary(csv_text, enricher=enricher, file_path=file_path)
    summary["report_id"] = report_id
    return summary


@app.post("/data-sources/cur/{source_id}/active")
async def ds_set_active_cur(source_id: str) -> dict:
    """Make a single CUR source the sole active one (used by the grid's
    Set Active button)."""
    reg = _require_registry()
    if source_id not in {m.source_id for m in reg.list_cur()}:
        raise HTTPException(status_code=404, detail=f"CUR source {source_id} not found")
    await reg.set_active_cur([source_id])
    return {"ok": True, "active": source_id}


@app.get("/data-sources/s3/status")
async def ds_s3_status() -> dict:
    """Return S3 sync status — latest available folder vs last synced."""
    import boto3
    from routes_settings import _config as _sc
    bucket = _sc.get("s3_bucket", "")
    prefix = _sc.get("s3_prefix", "")
    region = _sc.get("s3_region", "us-east-1")
    if not bucket or not prefix:
        return {"s3_configured": False}
    try:
        s3 = boto3.client("s3", region_name=region)
        billing_period = prefix.rstrip("/")
        resp = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=billing_period + "/",
            Delimiter="/"
        )
        prefixes = sorted([p["Prefix"] for p in resp.get("CommonPrefixes", [])])
        if not prefixes:
            return {"s3_configured": True, "latest_s3_folder": None}
        latest = prefixes[-1]
        files = s3.list_objects_v2(Bucket=bucket, Prefix=latest)
        contents = files.get("Contents", [])
        total_mb = sum(f["Size"] for f in contents) // 1024 // 1024
        # Extract timestamp from folder name
        folder_name = latest.rstrip("/").split("/")[-1]
        latest_ts = folder_name.split("T")[0] + "T" + folder_name.split("T")[1][:8] + "Z" if "T" in folder_name else folder_name
        # Estimate sync time:
        # - S3 download: ~22 MB/s compressed = total_mb / 22 seconds
        # - DuckDB ingest: ~5s per file
        # - Pre-aggregation: ~30s fixed
        download_secs = total_mb / 22
        ingest_secs = len(contents) * 5
        preaggregate_secs = 30
        est_secs = int(download_secs + ingest_secs + preaggregate_secs)
        est_mins = round(est_secs / 60, 1)
        last_synced_at = _sc.get("s3_last_synced_at", "")
        # Check if latest S3 files are newer than last sync
        latest_file_modified = max((f["LastModified"] for f in contents), default=None)
        latest_file_ts = latest_file_modified.isoformat() if latest_file_modified else None
        is_latest = False
        if last_synced_at and latest_file_modified:
            from datetime import datetime, timezone
            try:
                last_dt = datetime.fromisoformat(last_synced_at.replace("Z", "+00:00"))
                is_latest = latest_file_modified <= last_dt
            except Exception:
                is_latest = False
        return {
            "s3_configured": True,
            "bucket": bucket,
            "prefix": prefix,
            "latest_s3_folder": folder_name,
            "latest_s3_timestamp": latest_ts,
            "latest_s3_file_modified": latest_file_ts,
            "latest_s3_file_count": len(contents),
            "latest_s3_size_mb": total_mb,
            "last_synced_at": last_synced_at,
            "is_latest": is_latest,
            "estimated_sync_minutes": est_mins,
        }
    except Exception as e:
        logger.warning("ds_s3_status failed: %s", e)
        return {"s3_configured": True, "error": str(e)}


_s3_sync_jobs: dict = {}

@app.get("/data-sources/s3/browse")
async def ds_s3_browse(year: str | None = None, month: str | None = None) -> dict:
    """Browse S3 bucket — cascading Year → Month → Day structure.
    Optional filters: year (e.g. '2026'), month (e.g. '07')
    Returns latest export per day only."""
    import boto3
    from routes_settings import _config as _sc
    bucket = _sc.get("s3_bucket", "")
    prefix = _sc.get("s3_prefix", "")
    region = _sc.get("s3_region", "us-east-1")
    if not bucket or not prefix:
        return {"s3_configured": False}
    try:
        s3 = boto3.client("s3", region_name=region)
        base_prefix = prefix.rstrip("/")
        if "BILLING_PERIOD=" in base_prefix:
            base_prefix = base_prefix.rsplit("BILLING_PERIOD=", 1)[0]
        else:
            base_prefix = base_prefix + "/"
        # List all billing periods
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=base_prefix, Delimiter="/")
        all_periods = sorted([
            p["Prefix"].rstrip("/").split("/")[-1]
            for p in resp.get("CommonPrefixes", [])
            if "BILLING_PERIOD=" in p["Prefix"]
        ], reverse=True)
        # Filter by year/month if provided
        filtered_periods = []
        for bp in all_periods:
            period_val = bp.replace("BILLING_PERIOD=", "")  # e.g. 2026-07
            p_year, p_month = period_val.split("-") if "-" in period_val else (period_val, "")
            if year and p_year != year:
                continue
            if month and p_month != month:
                continue
            filtered_periods.append((bp, period_val, p_year, p_month))
        # Build year → month → day structure
        tree = {}
        for bp, period_val, p_year, p_month in filtered_periods:
            bp_prefix = base_prefix + bp + "/"
            bp_resp = s3.list_objects_v2(Bucket=bucket, Prefix=bp_prefix, Delimiter="/")
            all_folders = sorted([p["Prefix"] for p in bp_resp.get("CommonPrefixes", [])], reverse=True)
            # Group by date — keep only latest folder per date
            by_date = {}
            for fp in all_folders:
                folder_name = fp.rstrip("/").split("/")[-1]
                date = folder_name.split("T")[0] if "T" in folder_name else folder_name
                if date not in by_date:
                    by_date[date] = fp  # first = latest (sorted reverse)
            # For each unique date get metadata
            days = []
            for date in sorted(by_date.keys(), reverse=True):
                fp = by_date[date]
                files_resp = s3.list_objects_v2(Bucket=bucket, Prefix=fp)
                contents = [f for f in files_resp.get("Contents", []) if f["Key"].endswith(".csv.gz") or f["Key"].endswith(".csv")]
                total_mb = sum(f["Size"] for f in contents) // 1024 // 1024
                latest_modified = max((f["LastModified"] for f in contents), default=None)
                est_mins = round((total_mb / 22 + len(contents) * 24 + 30) / 60, 1)
                days.append({
                    "date": date,
                    "prefix": fp,
                    "file_count": len(contents),
                    "size_mb": total_mb,
                    "latest_modified": latest_modified.isoformat() if latest_modified else None,
                    "est_sync_mins": est_mins,
                })
            if p_year not in tree:
                tree[p_year] = {}
            tree[p_year][p_month] = {
                "billing_period": bp,
                "period_val": period_val,
                "total_days": len(all_folders),
                "unique_days": len(days),
                "days": days,
            }
        # Get available years for filter dropdown
        all_years = sorted(set(
            bp.replace("BILLING_PERIOD=", "").split("-")[0]
            for bp in all_periods
            if "BILLING_PERIOD=" in bp
        ), reverse=True)
        return {
            "s3_configured": True,
            "bucket": bucket,
            "base_prefix": base_prefix,
            "available_years": all_years,
            "tree": tree,
        }
    except Exception as e:
        logger.warning("ds_s3_browse failed: %s", e)
        return {"s3_configured": True, "error": str(e)}

@app.post("/data-sources/s3/sync")
async def ds_s3_sync(body: dict = None) -> dict:
    """Trigger a background S3 sync job.
    Optional body: {"folder_prefix": "full/s3/prefix/to/specific/folder/"}
    If not provided, uses latest folder automatically.
    """
    import uuid, asyncio
    from routes_settings import _config as _sc
    bucket = _sc.get("s3_bucket", "")
    prefix = _sc.get("s3_prefix", "")
    region = _sc.get("s3_region", "us-east-1")
    if not bucket or not prefix:
        raise HTTPException(status_code=400, detail="S3 not configured — set bucket and prefix in Settings")
    folder_prefix = (body or {}).get("folder_prefix")
    job_id = str(uuid.uuid4())
    _s3_sync_jobs[job_id] = {"status": "started", "progress": "Initialising..."}
    asyncio.create_task(_run_s3_sync(job_id, bucket, prefix, region, folder_prefix=folder_prefix))
    return {"job_id": job_id, "status": "started"}


@app.get("/data-sources/s3/sync-status/{job_id}")
async def ds_s3_sync_status(job_id: str) -> dict:
    """Poll S3 sync job status."""
    job = _s3_sync_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


async def _run_s3_sync(job_id: str, bucket: str, prefix: str, region: str, folder_prefix: str | None = None) -> None:
    """Background S3 sync: find latest folder (or use specified folder), convert to Parquet, pre-aggregate."""
    import boto3, tempfile, os, shutil
    from routes_settings import _config as _sc, _upsert
    from routes_dashboard import invalidate_tab_cache, invalidate_db_tab_cache

    def update(status: str, progress: str):
        _s3_sync_jobs[job_id] = {"status": status, "progress": progress}

    try:
        update("running", "Checking S3 for latest data...")
        s3 = boto3.client("s3", region_name=region)
        if folder_prefix:
            # Use specified folder directly
            latest = folder_prefix if folder_prefix.endswith("/") else folder_prefix + "/"
            folder_name = latest.rstrip("/").split("/")[-1]
        else:
            # Find latest folder automatically
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix.rstrip("/") + "/", Delimiter="/")
            prefixes = sorted([p["Prefix"] for p in resp.get("CommonPrefixes", [])])
            if not prefixes:
                update("failed", "No folders found in S3")
                return
            latest = prefixes[-1]
            folder_name = latest.rstrip("/").split("/")[-1]
        # Check if latest S3 files are newer than last sync
        files_obj = s3.list_objects_v2(Bucket=bucket, Prefix=latest)
        contents_check = files_obj.get("Contents", [])
        last_synced_at = _sc.get("s3_last_synced_at", "")
        if last_synced_at and contents_check:
            from datetime import datetime, timezone
            try:
                last_dt = datetime.fromisoformat(last_synced_at.replace("Z", "+00:00"))
                latest_mod = max(f["LastModified"] for f in contents_check)
                last_file_count = int(_sc.get("s3_last_synced_file_count", 0))
                current_file_count = len([f for f in contents_check if f["Key"].endswith(".csv.gz") or f["Key"].endswith(".csv")])
                files_unchanged = latest_mod <= last_dt
                count_unchanged = current_file_count == last_file_count
                if files_unchanged and count_unchanged:
                    update("complete", f"Already up to date — files unchanged since last sync")
                    return
            except Exception:
                pass
        files = files_obj
        contents = [f for f in files.get("Contents", []) if f["Key"].endswith(".csv.gz") or f["Key"].endswith(".csv")]
        if not contents:
            update("failed", "No CSV files found in latest folder")
            return
        update("running", f"Downloading {len(contents)} files from S3...")
        # Download to temp directory
        tmp_dir = tempfile.mkdtemp(prefix="s3_sync_")
        try:
            tmp_paths = []
            for i, obj in enumerate(contents):
                filename = obj["Key"].split("/")[-1]
                tmp_path = os.path.join(tmp_dir, filename)
                update("running", f"Downloading {i+1}/{len(contents)}: {filename}")
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda k=obj["Key"], p=tmp_path: s3.download_file(bucket, k, p)
                )
                tmp_paths.append(tmp_path)
            update("running", "Ingesting to local database...")
            # Use existing folder upload pipeline
            from report_store import add_report, persist_report, report_file_path, set_report_path
            from tools.duckdb_engine import get_total_cost
            folder_label = folder_name.split("T")[0] + f" ({len(tmp_paths)} parts)"
            import os as _os
            total_size = sum(_os.path.getsize(p) for p in tmp_paths)
            loop = asyncio.get_event_loop()
            # Find existing auto-sync report to replace (avoid accumulating reports)
            from report_store import list_reports, delete_report
            existing_auto = next((r for r in list_reports() if r.get("sync_type") == "auto"), None)
            if existing_auto:
                logger.info("S3 sync: replacing existing auto-sync report %d", existing_auto["id"])
                try:
                    await delete_report(existing_auto["id"])
                except Exception as e:
                    logger.warning("Failed to delete existing auto report: %s", e)
            report = add_report(
                filename=folder_label,
                csv_text="",
                row_count=0,
                total_cost=0.0,
                file_size=total_size,
                file_path=None,
                sync_type="auto",
            )
            # Convert each CSV.gz to Parquet sequentially (memory bounded ~1.2GB peak)
            import os as _os
            parquet_dir = report_file_path(report["id"], ".parquet_dir")
            _os.makedirs(parquet_dir, exist_ok=True)
            parquet_paths = []
            for i, tmp_path in enumerate(tmp_paths):
                parquet_path = _os.path.join(parquet_dir, f"part-{i+1:05d}.parquet")
                update("running", f"Converting to Parquet ({i+1}/{len(tmp_paths)})...")
                def _convert(src=tmp_path, dst=parquet_path):
                    import duckdb as _ddb
                    con = _ddb.connect(":memory:")
                    try:
                        con.execute("SET threads=2")
                        con.execute("SET memory_limit='1.2GB'")
                        src_safe = src.replace("'", "''")
                        dst_safe = dst.replace("'", "''")
                        con.execute(f"""
                            COPY (SELECT * FROM read_csv_auto('{src_safe}', ignore_errors=true))
                            TO '{dst_safe}' (FORMAT PARQUET, COMPRESSION ZSTD)
                        """)
                    finally:
                        con.close()
                await loop.run_in_executor(None, _convert)
                parquet_paths.append(parquet_path)
            # Compute totals from all Parquet files (instant)
            update("running", "Computing report totals...")
            def _compute_totals():
                import duckdb as _ddb
                paths_sql = "', '".join(p.replace("'", "''") for p in parquet_paths)
                con = _ddb.connect(":memory:")
                try:
                    result = con.execute(f"""
                        SELECT COUNT(*), SUM(line_item_unblended_cost)
                        FROM read_parquet(['{paths_sql}'])
                    """).fetchone()
                    return {"row_count": result[0] or 0, "total_cost": float(result[1] or 0)}
                finally:
                    con.close()
            summary = await loop.run_in_executor(None, _compute_totals)
            # Update report with actual totals
            from report_store import _get_internal
            rep = _get_internal(report["id"])
            if rep:
                rep["row_count"] = summary.get("row_count", 0)
                rep["total_cost"] = round(summary.get("total_cost", 0.0), 4)
                rep["file_size"] = sum(_os.path.getsize(p) for p in parquet_paths)
            # Store parquet directory as the report path
            set_report_path(report["id"], parquet_dir)
            await persist_report(report["id"])
            try:
                from report_store import cleanup_old_report_files
                await cleanup_old_report_files(keep_last=3)
            except Exception as _ce:
                logger.warning("cleanup_old_report_files failed: %s", _ce)
            # Update last synced info
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc).isoformat()
            await _upsert("s3_last_synced_folder", folder_name)
            await _upsert("s3_last_synced_at", now)
            await _upsert("s3_last_synced_file_count", str(len(contents)))
            _sc["s3_last_synced_folder"] = folder_name
            _sc["s3_last_synced_at"] = now
            _sc["s3_last_synced_file_count"] = str(len(contents))
            # Invalidate cache and pre-aggregate
            update("running", "Pre-aggregating dashboard data...")
            invalidate_tab_cache(report["id"])
            await invalidate_db_tab_cache(report["id"])
            await _precompute_all_tabs(report["id"])
            update("complete", f"Sync complete — {folder_label} loaded and ready")
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception as e:
        logger.exception("S3 sync failed")
        update("failed", f"Sync failed: {str(e)}")


def _ingest_s3_parts(file_paths: list[str], duckdb_path: str) -> None:
    """Ingest multiple S3 CUR part files into one DuckDB file."""
    import duckdb
    safe_paths = [p.replace("'", "''") for p in file_paths]
    paths_sql = "['" + "', '".join(safe_paths) + "']"
    con = duckdb.connect(database=duckdb_path)
    con.execute("PRAGMA threads=4")
    con.execute(f"CREATE TABLE cur_data AS SELECT * FROM read_csv_auto({paths_sql}, ignore_errors=true)")
    con.close()


# ── Job Management Endpoints ──────────────────────────────────────────────────
@app.get("/jobs")
async def list_jobs() -> list:
    if not hasattr(_jobs_module, '_jobs'):
        return []
    return [{"id": j["id"], "name": j["name"], "description": j["description"]}
            for j in _jobs_module._jobs.values()]

@app.get("/jobs/{job_id}/runs")
async def get_job_runs(job_id: str, limit: int = 50) -> list:
    return await _jobs_module.get_runs(job_id=job_id, limit=limit)

@app.get("/jobs/{job_id}/schedules")
async def get_job_schedules(job_id: str) -> list:
    return await _jobs_module.get_schedules(job_id=job_id)

@app.post("/jobs/{job_id}/trigger")
async def trigger_job(job_id: str) -> dict:
    return await _jobs_module.trigger_job(job_id, triggered_by="manual")

@app.post("/jobs/{job_id}/schedules")
async def create_job_schedule(job_id: str, body: dict) -> dict:
    cron = body.get("cron_expression", "0 * * * *")
    enabled = body.get("enabled", True)
    return await _jobs_module.create_schedule(job_id, cron, enabled)

@app.put("/jobs/{job_id}/schedules/{schedule_id}")
async def update_job_schedule(job_id: str, schedule_id: int, body: dict) -> dict:
    cron = body.get("cron_expression", "0 * * * *")
    enabled = body.get("enabled", True)
    return await _jobs_module.update_schedule(schedule_id, cron, enabled)

@app.delete("/jobs/{job_id}/schedules/{schedule_id}")
async def delete_job_schedule(job_id: str, schedule_id: int) -> dict:
    return await _jobs_module.delete_schedule(schedule_id)

@app.get("/runs")
async def get_all_runs(limit: int = 50) -> list:
    return await _jobs_module.get_runs(limit=limit)


@app.post("/internal/sync")
async def internal_sync(body: dict = None) -> dict:
    """Called by Job Server to trigger S3 sync. Runs in background."""
    import asyncio
    from routes_settings import _config as _sc
    bucket = _sc.get("s3_bucket", "")
    prefix = _sc.get("s3_prefix", "")
    region = _sc.get("s3_region", "us-east-1")
    if not bucket or not prefix:
        raise HTTPException(status_code=400, detail="S3 not configured")
    import uuid
    job_id = str(uuid.uuid4())
    _s3_sync_jobs[job_id] = {"status": "started", "progress": "Initialising..."}
    asyncio.create_task(_run_s3_sync(job_id, bucket, prefix, region))
    # Wait for completion (job server expects synchronous response)
    import asyncio as _asyncio
    for _ in range(120):  # wait up to 10 minutes
        await _asyncio.sleep(5)
        job = _s3_sync_jobs.get(job_id, {})
        if job.get("status") in ("complete", "failed"):
            if job["status"] == "failed":
                raise HTTPException(status_code=500, detail=job.get("progress", "Sync failed"))
            return {"ok": True, "message": job.get("progress", "Sync complete")}
    raise HTTPException(status_code=504, detail="Sync timeout")


@app.delete("/data-sources/cur/{source_id}")
async def ds_delete_cur(source_id: str) -> dict:
    from report_store import delete_report
    from routes_dashboard import invalidate_dashboard_cache

    reg = _require_registry()
    # Resolve the underlying report_id from the source meta.
    report_id: int | None = None
    found = False
    for m in reg.list_cur():
        if m.source_id == source_id:
            found = True
            rid = m.extra.get("report_id")
            report_id = int(rid) if rid is not None else None
            break
    if not found:
        raise HTTPException(status_code=404, detail=f"CUR source {source_id} not found")

    # report_store is the source of truth — delete the report there, then
    # reconcile the registry so its CUR sources match the remaining reports.
    if report_id is not None:
        await delete_report(report_id)
    await reg.sync_registry_from_reports()

    if report_id is not None:
        invalidate_dashboard_cache(report_id)
    invalidate_dashboard_cache(None)
    return {"ok": True, "deleted": source_id}


@app.delete("/data-sources/inventory")
async def ds_delete_inventory() -> dict:
    reg = _require_registry()
    removed = await reg.delete_inventory()
    if not removed:
        raise HTTPException(status_code=404, detail="No inventory loaded")
    return {"ok": True}


@app.post("/invoke", response_model=InvokeResponse)
async def invoke(
    body: InvokeRequest,
    x_anthropic_key: str | None = Header(default=None),
) -> InvokeResponse:
    ctx = body.context

    if "raw_data" in ctx:
        source = FileSource(ctx["raw_data"])
        _cur_cache[body.session_id] = await source.load_csv()
    elif "cur_csv" in ctx:
        _cur_cache[body.session_id] = ctx["cur_csv"]
    elif "cur_data" in ctx:
        rows: list[dict] = ctx["cur_data"]
        if rows:
            fieldnames = list(rows[0].keys())
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            _cur_cache[body.session_id] = buf.getvalue()
    # Load from DB by report_id if session cache is still empty. Skip large
    # files: loading a multi-GB CUR as csv_text would OOM, and the chat tools
    # are csv_text-only. has_data stays False so the agent tells the user to use
    # the dashboard (file-path pipeline) for large reports.
    if ctx.get("report_id"):
        _session_report_map[body.session_id] = int(ctx["report_id"])
    if not _cur_cache.get(body.session_id) and ctx.get("report_id"):
        _rid = int(ctx["report_id"])
        if _is_large_file(_rid):
            logger.info("invoke: skipping chat cache load for large report %s — tools will use file_path instead", _rid)
        else:
            from report_store import get_report_csv
            csv_text = get_report_csv(_rid)
            if csv_text:
                _cur_cache[body.session_id] = csv_text

    has_data = bool(_cur_cache.get(body.session_id)) or body.session_id in _session_report_map

    response_text, tokens = await _runner.run(
        user_message=body.user_message,
        context={"session_id": body.session_id, "has_data": has_data},
        history=body.history,
        api_key=x_anthropic_key,
    )

    chart_data = None
    if "```chart" in response_text:
        try:
            start = response_text.index("```chart") + 8
            end = response_text.index("```", start)
            chart_data = json.loads(response_text[start:end].strip())
            response_text = response_text[: response_text.index("```chart")].strip()
        except Exception:
            pass

    metadata: dict[str, Any] = {"tokens_used": tokens}
    if chart_data is not None:
        metadata["chart"] = chart_data

    return InvokeResponse(
        session_id=body.session_id,
        response=response_text,
        metadata=metadata,
    )


@app.post("/invoke/stream-insights")
async def stream_insights(
    body: InvokeRequest,
    x_anthropic_key: str | None = Header(default=None),
):
    prompt = body.user_message

    async def event_stream():
        try:
            async for chunk in _llm_stream(
                model=settings.model,
                max_tokens=8192,
                messages=(
                    [
                        {"role": "user", "content": prompt},
                        {"role": "assistant", "content": body.context.get("continuation_of", "")},
                        {"role": "user", "content": "Please continue the analysis from where you left off. Do not repeat what was already written."},
                    ]
                    if body.context.get("continuation_of")
                    else [{"role": "user", "content": prompt}]
                ),
                api_key=x_anthropic_key or settings.anthropic_api_key,
                session_id=body.session_id,
            ):
                if chunk.startswith("[STOP_REASON]"):
                    yield f"data: {chunk}\n\n"
                else:
                    yield f"data: {chunk.replace(chr(10), chr(92)+'n')}\n\n"
            yield "data: [DONE]\n\n"
        except Exception as e:
            yield f"data: [ERROR] {str(e)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/invoke/stream")
async def invoke_stream(
    body: InvokeRequest,
    x_anthropic_key: str | None = Header(default=None),
):
    ctx = body.context
    if "raw_data" in ctx:
        source = FileSource(ctx["raw_data"])
        _cur_cache[body.session_id] = await source.load_csv()
    elif "cur_csv" in ctx:
        _cur_cache[body.session_id] = ctx["cur_csv"]
    elif "cur_data" in ctx:
        rows: list[dict] = ctx["cur_data"]
        if rows:
            fieldnames = list(rows[0].keys())
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            _cur_cache[body.session_id] = buf.getvalue()
    if ctx.get("report_id"):
        _session_report_map[body.session_id] = int(ctx["report_id"])
    # Load from DB by report_id if session cache is still empty. Skip large
    # files: loading a multi-GB CUR as csv_text would OOM, and the chat tools
    # are csv_text-only. has_data stays False so the agent tells the user to use
    # the dashboard (file-path pipeline) for large reports.
    if not _cur_cache.get(body.session_id) and ctx.get("report_id"):
        _rid = int(ctx["report_id"])
        if _is_large_file(_rid):
            logger.info("invoke: skipping chat cache load for large report %s", _rid)
        else:
            from report_store import get_report_csv
            csv_text = get_report_csv(_rid)
            if csv_text:
                _cur_cache[body.session_id] = csv_text
    is_tab_chat = bool(ctx.get("tab_chat"))
    has_data = bool(_cur_cache.get(body.session_id)) or body.session_id in _session_report_map
    pre_aggregated = ctx.get("pre_aggregated")
    logger.info("invoke_stream: is_tab_chat=%s pre_aggregated=%s session=%s", is_tab_chat, bool(pre_aggregated), body.session_id)
    if is_tab_chat:
        system = """You are a CUR cost analysis assistant. Answer ONLY from the data provided in the user message. Do NOT call any tools. Do NOT make recommendations. State facts concisely with exact figures. Format currency as $X,XXX.XX.

If the question cannot be answered from the provided tab data, guide the user to the correct tab:
- Total cost, top services, gross/net cost, discounts → Overview tab
- Service breakdown, marketplace, reserved instances, savings plans → By Service tab
- Account-level costs → By Account tab
- Production/staging/lifecycle costs → By Environment tab
- Application, layer, function, budget code costs → Cost Centres & Tags tab
- Daily trends, regional costs → Trends tab
- Cross-dimensional questions (e.g. untagged EC2) → use the main Chat tab for deeper analysis

Example response when data not available: "This data is not available in the [current] tab. Switch to the [correct] tab to see [specific data].\""""
    elif pre_aggregated:
        import json as _json
        pre_agg_summary = f"""
Pre-aggregated CUR data available — use this to answer questions WITHOUT calling build_dashboard:

OVERVIEW: total_cost={pre_aggregated.get('overview', {}).get('total_cost')}, total_gross={pre_aggregated.get('overview', {}).get('total_gross')}, total_net={pre_aggregated.get('overview', {}).get('total_net')}, credits={pre_aggregated.get('overview', {}).get('credits_discounts')}, marketplace={pre_aggregated.get('overview', {}).get('marketplace_total')}, taxes={pre_aggregated.get('overview', {}).get('taxes')}
TOP SERVICES: {_json.dumps(pre_aggregated.get('overview', {}).get('service_breakdown', [])[:5])}
ACCOUNTS: {_json.dumps(pre_aggregated.get('accounts', {}).get('account_breakdown', [])[:5])}
LIFECYCLE: {_json.dumps(pre_aggregated.get('environments', {}).get('lifecycle_breakdown', []))}
HOSTING ENV: {_json.dumps(pre_aggregated.get('environments', {}).get('hosting_env_breakdown', [])[:5])}
LINE ITEM BREAKDOWN: {_json.dumps(pre_aggregated.get('services', {}).get('line_item_breakdown', {}))}
TAGS - APPLICATION: {_json.dumps(pre_aggregated.get('tags', {}).get('tag_application', [])[:10])}
TAGS - LAYER: {_json.dumps(pre_aggregated.get('tags', {}).get('tag_layer', [])[:10])}
TAGS - BUDGET CODE: {_json.dumps(pre_aggregated.get('tags', {}).get('tag_budget_code', [])[:10])}

Only call build_dashboard if the user asks for data NOT listed above.
"""
        system = _runner._build_system({"session_id": body.session_id, "has_data": has_data}) + "\n\n" + pre_agg_summary
    else:
        system = _runner._build_system({"session_id": body.session_id, "has_data": has_data})
    messages = _runner._build_messages(body.history, body.user_message)

    async def _execute_tool(name: str, tool_input: dict) -> str:
        executor = _runner._tool_map.get(name)
        if executor is None:
            return f"Unknown tool '{name}'"
        try:
            result = await executor.execute(**tool_input)
            return result if isinstance(result, str) else json.dumps(result, default=str)
        except Exception as exc:
            return f"Tool '{name}' error: {exc}"

    async def event_stream():
        try:
            yield f"data: Analyzing CUR data...\n\n"
            async for chunk in _llm_stream(
                model=settings.model,
                max_tokens=8192,
                system=system,
                messages=messages,
                tools=None if is_tab_chat else (_runner._anthropic_tools if _runner._tool_map else None),
                tool_executor=None if is_tab_chat else _execute_tool,
                api_key=x_anthropic_key or settings.anthropic_api_key,
                session_id=body.session_id,
            ):
                if chunk.startswith("[STOP_REASON]"):
                    yield f"data: {chunk}\n\n"
                else:
                    yield f"data: {chunk.replace(chr(10), chr(92)+'n')}\n\n"
            yield "data: [DONE]\n\n"
        except Exception as e:
            yield f"data: [ERROR] {str(e)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
