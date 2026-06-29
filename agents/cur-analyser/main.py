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
from routes_dashboard import router as dashboard_router
from routes_reports import router as reports_router
from report_store import load_from_db as load_reports_from_db
from routes_settings import load_config_from_db, router as settings_router
from tools.dashboard_builder import DashboardBuilderTool
from tools.duckdb_engine import CurQueryTool
from tools.source import FileSource

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── CUR CSV cache ─────────────────────────────────────────────────────────────
_cur_cache: dict[str, str] = {}

# ── Agent setup ───────────────────────────────────────────────────────────────
_runner = AgentRunner(
    tools=[
        CurQueryTool(_cur_cache),
        DashboardBuilderTool(_cur_cache),
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


@asynccontextmanager
async def _prewarm_dashboard_cache() -> None:
    """Background task: compute and cache the dashboard for the latest report on startup.
    Runs once after _init_config completes. Failures are logged and ignored — agent starts normally.
    Backout: remove the create_task call in lifespan. No other changes needed."""
    try:
        from report_store import get_report_path, list_reports
        reports = list_reports()
        if not reports:
            return
        latest = reports[0]
        report_id = latest.get("id")
        if not report_id:
            return
        path = get_report_path(report_id)
        if not path:
            return
        from routes_dashboard import _get_cached_dashboard, _set_cached_dashboard, compute_dashboard_for_report
        if _get_cached_dashboard(report_id):
            logger.info("_prewarm: report %s already cached — skipping", report_id)
            return
        logger.info("_prewarm: pre-warming dashboard cache for report %s (%s)", report_id, latest.get("filename", ""))
        dashboard = await compute_dashboard_for_report(report_id)
        if dashboard:
            _set_cached_dashboard(report_id, dashboard)
            logger.info("_prewarm: dashboard cache ready for report %s", report_id)
    except Exception:
        logger.exception("_prewarm: cache pre-warm failed (agent still starts normally)")


async def lifespan(app: FastAPI):
    try:
        await _register_self()
    except Exception:
        logger.exception("Self-registration raised an unexpected exception (agent will still start)")
    try:
        await _init_config()
    except Exception:
        logger.exception("Config initialisation raised an unexpected exception (agent will still start)")

    sync_task: asyncio.Task | None = None
    if settings.sync_interval_minutes > 0:
        sync_task = asyncio.create_task(_sync_loop())

    yield

    if sync_task:
        sync_task.cancel()
        try:
            await sync_task
        except asyncio.CancelledError:
            pass


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
async def health() -> dict[str, str]:
    return {"status": "ok", "agent": settings.agent_slug}


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
_LARGE_FILE_BYTES = 200 * 1024 * 1024  # 200 MB


def _is_large_file(report_id: int) -> bool:
    """True when the report's on-disk CUR exceeds the large-file threshold."""
    import os

    from report_store import get_report_path

    path = get_report_path(report_id)
    if not path or not os.path.exists(path):
        return False
    return os.path.getsize(path) > _LARGE_FILE_BYTES


def _quote_ident(name: str) -> str:
    """Quote a SQL identifier (column name) for safe interpolation, escaping any
    embedded double quotes per SQL rules. Column names here come from a
    user-uploaded CUR header, so they must never be trusted raw in a query."""
    return '"' + str(name).replace('"', '""') + '"'


@app.post("/data-sources/cur/upload")
async def ds_cur_upload(file: UploadFile) -> dict:
    """Upload a new CUR file (CSV / CSV.zip / Parquet) and register it as a
    data source. Reuses the existing report store so the dashboard picks it up."""
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
    from tools.data_sources.file_providers import (
        FileUploadCURProvider,
        UploadTooLarge,
        materialize_cur,
        stream_upload_to_temp,
    )
    from tools.duckdb_engine import get_total_cost

    reg = _require_registry()

    # Stream the upload to a temp file on disk (bounded memory) rather than
    # loading the whole CUR into memory via ``await file.read()``.
    try:
        tmp_path, file_size = await stream_upload_to_temp(
            file, max_bytes=_MAX_CUR_BYTES
        )
    except UploadTooLarge:
        raise HTTPException(status_code=413, detail="File too large (max 2 GB)")

    mat_path: str | None = None
    try:
        try:
            mat_path, resolved, ext = materialize_cur(file.filename or "", tmp_path)
        except ValueError as exc:
            code = 400 if "Unsupported" in str(exc) else 422
            raise HTTPException(status_code=code, detail=str(exc))

        # Summarise straight from the file on disk — never materialise a
        # multi-GB CSV string.
        summary = get_total_cost(file_path=mat_path)
        if "error" in summary:
            raise HTTPException(status_code=422, detail=summary["error"])

        report = add_report(
            filename=resolved,
            csv_text="",
            row_count=summary.get("row_count", 0),
            total_cost=summary.get("total_cost", 0.0),
            file_size=file_size,
            file_path=mat_path,
        )
        # Move the materialised file into permanent per-report storage.
        _ensure_data_dir()
        perm_path = report_file_path(report["id"], ext)
        shutil.move(mat_path, perm_path)
        mat_path = None  # moved — nothing left to clean up
        set_report_path(report["id"], perm_path)
    finally:
        for p in (tmp_path, mat_path):
            if p and os.path.exists(p):
                try:
                    os.unlink(p)
                except OSError:
                    pass

    await persist_report(report["id"])
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
    meta = await reg.register_cur(provider)
    return {"ok": True, "report": report, "source": meta.to_dict()}


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


@app.get("/data-sources/status")
async def ds_status() -> dict:
    """Registry status — sources, active selection, staleness, archives."""
    reg = _require_registry()
    status = reg.status()
    status["enabled"] = settings.enable_inventory_enrichment
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
    if report_id is not None and _is_large_file(report_id):
        import duckdb

        from report_store import get_report_path
        from tools.duckdb_engine import _detect_account_col

        path = get_report_path(report_id)
        con = duckdb.connect(":memory:")
        try:
            safe_path = str(path).replace("'", "''")
            con.execute(
                f"CREATE VIEW f AS SELECT * FROM read_csv_auto('{safe_path}', ignore_errors=true)"
            )
            cols = [r[0] for r in con.execute("DESCRIBE f").fetchall()]
            acct_col = _detect_account_col(cols)
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
            "enrichment_level": "account",
            "has_resource_column": False,
            "matched_accounts": len(matched),
            "total_accounts": len(cur_accounts),
            "account_match_rate": round(match_rate * 100, 1),
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
        con = duckdb.connect(":memory:")
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
        return {
            "report_id": report_id,
            "active": True,
            "joinable": True,
            "enrichment_level": "account",
            "has_resource_column": False,
            "reason": "Account-level enrichment — resource IDs not in this CUR format",
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
    if not _cur_cache.get(body.session_id) and ctx.get("report_id"):
        _rid = int(ctx["report_id"])
        if _is_large_file(_rid):
            logger.info("invoke: skipping chat cache load for large report %s", _rid)
        else:
            from report_store import get_report_csv
            csv_text = get_report_csv(_rid)
            if csv_text:
                _cur_cache[body.session_id] = csv_text

    has_data = bool(_cur_cache.get(body.session_id))

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
    import anthropic as _anthropic
    from config import settings as _settings

    resolved_key = x_anthropic_key or _settings.anthropic_api_key
    prompt = body.user_message

    async def event_stream():
        try:
            client = _anthropic.AsyncAnthropic(api_key=resolved_key)
            async with client.messages.stream(
                model="claude-sonnet-4-6",
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
            ) as stream:
                async for text in stream.text_stream:
                    # SSE format: data: <chunk>\n\n
                    escaped = text.replace("\n", "\\n")
                    yield f"data: {escaped}\n\n"
                try:
                    final_msg = await stream.get_final_message()
                    stop_reason = final_msg.stop_reason
                except Exception:
                    stop_reason = "end_turn"
                yield f"data: [STOP_REASON] {stop_reason}\n\n"
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
    import anthropic as _anthropic
    from config import settings as _settings
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
    if not _cur_cache.get(body.session_id) and ctx.get("report_id"):
        _rid = int(ctx["report_id"])
        if _is_large_file(_rid):
            logger.info("invoke: skipping chat cache load for large report %s", _rid)
        else:
            from report_store import get_report_csv
            csv_text = get_report_csv(_rid)
            if csv_text:
                _cur_cache[body.session_id] = csv_text
    has_data = bool(_cur_cache.get(body.session_id))
    resolved_key = x_anthropic_key or _settings.anthropic_api_key
    system = _runner._build_system({"session_id": body.session_id, "has_data": has_data})
    messages = _runner._build_messages(body.history, body.user_message)

    async def event_stream():
        try:
            client = _anthropic.AsyncAnthropic(api_key=resolved_key)
            async with client.messages.stream(
                model=settings.model,
                max_tokens=8192,
                system=system,
                messages=messages,
            ) as stream:
                async for text in stream.text_stream:
                    yield f"data: {text.replace(chr(10), chr(92)+'n')}\n\n"
            yield "data: [DONE]\n\n"
        except Exception as e:
            yield f"data: [ERROR] {str(e)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
