import asyncio
import csv
import io
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Header, HTTPException, UploadFile
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

_MAX_CUR_BYTES = 100 * 1024 * 1024       # 100 MB (matches /reports/upload)
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


@app.post("/data-sources/cur/upload")
async def ds_cur_upload(file: UploadFile) -> dict:
    """Upload a new CUR file (CSV / CSV.zip / Parquet) and register it as a
    data source. Reuses the existing report store so the dashboard picks it up."""
    from report_store import add_report, persist_report
    from routes_dashboard import invalidate_dashboard_cache
    from tools.data_sources.file_providers import FileUploadCURProvider, cur_bytes_to_csv
    from tools.duckdb_engine import get_total_cost

    reg = _require_registry()
    raw = await file.read()
    if len(raw) > _MAX_CUR_BYTES:
        raise HTTPException(status_code=413, detail="File too large (max 100 MB)")

    try:
        csv_text, resolved = cur_bytes_to_csv(file.filename or "", raw)
    except ValueError as exc:
        code = 400 if "Unsupported" in str(exc) else 422
        raise HTTPException(status_code=code, detail=str(exc))

    summary = get_total_cost(csv_text)
    if "error" in summary:
        raise HTTPException(status_code=422, detail=summary["error"])

    report = add_report(
        filename=resolved,
        csv_text=csv_text,
        row_count=summary.get("row_count", 0),
        total_cost=summary.get("total_cost", 0.0),
        file_size=len(raw),
    )
    await persist_report(report["id"])
    invalidate_dashboard_cache(report["id"])
    invalidate_dashboard_cache(None)

    provider = FileUploadCURProvider(
        source_id=f"cur-{report['id']}",
        filename=resolved,
        csv_text=csv_text,
        record_count=summary.get("row_count", 0),
        total_cost=summary.get("total_cost", 0.0),
        file_size=len(raw),
        report_id=report["id"],
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
async def ds_inventory_coverage() -> dict:
    """Per-service match rates and cost coverage after enriching the active CUR
    with the loaded inventory."""
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

    csv_text = await _active_cur_csv(reg)
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

    from report_store import get_report_rows
    from tools.data_sources.registry import get_registry
    from tools.duckdb_engine import _detect_account_col
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
        resource_col = "resource_id" if "resource_id" in cols else None
        if account_col and resource_col:
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


@app.delete("/data-sources/cur/{source_id}")
async def ds_delete_cur(source_id: str) -> dict:
    reg = _require_registry()
    removed = await reg.delete_cur(source_id)
    if not removed:
        raise HTTPException(status_code=404, detail=f"CUR source {source_id} not found")
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
    # Load from DB by report_id if session cache is still empty
    if not _cur_cache.get(body.session_id) and ctx.get("report_id"):
        from report_store import get_report_csv
        csv_text = get_report_csv(int(ctx["report_id"]))
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
    # Load from DB by report_id if session cache is still empty
    if not _cur_cache.get(body.session_id) and ctx.get("report_id"):
        from report_store import get_report_csv
        csv_text = get_report_csv(int(ctx["report_id"]))
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
