import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Header
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from agent import AgentRunner
from config import settings
from routes_dashboard import router as dashboard_router
from routes_reports import router as reports_router
from routes_settings import _config, _run_opsgenie_sync, _sync_changed, load_config_from_db, router as settings_router
import jobs as _jobs_module
from tools.dashboard_builder import DashboardBuilderTool
from tools.noise_detector import NoiseDetectorTool, classify_alerts
from tools.source import FileSource
from tools.suppression_advisor import SuppressionAdvisorTool

from shared.llm import stream_message as _llm_stream

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Alert cache ───────────────────────────────────────────────────────────────
import time as _time
_ALERT_CACHE_TTL_SECS = 1800  # 30 minutes
_alert_cache: dict[str, tuple[list[dict], float]] = {}  # session_id -> (alerts, timestamp)

def _evict_alert_cache() -> None:
    """Remove cache entries older than _ALERT_CACHE_TTL_SECS."""
    now = _time.time()
    expired = [k for k, (_, ts) in _alert_cache.items() if now - ts > _ALERT_CACHE_TTL_SECS]
    for k in expired:
        del _alert_cache[k]

# ── Agent setup ───────────────────────────────────────────────────────────────
_runner = AgentRunner(
    tools=[
        NoiseDetectorTool(_alert_cache),
        DashboardBuilderTool(_alert_cache),
        SuppressionAdvisorTool(_alert_cache),
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


async def _run_migrations() -> None:
    from database import SessionLocal
    from sqlalchemy import text

    if SessionLocal is None:
        logger.warning("_run_migrations: DATABASE_URL not configured — skipping")
        return

    try:
        migration_sql = (Path(__file__).parent / "migrations/incident_management.sql").read_text()
        statements = migration_sql.split(';')

        async with SessionLocal() as session:
            for stmt in statements:
                stmt = stmt.strip()

                # Skip empty statements and comment-only lines
                if not stmt or stmt.startswith('--'):
                    continue

                try:
                    await session.execute(text(stmt))
                    await session.commit()
                except Exception as stmt_error:
                    logger.warning("_run_migrations: statement failed: %s", stmt_error)
                    # Continue with next statement

        logger.info("_run_migrations: completed successfully")
    except Exception as e:
        logger.warning("_run_migrations: failed (agent will still start): %s", e)


async def _init_config() -> None:
    from database import engine
    from models import Base

    if engine is None:
        logger.warning("_init_config: DATABASE_URL not configured — skipping DB config load")
        return

    # Log masked URL so we can verify it's pointing at the right database.
    logger.info("_init_config: connecting to %s", str(engine.url))

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("_init_config: agent_config table ensured")

    db_cfg = await load_config_from_db()
    if not db_cfg:
        logger.info("_init_config: no saved config found — waiting for user setup")
        return

    logger.info("_init_config: config loaded from DB — source_type: %s", db_cfg.get("source_type", "file"))

    # DB values take priority over env vars for runtime-tunable thresholds.
    if "noise_threshold_repeat" in db_cfg:
        settings.noise_threshold_repeat = db_cfg["noise_threshold_repeat"]
    if "noise_threshold_close_secs" in db_cfg:
        settings.noise_threshold_close_secs = db_cfg["noise_threshold_close_secs"]
    logger.info(
        "_init_config: noise thresholds — repeat=%d, close_secs=%d",
        settings.noise_threshold_repeat,
        settings.noise_threshold_close_secs,
    )

    if (
        db_cfg.get("source_type") == "opsgenie"
        and db_cfg.get("cloud_id")
        and db_cfg.get("email")
        and db_cfg.get("api_token")
    ):
        logger.info("Config loaded from DB — OpsGenie auto-sync running")
        try:
            result = await _run_opsgenie_sync()
            logger.info("OpsGenie auto-sync complete — %d alerts loaded", result["alert_count"])
        except Exception:
            logger.exception("OpsGenie auto-sync failed")


async def _sync_loop() -> None:
    """Background sync task.

    Reads sync_interval_minutes from _config on every tick so changes made via
    the settings page take effect immediately — no restart required.

    When disabled (interval=0) the loop parks on _sync_changed and wakes the
    moment the user saves a non-zero interval.  When the user shortens the
    interval mid-sleep, _sync_changed fires and the loop re-evaluates without
    waiting for the old timeout to expire.
    """
    logger.info("Auto-sync loop started")
    while True:
        _sync_changed.clear()
        interval = _config.get("sync_interval_minutes", 0)

        if interval <= 0:
            # Disabled — park until settings change.
            await _sync_changed.wait()
            continue

        # Sleep for the configured interval, but wake early on settings change.
        try:
            await asyncio.wait_for(_sync_changed.wait(), timeout=interval * 60)
            # Settings changed before timeout — re-evaluate without syncing.
            continue
        except asyncio.TimeoutError:
            pass

        # Interval elapsed — run sync if OpsGenie is fully configured.
        source_type = _config.get("source_type", "")
        api_token = _config.get("api_token", "")
        if source_type in ("standalone", "opsgenie") and api_token:
            try:
                result = await _run_opsgenie_sync()
                logger.info("Auto-sync: %d alerts loaded", result["alert_count"])
            except Exception:
                logger.exception("Auto-sync: sync failed")
        else:
            logger.debug("Auto-sync: OpsGenie not fully configured — skipping tick")


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await _ensure_database()
    except Exception:
        logger.exception("_ensure_database failed")
    try:
        await _run_migrations()
    except Exception:
        logger.exception("Migration raised an unexpected exception (agent will still start)")
    try:
        await _register_self()
    except Exception:
        logger.exception("Self-registration raised an unexpected exception (agent will still start)")
    try:
        await _init_config()
    except Exception:
        logger.exception("Config initialisation raised an unexpected exception (agent will still start)")

    try:
        from report_store import load_latest_from_db
        loaded = await load_latest_from_db()
        if loaded:
            logger.info("Startup: loaded last report from DB")
        else:
            logger.info("Startup: no previous report found")
    except Exception:
        logger.exception("Startup: loading last report from DB failed (agent will still start)")

    # Register and start self-contained job scheduler
    from database import engine as _db_engine, SessionLocal
    from models import Base, AlertJobSchedule, AlertJobRun
    if _db_engine is not None:
        async with _db_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    # Startup reconciliation: mark orphaned 'running' job rows as failed.
    # A fresh process start means nothing could genuinely still be running -
    # these are leftover from a container restart/crash mid-sync. Without
    # this cleanup, trigger_job()'s active-run check silently blocks all
    # future sync triggers forever (recurring bug, documented in
    # DASHBOARD_AUDIT.md - third occurrence 2026-08-17).
    try:
        from sqlalchemy import text as _text
        async with SessionLocal() as _cleanup_sess:
            result = await _cleanup_sess.execute(
                _text("""
                    UPDATE alert_job_runs
                    SET status = 'failed', ended_at = now(),
                        error_message = 'Orphaned by container restart - marked failed on startup'
                    WHERE status = 'running'
                """)
            )
            await _cleanup_sess.commit()
            if result.rowcount > 0:
                logger.warning("Startup: cleaned up %d orphaned 'running' job run(s)", result.rowcount)
    except Exception:
        logger.exception("Startup: orphaned job run cleanup failed (agent will still start)")
    # Register OpsGenie sync job
    async def _opsgenie_sync_job():
        from routes_settings import _config as _ac
        source_type = _ac.get("source_type", "")
        api_token = _ac.get("api_token", "")
        if source_type not in ("standalone", "opsgenie") or not api_token:
            _opsgenie_sync_job._last_result = "OpsGenie not configured — skipped"
            return
        result = await _run_opsgenie_sync()
        alert_count = result.get('alert_count', result.get('total', 0))
        _opsgenie_sync_job._last_result = f"Synced {alert_count:,} alerts"
    _jobs_module.register_job(
        "alert-opsgenie-sync",
        "OpsGenie Alert Sync",
        "Poll OpsGenie for new alerts and update local store",
        _opsgenie_sync_job,
    )
    # Register default schedule if none exists
    from sqlalchemy import select as _sel
    async with SessionLocal() as _sess:
        existing = await _sess.execute(_sel(AlertJobSchedule).where(AlertJobSchedule.job_id == "alert-opsgenie-sync"))
        if not existing.scalar_one_or_none():
            await _jobs_module.create_schedule("alert-opsgenie-sync", "*/15 * * * *", enabled=True)
            logger.info("Created default 15-min schedule for alert-opsgenie-sync")
    count = await _jobs_module.load_schedules()
    logger.info("Job scheduler: loaded %d schedule(s)", count)
    _jobs_module.start_scheduler()

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
    cron = body.get("cron_expression", "*/15 * * * *")
    enabled = body.get("enabled", True)
    return await _jobs_module.create_schedule(job_id, cron, enabled)

@app.put("/jobs/{job_id}/schedules/{schedule_id}")
async def update_job_schedule(job_id: str, schedule_id: int, body: dict) -> dict:
    cron = body.get("cron_expression", "*/15 * * * *")
    enabled = body.get("enabled", True)
    return await _jobs_module.update_schedule(schedule_id, cron, enabled)

@app.delete("/jobs/{job_id}/schedules/{schedule_id}")
async def delete_job_schedule(job_id: str, schedule_id: int) -> dict:
    return await _jobs_module.delete_schedule(schedule_id)

@app.get("/runs")
async def get_all_runs(limit: int = 50) -> list:
    return await _jobs_module.get_runs(limit=limit)


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


@app.post("/invoke", response_model=InvokeResponse)
async def invoke(
    body: InvokeRequest,
    x_anthropic_key: str | None = Header(default=None),
) -> InvokeResponse:
    ctx = body.context

    _evict_alert_cache()
    if "raw_data" in ctx:
        source = FileSource(ctx["raw_data"], ctx.get("format", "json"))
        _alert_cache[body.session_id] = (await source.load_alerts(), _time.time())
    elif "alerts" in ctx:
        _alert_cache[body.session_id] = (ctx["alerts"], _time.time())
    alerts, _ = _alert_cache.get(body.session_id, ([], 0.0))
    if not alerts:
        from report_store import get_latest_classified
        cached = get_latest_classified()
        if cached:
            alerts = cached
            _alert_cache[body.session_id] = (alerts, _time.time())
    has_data = bool(alerts)
    alert_count = len(alerts)

    # Cache holds raw alerts; classify so the summary reflects genuine/noise/suspect.
    # Send only a bounded summary to the LLM — never the full alert list — to keep
    # the system prompt small.
    classified = classify_alerts(alerts) if alerts else []
    alert_summary = {
        "total": len(classified),
        "genuine": sum(1 for a in classified if a.get("classification") == "genuine"),
        "noise": sum(1 for a in classified if a.get("classification") == "noise"),
        "suspect": sum(1 for a in classified if a.get("classification") == "noise-suspect"),
        "sample_noise": [
            {"message": a.get("message", "")[:100], "source": a.get("source", ""),
             "priority": a.get("priority", ""), "reasons": a.get("noise_reasons", [])}
            for a in classified if a.get("classification") == "noise"
        ][:20],
        "sample_genuine": [
            {"message": a.get("message", "")[:100], "source": a.get("source", ""),
             "priority": a.get("priority", ""), "reasons": a.get("genuine_reasons", [])}
            for a in classified if a.get("classification") == "genuine"
        ][:20],
    }

    response_text, tokens = await _runner.run(
        user_message=body.user_message,
        context={
            "session_id": body.session_id,
            "has_data": has_data,
            "alert_count": alert_count,
            "alert_summary": alert_summary,
        },
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
    async def event_stream():
        try:
            async for chunk in _llm_stream(
                model=settings.model,
                max_tokens=8192,
                messages=(
                    [
                        {"role": "user", "content": body.user_message},
                        {"role": "assistant", "content": body.context.get("continuation_of", "")},
                        {"role": "user", "content": "Please continue the analysis from where you left off. Do not repeat what was already written."},
                    ]
                    if body.context.get("continuation_of")
                    else [{"role": "user", "content": body.user_message}]
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
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/invoke/stream")
async def invoke_stream(
    body: InvokeRequest,
    x_anthropic_key: str | None = Header(default=None),
):
    _evict_alert_cache()
    ctx = body.context
    if "raw_data" in ctx:
        source = FileSource(ctx["raw_data"], ctx.get("format", "json"))
        _alert_cache[body.session_id] = (await source.load_alerts(), _time.time())
    elif "alerts" in ctx:
        _alert_cache[body.session_id] = (ctx["alerts"], _time.time())
    alerts, _ = _alert_cache.get(body.session_id, ([], 0.0))
    if not alerts:
        from report_store import get_latest_classified
        cached = get_latest_classified()
        if cached:
            alerts = cached
            _alert_cache[body.session_id] = (alerts, _time.time())
    has_data = bool(alerts)
    alert_count = len(alerts)
    classified = classify_alerts(alerts) if alerts else []
    alert_summary = {
        "total": len(classified),
        "genuine": sum(1 for a in classified if a.get("classification") == "genuine"),
        "noise": sum(1 for a in classified if a.get("classification") == "noise"),
        "suspect": sum(1 for a in classified if a.get("classification") == "noise-suspect"),
        "sample_noise": [
            {"message": a.get("message", "")[:100], "source": a.get("source", ""),
             "priority": a.get("priority", ""), "reasons": a.get("noise_reasons", [])}
            for a in classified if a.get("classification") == "noise"
        ][:20],
        "sample_genuine": [
            {"message": a.get("message", "")[:100], "source": a.get("source", ""),
             "priority": a.get("priority", ""), "reasons": a.get("genuine_reasons", [])}
            for a in classified if a.get("classification") == "genuine"
        ][:20],
    }
    system = _runner._build_system({
        "session_id": body.session_id,
        "has_data": has_data,
        "alert_count": alert_count,
        "alert_summary": alert_summary,
    })
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
            yield f"data: Analyzing {alert_count:,} alerts...\n\n"
            async for chunk in _llm_stream(
                model=settings.model,
                max_tokens=8192,
                system=system,
                messages=messages,
                tools=_runner._anthropic_tools if _runner._tool_map else None,
                tool_executor=_execute_tool,
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
