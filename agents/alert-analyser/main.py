import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Header
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from agent import AgentRunner
from config import settings
from routes_dashboard import router as dashboard_router
from routes_reports import router as reports_router
from routes_settings import _config, _run_opsgenie_sync, _sync_changed, load_config_from_db, router as settings_router
from tools.dashboard_builder import DashboardBuilderTool
from tools.noise_detector import NoiseDetectorTool, classify_alerts
from tools.source import FileSource
from tools.suppression_advisor import SuppressionAdvisorTool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Alert cache ───────────────────────────────────────────────────────────────
_alert_cache: dict[str, list[dict]] = {}

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
        if (
            _config.get("source_type") == "opsgenie"
            and _config.get("cloud_id")
            and _config.get("email")
            and _config.get("api_token")
        ):
            try:
                result = await _run_opsgenie_sync()
                logger.info("Auto-sync: %d alerts loaded", result["alert_count"])
            except Exception:
                logger.exception("Auto-sync: sync failed")
        else:
            logger.debug("Auto-sync: OpsGenie not fully configured — skipping tick")


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

    try:
        from report_store import load_latest_from_db
        loaded = await load_latest_from_db()
        if loaded:
            logger.info("Startup: loaded last report from DB")
        else:
            logger.info("Startup: no previous report found")
    except Exception:
        logger.exception("Startup: loading last report from DB failed (agent will still start)")

    sync_task = asyncio.create_task(_sync_loop())

    yield

    sync_task.cancel()
    try:
        await sync_task
    except asyncio.CancelledError:
        pass


app = FastAPI(title=settings.agent_name, version="0.1.0", lifespan=lifespan)
app.mount("/ui", StaticFiles(directory=Path(__file__).parent / "static", html=True), name="ui")
app.include_router(dashboard_router)
app.include_router(reports_router)
app.include_router(settings_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "agent": settings.agent_slug}


@app.post("/invoke", response_model=InvokeResponse)
async def invoke(
    body: InvokeRequest,
    x_anthropic_key: str | None = Header(default=None),
) -> InvokeResponse:
    ctx = body.context

    if "raw_data" in ctx:
        source = FileSource(ctx["raw_data"], ctx.get("format", "json"))
        _alert_cache[body.session_id] = await source.load_alerts()
    elif "alerts" in ctx:
        _alert_cache[body.session_id] = ctx["alerts"]

    alerts = _alert_cache.get(body.session_id, [])
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
