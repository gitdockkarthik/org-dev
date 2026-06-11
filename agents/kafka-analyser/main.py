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
from routes_settings import load_config_from_db, router as settings_router
from storage import init_storage
from tools.kafka_tools import (
    AnomalyTool,
    BrokerMetricsTool,
    ClusterOverviewTool,
    ConsumerLagTool,
    TopicMetricsTool,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Agent setup ───────────────────────────────────────────────────────────────
_runner = AgentRunner(
    tools=[
        ClusterOverviewTool(),
        ConsumerLagTool(),
        BrokerMetricsTool(),
        TopicMetricsTool(),
        AnomalyTool(),
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

        pub_resp = await client.post(
            f"{base}/api/registry/agents/{agent_id}/publish", headers=headers
        )
        if pub_resp.status_code == 200:
            logger.info("Self-registration: published successfully")
        else:
            logger.error(
                "Self-registration publish failed: %s — %s",
                pub_resp.status_code, pub_resp.text,
            )


# ── App init ──────────────────────────────────────────────────────────────────


async def _init_config() -> None:
    from database import engine
    from models import Base

    if engine is not None:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("_init_config: DB tables ensured")

    init_storage(settings.agent_slug)
    db_cfg = await load_config_from_db()
    if not db_cfg:
        logger.info("_init_config: no saved config found — waiting for user setup")
    else:
        logger.info(
            "_init_config: config loaded from DB — source_type: %s",
            db_cfg.get("source_type", "synthetic"),
        )


async def _collection_loop() -> None:
    """Background collection loop — syncs all enabled clusters at configured interval."""
    logger.info("Collection loop started")
    while True:
        try:
            from routes_settings import _config as _rs_config
            interval_secs = int(_rs_config.get("collection_interval_secs", 0))
            if interval_secs <= 0:
                await asyncio.sleep(30)
                continue

            await asyncio.sleep(interval_secs)

            from storage import get_backend
            clusters = await get_backend().get_clusters(settings.agent_slug)
            enabled = [c for c in clusters if c.get("enabled")]
            if not enabled:
                logger.debug("Collection loop: no enabled clusters — skipping")
                continue

            source_type = _rs_config.get("source_type", "synthetic")
            if source_type not in ("kafka_internal", "kafka_sasl", "live"):
                logger.debug("Collection loop: source_type=%s — skipping", source_type)
                continue

            from tools.real_kafka import RealKafkaCollector
            import kafka_store as _ks
            for c in enabled:
                try:
                    collector = RealKafkaCollector({
                        "bootstrap_servers": c["bootstrap_servers"],
                        "auth_type": "none" if c["auth_type"] == "none" else "sasl",
                        "sasl_username": c.get("sasl_username"),
                        "sasl_password": c.get("sasl_password"),
                        "sasl_mechanism": c.get("sasl_mechanism", "PLAIN"),
                        "tls_enabled": c.get("tls_enabled", False),
                        "cluster_label": c["name"],
                        "jmx_port": c.get("jmx_port"),
                    })
                    try:
                        data = await collector.collect_summary()
                    except RuntimeError as exc:
                        # Check if this is a background aiokafka error after data collection
                        if "Buffer underrun" in str(exc) or "KafkaConnectionError" in str(exc):
                            logger.warning(
                                "Collection loop: background error on cluster '%s' — skipping this tick: %s",
                                c["name"], exc,
                            )
                            continue
                        raise
                    _ks.set_cluster_data(
                        data,
                        source_type=c.get("source_type", "kafka_internal"),
                        cluster_id=str(c.get("id", "default")),
                    )
                    from datetime import datetime, timezone
                    from storage import get_backend
                    topics = data.get("topics", [])
                    if topics and c.get("id"):
                        try:
                            await get_backend().save_topic_metrics(
                                cluster_id=int(c["id"]),
                                topics=topics,
                                collected_at=datetime.now(timezone.utc),
                            )
                        except Exception as _te:
                            logger.warning("save_topic_metrics failed for cluster %s: %s", c["name"], _te)
                    logger.info(
                        "Collection loop: synced cluster '%s' (id=%s)",
                        c["name"], c.get("id"),
                    )
                    # Detect anomalies and escalate to Teams
                    try:
                        from tools.anomaly_detector import detect_anomalies as _detect_anomalies
                        from tools.escalation_notifier import send_anomaly_summary
                        anomalies = _detect_anomalies(data, thresholds=_rs_config)
                        if anomalies:
                            teams_cfg = {
                                "teams_enabled": _rs_config.get("teams_enabled", False),
                                "teams_webhook_url": _rs_config.get("teams_webhook_url", ""),
                                "teams_severity_filter": _rs_config.get("teams_severity_filter",
                                                                         ["critical", "warning"]),
                                "teams_cooldown_mins": _rs_config.get("teams_cooldown_mins", 10),
                            }
                            cooldown_key = f"summary_{c['name']}"
                            import time
                            now = time.time()
                            if not hasattr(_collection_loop, '_summary_cooldown'):
                                _collection_loop._summary_cooldown = {}
                            last = _collection_loop._summary_cooldown.get(cooldown_key, 0)
                            cooldown_mins = teams_cfg.get("teams_cooldown_mins", 10)
                            if now - last >= cooldown_mins * 60:
                                sent = await send_anomaly_summary(
                                    agent_name="Kafka Analyser",
                                    cluster_name=c["name"],
                                    anomalies=anomalies,
                                    config=teams_cfg,
                                    dashboard_url="http://kpi-internal.cloud.operative.com:3000/agents/kafka-analyser/dashboard",
                                )
                                if sent:
                                    _collection_loop._summary_cooldown[cooldown_key] = now
                    except Exception as _esc_exc:
                        logger.warning("Escalation failed for cluster '%s': %s", c["name"], _esc_exc)
                except Exception as exc:
                    logger.warning(
                        "Collection loop: failed to sync cluster '%s': %s",
                        c["name"], exc,
                    )
        except Exception as exc:
            logger.warning("Collection loop error: %s", exc)
            await asyncio.sleep(30)


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

    # Re-sync enabled clusters on startup
    try:
        from storage import get_backend
        from routes_settings import _config
        clusters = await get_backend().get_clusters(settings.agent_slug)
        enabled = [c for c in clusters if c.get("enabled")]
        if enabled:
            logger.info("Startup: found %d enabled cluster(s) — syncing in background", len(enabled))
            async def _startup_sync():
                from tools.real_kafka import RealKafkaCollector
                import kafka_store as _ks
                for c in enabled:
                    try:
                        collector = RealKafkaCollector({
                            "bootstrap_servers": c["bootstrap_servers"],
                            "auth_type": "none" if c["auth_type"] == "none" else "sasl",
                            "sasl_username": c.get("sasl_username"),
                            "sasl_password": c.get("sasl_password"),
                            "sasl_mechanism": c.get("sasl_mechanism", "PLAIN"),
                            "tls_enabled": c.get("tls_enabled", False),
                            "cluster_label": c["name"],
                            "jmx_port": c.get("jmx_port"),
                        })
                        data = await collector.collect_summary()
                        _ks.set_cluster_data(
                            data,
                            source_type=c.get("source_type", "kafka_internal"),
                            cluster_id=str(c.get("id", "default")),
                        )
                        from datetime import datetime, timezone
                        from storage import get_backend
                        topics = data.get("topics", [])
                        if topics and c.get("id"):
                            try:
                                await get_backend().save_topic_metrics(
                                    cluster_id=int(c["id"]),
                                    topics=topics,
                                    collected_at=datetime.now(timezone.utc),
                                )
                            except Exception as _te:
                                logger.warning("save_topic_metrics failed for cluster %s: %s", c["name"], _te)
                        logger.info(
                            "Startup: synced cluster '%s' (id=%s) — brokers=%d, topics=%d",
                            c["name"], c.get("id"),
                            len(data.get("brokers", [])),
                            len(data.get("topics", [])),
                        )
                        # Background parallel topic describe — ALL topics for accurate KPIs
                        try:
                            import time as _t2
                            _topic_start = _t2.time()
                            # Get ALL topic names from broker
                            all_topic_names = await collector.list_all_topics()
                            if all_topic_names:
                                logger.info("Topic scan: starting parallel describe for ALL %d topics on '%s'", len(all_topic_names), c["name"])
                                described_topics, total_urp = await collector.describe_all_topics(all_topic_names, workers=10)
                                if described_topics:
                                    total_rf1 = sum(1 for t in described_topics if t.get("replication_factor") == 1)
                                    total_partitions = sum(t.get("partition_count", 0) for t in described_topics)

                                    # Store accurate counts for KPI display
                                    if "counts" not in data:
                                        data["counts"] = {}
                                    data["counts"]["total_topics"] = len(all_topic_names)
                                    data["counts"]["total_rf1"] = total_rf1
                                    data["counts"]["total_urp"] = total_urp
                                    data["counts"]["total_partitions"] = total_partitions

                                    if "cluster" in data:
                                        data["cluster"]["under_replicated_partitions"] = total_urp
                                        data["cluster"]["partition_count"] = total_partitions

                                    # Keep top 500 for display: anomalous first (already sorted by describe_all_topics)
                                    data["topics"] = described_topics[:500]

                                    _ks.set_cluster_data(
                                        data,
                                        source_type=c.get("source_type", "kafka_internal"),
                                        cluster_id=str(c.get("id", "default")),
                                    )
                                _topic_elapsed = round(_t2.time() - _topic_start, 1)
                                logger.info(
                                    "Topic scan: described %d topics (%d RF=1, %d URP, %d partitions) for '%s' in %ss",
                                    len(described_topics), total_rf1, total_urp, total_partitions, c["name"], _topic_elapsed
                                )
                        except Exception as _topic_exc:
                            logger.warning("Topic scan failed for '%s': %s", c["name"], _topic_exc)
                        # Background parallel lag scan — enriches consumer groups
                        try:
                            active_gids = [g["group_id"] for g in data.get("consumer_groups", [])
                                          if g.get("state", "Unknown") not in ("Empty", "Dead")]
                            if active_gids:
                                logger.info("Lag scan: starting parallel scan for %d active groups on '%s'", len(active_gids), c["name"])
                                import time as _t
                                _lag_start = _t.time()
                                lags = await collector.fetch_all_group_lags(active_gids, workers=10)
                                lag_map = {g["group_id"]: g for g in lags}
                                enriched = 0
                                for cg in data["consumer_groups"]:
                                    if cg["group_id"] in lag_map:
                                        lag_data = lag_map[cg["group_id"]]
                                        cg["total_lag"] = lag_data.get("total_lag", -1)
                                        cg["topic_count"] = lag_data.get("topic_count", 0)
                                        # Extract primary topic (highest lag contributor)
                                        parts = lag_data.get("partitions", [])
                                        if parts:
                                            topic_lags = {}
                                            for p in parts:
                                                t = p.get("topic", "")
                                                if t:
                                                    topic_lags[t] = topic_lags.get(t, 0) + p.get("lag", 0)
                                            if topic_lags:
                                                cg["topic"] = max(topic_lags, key=topic_lags.get)
                                        enriched += 1
                                # Re-sort by lag descending
                                data["consumer_groups"].sort(key=lambda g: g.get("total_lag", -1), reverse=True)
                                # Re-store enriched data
                                _ks.set_cluster_data(
                                    data,
                                    source_type=c.get("source_type", "kafka_internal"),
                                    cluster_id=str(c.get("id", "default")),
                                )
                                _lag_elapsed = round(_t.time() - _lag_start, 1)
                                logger.info("Lag scan: enriched %d/%d groups for '%s' in %ss", enriched, len(active_gids), c["name"], _lag_elapsed)
                        except Exception as _lag_exc:
                            logger.warning("Lag scan failed for '%s': %s", c["name"], _lag_exc)
                    except Exception as exc:
                        logger.warning("Startup: failed to sync cluster '%s': %s", c["name"], exc)
            asyncio.create_task(_startup_sync())
        else:
            logger.info("Startup: no enabled clusters — starting with empty store")
    except Exception as exc:
        logger.warning("Startup: could not restore from DB: %s", exc)

    collection_task = asyncio.create_task(_collection_loop())

    yield

    collection_task.cancel()
    try:
        await collection_task
    except asyncio.CancelledError:
        pass


# ── App ───────────────────────────────────────────────────────────────────────

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


@app.post("/invoke", response_model=InvokeResponse)
async def invoke(
    body: InvokeRequest,
    x_anthropic_key: str | None = Header(default=None),
) -> InvokeResponse:
    import kafka_store as ks

    data = ks.get_cluster_data()
    has_data = data is not None

    response_text, tokens = await _runner.run(
        user_message=body.user_message,
        context={
            "session_id": body.session_id,
            "has_data": has_data,
            "broker_count": len(data["brokers"]) if data else 0,
            "consumer_group_count": len(data["consumer_groups"]) if data else 0,
            "topic_count": len(data["topics"]) if data else 0,
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


@app.post("/invoke/stream")
async def invoke_stream(
    body: InvokeRequest,
    x_anthropic_key: str | None = Header(default=None),
):
    import anthropic as _anthropic
    import kafka_store as ks
    ctx = body.context
    # Use multi-cluster summary from context if provided (standalone/chat mode)
    cluster_summary = ctx.get("summary", "")
    clusters_from_ctx = ctx.get("clusters", [])
    if clusters_from_ctx:
        has_data = True
        broker_count = sum(c.get("broker_count", 0) for c in clusters_from_ctx)
        topic_count = sum(c.get("topic_count", 0) for c in clusters_from_ctx)
        consumer_group_count = sum(c.get("consumer_group_count", 0) for c in clusters_from_ctx)
    else:
        data = ks.get_cluster_data()
        has_data = data is not None
        broker_count = len(data["brokers"]) if data else 0
        topic_count = len(data["topics"]) if data else 0
        consumer_group_count = len(data["consumer_groups"]) if data else 0
    resolved_key = x_anthropic_key or settings.anthropic_api_key
    system = _runner._build_system({
        "session_id": body.session_id,
        "has_data": has_data,
        "broker_count": broker_count,
        "consumer_group_count": consumer_group_count,
        "topic_count": topic_count,
        "cluster_summary": cluster_summary,
    })
    messages = _runner._build_messages(body.history, body.user_message)

    async def event_stream():
        try:
            client = _anthropic.AsyncAnthropic(api_key=resolved_key)
            async with client.messages.stream(
                model=settings.model,
                max_tokens=4096,
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
