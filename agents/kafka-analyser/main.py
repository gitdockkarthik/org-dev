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
    """Background collection loop — placeholder for Phase 2 live collection."""
    logger.info("Collection loop started (Phase 1: manual sync via /settings/sync)")
    while True:
        await asyncio.sleep(3600)
        logger.debug("Collection loop: no live source configured — skipping tick")


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

    # ── Restore latest cluster data from DB ───────────────────────────────────
    try:
        from collections import defaultdict

        from database import SessionLocal
        from models import (
            KafkaAnomaly,
            KafkaBrokerMetrics,
            KafkaCluster,
            KafkaConnectorStatus,
            KafkaConsumerLag,
            KafkaTopicMetrics,
        )
        from sqlalchemy import select

        if SessionLocal is not None:
            async with SessionLocal() as session:
                # Load latest cluster for this agent
                result = await session.execute(
                    select(KafkaCluster)
                    .where(KafkaCluster.agent_slug == settings.agent_slug)
                    .order_by(KafkaCluster.created_at.desc())
                    .limit(1)
                )
                cluster_row = result.scalar_one_or_none()

                if cluster_row is None:
                    logger.info("Startup: no cluster data in DB — starting with empty store")
                else:
                    cid = cluster_row.id

                    broker_rows = (await session.execute(
                        select(KafkaBrokerMetrics).where(KafkaBrokerMetrics.cluster_id == cid)
                    )).scalars().all()

                    lag_rows = (await session.execute(
                        select(KafkaConsumerLag).where(KafkaConsumerLag.cluster_id == cid)
                    )).scalars().all()

                    topic_rows = (await session.execute(
                        select(KafkaTopicMetrics).where(KafkaTopicMetrics.cluster_id == cid)
                    )).scalars().all()

                    connector_rows = (await session.execute(
                        select(KafkaConnectorStatus).where(KafkaConnectorStatus.cluster_id == cid)
                    )).scalars().all()

                    anomaly_rows = (await session.execute(
                        select(KafkaAnomaly).where(
                            KafkaAnomaly.cluster_id == cid,
                            KafkaAnomaly.resolved_at.is_(None),
                        )
                    )).scalars().all()

                    if not (broker_rows or topic_rows):
                        logger.info(
                            "Startup: cluster record found (id=%d) but no metrics — skipping restore",
                            cid,
                        )
                    else:
                        # Reconstruct brokers
                        brokers = [
                            {
                                "broker_id": b.broker_id,
                                "host": f"{b.broker_id}.internal:9092",
                                "heap_pct": b.heap_pct,
                                "gc_pause_count": 1 if b.gc_pause_ms > 0 else 0,
                                "gc_pause_ms": b.gc_pause_ms,
                                "cpu_pct": b.cpu_pct,
                                "disk_pct": b.disk_pct,
                                "request_handler_idle_pct": b.request_handler_idle_pct,
                                "urp_count": b.urp_count,
                                "messages_in_per_sec": b.messages_in_per_sec,
                                "status": (
                                    "warning" if b.heap_pct >= 75 or b.urp_count > 0
                                    else "healthy"
                                ),
                            }
                            for b in broker_rows
                        ]

                        # Re-group partition rows back into consumer groups
                        groups_map: dict = defaultdict(lambda: {
                            "partitions": [],
                            "total_lag": 0,
                            "lag_trend": "stable",
                            "lag_rate_per_min": 0,
                        })
                        for row in lag_rows:
                            key = (row.group_name, row.topic)
                            g = groups_map[key]
                            g["group_name"] = row.group_name
                            g["topic"] = row.topic
                            g["state"] = row.group_state
                            g["total_lag"] += row.lag
                            g["partitions"].append({
                                "partition": row.partition,
                                "lag": row.lag,
                                "log_end_offset": row.log_end_offset,
                                "consumer_offset": row.consumer_offset,
                            })
                        consumer_groups = []
                        for g in groups_map.values():
                            g["status"] = (
                                "critical" if g["total_lag"] > 10000
                                else "warning" if g["total_lag"] > 1000
                                else "healthy"
                            )
                            consumer_groups.append(g)

                        # Reconstruct topics
                        topics = [
                            {
                                "topic": t.topic,
                                "partition_count": t.partition_count,
                                "replication_factor": t.replication_factor,
                                "messages_in_per_sec": t.messages_in_per_sec,
                                "bytes_in_per_sec": t.bytes_in_per_sec,
                                "bytes_out_per_sec": t.bytes_out_per_sec,
                                "total_messages": t.total_messages,
                                "size_bytes": t.size_bytes,
                                "retention_bytes": t.retention_bytes,
                                "retention_pct": t.retention_pct,
                                "status": "warning" if t.retention_pct >= 80 else "healthy",
                            }
                            for t in topic_rows
                        ]

                        # Reconstruct connectors
                        connectors = [
                            {
                                "connector_name": c.connector_name,
                                "connector_type": c.connector_type,
                                "state": c.state,
                                "failed_tasks": c.failed_tasks,
                                "total_tasks": c.total_tasks,
                                "task_health": [],
                                "last_updated": c.time.isoformat(),
                            }
                            for c in connector_rows
                        ]

                        # Reconstruct anomalies
                        anomalies = [
                            {
                                "id": a.id,
                                "severity": a.severity,
                                "category": a.category,
                                "description": a.description,
                                "detected_at": a.detected_at.isoformat(),
                                "resolved_at": None,
                                "recommendations": [],
                            }
                            for a in anomaly_rows
                        ]

                        # Compute cluster health summary
                        critical_count = sum(1 for a in anomalies if a["severity"] == "critical")
                        warning_count  = sum(1 for a in anomalies if a["severity"] == "warning")
                        health_score = max(0, 100 - critical_count * 15 - warning_count * 5)

                        import kafka_store as _ks
                        _ks.set_cluster_data(
                            {
                                "cluster": {
                                    "id": cluster_row.id,
                                    "name": cluster_row.name,
                                    "source_type": cluster_row.source_type,
                                    "broker_count": len(brokers),
                                    "status": cluster_row.status,
                                    "collected_at": cluster_row.created_at.isoformat(),
                                    "health_score": health_score,
                                    "anomaly_count": len(anomalies),
                                    "critical_count": critical_count,
                                    "warning_count": warning_count,
                                },
                                "brokers": brokers,
                                "consumer_groups": consumer_groups,
                                "topics": topics,
                                "connectors": connectors,
                                "anomalies": anomalies,
                            },
                            source_type=cluster_row.source_type,
                            cluster_id=str(cluster_row.id),
                        )
                        logger.info(
                            "Startup: cluster data restored from DB — cluster_id=%d, "
                            "brokers=%d, groups=%d, topics=%d",
                            cid, len(brokers), len(consumer_groups), len(topics),
                        )
    except Exception as exc:
        logger.warning("Startup: could not restore from DB: %s", exc)
        # Non-fatal — agent works with empty store

    collection_task = asyncio.create_task(_collection_loop())

    yield

    collection_task.cancel()
    try:
        await collection_task
    except asyncio.CancelledError:
        pass


# ── App ───────────────────────────────────────────────────────────────────────

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
