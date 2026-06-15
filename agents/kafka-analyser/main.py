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
from kafka_store import save_to_db, restore_from_db, save_brokers, save_topics_structure, save_topics_metrics, save_groups
from tools.kafka_tools import (
    AnomalyTool,
    BrokerMetricsTool,
    ClusterOverviewTool,
    ConsumerLagTool,
    TopicMetricsTool,
)

try:
    from tools.prometheus_collector import scrape_all_brokers, scrape_topic_metrics_and_top_by_size
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

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
    from sqlalchemy import text

    if engine is not None:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            # Lightweight migrations: add columns that post-date the original schema
            try:
                await conn.execute(text(
                    "ALTER TABLE kafka_clusters ADD COLUMN IF NOT EXISTS prometheus_port INTEGER"
                ))
            except Exception as _mig_exc:
                logger.warning("prometheus_port migration skipped: %s", _mig_exc)
            try:
                await conn.execute(text(
                    "ALTER TABLE kafka_clusters ADD COLUMN IF NOT EXISTS cpu_cores INTEGER"
                ))
            except Exception as _mig_exc3:
                logger.warning("cpu_cores migration skipped: %s", _mig_exc3)
            try:
                await conn.execute(text(
                    "ALTER TABLE kafka_topic_names ADD COLUMN IF NOT EXISTS partition_count INTEGER DEFAULT 0"
                ))
                await conn.execute(text(
                    "ALTER TABLE kafka_topic_names ADD COLUMN IF NOT EXISTS replication_factor INTEGER DEFAULT 0"
                ))
            except Exception as _mig_exc2:
                logger.warning("kafka_topic_names migration skipped: %s", _mig_exc2)
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
    """Background collection loop — runs same full enrichment as startup every interval."""
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

                    # Initial summary — brokers, topic names, group states
                    try:
                        data = await collector.collect_summary()
                    except RuntimeError as exc:
                        if "Buffer underrun" in str(exc) or "KafkaConnectionError" in str(exc):
                            logger.warning(
                                "Collection loop: background error on cluster '%s' — skipping: %s",
                                c["name"], exc)
                            continue
                        raise

                    cid = str(c.get("id", "default"))

                    # Preserve enriched topics/groups from last snapshot
                    # (topic describe and lag scan run in parallel below)
                    last = _ks.get_cluster_data(cid)
                    if last:
                        # Keep enriched topic data — only update if we have new describe data
                        if any(t.get("partition_count", 0) > 0 for t in last.get("topics", [])):
                            data["topics"] = last["topics"]
                        if last.get("counts"):
                            data["counts"] = last["counts"]

                    # Run topic describe, prometheus, lag scan in parallel
                    async def _loop_topic_describe():
                        try:
                            all_topic_names = await collector.list_all_topics()
                            if all_topic_names:
                                described_topics, total_urp = await collector.describe_all_topics(
                                    all_topic_names, workers=10)
                                if described_topics:
                                    total_rf1 = sum(1 for t in described_topics
                                                   if t.get("replication_factor") == 1)
                                    total_partitions = sum(t.get("partition_count", 0)
                                                          for t in described_topics)
                                    if "counts" not in data:
                                        data["counts"] = {}
                                    data["counts"]["total_topics"] = len(all_topic_names)
                                    data["counts"]["total_rf1"] = total_rf1
                                    data["counts"]["total_urp"] = total_urp
                                    data["counts"]["total_partitions"] = total_partitions
                                    if "cluster" in data:
                                        data["cluster"]["under_replicated_partitions"] = total_urp
                                        data["cluster"]["partition_count"] = total_partitions
                                    # data["topics"] owned by _loop_prometheus (parallel-safe; no write here)
                                    logger.info(
                                        "Collection loop topic scan: %d topics (%d RF=1) for '%s'",
                                        len(described_topics), total_rf1, c["name"])
                                    # Upsert ALL topic names to kafka_topic_names for search autocomplete
                                    try:
                                        from storage import get_backend as _gb_names2
                                        await _gb_names2().upsert_topic_names(
                                            cluster_id=int(c.get("id", 0)),
                                            topics=described_topics,
                                        )
                                    except Exception as _tne2:
                                        logger.warning("upsert_topic_names (loop) failed: %s", _tne2)
                        except Exception as _te:
                            logger.warning("Collection loop topic scan failed for '%s': %s",
                                          c["name"], _te)

                    async def _loop_prometheus():
                        _prom_port = c.get("prometheus_port")
                        _jmx_port = c.get("jmx_port")
                        if _prom_port and _PROMETHEUS_AVAILABLE:
                            try:
                                from storage import get_backend as _gb_all
                                import json as _j_all
                                _all_cfg = await _gb_all().get_all()
                                # Skip brokers marked throughput_available=False in DB — saves timeout wait
                                try:
                                    scrape_brokers = [
                                        b for b in data.get("brokers", [])
                                        if b.get("host") and _j_all.loads(
                                            _all_cfg.get(f"phase2_{b['host']}:{_prom_port}", '{"throughput_available": true}')
                                        ).get("throughput_available") is not False
                                    ]
                                    if not scrape_brokers:
                                        scrape_brokers = data.get("brokers", [])
                                except Exception:
                                    scrape_brokers = data.get("brokers", [])
                                broker_metrics = await scrape_all_brokers(scrape_brokers, _prom_port, cpu_cores=c.get("cpu_cores"))
                                for broker in data.get("brokers", []):
                                    bid = str(broker.get("broker_id", broker.get("host", "")))
                                    if bid in broker_metrics and broker_metrics[bid]:
                                        broker.update(broker_metrics[bid])
                                if data.get("brokers"):
                                    first_broker = data["brokers"][0].get("host", "")
                                    # Use first broker with throughput available for topic metrics
                                    # Pick available broker from DB phase2 state — single DB read
                                    available_broker = ""
                                    try:
                                        for b in data.get("brokers", []):
                                            _host = b.get("host", "")
                                            if not _host:
                                                continue
                                            _p2 = _all_cfg.get(f"phase2_{_host}:{_prom_port}")
                                            if _p2 and _j_all.loads(_p2).get("throughput_available") is True:
                                                available_broker = _host
                                                break
                                    except Exception as _p2e:
                                        logger.warning("phase2 broker lookup failed: %s", _p2e)
                                    if not available_broker:
                                        available_broker = next(
                                            (b.get("host","") for b in data.get("brokers",[]) if b.get("host")),
                                            ""
                                        )
                                    logger.info("Prometheus topic scrape: using broker %s", available_broker)
                                    if available_broker:
                                        topic_metrics, top_by_size, top_by_msg_rate = await scrape_topic_metrics_and_top_by_size(
                                            available_broker, _prom_port, [], top_n=200)
                                        if "counts" not in data:
                                            data["counts"] = {}
                                        data["counts"]["top_topics_by_size"] = top_by_size
                                        data["counts"]["top_topics_by_msg_rate"] = top_by_msg_rate
                                        data["topics"] = list(top_by_msg_rate)
                                        data["counts"]["total_hot"] = sum(
                                            1 for t in top_by_msg_rate
                                            if (t.get("messages_in_per_sec") or 0) > 1000)
                                        # Persist counts directly to DB — bypasses cache dependency
                                        try:
                                            from routes_settings import _upsert
                                            import json as _j
                                            await _upsert(f"kafka_counts_metrics_{cid}",
                                                _j.dumps({
                                                    "top_topics_by_size": top_by_size,
                                                    "top_topics_by_msg_rate": top_by_msg_rate,
                                                    "total_hot": data["counts"].get("total_hot", 0),
                                                }))
                                        except Exception as _ue:
                                            logger.warning("Failed to persist counts to DB: %s", _ue)
                                logger.info("Collection loop Prometheus: completed for '%s'", c["name"])
                            except Exception as _pe:
                                logger.warning("Collection loop Prometheus failed for '%s': %s",
                                              c["name"], _pe)
                        elif _jmx_port:
                            try:
                                jmx_port = int(_jmx_port)
                                for broker in data.get("brokers", []):
                                    host = broker.get("host", "")
                                    if host:
                                        broker.update(collector._query_jmx(host, jmx_port))
                                logger.info("Collection loop JMX: completed for '%s'", c["name"])
                            except Exception as _je:
                                logger.warning("Collection loop JMX failed for '%s': %s",
                                              c["name"], _je)

                    async def _loop_lag_scan():
                        try:
                            active_gids = [g.get("group_id") or g.get("group_name")
                                          for g in data.get("consumer_groups", [])
                                          if g.get("group_id") or g.get("group_name")]
                            if active_gids:
                                lag_results = await collector.fetch_all_group_lags(active_gids)
                                lag_map = {(lr.get("group_id") or lr.get("group_name")): lr
                                          for lr in lag_results}
                                enriched = 0
                                for g in data.get("consumer_groups", []):
                                    gid = g.get("group_id") or g.get("group_name")
                                    if gid and gid in lag_map:
                                        g.update(lag_map[gid])
                                        enriched += 1
                                data["consumer_groups"].sort(
                                    key=lambda g: g.get("total_lag", 0), reverse=True)
                                logger.info(
                                    "Collection loop lag scan: enriched %d/%d groups for '%s'",
                                    enriched, len(active_gids), c["name"])
                        except Exception as _le:
                            logger.warning("Collection loop lag scan failed for '%s': %s",
                                          c["name"], _le)

                    # Topic describe first (Prometheus reads topic list)
                    # Then Prometheus + lag in parallel (independent data keys)
                    # Run all 3 scans in parallel — no dependencies between them
                    await asyncio.gather(
                        _loop_topic_describe(),
                        _loop_prometheus(),
                        _loop_lag_scan()
                    )

                    # Save complete enriched snapshot
                    _ks.set_cluster_data(
                        data,
                        source_type=c.get("source_type", "kafka_internal"),
                        cluster_id=cid,
                    )
                    await save_topics_structure(cid)
                    await save_brokers(cid, brokers=data.get("brokers", []))
                    await save_topics_metrics(cid)
                    await save_groups(cid)

                    # Insert lag snapshot for fast trend queries
                    try:
                        from database import SessionLocal
                        from sqlalchemy import text as _text
                        groups = data.get("consumer_groups", [])
                        _total_lag = sum(
                            g.get("total_lag", 0)
                            for g in groups
                            if isinstance(g, dict)
                        )
                        _group_count = len(groups)
                        async with SessionLocal() as _sess:
                            await _sess.execute(
                                _text("""
                                    INSERT INTO kafka_lag_snapshots
                                      (cluster_id, total_lag, group_count, collected_at)
                                    VALUES (:cid, :lag, :cnt, NOW())
                                """),
                                {
                                    "cid": str(cid),
                                    "lag": int(_total_lag),
                                    "cnt": int(_group_count)
                                }
                            )
                            await _sess.commit()
                    except Exception as _e:
                        logger.warning(f"[cluster {cid}] lag snapshot insert failed: {_e}")

                    # Save topic msg/sec to hourly aggregation table + upsert topic names
                    from datetime import datetime, timezone
                    from storage import get_backend
                    topics = data.get("topics", [])
                    if topics and c.get("id"):
                        try:
                            now_utc = datetime.now(timezone.utc)
                            await get_backend().upsert_topic_metrics_hourly(
                                cluster_id=int(c["id"]),
                                topics=topics,
                                collected_at=now_utc,
                            )
                            await get_backend().cleanup_topic_metrics_hourly(
                                cluster_id=int(c["id"]),
                            )
                        except Exception as _te:
                            logger.warning("upsert_topic_metrics_hourly failed for '%s': %s", c["name"], _te)

                    logger.info("Collection loop: completed full enrichment for '%s'", c["name"])
                    # Update last_synced timestamp
                    try:
                        from routes_settings import _config as _rs_config, _upsert
                        from datetime import datetime, timezone
                        _rs_config["last_synced"] = datetime.now(timezone.utc).isoformat()
                        await _upsert("last_synced", _rs_config["last_synced"])
                    except Exception as _ls_exc:
                        logger.debug("Failed to update last_synced: %s", _ls_exc)

                    # Anomaly detection and Teams escalation
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
                            last_sent = _collection_loop._summary_cooldown.get(cooldown_key, 0)
                            cooldown_mins = teams_cfg.get("teams_cooldown_mins", 10)
                            if now - last_sent >= cooldown_mins * 60:
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
                        logger.warning("Escalation failed for '%s': %s", c["name"], _esc_exc)

                except Exception as exc:
                    logger.warning("Collection loop: failed for cluster '%s': %s", c["name"], exc)

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
                from tools.prometheus_collector import restore_scrape_states
                await restore_scrape_states()
                for c in enabled:
                    # Restore last known snapshot — instant dashboard while scans run
                    cid = str(c.get("id", "default"))
                    restored = await restore_from_db(cid)
                    if restored:
                        logger.info("Startup: restored snapshot for '%s' from DB", c["name"])
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
                        # Do NOT overwrite restored cache with partial collect_summary data
                        # Parallel scans will enrich data and update cache incrementally
                        from datetime import datetime, timezone
                        from storage import get_backend
                        logger.info(
                            "Startup: synced cluster '%s' (id=%s) — brokers=%d, topics=%d",
                            c["name"], c.get("id"),
                            len(data.get("brokers", [])),
                            len(data.get("topics", [])),
                        )
                        # Background parallel topic describe — ALL topics for accurate KPIs
                        # Skip topic describe if restored snapshot already has enriched data
                        _existing_topics = data.get("topics", [])
                        _has_enriched_topics = any(t.get("partition_count", 0) > 0 for t in _existing_topics)
                        if not _has_enriched_topics:
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
                                        data["topics"] = []  # KPI counts computed above; topics populated after Prometheus
                                        _ks.update_topics_structure(
                                            str(c.get("id", "default")),
                                            [],
                                            {k: data["counts"][k] for k in
                                             ["total_topics","total_rf1","total_urp","total_partitions","total_brokers","total_groups"]
                                             if k in data.get("counts",{})}
                                        )
                                    _topic_elapsed = round(_t2.time() - _topic_start, 1)
                                    logger.info(
                                        "Topic scan: described %d topics (%d RF=1, %d URP, %d partitions) for '%s' in %ss",
                                        len(described_topics), total_rf1, total_urp, total_partitions, c["name"], _topic_elapsed
                                    )
                                    # Upsert ALL topic names to kafka_topic_names for search autocomplete
                                    try:
                                        from storage import get_backend as _gb_names
                                        await _gb_names().upsert_topic_names(
                                            cluster_id=int(c["id"]),
                                            topics=described_topics,
                                        )
                                        logger.info("Topic names upserted: %d topics for '%s'", len(described_topics), c["name"])
                                    except Exception as _tne:
                                        logger.warning("upsert_topic_names failed for '%s': %s", c["name"], _tne)
                            except Exception as _topic_exc:
                                logger.warning("Topic scan failed for '%s': %s", c["name"], _topic_exc)

                        # Run Prometheus enrichment and lag scan in parallel
                        # (safe: prometheus touches brokers/topics, lag touches consumer_groups only)
                        async def _run_prometheus():
                            _prom_port = c.get("prometheus_port")
                            _jmx_port = c.get("jmx_port")
                            if _prom_port and _PROMETHEUS_AVAILABLE:
                                try:
                                    from storage import get_backend as _gb_all
                                    import json as _j_all
                                    _all_cfg = await _gb_all().get_all()
                                    import time as _t3
                                    from tools.prometheus_collector import scrape_all_brokers
                                    _prom_start = _t3.time()
                                    logger.info("Prometheus scan: scraping %d brokers on port %d for '%s'",
                                               len(data.get("brokers", [])), _prom_port, c["name"])
                                    # Single scrape at startup — rates build over collection cycles
                                    # Skip brokers marked throughput_available=False in DB — saves timeout wait
                                    try:
                                        scrape_brokers = [
                                            b for b in data.get("brokers", [])
                                            if b.get("host") and _j_all.loads(
                                                _all_cfg.get(f"phase2_{b['host']}:{_prom_port}", '{"throughput_available": true}')
                                            ).get("throughput_available") is not False
                                        ]
                                        if not scrape_brokers:
                                            scrape_brokers = data.get("brokers", [])
                                    except Exception:
                                        scrape_brokers = data.get("brokers", [])
                                    broker_metrics = await scrape_all_brokers(scrape_brokers, _prom_port, cpu_cores=c.get("cpu_cores"))
                                    for broker in data.get("brokers", []):
                                        bid = str(broker.get("broker_id", broker.get("host", "")))
                                        if bid in broker_metrics and broker_metrics[bid]:
                                            broker.update(broker_metrics[bid])
                                    # Update brokers in cache
                                    _ks.update_brokers(str(c.get("id", "default")), data.get("brokers", []))
                                    if data.get("brokers"):
                                        first_broker = data["brokers"][0].get("host", "")
                                        # Pick available broker from DB phase2 state — single DB read
                                        available_broker = ""
                                        try:
                                            for b in data.get("brokers", []):
                                                _host = b.get("host", "")
                                                if not _host:
                                                    continue
                                                _p2 = _all_cfg.get(f"phase2_{_host}:{_prom_port}")
                                                if _p2 and _j_all.loads(_p2).get("throughput_available") is True:
                                                    available_broker = _host
                                                    break
                                        except Exception as _p2e:
                                            logger.warning("phase2 broker lookup failed: %s", _p2e)
                                        if not available_broker:
                                            available_broker = next(
                                                (b.get("host","") for b in data.get("brokers",[]) if b.get("host")),
                                                ""
                                            )
                                        logger.info("Prometheus topic scrape: using broker %s", available_broker)
                                        if available_broker:
                                            topic_metrics, top_by_size, top_by_msg_rate = await scrape_topic_metrics_and_top_by_size(
                                                available_broker, _prom_port, [], top_n=200)
                                            if "counts" not in data:
                                                data["counts"] = {}
                                            data["counts"]["top_topics_by_size"] = top_by_size
                                            data["counts"]["top_topics_by_msg_rate"] = top_by_msg_rate
                                            data["topics"] = list(top_by_msg_rate)
                                            data["counts"]["total_hot"] = sum(
                                                1 for t in top_by_msg_rate
                                                if (t.get("messages_in_per_sec") or 0) > 1000)
                                            # Persist counts directly to DB — bypasses cache dependency
                                            try:
                                                from routes_settings import _upsert
                                                import json as _j
                                                await _upsert(f"kafka_counts_metrics_{cid}",
                                                    _j.dumps({
                                                        "top_topics_by_size": top_by_size,
                                                        "top_topics_by_msg_rate": top_by_msg_rate,
                                                        "total_hot": data["counts"].get("total_hot", 0),
                                                    }))
                                            except Exception as _ue:
                                                logger.warning("Failed to persist counts to DB: %s", _ue)
                                            _ks.update_topics_metrics(
                                                str(c.get("id", "default")),
                                                {},
                                                {"top_topics_by_size": top_by_size,
                                                 "top_topics_by_msg_rate": top_by_msg_rate,
                                                 "total_hot": data["counts"].get("total_hot", 0)}
                                            )
                                    _prom_elapsed = round(_t3.time() - _prom_start, 1)
                                    logger.info("Prometheus scan: completed for '%s' in %ss", c["name"], _prom_elapsed)
                                except Exception as _prom_exc:
                                    logger.warning("Prometheus scan failed for '%s': %s", c["name"], _prom_exc)
                            elif c.get("jmx_port"):
                                try:
                                    import time as _t3
                                    _jmx_start = _t3.time()
                                    jmx_port = int(c["jmx_port"])
                                    broker_hosts = [b.get("host", "") for b in data.get("brokers", []) if b.get("host")]
                                    if broker_hosts:
                                        logger.info("JMX scan: querying %d brokers on port %d for '%s'",
                                                   len(broker_hosts), jmx_port, c["name"])
                                        for broker in data.get("brokers", []):
                                            host = broker.get("host", "")
                                            if host:
                                                jmx_data = collector._query_jmx(host, jmx_port)
                                                broker.update(jmx_data)
                                    _jmx_elapsed = round(_t3.time() - _jmx_start, 1)
                                    logger.info("JMX scan: completed for '%s' in %ss", c["name"], _jmx_elapsed)
                                except Exception as _jmx_exc:
                                    logger.warning("JMX scan failed for '%s': %s", c["name"], _jmx_exc)

                        async def _run_lag_scan():
                            try:
                                import time as _t
                                _lag_start = _t.time()
                                active_gids = [g.get("group_id") or g.get("group_name")
                                              for g in data.get("consumer_groups", [])
                                              if g.get("group_id") or g.get("group_name")]
                                if active_gids:
                                    logger.info("Lag scan: starting parallel scan for %d active groups on '%s'",
                                               len(active_gids), c["name"])
                                    enriched = 0
                                    lag_results = await collector.fetch_all_group_lags(active_gids)
                                    # fetch_all_group_lags returns a list — index it by group_id
                                    lag_map = {(lr.get("group_id") or lr.get("group_name")): lr for lr in lag_results}
                                    for g in data.get("consumer_groups", []):
                                        gid = g.get("group_id") or g.get("group_name")
                                        if gid and gid in lag_map:
                                            g.update(lag_map[gid])
                                            enriched += 1
                                    data["consumer_groups"].sort(
                                        key=lambda g: g.get("total_lag", 0), reverse=True)
                                    _ks.update_groups(
                                        str(c.get("id", "default")),
                                        data["consumer_groups"]
                                    )
                                    _lag_elapsed = round(_t.time() - _lag_start, 1)
                                    logger.info("Lag scan: enriched %d/%d groups for '%s' in %ss",
                                               enriched, len(active_gids), c["name"], _lag_elapsed)
                            except Exception as _lag_exc:
                                logger.warning("Lag scan failed for '%s': %s", c["name"], _lag_exc)

                        # Run both in parallel — independent data keys
                        await asyncio.gather(_run_prometheus(), _run_lag_scan())

                        # Final save with all enrichments — split keys per scan type
                        _ks.set_cluster_data(
                            data,
                            source_type=c.get("source_type", "kafka_internal"),
                            cluster_id=str(c.get("id", "default")),
                        )
                        await save_topics_structure(cid)
                        await save_brokers(cid, brokers=data.get("brokers", []))
                        await save_topics_metrics(cid)
                        await save_groups(cid)

                        # Insert lag snapshot for fast trend queries
                        try:
                            from database import SessionLocal
                            from sqlalchemy import text as _text
                            groups = data.get("consumer_groups", [])
                            _total_lag = sum(
                                g.get("total_lag", 0)
                                for g in groups
                                if isinstance(g, dict)
                            )
                            _group_count = len(groups)
                            async with SessionLocal() as _sess:
                                await _sess.execute(
                                    _text("""
                                        INSERT INTO kafka_lag_snapshots
                                          (cluster_id, total_lag, group_count, collected_at)
                                        VALUES (:cid, :lag, :cnt, NOW())
                                    """),
                                    {
                                        "cid": str(cid),
                                        "lag": int(_total_lag),
                                        "cnt": int(_group_count)
                                    }
                                )
                                await _sess.commit()
                        except Exception as _e:
                            logger.warning(f"[cluster {cid}] lag snapshot insert failed: {_e}")

                        # Save topic msg/sec to hourly aggregation table + upsert topic names
                        topics = data.get("topics", [])
                        if topics and c.get("id"):
                            try:
                                await get_backend().upsert_topic_metrics_hourly(
                                    cluster_id=int(c["id"]),
                                    topics=topics,
                                    collected_at=datetime.now(timezone.utc),
                                )
                            except Exception as _te:
                                logger.warning("upsert_topic_metrics_hourly failed for '%s': %s", c["name"], _te)
                    except Exception as exc:
                        logger.warning("Startup: failed to sync cluster '%s': %s", c["name"], exc)
            asyncio.create_task(_startup_sync())
        else:
            logger.info("Startup: no enabled clusters — starting with empty store")
    except Exception as exc:
        logger.warning("Startup: could not restore from DB: %s", exc)

    async def _delayed_collection_loop():
        """Wait for startup scans to complete before starting collection loop."""
        await asyncio.sleep(300)  # 5 mins — enough for topic describe + prometheus + lag
        await _collection_loop()

    collection_task = asyncio.create_task(_delayed_collection_loop())

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
