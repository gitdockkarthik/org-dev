import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import settings
from encryption import decrypt, encrypt, is_secret_key
from tools.real_kafka import RealKafkaCollector
from storage import get_backend
import kafka_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/settings", tags=["settings"])

_DEFAULTS: dict = {
    "source_type": "synthetic",
    "bootstrap_servers": "",
    "sasl_username": "",
    "sasl_password": "",
    "tls_enabled": False,
    "collection_interval_secs": 0,
    "last_synced": None,
    "broker_count": None,
    "consumer_group_count": None,
    "topic_count": None,
    "connector_count": None,
    "lag_threshold": 10000,
    "heap_threshold_pct": 80,
    "urp_threshold": 0,
    "connector_alert_enabled": True,
    "retention_threshold_pct": 80,
    # Cluster A — Internal (no auth)
    "cluster_a_enabled": False,
    "cluster_a_label": "Internal",
    "cluster_a_bootstrap": "",
    # Cluster B — External (SASL)
    "cluster_b_enabled": False,
    "cluster_b_label": "External",
    "cluster_b_bootstrap": "",
    "cluster_b_sasl_username": "",
    "cluster_b_sasl_password": "",
    "cluster_b_sasl_mechanism": "PLAIN",
    "cluster_b_tls_enabled": True,
    # Teams escalation
    "teams_enabled": False,
    "teams_webhook_url": "",
    "teams_severity_filter": ["critical", "warning"],
    "teams_cooldown_mins": 60,
}

# Write-through in-memory cache; populated from DB on startup.
_config: dict = dict(_DEFAULTS)


async def _upsert(key: str, value) -> None:
    raw = json.dumps(value)
    stored = encrypt(raw) if is_secret_key(key) else raw
    await get_backend().set(key, stored)


async def load_config_from_db() -> dict:
    try:
        raw_dict = await get_backend().get_all()
        if not raw_dict:
            return {}
        db_cfg: dict = {}
        for key, stored in raw_dict.items():
            secret = is_secret_key(key)
            try:
                raw = decrypt(stored) if secret else stored
                db_cfg[key] = json.loads(raw)
            except Exception as exc:
                logger.error("load_config_from_db: failed to decode key=%r: %s", key, exc)
        _config.update(db_cfg)
        logger.info("load_config_from_db: loaded keys: %s", list(db_cfg))
        return db_cfg
    except Exception:
        logger.exception("load_config_from_db: failed")
        return {}


class SettingsPayload(BaseModel):
    source_type: str = "synthetic"
    bootstrap_servers: str = ""
    sasl_username: str = ""
    sasl_password: str = ""
    tls_enabled: bool = False
    collection_interval_secs: int = 0
    lag_threshold: int = 10000
    heap_threshold_pct: int = 80
    urp_threshold: int = 0
    connector_alert_enabled: bool = True
    retention_threshold_pct: int = 80
    api_key: str = ""

    # Cluster A — Internal (no auth)
    cluster_a_enabled: bool = False
    cluster_a_label: str = "Internal"
    cluster_a_bootstrap: str = ""

    # Cluster B — External (SASL)
    cluster_b_enabled: bool = False
    cluster_b_label: str = "External"
    cluster_b_bootstrap: str = ""
    cluster_b_sasl_username: str = ""
    cluster_b_sasl_password: str = ""
    cluster_b_sasl_mechanism: str = "PLAIN"
    cluster_b_tls_enabled: bool = True

    # Teams escalation
    teams_enabled: bool = False
    teams_webhook_url: str = ""
    teams_severity_filter: list[str] = ["critical", "warning"]
    teams_cooldown_mins: int = 60


class TestConnectionPayload(BaseModel):
    cluster: str  # "a" | "b"
    bootstrap_servers: str
    auth_type: str  # "none" | "sasl"
    sasl_username: str | None = None
    sasl_password: str | None = None
    sasl_mechanism: str = "PLAIN"
    tls_enabled: bool = False
    cluster_label: str = ""
    jmx_port: int | None = None


class ClusterPayload(BaseModel):
    id: int | None = None
    name: str
    environment: str = "internal"
    bootstrap_servers: str
    auth_type: str = "none"
    sasl_username: str = ""
    sasl_password: str = ""
    sasl_mechanism: str = "PLAIN"
    tls_enabled: bool = False
    schema_registry_url: str = ""
    zookeeper_url: str = ""
    kafka_connect_url: str = ""
    jmx_port: int | None = None
    mirror_source_cluster_id: int | None = None
    mirror_mode: str = "none"
    enabled: bool = False


@router.get("")
async def get_settings() -> dict:
    await load_config_from_db()
    cfg = dict(_config)
    api_key = cfg.get("api_key", "")
    cfg["api_key_configured"] = bool(api_key)
    cfg["api_key_last4"] = api_key[-4:] if api_key else ""
    cfg.pop("api_key", None)
    cfg.pop("sasl_password", None)
    return cfg


@router.post("")
async def save_settings(request: Request) -> dict:
    data = await request.json()
    _config.update(data)
    for k, v in data.items():
        await _upsert(k, v)
    return {"ok": True}


@router.post("/test-teams")
async def test_teams_webhook(request: Request) -> dict:
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    webhook_url = body.get("webhook_url", "").strip()
    if not webhook_url:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="webhook_url required")

    from tools.escalation_notifier import escalate as _escalate_teams

    test_anomaly = {
        "severity": "info",
        "category": "test",
        "description": "This is a test message from Operative Intelligence. "
                       "Teams escalation is configured correctly.",
        "recommended_action": "No action required — this is a connectivity test.",
    }

    teams_cfg = {
        "teams_enabled": True,
        "teams_webhook_url": webhook_url,
        "teams_severity_filter": ["critical", "warning", "info"],
        "teams_cooldown_mins": 0,
    }

    success = await _escalate_teams(
        agent_name="Kafka Analyser",
        cluster_name="Test",
        anomaly=test_anomaly,
        config=teams_cfg,
        dashboard_url="",
    )

    if success:
        return {"ok": True, "message": "Test message sent successfully"}
    else:
        from fastapi import HTTPException
        raise HTTPException(status_code=502, detail="Failed to send test message")


@router.post("/test-connection")
async def test_connection(payload: TestConnectionPayload) -> dict:
    collector = RealKafkaCollector({
        "bootstrap_servers": payload.bootstrap_servers,
        "auth_type": payload.auth_type,
        "sasl_username": payload.sasl_username,
        "sasl_password": payload.sasl_password,
        "sasl_mechanism": payload.sasl_mechanism,
        "tls_enabled": payload.tls_enabled,
        "cluster_label": payload.cluster_label,
        "jmx_port": getattr(payload, 'jmx_port', None),
    })
    try:
        data = await collector.collect()
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001 — surface any failure to the UI as 400
        raise HTTPException(status_code=400, detail=str(exc))

    cluster = data.get("cluster", {})
    return {
        "ok": True,
        "broker_count": cluster.get("broker_count", len(data.get("brokers", []))),
        "topic_count": len(data.get("topics", [])),
        "cluster_id": str(cluster.get("id", "")),
    }


@router.get("/clusters")
async def list_clusters() -> dict:
    clusters = await get_backend().get_clusters(settings.agent_slug)
    # Never return sasl_password in plaintext — mask it
    for c in clusters:
        if c.get("sasl_password"):
            c["sasl_password"] = "••••••••"
    return {"clusters": clusters}


@router.post("/clusters")
async def create_cluster(payload: ClusterPayload) -> dict:
    cluster = payload.model_dump()
    cluster["agent_slug"] = settings.agent_slug
    cluster["source_type"] = (
        "kafka_sasl" if payload.auth_type != "none" else "kafka_internal"
    )
    cluster["status"] = "unchecked"
    saved = await get_backend().save_cluster(cluster)
    if saved.get("sasl_password"):
        saved["sasl_password"] = "••••••••"
    return {"cluster": saved}


@router.put("/clusters/{cluster_id}")
async def update_cluster(cluster_id: int, payload: ClusterPayload) -> dict:
    existing = await get_backend().get_cluster(cluster_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Cluster not found")
    cluster = payload.model_dump()
    cluster["id"] = cluster_id
    cluster["agent_slug"] = settings.agent_slug
    cluster["source_type"] = (
        "kafka_sasl" if payload.auth_type != "none" else "kafka_internal"
    )
    # If password is the mask, keep existing password
    if cluster.get("sasl_password") == "••••••••":
        cluster["sasl_password"] = existing.get("sasl_password", "")
    saved = await get_backend().save_cluster(cluster)
    if saved.get("sasl_password"):
        saved["sasl_password"] = "••••••••"
    return {"cluster": saved}


@router.delete("/clusters/{cluster_id}")
async def delete_cluster(cluster_id: int) -> dict:
    deleted = await get_backend().delete_cluster(cluster_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Cluster not found")
    return {"ok": True}


@router.post("/clusters/{cluster_id}/test")
async def test_cluster(cluster_id: int) -> dict:
    cluster = await get_backend().get_cluster(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    try:
        collector = RealKafkaCollector({
            "bootstrap_servers": cluster["bootstrap_servers"],
            "auth_type": "none" if cluster["auth_type"] == "none" else "sasl",
            "sasl_username": cluster.get("sasl_username"),
            "sasl_password": cluster.get("sasl_password"),
            "sasl_mechanism": cluster.get("sasl_mechanism", "PLAIN"),
            "tls_enabled": cluster.get("tls_enabled", False),
            "cluster_label": cluster["name"],
            "jmx_port": cluster.get("jmx_port"),
        })
        data = await collector.collect()
        await get_backend().update_cluster_status(
            cluster_id, "healthy",
            last_tested_at=datetime.now(timezone.utc)
        )
        return {
            "ok": True,
            "broker_count": len(data.get("brokers", [])),
            "topic_count": len(data.get("topics", [])),
            "cluster_id": data.get("cluster", {}).get("id", ""),
        }
    except Exception as exc:
        await get_backend().update_cluster_status(cluster_id, "error")
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/clusters/{cluster_id}/enable")
async def toggle_cluster(cluster_id: int, enabled: bool) -> dict:
    cluster = await get_backend().get_cluster(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    cluster["enabled"] = enabled
    await get_backend().save_cluster(cluster)
    return {"ok": True, "enabled": enabled}


@router.post("/sync")
async def sync_metrics() -> dict:
    source = _config.get("source_type", "synthetic")

    if source in ("kafka_internal", "kafka_sasl", "live"):
        clusters = await get_backend().get_clusters(settings.agent_slug)
        enabled = [c for c in clusters if c.get("enabled")]
        if not enabled:
            raise HTTPException(status_code=400,
                detail="No clusters enabled. Enable at least one cluster in Settings.")
        last_meta = None
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
                data = await collector.collect()
                kafka_store.set_cluster_data(
                    data,
                    source_type=c.get("source_type", "kafka_internal"),
                    cluster_id=str(c.get("id", "default"))
                )
                # Persist topic metrics to PostgreSQL for historical trending
                from datetime import datetime, timezone
                collected_at = datetime.now(timezone.utc)
                topics = data.get("topics", [])
                if topics and c.get("id"):
                    try:
                        await get_backend().save_topic_metrics(
                            cluster_id=int(c["id"]),
                            topics=topics,
                            collected_at=collected_at,
                        )
                    except Exception as _te:
                        import logging
                        logging.getLogger(__name__).warning(
                            "save_topic_metrics failed for cluster %s: %s", c["name"], _te
                        )
                last_meta = kafka_store.get_sync_meta(str(c.get("id", "default")))

                # Detect anomalies and escalate
                from tools.anomaly_detector import detect_anomalies as _detect_anomalies
                from tools.escalation_notifier import escalate

                anomalies = _detect_anomalies(data)

                teams_cfg = {
                    "teams_enabled": _config.get("teams_enabled", False),
                    "teams_webhook_url": _config.get("teams_webhook_url", ""),
                    "teams_severity_filter": _config.get("teams_severity_filter",
                                                         ["critical", "warning"]),
                    "teams_cooldown_mins": _config.get("teams_cooldown_mins", 60),
                }

                for anomaly in anomalies:
                    await escalate(
                        agent_name="Kafka Analyser",
                        cluster_name=c["name"],
                        anomaly=anomaly,
                        config=teams_cfg,
                        dashboard_url="",
                    )
            except Exception as exc:
                import logging
                logging.getLogger(__name__).warning(
                    "sync_metrics: failed to sync cluster '%s': %s", c["name"], exc
                )
        meta = last_meta or kafka_store.get_sync_meta()
        _config["last_synced"] = meta["last_synced"]
        _config["broker_count"] = meta["broker_count"]
        _config["consumer_group_count"] = meta["consumer_group_count"]
        _config["topic_count"] = meta["topic_count"]
        _config["connector_count"] = meta["connector_count"]
        await _upsert("last_synced", _config["last_synced"])
        await _upsert("broker_count", _config["broker_count"])
        await _upsert("consumer_group_count", _config["consumer_group_count"])
        await _upsert("topic_count", _config["topic_count"])
        await _upsert("connector_count", _config["connector_count"])
        return {
            "ok": True,
            "broker_count": meta["broker_count"],
            "consumer_group_count": meta["consumer_group_count"],
            "topic_count": meta["topic_count"],
            "connector_count": meta["connector_count"],
            "last_synced": meta["last_synced"],
        }
    if source in ("kafka", "msk"):
        raise HTTPException(
            status_code=400,
            detail=f"Source type '{source}' is available in Phase 3.",
        )

    from tools.synthetic import SyntheticCollector
    collector = SyntheticCollector()
    data = await collector.collect()
    kafka_store.set_cluster_data(data, source_type=source)

    meta = kafka_store.get_sync_meta()
    _config["last_synced"] = meta["last_synced"]
    _config["broker_count"] = meta["broker_count"]
    _config["consumer_group_count"] = meta["consumer_group_count"]
    _config["topic_count"] = meta["topic_count"]
    _config["connector_count"] = meta["connector_count"]

    await _upsert("last_synced", _config["last_synced"])
    await _upsert("broker_count", _config["broker_count"])
    await _upsert("consumer_group_count", _config["consumer_group_count"])
    await _upsert("topic_count", _config["topic_count"])
    await _upsert("connector_count", _config["connector_count"])

    return {
        "ok": True,
        "broker_count": meta["broker_count"],
        "consumer_group_count": meta["consumer_group_count"],
        "topic_count": meta["topic_count"],
        "connector_count": meta["connector_count"],
        "last_synced": meta["last_synced"],
    }
