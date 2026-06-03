import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
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


class TestConnectionPayload(BaseModel):
    cluster: str  # "a" | "b"
    bootstrap_servers: str
    auth_type: str  # "none" | "sasl"
    sasl_username: str | None = None
    sasl_password: str | None = None
    sasl_mechanism: str = "PLAIN"
    tls_enabled: bool = False
    cluster_label: str = ""


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
async def save_settings(payload: SettingsPayload) -> dict:
    data = payload.model_dump()
    _config.update(data)
    for k, v in data.items():
        await _upsert(k, v)
    return {"ok": True}


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
        c = enabled[0]
        collector = RealKafkaCollector({
            "bootstrap_servers": c["bootstrap_servers"],
            "auth_type": "none" if c["auth_type"] == "none" else "sasl",
            "sasl_username": c.get("sasl_username"),
            "sasl_password": c.get("sasl_password"),
            "sasl_mechanism": c.get("sasl_mechanism", "PLAIN"),
            "tls_enabled": c.get("tls_enabled", False),
            "cluster_label": c["name"],
        })
        data = await collector.collect()
        kafka_store.set_cluster_data(data, source_type=c.get("source_type", "kafka_internal"))
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
