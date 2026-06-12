"""Multi-cluster Kafka store — split column persistence per scan type.
Each scan saves only its own data keys — zero conflict between parallel scans.
In-memory cache serves dashboard instantly. PostgreSQL persists across restarts.
"""
from __future__ import annotations
import json
import logging
import threading
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("kafka-analyser")

_lock = threading.Lock()
_cache: dict[str, dict[str, Any]] = {}
_last_active: str | None = None


def set_cluster_data(data: dict[str, Any], source_type: str = "synthetic",
                     cluster_id: str | None = None) -> None:
    """Write full cluster data to memory cache."""
    global _last_active
    cid = cluster_id or data.get("cluster", {}).get("id") or "default"
    with _lock:
        _cache[cid] = data
        _last_active = cid


def get_cluster_data(cluster_id: str | None = None,
                     hours: int | None = None) -> dict[str, Any] | None:
    """Read latest cluster data from memory cache."""
    with _lock:
        cid = cluster_id or _last_active
        if not cid or cid not in _cache:
            return None
        return _cache[cid]


def update_brokers(cluster_id: str, brokers: list) -> None:
    """Update only broker data in memory cache — called by Prometheus scan."""
    with _lock:
        if cluster_id in _cache:
            _cache[cluster_id]["brokers"] = brokers


def update_topics_structure(cluster_id: str, topics: list,
                             counts_structure: dict) -> None:
    """Update topic structure data — called by topic describe scan."""
    with _lock:
        if cluster_id in _cache:
            _cache[cluster_id]["topics"] = topics
            if "counts" not in _cache[cluster_id]:
                _cache[cluster_id]["counts"] = {}
            _cache[cluster_id]["counts"].update(counts_structure)


def update_topics_metrics(cluster_id: str, topic_metrics: dict,
                           counts_metrics: dict) -> None:
    """Update topic metrics — called by Prometheus scan."""
    with _lock:
        if cluster_id in _cache:
            topics = _cache[cluster_id].get("topics", [])
            for t in topics:
                name = t.get("name", "")
                if name in topic_metrics:
                    t.update(topic_metrics[name])
            if "counts" not in _cache[cluster_id]:
                _cache[cluster_id]["counts"] = {}
            _cache[cluster_id]["counts"].update(counts_metrics)


def update_groups(cluster_id: str, groups: list) -> None:
    """Update consumer group data — called by lag scan."""
    with _lock:
        if cluster_id in _cache:
            _cache[cluster_id]["consumer_groups"] = groups


def get_cluster_history(cluster_id: str | None = None,
                        hours: float | None = None) -> list[dict[str, Any]]:
    data = get_cluster_data(cluster_id)
    if not data:
        return []
    return [{"data": data, "collected_at": datetime.now(timezone.utc).isoformat()}]


def get_all_cluster_ids() -> list[str]:
    with _lock:
        return list(_cache.keys())


def get_sync_meta(cluster_id: str | None = None) -> dict[str, Any]:
    with _lock:
        cid = cluster_id or _last_active
        if not cid or cid not in _cache:
            return {
                "loaded": False, "source_type": "synthetic",
                "last_synced": None, "broker_count": 0,
                "consumer_group_count": 0, "topic_count": 0, "connector_count": 0,
            }
        d = _cache[cid]
        return {
            "loaded": True, "source_type": "kafka_internal",
            "last_synced": datetime.now(timezone.utc).isoformat(),
            "broker_count": len(d.get("brokers", [])),
            "consumer_group_count": len(d.get("consumer_groups", [])),
            "topic_count": len(d.get("topics", [])),
            "connector_count": len(d.get("connectors", [])),
        }


# ── Postgres persistence — split keys per scan type ──────────────────────────

async def _upsert_key(key: str, value: str) -> None:
    """Upsert a single key in AgentConfig table."""
    try:
        from database import SessionLocal
        from models import AgentConfig
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        if SessionLocal is None:
            return
        now = datetime.now(timezone.utc)
        async with SessionLocal() as session:
            stmt = (
                pg_insert(AgentConfig)
                .values(agent_slug="kafka-analyser", key=key, value=value, updated_at=now)
                .on_conflict_do_update(
                    index_elements=["agent_slug", "key"],
                    set_={"value": value, "updated_at": now},
                )
            )
            await session.execute(stmt)
            await session.commit()
    except Exception as exc:
        logger.warning("Failed to upsert key %s: %s", key, exc)


async def _get_key(key: str) -> str | None:
    """Get a single key from AgentConfig table."""
    try:
        from database import SessionLocal
        from models import AgentConfig
        from sqlalchemy import select
        if SessionLocal is None:
            return None
        async with SessionLocal() as session:
            row = (await session.execute(
                select(AgentConfig).where(
                    AgentConfig.agent_slug == "kafka-analyser",
                    AgentConfig.key == key,
                )
            )).scalar_one_or_none()
        return row.value if row else None
    except Exception as exc:
        logger.warning("Failed to get key %s: %s", key, exc)
        return None


async def save_brokers(cluster_id: str) -> None:
    """Save broker data to DB — called by Prometheus scan."""
    data = get_cluster_data(cluster_id)
    if not data:
        return
    brokers = data.get("brokers", [])
    await _upsert_key(f"kafka_brokers_{cluster_id}", json.dumps(brokers, default=str))
    logger.info("Brokers saved to DB for cluster %s", cluster_id)


async def save_topics_structure(cluster_id: str) -> None:
    """Save topic structure to DB — called by topic describe scan."""
    data = get_cluster_data(cluster_id)
    if not data:
        return
    topics = data.get("topics", [])
    counts = data.get("counts", {})
    counts_structure = {
        k: counts[k] for k in
        ["total_topics", "total_rf1", "total_urp", "total_partitions", "total_brokers", "total_groups"]
        if k in counts
    }
    await _upsert_key(f"kafka_topics_structure_{cluster_id}",
                      json.dumps(topics, default=str))
    await _upsert_key(f"kafka_counts_structure_{cluster_id}",
                      json.dumps(counts_structure, default=str))
    logger.info("Topics structure saved to DB for cluster %s", cluster_id)


async def save_topics_metrics(cluster_id: str) -> None:
    """Save topic metrics to DB — called by Prometheus scan."""
    data = get_cluster_data(cluster_id)
    if not data:
        return
    topics = data.get("topics", [])
    topic_metrics = {
        t["name"]: {
            "messages_in_per_sec": t.get("messages_in_per_sec", 0.0),
            "bytes_in_per_sec": t.get("bytes_in_per_sec", 0.0),
            "bytes_out_per_sec": t.get("bytes_out_per_sec", 0.0),
            "size_bytes": t.get("size_bytes", 0),
        }
        for t in topics if t.get("name")
    }
    counts = data.get("counts", {})
    counts_metrics = {
        k: counts[k] for k in ["total_hot", "top_topics_by_size"]
        if k in counts
    }
    await _upsert_key(f"kafka_topics_metrics_{cluster_id}",
                      json.dumps(topic_metrics, default=str))
    await _upsert_key(f"kafka_counts_metrics_{cluster_id}",
                      json.dumps(counts_metrics, default=str))
    logger.info("Topics metrics saved to DB for cluster %s", cluster_id)


async def save_groups(cluster_id: str) -> None:
    """Save consumer groups to DB — called by lag scan."""
    data = get_cluster_data(cluster_id)
    if not data:
        return
    groups = data.get("consumer_groups", [])
    await _upsert_key(f"kafka_groups_{cluster_id}", json.dumps(groups, default=str))
    logger.info("Groups saved to DB for cluster %s", cluster_id)


async def restore_from_db(cluster_id: str) -> bool:
    """Restore all scan data from DB into memory cache."""
    try:
        # Load each data type independently
        brokers_raw = await _get_key(f"kafka_brokers_{cluster_id}")
        topics_structure_raw = await _get_key(f"kafka_topics_structure_{cluster_id}")
        topics_metrics_raw = await _get_key(f"kafka_topics_metrics_{cluster_id}")
        groups_raw = await _get_key(f"kafka_groups_{cluster_id}")
        counts_structure_raw = await _get_key(f"kafka_counts_structure_{cluster_id}")
        counts_metrics_raw = await _get_key(f"kafka_counts_metrics_{cluster_id}")

        # Need at least brokers or topics to be useful
        if not brokers_raw and not topics_structure_raw:
            logger.info("No snapshot in DB for cluster %s", cluster_id)
            return False

        # Build merged data dict
        data: dict[str, Any] = {"cluster": {"id": cluster_id}}

        if brokers_raw:
            data["brokers"] = json.loads(brokers_raw)

        if topics_structure_raw:
            topics = json.loads(topics_structure_raw)
            # Merge topic metrics into topics
            if topics_metrics_raw:
                topic_metrics = json.loads(topics_metrics_raw)
                for t in topics:
                    name = t.get("name", "")
                    if name in topic_metrics:
                        t.update(topic_metrics[name])
            data["topics"] = topics

        if groups_raw:
            data["consumer_groups"] = json.loads(groups_raw)

        # Merge counts
        counts: dict = {}
        if counts_structure_raw:
            counts.update(json.loads(counts_structure_raw))
        if counts_metrics_raw:
            counts.update(json.loads(counts_metrics_raw))
        if counts:
            data["counts"] = counts

        set_cluster_data(data, source_type="kafka_internal", cluster_id=cluster_id)
        logger.info("Restored snapshot from DB for cluster %s", cluster_id)
        return True
    except Exception as exc:
        logger.warning("Failed to restore snapshot for cluster %s: %s", cluster_id, exc)
        return False


# Keep backward compatibility — old save_to_db saves everything
async def save_to_db(cluster_id: str) -> None:
    """Save all scan data to DB. Use specific save_* functions for partial saves."""
    await save_brokers(cluster_id)
    await save_topics_structure(cluster_id)
    await save_topics_metrics(cluster_id)
    await save_groups(cluster_id)
