"""Multi-cluster Kafka store backed by PostgreSQL via AgentConfig table.
Follows the same pattern as Alert Analyser storage.py.
Falls back to in-memory if DB unavailable.
"""
from __future__ import annotations
import json
import logging
import threading
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("kafka-analyser")

# In-memory cache — always kept in sync with DB writes
# This ensures get_cluster_data() is instant (reads from memory, not DB)
_lock = threading.Lock()
_cache: dict[str, dict[str, Any]] = {}   # cluster_id → latest data
_last_active: str | None = None


def set_cluster_data(data: dict[str, Any], source_type: str = "synthetic",
                     cluster_id: str | None = None) -> None:
    """Write cluster data to memory cache. Call save_to_db() separately to persist."""
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


def get_cluster_history(cluster_id: str | None = None,
                        hours: float | None = None) -> list[dict[str, Any]]:
    """Single snapshot — no history in this backend."""
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
                "loaded": False,
                "source_type": "synthetic",
                "last_synced": None,
                "broker_count": 0,
                "consumer_group_count": 0,
                "topic_count": 0,
                "connector_count": 0,
            }
        d = _cache[cid]
        return {
            "loaded": True,
            "source_type": "kafka_internal",
            "last_synced": datetime.now(timezone.utc).isoformat(),
            "broker_count": len(d.get("brokers", [])),
            "consumer_group_count": len(d.get("consumer_groups", [])),
            "topic_count": len(d.get("topics", [])),
            "connector_count": len(d.get("connectors", [])),
        }


# ── Postgres persistence ─────────────────────────────────────────────────────
# Uses AgentConfig table (same as Alert Analyser) with key = "cluster_snapshot_{cid}"

async def save_to_db(cluster_id: str) -> None:
    """Persist cluster snapshot to AgentConfig table."""
    data = get_cluster_data(cluster_id)
    if not data:
        return
    try:
        from database import SessionLocal
        from models import AgentConfig
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        if SessionLocal is None:
            return
        key = f"cluster_snapshot_{cluster_id}"
        value = json.dumps(data, default=str)
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
        logger.info("Snapshot saved to DB for cluster %s", cluster_id)
    except Exception as exc:
        logger.warning("Failed to save snapshot for cluster %s: %s", cluster_id, exc)


async def restore_from_db(cluster_id: str) -> bool:
    """Restore cluster snapshot from AgentConfig table into memory cache."""
    try:
        from database import SessionLocal
        from models import AgentConfig
        from sqlalchemy import select
        if SessionLocal is None:
            return False
        key = f"cluster_snapshot_{cluster_id}"
        async with SessionLocal() as session:
            row = (await session.execute(
                select(AgentConfig).where(
                    AgentConfig.agent_slug == "kafka-analyser",
                    AgentConfig.key == key,
                )
            )).scalar_one_or_none()
        if not row:
            logger.info("No snapshot in DB for cluster %s", cluster_id)
            return False
        data = json.loads(row.value)
        set_cluster_data(data, source_type="kafka_internal", cluster_id=cluster_id)
        logger.info("Restored snapshot from DB for cluster %s (saved at %s)",
                    cluster_id, row.updated_at)
        return True
    except Exception as exc:
        logger.warning("Failed to restore snapshot for cluster %s: %s", cluster_id, exc)
        return False
