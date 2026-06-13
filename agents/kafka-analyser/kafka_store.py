"""Multi-cluster Kafka store — time-series persistence per scan type.

Each scan appends a NEW immutable row to `kafka_metrics_history` (INSERT, never
upsert), so the table holds a history of every collection. The dashboard always
reads the *latest* row per scan_type; the in-memory cache continues to serve the
dashboard instantly and is rehydrated from the latest rows on startup.

Retention (enforced on startup and once per day):
  - brokers / groups / counts ....... 30 days
  - topics_metrics .................. 7 days
  - topics_structure ................ written at most once per day; kept 90 days

The four `save_*` functions and `restore_from_db` keep their original signatures;
`save_to_db` remains a convenience wrapper that calls all four.
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

# ── Time-series config ───────────────────────────────────────────────────────

TABLE = "kafka_metrics_history"

SCAN_BROKERS = "brokers"
SCAN_GROUPS = "groups"
SCAN_COUNTS = "counts"
SCAN_TOPICS_METRICS = "topics_metrics"
SCAN_TOPICS_STRUCTURE = "topics_structure"

# Scan types the dashboard rehydrates and the read path looks up.
SCAN_TYPES = (
    SCAN_BROKERS,
    SCAN_GROUPS,
    SCAN_COUNTS,
    SCAN_TOPICS_METRICS,
    SCAN_TOPICS_STRUCTURE,
)

# Retention in days per scan type.
RETENTION_DAYS = {
    SCAN_BROKERS: 30,
    SCAN_GROUPS: 30,
    SCAN_COUNTS: 30,
    SCAN_TOPICS_METRICS: 7,
    SCAN_TOPICS_STRUCTURE: 90,
}

# Lazy one-time table creation + daily cleanup bookkeeping.
_table_ready = False
_table_lock = threading.Lock()
_last_cleanup_date: Any = None


# ── In-memory cache (unchanged — serves the dashboard instantly) ─────────────

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
            "broker_count": d.get("counts", {}).get("total_brokers") or len(d.get("brokers", [])),
            "consumer_group_count": d.get("counts", {}).get("total_groups") or len(d.get("consumer_groups", [])),
            "topic_count": d.get("counts", {}).get("total_topics") or len(d.get("topics", [])),
            "connector_count": len(d.get("connectors", [])),
        }


# ── Postgres time-series persistence ─────────────────────────────────────────

async def _ensure_table() -> bool:
    """Create the history table + index once. Returns False if no DB configured."""
    global _table_ready
    try:
        from database import SessionLocal
        from sqlalchemy import text
        if SessionLocal is None:
            return False
        with _table_lock:
            if _table_ready:
                return True
        async with SessionLocal() as session:
            await session.execute(text(
                f"""
                CREATE TABLE IF NOT EXISTS {TABLE} (
                    id BIGSERIAL PRIMARY KEY,
                    cluster_id TEXT NOT NULL,
                    scan_type TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    collected_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            ))
            await session.execute(text(
                f"""
                CREATE INDEX IF NOT EXISTS ix_kmh_cluster_scan_time
                ON {TABLE} (cluster_id, scan_type, collected_at DESC)
                """
            ))
            await session.commit()
        with _table_lock:
            _table_ready = True
        return True
    except Exception as exc:
        logger.warning("Failed to ensure %s table: %s", TABLE, exc)
        return False


async def _insert_metric(cluster_id: str, scan_type: str, payload: Any) -> None:
    """Append a new history row for one scan type (INSERT, never upsert)."""
    if not await _ensure_table():
        return
    try:
        from database import SessionLocal
        from sqlalchemy import text
        value = json.dumps(payload, default=str)
        async with SessionLocal() as session:
            await session.execute(
                text(
                    f"INSERT INTO {TABLE} (cluster_id, scan_type, data_json) "
                    f"VALUES (:cid, :st, :val)"
                ),
                {"cid": cluster_id, "st": scan_type, "val": value},
            )
            await session.commit()
    except Exception as exc:
        logger.warning("Failed to insert %s metric for %s: %s",
                       scan_type, cluster_id, exc)


async def _latest_metric(cluster_id: str, scan_type: str) -> Any | None:
    """Read and decode the latest history row for one scan type."""
    try:
        from database import SessionLocal
        from sqlalchemy import text
        if SessionLocal is None:
            return None
        async with SessionLocal() as session:
            row = (await session.execute(
                text(
                    f"SELECT data_json FROM {TABLE} "
                    f"WHERE cluster_id = :cid AND scan_type = :st "
                    f"ORDER BY collected_at DESC, id DESC LIMIT 1"
                ),
                {"cid": cluster_id, "st": scan_type},
            )).first()
        if not row:
            return None
        return json.loads(row[0])
    except Exception as exc:
        logger.warning("Failed to read latest %s for %s: %s",
                       scan_type, cluster_id, exc)
        return None


async def _structure_written_today(cluster_id: str) -> bool:
    """True if a topics_structure row already exists for today (once-per-day write)."""
    try:
        from database import SessionLocal
        from sqlalchemy import text
        if SessionLocal is None:
            return False
        async with SessionLocal() as session:
            row = (await session.execute(
                text(
                    f"SELECT 1 FROM {TABLE} "
                    f"WHERE cluster_id = :cid AND scan_type = :st "
                    f"AND collected_at >= date_trunc('day', now()) LIMIT 1"
                ),
                {"cid": cluster_id, "st": SCAN_TOPICS_STRUCTURE},
            )).first()
        return row is not None
    except Exception as exc:
        logger.warning("Failed to check topics_structure freshness for %s: %s",
                       cluster_id, exc)
        return False


async def cleanup_old_metrics() -> None:
    """Delete history rows older than each scan type's retention window."""
    if not await _ensure_table():
        return
    try:
        from database import SessionLocal
        from sqlalchemy import text
        async with SessionLocal() as session:
            for scan_type, days in RETENTION_DAYS.items():
                await session.execute(
                    text(
                        f"DELETE FROM {TABLE} WHERE scan_type = :st "
                        f"AND collected_at < now() - make_interval(days => :days)"
                    ),
                    {"st": scan_type, "days": int(days)},
                )
            await session.commit()
        logger.info("Cleaned up %s rows beyond retention", TABLE)
    except Exception as exc:
        logger.warning("Failed to clean up %s: %s", TABLE, exc)


async def _maybe_cleanup() -> None:
    """Run cleanup at most once per calendar day (UTC)."""
    global _last_cleanup_date
    today = datetime.now(timezone.utc).date()
    with _table_lock:
        if _last_cleanup_date == today:
            return
        _last_cleanup_date = today
    await cleanup_old_metrics()


async def save_brokers(cluster_id: str) -> None:
    """Append broker data to history — called by Prometheus scan."""
    data = get_cluster_data(cluster_id)
    if not data:
        return
    await _insert_metric(cluster_id, SCAN_BROKERS, data.get("brokers", []))
    await _maybe_cleanup()
    logger.info("Brokers appended to history for cluster %s", cluster_id)


async def save_topics_structure(cluster_id: str) -> None:
    """Append topic structure to history (once per day) — called by describe scan."""
    data = get_cluster_data(cluster_id)
    if not data:
        return
    if await _structure_written_today(cluster_id):
        logger.info("Topics structure already saved today for cluster %s — skipped",
                    cluster_id)
        return
    topics = data.get("topics", [])
    counts = data.get("counts", {})
    counts_structure = {
        k: counts[k] for k in
        ["total_topics", "total_rf1", "total_urp", "total_partitions",
         "total_brokers", "total_groups"]
        if k in counts
    }
    await _insert_metric(cluster_id, SCAN_TOPICS_STRUCTURE,
                         {"topics": topics, "counts": counts_structure})
    await _maybe_cleanup()
    logger.info("Topics structure appended to history for cluster %s", cluster_id)


async def save_topics_metrics(cluster_id: str) -> None:
    """Append topic metrics + counts snapshot to history — called by Prometheus scan."""
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
    await _insert_metric(cluster_id, SCAN_TOPICS_METRICS, topic_metrics)
    # Persist the full merged counts as its own (30-day) scan_type.
    counts = data.get("counts", {})
    if counts:
        await _insert_metric(cluster_id, SCAN_COUNTS, counts)
    await _maybe_cleanup()
    logger.info("Topics metrics appended to history for cluster %s", cluster_id)


async def save_groups(cluster_id: str) -> None:
    """Append consumer groups to history — called by lag scan."""
    data = get_cluster_data(cluster_id)
    if not data:
        return
    await _insert_metric(cluster_id, SCAN_GROUPS, data.get("consumer_groups", []))
    await _maybe_cleanup()
    logger.info("Groups appended to history for cluster %s", cluster_id)


async def restore_from_db(cluster_id: str) -> bool:
    """Rehydrate the memory cache from the latest history row per scan type."""
    if not await _ensure_table():
        return False
    # Startup cleanup.
    await _maybe_cleanup()
    try:
        # One-time migration from old AgentConfig keys
        await migrate_from_agent_config(cluster_id)
        brokers = await _latest_metric(cluster_id, SCAN_BROKERS)
        structure = await _latest_metric(cluster_id, SCAN_TOPICS_STRUCTURE)
        topic_metrics = await _latest_metric(cluster_id, SCAN_TOPICS_METRICS)
        groups = await _latest_metric(cluster_id, SCAN_GROUPS)
        counts_row = await _latest_metric(cluster_id, SCAN_COUNTS)

        # Need at least brokers or topics to be useful.
        if brokers is None and structure is None:
            logger.info("No history rows in DB for cluster %s", cluster_id)
            return False

        data: dict[str, Any] = {"cluster": {"id": cluster_id}}
        counts: dict[str, Any] = {}

        if brokers is not None:
            data["brokers"] = brokers

        if structure is not None:
            topics = structure.get("topics", []) if isinstance(structure, dict) else structure
            if isinstance(structure, dict):
                counts.update(structure.get("counts", {}))
            # Merge latest per-topic metrics into the structure topics.
            if topic_metrics:
                for t in topics:
                    name = t.get("name", "")
                    if name in topic_metrics:
                        t.update(topic_metrics[name])
            data["topics"] = topics

        if groups is not None:
            data["consumer_groups"] = groups

        # Standalone counts row (latest, full merged) wins over structure counts.
        if counts_row:
            counts.update(counts_row)
        if counts:
            data["counts"] = counts

        set_cluster_data(data, source_type="kafka_internal", cluster_id=cluster_id)
        logger.info("Restored latest history into cache for cluster %s", cluster_id)
        return True
    except Exception as exc:
        logger.warning("Failed to restore history for cluster %s: %s", cluster_id, exc)
        return False


async def migrate_from_agent_config(cluster_id: str) -> bool:
    """One-time migration: seed kafka_metrics_history from old AgentConfig split keys.
    Runs only if no rows exist in kafka_metrics_history for this cluster_id.
    Returns True if migration was performed."""
    try:
        from database import SessionLocal
        from sqlalchemy import text
        if SessionLocal is None:
            return False
        async with SessionLocal() as session:
            # Check if history table already has data for this cluster
            existing = await session.execute(text(
                "SELECT COUNT(*) FROM kafka_metrics_history WHERE cluster_id = :cid"
            ), {"cid": cluster_id})
            if existing.scalar() > 0:
                return False  # Already migrated

            # Read old AgentConfig keys
            from models import AgentConfig
            from sqlalchemy import select
            old_keys = {
                "brokers": f"kafka_brokers_{cluster_id}",
                "topics_structure": f"kafka_topics_structure_{cluster_id}",
                "topics_metrics": f"kafka_topics_metrics_{cluster_id}",
                "groups": f"kafka_groups_{cluster_id}",
                "counts": f"kafka_counts_structure_{cluster_id}",
            }
            now = datetime.now(timezone.utc)
            migrated = 0
            for scan_type, key in old_keys.items():
                row = (await session.execute(
                    select(AgentConfig).where(
                        AgentConfig.agent_slug == "kafka-analyser",
                        AgentConfig.key == key,
                    )
                )).scalar_one_or_none()
                if row:
                    await session.execute(text("""
                        INSERT INTO kafka_metrics_history (cluster_id, scan_type, data_json, collected_at)
                        VALUES (:cid, :scan_type, :data, :ts)
                    """), {"cid": cluster_id, "scan_type": scan_type,
                           "data": row.value, "ts": now})
                    migrated += 1
            await session.commit()
            if migrated > 0:
                logger.info("Migrated %d scan types from AgentConfig to kafka_metrics_history for cluster %s",
                           migrated, cluster_id)
            return migrated > 0
    except Exception as exc:
        logger.warning("Migration failed for cluster %s: %s", cluster_id, exc)
        return False


# Backward compatibility — old save_to_db saves everything.
async def save_to_db(cluster_id: str) -> None:
    """Append all scan data to history. Use specific save_* functions for partial saves."""
    await save_brokers(cluster_id)
    await save_topics_structure(cluster_id)
    await save_topics_metrics(cluster_id)
    await save_groups(cluster_id)
