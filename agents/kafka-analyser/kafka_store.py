"""Multi-cluster in-memory Kafka store with time-series history.
Stores timestamped snapshots per cluster_id. Retains up to 7 days.
"""
from __future__ import annotations
import threading
from datetime import datetime, timezone, timedelta
from typing import Any

_lock = threading.Lock()
_history: dict[str, list[dict[str, Any]]] = {}   # cluster_id → [snapshots]
_last_active: str | None = None
_MAX_AGE = timedelta(days=7)


def set_cluster_data(data: dict[str, Any], source_type: str = "synthetic", cluster_id: str | None = None) -> None:
    global _last_active
    cid = cluster_id or data.get("cluster", {}).get("id") or "default"
    now = datetime.now(timezone.utc)
    with _lock:
        if cid not in _history:
            _history[cid] = []
        _history[cid].append({
            "data": data,
            "source_type": source_type,
            "collected_at": now.isoformat(),
            "ts": now,
        })
        # Purge snapshots older than 7 days
        cutoff = now - _MAX_AGE
        _history[cid] = [s for s in _history[cid] if s["ts"] >= cutoff]
        _last_active = cid


def get_cluster_data(cluster_id: str | None = None, hours: int | None = None) -> dict[str, Any] | None:
    """Return latest snapshot within the time window. None if no data in window."""
    with _lock:
        cid = cluster_id or _last_active
        if not cid or cid not in _history or not _history[cid]:
            return None
        snapshots = _history[cid]
        if hours:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=float(hours))
            snapshots = [s for s in snapshots if s["ts"] >= cutoff]
        if not snapshots:
            return None
        return snapshots[-1]["data"]


def get_cluster_history(cluster_id: str | None = None, hours: float | None = None) -> list[dict[str, Any]]:
    """Return all snapshots within the time window for trending."""
    with _lock:
        cid = cluster_id or _last_active
        if not cid or cid not in _history:
            return []
        snapshots = _history[cid]
        if hours:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=float(hours))
            snapshots = [s for s in snapshots if s["ts"] >= cutoff]
        return [{"data": s["data"], "collected_at": s["collected_at"]} for s in snapshots]


def get_all_cluster_ids() -> list[str]:
    with _lock:
        return list(_history.keys())


def get_sync_meta(cluster_id: str | None = None) -> dict[str, Any]:
    with _lock:
        cid = cluster_id or _last_active
        if not cid or cid not in _history or not _history[cid]:
            return {
                "loaded": False,
                "source_type": "synthetic",
                "last_synced": None,
                "broker_count": 0,
                "consumer_group_count": 0,
                "topic_count": 0,
                "connector_count": 0,
            }
        entry = _history[cid][-1]
        d = entry["data"]
        return {
            "loaded": True,
            "source_type": entry["source_type"],
            "last_synced": entry["collected_at"],
            "broker_count": len(d.get("brokers", [])),
            "consumer_group_count": len(d.get("consumer_groups", [])),
            "topic_count": len(d.get("topics", [])),
            "connector_count": len(d.get("connectors", [])),
            "snapshot_count": len(_history[cid]),
        }
