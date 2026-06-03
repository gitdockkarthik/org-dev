"""Multi-cluster in-memory Kafka store.
Stores data per cluster_id. Dashboard endpoints accept cluster_id param.
"""
from __future__ import annotations
import threading
from datetime import datetime, timezone
from typing import Any

_lock = threading.Lock()
_clusters: dict[str, dict[str, Any]] = {}   # cluster_id → data
_last_active: str | None = None              # last synced cluster_id

def set_cluster_data(data: dict[str, Any], source_type: str = "synthetic", cluster_id: str | None = None) -> None:
    global _last_active
    cid = cluster_id or data.get("cluster", {}).get("id") or "default"
    with _lock:
        _clusters[cid] = {
            "data": data,
            "source_type": source_type,
            "last_synced": datetime.now(timezone.utc).isoformat(),
        }
        _last_active = cid

def get_cluster_data(cluster_id: str | None = None) -> dict[str, Any] | None:
    with _lock:
        if cluster_id and cluster_id in _clusters:
            return _clusters[cluster_id]["data"]
        if _last_active and _last_active in _clusters:
            return _clusters[_last_active]["data"]
        return None

def get_all_cluster_ids() -> list[str]:
    with _lock:
        return list(_clusters.keys())

def get_sync_meta(cluster_id: str | None = None) -> dict[str, Any]:
    with _lock:
        cid = cluster_id or _last_active
        if not cid or cid not in _clusters:
            return {
                "loaded": False,
                "source_type": "synthetic",
                "last_synced": None,
                "broker_count": 0,
                "consumer_group_count": 0,
                "topic_count": 0,
                "connector_count": 0,
            }
        entry = _clusters[cid]
        d = entry["data"]
        return {
            "loaded": True,
            "source_type": entry["source_type"],
            "last_synced": entry["last_synced"],
            "broker_count": len(d.get("brokers", [])),
            "consumer_group_count": len(d.get("consumer_groups", [])),
            "topic_count": len(d.get("topics", [])),
            "connector_count": len(d.get("connectors", [])),
        }
