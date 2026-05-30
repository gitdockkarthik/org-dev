"""In-memory Kafka cluster store.

State is process-scoped and resets on restart. Suitable for single-instance deployment.
The store is populated by SyntheticCollector or future live collectors.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

_lock = threading.Lock()

_cluster_data: dict[str, Any] | None = None
_last_synced: str | None = None
_source_type: str = "synthetic"


def set_cluster_data(data: dict[str, Any], source_type: str = "synthetic") -> None:
    global _cluster_data, _last_synced, _source_type
    with _lock:
        _cluster_data = data
        _last_synced = datetime.now(timezone.utc).isoformat()
        _source_type = source_type


def get_cluster_data() -> dict[str, Any] | None:
    with _lock:
        return _cluster_data


def get_sync_meta() -> dict[str, Any]:
    with _lock:
        if _cluster_data is None:
            return {
                "loaded": False,
                "source_type": _source_type,
                "last_synced": _last_synced,
                "broker_count": 0,
                "consumer_group_count": 0,
                "topic_count": 0,
                "connector_count": 0,
            }
        return {
            "loaded": True,
            "source_type": _source_type,
            "last_synced": _last_synced,
            "broker_count": len(_cluster_data.get("brokers", [])),
            "consumer_group_count": len(_cluster_data.get("consumer_groups", [])),
            "topic_count": len(_cluster_data.get("topics", [])),
            "connector_count": len(_cluster_data.get("connectors", [])),
        }
