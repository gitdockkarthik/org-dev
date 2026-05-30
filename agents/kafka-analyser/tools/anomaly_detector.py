"""Anomaly detector — re-classifies cluster data against configurable thresholds."""
from __future__ import annotations

from typing import Any


def detect_anomalies(
    cluster_data: dict[str, Any],
    lag_threshold: int = 10000,
    heap_threshold_pct: float = 80.0,
    urp_threshold: int = 0,
    retention_threshold_pct: float = 80.0,
    connector_alert_enabled: bool = True,
) -> list[dict[str, Any]]:
    """Re-classify anomalies from live cluster data against current thresholds."""
    anomalies: list[dict[str, Any]] = []

    for broker in cluster_data.get("brokers", []):
        if broker["heap_pct"] >= heap_threshold_pct:
            anomalies.append({
                "severity": "critical" if broker["heap_pct"] >= 90 else "warning",
                "category": "broker_heap",
                "description": (
                    f"{broker['broker_id']} heap at {broker['heap_pct']:.0f}% "
                    f"(threshold: {heap_threshold_pct:.0f}%)."
                    + (
                        f" {broker['gc_pause_count']} GC pauses >{broker['gc_pause_ms']}ms detected."
                        if broker.get("gc_pause_count", 0) > 0
                        else ""
                    )
                ),
            })
        if broker.get("urp_count", 0) > urp_threshold:
            anomalies.append({
                "severity": "critical",
                "category": "under_replicated_partitions",
                "description": (
                    f"{broker['urp_count']} under-replicated partition(s) on {broker['broker_id']}."
                ),
            })

    for group in cluster_data.get("consumer_groups", []):
        if group["total_lag"] >= lag_threshold:
            severity = "critical" if group["lag_trend"] == "growing" else "warning"
            anomalies.append({
                "severity": severity,
                "category": "consumer_lag",
                "description": (
                    f"{group['group_name']} lag at {group['total_lag']:,} messages"
                    + (
                        f", growing at +{group['lag_rate_per_min']:,}/min"
                        if group["lag_trend"] == "growing"
                        else ""
                    )
                    + "."
                ),
            })
        if group.get("state") == "Dead":
            anomalies.append({
                "severity": "warning",
                "category": "consumer_group_dead",
                "description": f"{group['group_name']} is in Dead state — no active consumers.",
            })

    for topic in cluster_data.get("topics", []):
        if topic["retention_pct"] >= retention_threshold_pct:
            anomalies.append({
                "severity": "critical" if topic["retention_pct"] >= 95 else "warning",
                "category": "topic_retention",
                "description": (
                    f"{topic['topic']} at {topic['retention_pct']:.1f}% retention capacity."
                ),
            })

    if connector_alert_enabled:
        for conn in cluster_data.get("connectors", []):
            if conn["state"] == "FAILED" or conn.get("failed_tasks", 0) > 0:
                anomalies.append({
                    "severity": "critical",
                    "category": "connector_failure",
                    "description": (
                        f"{conn['connector_name']} {conn['state']} — "
                        f"{conn['failed_tasks']}/{conn['total_tasks']} tasks failed."
                    ),
                })

    return anomalies
