from __future__ import annotations
from typing import Any


def _broker_id(broker: dict) -> str:
    return str(broker.get("id") or broker.get("broker_id") or "unknown")

def _group_id(group: dict) -> str:
    return str(group.get("group_id") or group.get("group_name") or "unknown")

def _topic_name(topic: dict) -> str:
    return str(topic.get("name") or topic.get("topic") or "unknown")


def detect_anomalies(
    cluster_data: dict[str, Any],
    lag_threshold: int = 10000,
    heap_threshold_pct: float = 80.0,
    urp_threshold: int = 0,
    retention_threshold_pct: float = 80.0,
    connector_alert_enabled: bool = True,
    thresholds: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    thresholds = thresholds or {}
    anomalies: list[dict[str, Any]] = []

    # ── Broker anomalies ──────────────────────────────────────────
    for broker in cluster_data.get("brokers", []):
        bid = _broker_id(broker)
        heap = broker.get("heap_pct", 0.0)
        gc_ms = broker.get("gc_pause_ms") or broker.get("gc_pause_count", 0)
        urp = broker.get("urp_count", 0)

        heap_warning = thresholds.get("heap_warning_pct", 70)
        heap_critical = thresholds.get("heap_threshold_pct", 85)
        if heap >= heap_warning:
            severity = "critical" if heap >= heap_critical else "warning"
            anomalies.append({
                "severity": severity,
                "category": "broker_heap",
                "description": (
                    f"Broker {bid} heap at {heap:.0f}% — "
                    f"{'critical' if heap >= heap_critical else 'warning'} threshold is {heap_critical if heap >= heap_critical else heap_warning:.0f}%. "
                    f"Monitor for OOM risk."
                    + (f" GC pause: {gc_ms}ms." if gc_ms else "")
                ),
                "recommendations": [
                    f"Trigger GC on broker {bid}: kafka-jmx-tool or rolling restart",
                    "Review heap allocation — increase -Xmx if consistently above threshold",
                    "Check for message size spikes or consumer lag causing retention pressure",
                ],
            })

        if urp > urp_threshold:
            anomalies.append({
                "severity": "critical",
                "category": "under_replicated_partitions",
                "description": f"{urp} under-replicated partition(s) on broker {bid}.",
                "recommendations": [
                    "Check broker connectivity and disk I/O",
                    f"Run: kafka-topics --describe --under-replicated-partitions",
                    "Verify replication factor matches ISR count",
                    "Check for broker restarts or network partitions",
                ],
            })

        gc_ms = broker.get("gc_pause_ms", 0)
        gc_warning = thresholds.get("gc_warning_ms", 500)
        gc_critical = thresholds.get("gc_critical_ms", 1000)
        if gc_ms >= gc_warning:
            anomalies.append({
                "severity": "critical" if gc_ms >= gc_critical else "warning",
                "category": "broker_gc",
                "description": f"Broker {bid} GC pause {gc_ms}ms — "
                              f"{'critical' if gc_ms >= gc_critical else 'elevated'}.",
            })

        fetch_ms = broker.get("fetch_latency_ms", 0)
        fetch_warning = thresholds.get("fetch_latency_warning_ms", 200)
        fetch_critical = thresholds.get("fetch_latency_critical_ms", 500)
        if fetch_ms >= fetch_warning:
            anomalies.append({
                "severity": "critical" if fetch_ms >= fetch_critical else "warning",
                "category": "broker_fetch_latency",
                "description": f"Broker {bid} fetch latency {fetch_ms}ms — "
                              f"{'critical' if fetch_ms >= fetch_critical else 'elevated'}.",
            })

    # ── Consumer group anomalies ──────────────────────────────────
    for group in cluster_data.get("consumer_groups", []):
        gid = _group_id(group)
        lag = group.get("total_lag", 0)
        trend = group.get("lag_trend", "stable")
        rate = group.get("lag_rate_per_min", 0.0)
        state = group.get("state", "")

        lag_warning_pct = thresholds.get("lag_warning_pct", 60) / 100
        lag_critical_pct = thresholds.get("lag_critical_pct", 80) / 100
        warning_lag = lag_threshold * lag_warning_pct
        critical_lag = lag_threshold * lag_critical_pct

        if lag >= warning_lag:
            if lag >= critical_lag or trend == "growing":
                severity = "critical"
            else:
                severity = "warning"

            # ETA calculation
            eta_str = ""
            if trend == "growing" and rate > 0:
                mins_to_double = lag / rate
                if mins_to_double < 60:
                    eta_str = f" ETA to double: ~{mins_to_double:.0f} min"
                else:
                    eta_str = f" ETA to double: ~{mins_to_double/60:.1f} hrs"

            anomalies.append({
                "severity": severity,
                "category": "consumer_lag",
                "description": (
                    f"{gid} lag: {lag:,} messages unprocessed — "
                    f"{'growing, immediate action required' if trend == 'growing' else 'backlog not clearing'}."
                ),
                "recommendations": [
                    f"Scale up consumer group '{gid}' — add more consumer instances",
                    "Check for consumer processing bottlenecks or errors",
                    "Review topic partition count — insufficient partitions limit parallelism",
                    f"Monitor with: kafka-consumer-groups --describe --group {gid}",
                ],
            })

        if state == "Dead":
            anomalies.append({
                "severity": "warning",
                "category": "consumer_group_dead",
                "description": f"{gid} is in Dead state — no active consumers.",
                "recommendations": [
                    f"Restart consumer application for group '{gid}'",
                    "Check application logs for crash or connection errors",
                    "Verify consumer group configuration and broker connectivity",
                ],
            })

    # ── Topic anomalies ───────────────────────────────────────────
    for topic in cluster_data.get("topics", []):
        tname = _topic_name(topic)
        if tname.startswith("__") or tname == "_schemas":
            continue  # skip internal topics
        ret_pct = topic.get("retention_pct", 0.0)
        retention_warning = thresholds.get("retention_warning_pct", 70)
        retention_critical = thresholds.get("retention_threshold_pct", 85)
        if ret_pct < retention_warning:
            continue
        severity = "critical" if ret_pct >= retention_critical else "warning"
        anomalies.append({
            "severity": severity,
            "category": "topic_retention",
            "description": f"{tname} at {ret_pct:.1f}% retention capacity.",
            "recommendations": [
                f"Increase retention.bytes for topic '{tname}'",
                "Check consumer lag — slow consumers cause retention buildup",
                "Consider adding partitions to distribute load",
                f"Run: kafka-configs --alter --topic {tname} --add-config retention.bytes=<new-value>",
            ],
        })

    # ── Connector anomalies ───────────────────────────────────────
    if connector_alert_enabled:
        for conn in cluster_data.get("connectors", []):
            cname = conn.get("connector_name") or conn.get("name") or "unknown"
            failed = conn.get("failed_tasks", 0)
            total = conn.get("total_tasks", 0)
            state = conn.get("state", "")
            if state == "FAILED" or failed > 0:
                anomalies.append({
                    "severity": "critical",
                    "category": "connector_failure",
                    "description": f"{cname} {state} — {failed}/{total} tasks failed.",
                    "recommendations": [
                        f"Check connector logs: GET /connectors/{cname}/tasks/0/status",
                        "Verify source/sink connectivity and credentials",
                        f"Restart connector: POST /connectors/{cname}/restart",
                        "Review connector configuration for schema or format issues",
                    ],
                })

    # Sort: critical first, then warning
    anomalies.sort(key=lambda a: 0 if a["severity"] == "critical" else 1)
    return anomalies
