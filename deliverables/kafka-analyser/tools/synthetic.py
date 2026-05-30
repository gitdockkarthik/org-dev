"""SyntheticCollector — realistic Kafka cluster metrics for Phase 1 demos."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from tools.base import KafkaCollector


class SyntheticCollector(KafkaCollector):
    """Generates a prod-kafka-cluster snapshot with intentional injected anomalies."""

    async def collect(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()

        cluster = {
            "id": 1,
            "name": "prod-kafka-cluster",
            "source_type": "synthetic",
            "broker_count": 3,
            "status": "warning",
            "collected_at": now,
        }

        brokers = [
            {
                "broker_id": "broker-1",
                "host": "kafka-1.internal:9092",
                "heap_pct": 45.0,
                "gc_pause_count": 0,
                "gc_pause_ms": 0,
                "cpu_pct": 35.0,
                "disk_pct": 42.0,
                "request_handler_idle_pct": 78.0,
                "urp_count": 0,
                "messages_in_per_sec": 420000.0,
                "status": "healthy",
            },
            {
                "broker_id": "broker-2",
                "host": "kafka-2.internal:9092",
                "heap_pct": 78.0,
                "gc_pause_count": 3,
                "gc_pause_ms": 620,
                "cpu_pct": 62.0,
                "disk_pct": 67.0,
                "request_handler_idle_pct": 34.0,
                "urp_count": 3,
                "messages_in_per_sec": 380000.0,
                "status": "warning",
            },
            {
                "broker_id": "broker-3",
                "host": "kafka-3.internal:9092",
                "heap_pct": 52.0,
                "gc_pause_count": 0,
                "gc_pause_ms": 0,
                "cpu_pct": 41.0,
                "disk_pct": 51.0,
                "request_handler_idle_pct": 71.0,
                "urp_count": 0,
                "messages_in_per_sec": 395000.0,
                "status": "healthy",
            },
        ]

        consumer_groups = [
            {
                "group_name": "checkout-service",
                "topic": "orders",
                "total_lag": 45200,
                "lag_trend": "growing",
                "lag_rate_per_min": 1200,
                "state": "Stable",
                "status": "critical",
                "partitions": [
                    {
                        "partition": i,
                        "lag": 45200 // 12,
                        "log_end_offset": 5000000 + i * 10000,
                        "consumer_offset": 5000000 + i * 10000 - 45200 // 12,
                    }
                    for i in range(12)
                ],
            },
            {
                "group_name": "inventory-sync",
                "topic": "inventory",
                "total_lag": 1200,
                "lag_trend": "stable",
                "lag_rate_per_min": 0,
                "state": "Stable",
                "status": "healthy",
                "partitions": [
                    {
                        "partition": i,
                        "lag": 300,
                        "log_end_offset": 2000000 + i * 5000,
                        "consumer_offset": 2000000 + i * 5000 - 300,
                    }
                    for i in range(4)
                ],
            },
            {
                "group_name": "analytics-pipeline",
                "topic": "user-events",
                "total_lag": 0,
                "lag_trend": "stable",
                "lag_rate_per_min": 0,
                "state": "Stable",
                "status": "healthy",
                "partitions": [
                    {
                        "partition": i,
                        "lag": 0,
                        "log_end_offset": 1500000 + i * 3000,
                        "consumer_offset": 1500000 + i * 3000,
                    }
                    for i in range(4)
                ],
            },
            {
                "group_name": "payment-processor",
                "topic": "payments",
                "total_lag": 8500,
                "lag_trend": "recovering",
                "lag_rate_per_min": -200,
                "state": "Stable",
                "status": "warning",
                "partitions": [
                    {
                        "partition": i,
                        "lag": 8500 // 6,
                        "log_end_offset": 3000000 + i * 8000,
                        "consumer_offset": 3000000 + i * 8000 - 8500 // 6,
                    }
                    for i in range(6)
                ],
            },
            {
                "group_name": "dead-letter-handler",
                "topic": "dead-letter",
                "total_lag": 0,
                "lag_trend": "stable",
                "lag_rate_per_min": 0,
                "state": "Dead",
                "status": "warning",
                "partitions": [
                    {
                        "partition": i,
                        "lag": 0,
                        "log_end_offset": 500000,
                        "consumer_offset": 500000,
                    }
                    for i in range(2)
                ],
            },
        ]

        topics = [
            {
                "topic": "orders",
                "partition_count": 12,
                "replication_factor": 3,
                "messages_in_per_sec": 850000,
                "bytes_in_per_sec": 425000000,
                "bytes_out_per_sec": 1275000000,
                "total_messages": 500000000,
                "size_bytes": 250000000000,
                "retention_bytes": 1073741824000,
                "retention_pct": 23.3,
                "status": "healthy",
            },
            {
                "topic": "payments",
                "partition_count": 6,
                "replication_factor": 3,
                "messages_in_per_sec": 320000,
                "bytes_in_per_sec": 160000000,
                "bytes_out_per_sec": 480000000,
                "total_messages": 200000000,
                "size_bytes": 100000000000,
                "retention_bytes": 1073741824000,
                "retention_pct": 9.3,
                "status": "healthy",
            },
            {
                "topic": "inventory",
                "partition_count": 4,
                "replication_factor": 3,
                "messages_in_per_sec": 180000,
                "bytes_in_per_sec": 90000000,
                "bytes_out_per_sec": 270000000,
                "total_messages": 120000000,
                "size_bytes": 60000000000,
                "retention_bytes": 1073741824000,
                "retention_pct": 5.6,
                "status": "healthy",
            },
            {
                "topic": "user-events",
                "partition_count": 4,
                "replication_factor": 2,
                "messages_in_per_sec": 95000,
                "bytes_in_per_sec": 47500000,
                "bytes_out_per_sec": 95000000,
                "total_messages": 80000000,
                "size_bytes": 40000000000,
                "retention_bytes": 536870912000,
                "retention_pct": 7.4,
                "status": "healthy",
            },
            {
                "topic": "notifications",
                "partition_count": 3,
                "replication_factor": 2,
                "messages_in_per_sec": 75000,
                "bytes_in_per_sec": 37500000,
                "bytes_out_per_sec": 75000000,
                "total_messages": 60000000,
                "size_bytes": 30000000000,
                "retention_bytes": 536870912000,
                "retention_pct": 5.6,
                "status": "healthy",
            },
            {
                "topic": "audit-log",
                "partition_count": 3,
                "replication_factor": 3,
                "messages_in_per_sec": 45000,
                "bytes_in_per_sec": 22500000,
                "bytes_out_per_sec": 22500000,
                "total_messages": 500000000,
                "size_bytes": 1021655040000,
                "retention_bytes": 1073741824000,
                "retention_pct": 95.1,
                "status": "warning",
            },
            {
                "topic": "dead-letter",
                "partition_count": 2,
                "replication_factor": 2,
                "messages_in_per_sec": 12000,
                "bytes_in_per_sec": 6000000,
                "bytes_out_per_sec": 6000000,
                "total_messages": 15000000,
                "size_bytes": 7500000000,
                "retention_bytes": 536870912000,
                "retention_pct": 1.4,
                "status": "healthy",
            },
            {
                "topic": "schema-changes",
                "partition_count": 1,
                "replication_factor": 3,
                "messages_in_per_sec": 3500,
                "bytes_in_per_sec": 1750000,
                "bytes_out_per_sec": 3500000,
                "total_messages": 5000000,
                "size_bytes": 2500000000,
                "retention_bytes": 1073741824000,
                "retention_pct": 0.2,
                "status": "healthy",
            },
            {
                "topic": "config-updates",
                "partition_count": 1,
                "replication_factor": 2,
                "messages_in_per_sec": 1200,
                "bytes_in_per_sec": 600000,
                "bytes_out_per_sec": 1200000,
                "total_messages": 2000000,
                "size_bytes": 1000000000,
                "retention_bytes": 536870912000,
                "retention_pct": 0.2,
                "status": "healthy",
            },
            {
                "topic": "system-health",
                "partition_count": 1,
                "replication_factor": 1,
                "messages_in_per_sec": 800,
                "bytes_in_per_sec": 400000,
                "bytes_out_per_sec": 400000,
                "total_messages": 1000000,
                "size_bytes": 500000000,
                "retention_bytes": 107374182400,
                "retention_pct": 0.5,
                "status": "healthy",
            },
        ]

        connectors = [
            {
                "connector_name": "jdbc-sink-orders",
                "connector_type": "sink",
                "state": "RUNNING",
                "failed_tasks": 0,
                "total_tasks": 3,
                "task_health": [{"task_id": i, "state": "RUNNING"} for i in range(3)],
                "last_updated": now,
            },
            {
                "connector_name": "s3-sink-audit",
                "connector_type": "sink",
                "state": "FAILED",
                "failed_tasks": 2,
                "total_tasks": 3,
                "task_health": [
                    {"task_id": 0, "state": "RUNNING"},
                    {"task_id": 1, "state": "FAILED", "error": "S3 connection timeout"},
                    {"task_id": 2, "state": "FAILED", "error": "S3 connection timeout"},
                ],
                "last_updated": now,
            },
        ]

        anomalies = [
            {
                "id": 1,
                "severity": "warning",
                "category": "broker_heap",
                "description": "broker-2 heap usage at 78% — above 75% threshold. 3 GC pauses >500ms detected.",
                "detected_at": now,
                "resolved_at": None,
                "recommendations": [
                    "Investigate memory-intensive consumers on broker-2",
                    "Consider increasing heap allocation (-Xmx) if pattern persists",
                    "Review GC configuration — G1GC recommended for Kafka brokers",
                ],
            },
            {
                "id": 2,
                "severity": "critical",
                "category": "under_replicated_partitions",
                "description": "3 under-replicated partitions detected on broker-2. Data durability at risk.",
                "detected_at": now,
                "resolved_at": None,
                "recommendations": [
                    "Check broker-2 disk I/O and network connectivity",
                    "Monitor replication lag — if persistent, consider rolling restart",
                    "Ensure ISR count does not drop to 1 for RF=3 topics",
                ],
            },
            {
                "id": 3,
                "severity": "critical",
                "category": "consumer_lag",
                "description": "checkout-service lag at 45,200 messages and growing at +1,200/min.",
                "detected_at": now,
                "resolved_at": None,
                "recommendations": [
                    "Scale out checkout-service consumer instances",
                    "Check for processing bottlenecks in checkout-service",
                    "Review consumer commit interval — slow commits inflate apparent lag",
                ],
            },
            {
                "id": 4,
                "severity": "critical",
                "category": "connector_failure",
                "description": "s3-sink-audit connector FAILED — 2/3 tasks failed with S3 connection timeout.",
                "detected_at": now,
                "resolved_at": None,
                "recommendations": [
                    "Check S3 endpoint reachability from Kafka Connect workers",
                    "Verify IAM permissions for the connector's service account",
                    "Review connector logs: GET /connectors/s3-sink-audit/tasks/1/status",
                    "Restart failed tasks after fixing: POST /connectors/s3-sink-audit/tasks/1/restart",
                ],
            },
            {
                "id": 5,
                "severity": "warning",
                "category": "topic_retention",
                "description": "audit-log topic at 95.1% retention capacity. Risk of data loss if exceeded.",
                "detected_at": now,
                "resolved_at": None,
                "recommendations": [
                    "Increase retention.bytes for audit-log topic",
                    "Add partitions to distribute storage load",
                    "Review audit log ingestion rate — consider archival to S3",
                ],
            },
            {
                "id": 6,
                "severity": "warning",
                "category": "consumer_group_dead",
                "description": "dead-letter-handler consumer group is in Dead state — no active consumers.",
                "detected_at": now,
                "resolved_at": None,
                "recommendations": [
                    "Restart dead-letter-handler service",
                    "Check pods: kubectl get pods -l app=dead-letter-handler",
                    "Review dead-letter topic for backlog of failed messages",
                ],
            },
        ]

        critical_count = sum(1 for a in anomalies if a["severity"] == "critical")
        warning_count = sum(1 for a in anomalies if a["severity"] == "warning")
        health_score = max(0, 100 - (critical_count * 15) - (warning_count * 5))

        cluster["health_score"] = health_score
        cluster["anomaly_count"] = len(anomalies)
        cluster["critical_count"] = critical_count
        cluster["warning_count"] = warning_count

        return {
            "cluster": cluster,
            "brokers": brokers,
            "consumer_groups": consumer_groups,
            "topics": topics,
            "connectors": connectors,
            "anomalies": anomalies,
        }
