"""ToolExecutor implementations for the Kafka Analyser agent."""
from __future__ import annotations

import json
from typing import Any, ClassVar

import kafka_store
from tools.base import ToolExecutor


class ClusterOverviewTool(ToolExecutor):
    name: ClassVar[str] = "get_cluster_overview"
    description: ClassVar[str] = (
        "Get Kafka cluster health overview including broker status, "
        "under-replicated partition count, health score, and active anomaly summary."
    )
    input_schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {},
        "required": [],
    }

    async def execute(self, **kwargs: Any) -> Any:
        data = kafka_store.get_cluster_data()
        if data is None:
            return {"error": "No cluster data loaded. Generate synthetic data or connect a cluster."}
        return {
            "cluster": data["cluster"],
            "brokers": data["brokers"],
            "anomaly_count": len(data["anomalies"]),
            "critical_count": sum(1 for a in data["anomalies"] if a["severity"] == "critical"),
            "warning_count": sum(1 for a in data["anomalies"] if a["severity"] == "warning"),
        }


class ConsumerLagTool(ToolExecutor):
    name: ClassVar[str] = "get_consumer_lag"
    description: ClassVar[str] = (
        "Get all consumer group lag sorted worst-first. "
        "Includes lag trend (growing/stable/recovering), rate per minute, and group state."
    )
    input_schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {},
        "required": [],
    }

    async def execute(self, **kwargs: Any) -> Any:
        data = kafka_store.get_cluster_data()
        if data is None:
            return {"error": "No cluster data loaded."}
        groups = sorted(data["consumer_groups"], key=lambda g: g["total_lag"], reverse=True)
        return {"consumer_groups": groups}


class BrokerMetricsTool(ToolExecutor):
    name: ClassVar[str] = "get_broker_metrics"
    description: ClassVar[str] = (
        "Get per-broker metrics including CPU%, heap%, GC pauses, "
        "request handler idle%, URP count, and throughput."
    )
    input_schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {},
        "required": [],
    }

    async def execute(self, **kwargs: Any) -> Any:
        data = kafka_store.get_cluster_data()
        if data is None:
            return {"error": "No cluster data loaded."}
        return {"brokers": data["brokers"]}


class TopicMetricsTool(ToolExecutor):
    name: ClassVar[str] = "get_topic_metrics"
    description: ClassVar[str] = (
        "Get topic metrics sorted by message rate descending: "
        "throughput, retention usage%, partition count, replication factor."
    )
    input_schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {},
        "required": [],
    }

    async def execute(self, **kwargs: Any) -> Any:
        data = kafka_store.get_cluster_data()
        if data is None:
            return {"error": "No cluster data loaded."}
        topics = sorted(data["topics"], key=lambda t: t["messages_in_per_sec"], reverse=True)
        return {"topics": topics}


class AnomalyTool(ToolExecutor):
    name: ClassVar[str] = "detect_anomalies"
    description: ClassVar[str] = (
        "Get active anomalies with severity (critical/warning), category, "
        "root cause description, and actionable recommendations."
    )
    input_schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {},
        "required": [],
    }

    async def execute(self, **kwargs: Any) -> Any:
        data = kafka_store.get_cluster_data()
        if data is None:
            return {"error": "No cluster data loaded."}
        return {"anomalies": data["anomalies"]}
