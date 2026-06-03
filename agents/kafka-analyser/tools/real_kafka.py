"""RealKafkaCollector — collect live cluster state from a real Kafka/Redpanda broker.

Uses aiokafka's admin client (and a short-lived consumer for end offsets) to
build the same snapshot dict the synthetic collector produces, so the rest of
the pipeline (kafka_store, anomaly_detector) is unchanged.

Only depends on aiokafka, asyncio, and the stdlib — plus tools/base.py.
"""
from __future__ import annotations

from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.helpers import create_ssl_context

from tools.base import KafkaCollector

import logging

class _SuppressAiokafkaBufferError(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return "Buffer underrun decoding string" not in record.getMessage()

logging.getLogger("aiokafka").addFilter(_SuppressAiokafkaBufferError())
logging.getLogger("aiokafka.conn").addFilter(_SuppressAiokafkaBufferError())

# Consumer-group states that have no live members / offsets worth querying.
_INACTIVE_GROUP_STATES = {"Dead", "Empty"}


class RealKafkaCollector(KafkaCollector):
    """Connects to a real Kafka or Redpanda broker and returns a snapshot dict."""

    def __init__(self, config: dict) -> None:
        self.bootstrap_servers: str = config["bootstrap_servers"]
        self.auth_type: str = config.get("auth_type", "none")
        self.sasl_username: str | None = config.get("sasl_username")
        self.sasl_password: str | None = config.get("sasl_password")
        self.sasl_mechanism: str = config.get("sasl_mechanism") or "PLAIN"
        self.tls_enabled: bool = bool(config.get("tls_enabled", False))
        self.cluster_label: str = config.get("cluster_label") or "Kafka"

    # ------------------------------------------------------------------ #
    # Security                                                            #
    # ------------------------------------------------------------------ #
    def _security_kwargs(self) -> dict[str, Any]:
        """Build the aiokafka security kwargs for the configured auth mode."""
        if self.auth_type == "none":
            return {"security_protocol": "PLAINTEXT"}

        if self.auth_type == "sasl":
            kwargs: dict[str, Any] = {
                "security_protocol": "SASL_SSL" if self.tls_enabled else "SASL_PLAINTEXT",
                "sasl_mechanism": self.sasl_mechanism,
                "sasl_plain_username": self.sasl_username,
                "sasl_plain_password": self.sasl_password,
            }
            if self.tls_enabled:
                kwargs["ssl_context"] = create_ssl_context()
            return kwargs

        raise RuntimeError(
            f"Unknown auth_type {self.auth_type!r} (expected 'none' or 'sasl') "
            f"for bootstrap_servers={self.bootstrap_servers!r}"
        )

    @property
    def _source_type(self) -> str:
        return "kafka_sasl" if self.auth_type == "sasl" else "kafka_internal"

    # ------------------------------------------------------------------ #
    # Collection                                                          #
    # ------------------------------------------------------------------ #
    async def collect(self) -> dict[str, Any]:
        security = self._security_kwargs()

        admin = AIOKafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            **security,
        )

        try:
            try:
                await admin.start()
            except Exception as exc:  # noqa: BLE001 — surface any connect failure as 400
                raise RuntimeError(
                    f"Failed to connect to Kafka at bootstrap_servers="
                    f"{self.bootstrap_servers!r} (auth_type={self.auth_type!r}): {exc}"
                ) from exc

            cluster_info = await admin.describe_cluster()
            brokers = self._build_brokers(cluster_info)

            topics, total_urp = await self._build_topics(admin)
            consumer_groups = await self._build_consumer_groups(admin, security)

            cluster = self._build_cluster(cluster_info, len(brokers), total_urp)

            return {
                "cluster": cluster,
                "brokers": brokers,
                "consumer_groups": consumer_groups,
                "topics": topics,
                "connectors": [],
                "anomalies": [],
            }
        except RuntimeError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"Failed to collect from Kafka at bootstrap_servers="
                f"{self.bootstrap_servers!r} (auth_type={self.auth_type!r}): {exc}"
            ) from exc
        finally:
            try:
                await admin.close()
            except Exception:  # noqa: BLE001 — best-effort cleanup
                pass

    # ------------------------------------------------------------------ #
    # Builders                                                            #
    # ------------------------------------------------------------------ #
    def _build_cluster(
        self, cluster_info: dict[str, Any], broker_count: int, total_urp: int
    ) -> dict[str, Any]:
        cluster_id = cluster_info.get("cluster_id")
        health_score = 100 if total_urp == 0 else max(50, 100 - total_urp * 5)
        return {
            "id": str(cluster_id) if cluster_id is not None else self.cluster_label,
            "name": self.cluster_label,
            "source_type": self._source_type,
            "broker_count": broker_count,
            "health_score": health_score,
            "status": "healthy" if total_urp == 0 else "degraded",
        }

    def _build_brokers(self, cluster_info: dict[str, Any]) -> list[dict[str, Any]]:
        brokers: list[dict[str, Any]] = []
        for node in cluster_info.get("brokers", []):
            brokers.append(
                {
                    "id": str(node.get("node_id")),
                    "host": node.get("host", ""),
                    "port": int(node.get("port", 0)),
                    "heap_pct": 0.0,
                    "cpu_pct": 0.0,
                    "disk_pct": 0.0,
                    "gc_pause_ms": 0,
                    "urp_count": 0,
                    "messages_in_per_sec": 0.0,
                    "request_handler_idle_pct": 100.0,
                    "status": "healthy",
                }
            )
        return brokers

    async def _build_topics(
        self, admin: AIOKafkaAdminClient
    ) -> tuple[list[dict[str, Any]], int]:
        """Return (topics, total_under_replicated_partition_count)."""
        names = [n for n in await admin.list_topics() if not n.startswith("__")]
        if not names:
            return [], 0

        described = await admin.describe_topics(names)

        topics: list[dict[str, Any]] = []
        total_urp = 0
        for meta in described:
            name = meta.get("topic", "")
            if not name or name.startswith("__"):
                continue
            partitions = meta.get("partitions", []) or []
            partition_count = len(partitions)
            replication_factor = len(partitions[0].get("replicas", [])) if partitions else 0

            urp = 0
            for part in partitions:
                replicas = part.get("replicas", []) or []
                isr = part.get("isr", []) or []
                if len(isr) < len(replicas):
                    urp += 1
            total_urp += urp

            topics.append(
                {
                    "name": name,
                    "partition_count": partition_count,
                    "replication_factor": replication_factor,
                    "messages_in_per_sec": 0.0,
                    "bytes_in_per_sec": 0.0,
                    "bytes_out_per_sec": 0.0,
                    "total_messages": 0,
                    "size_bytes": 0,
                    "retention_bytes": -1,
                    "retention_pct": 0.0,
                    "status": "degraded" if urp else "healthy",
                }
            )
        return topics, total_urp

    async def _build_consumer_groups(
        self, admin: AIOKafkaAdminClient, security: dict[str, Any]
    ) -> list[dict[str, Any]]:
        listed = await admin.list_consumer_groups()
        group_ids = [entry[0] for entry in listed]
        if not group_ids:
            return []

        states = await self._describe_group_states(admin, group_ids)

        # Only active groups need an offset/lag round-trip.
        active = [g for g in group_ids if states.get(g, "Unknown") not in _INACTIVE_GROUP_STATES]

        consumer: AIOKafkaConsumer | None = None
        try:
            groups: list[dict[str, Any]] = []
            for group_id in group_ids:
                state = states.get(group_id, "Unknown")
                if group_id not in active:
                    groups.append(self._empty_group(group_id, state))
                    continue

                offsets = await admin.list_consumer_group_offsets(group_id)
                if not offsets:
                    groups.append(self._empty_group(group_id, state))
                    continue

                if consumer is None:
                    consumer = AIOKafkaConsumer(
                        bootstrap_servers=self.bootstrap_servers,
                        enable_auto_commit=False,
                        group_id=None,
                        **security,
                    )
                    await consumer.start()

                partitions, total_lag, topics = await self._group_lag(consumer, offsets)
                groups.append(
                    {
                        "group_id": group_id,
                        "state": state,
                        "total_lag": total_lag,
                        "topic_count": len(topics),
                        "lag_trend": "stable",
                        "lag_rate_per_min": 0.0,
                        "partitions": partitions,
                    }
                )
            return groups
        finally:
            if consumer is not None:
                try:
                    await consumer.stop()
                except Exception:  # noqa: BLE001 — best-effort cleanup
                    pass

    async def _describe_group_states(
        self, admin: AIOKafkaAdminClient, group_ids: list[str]
    ) -> dict[str, str]:
        """Map group_id -> state, tolerating describe failures."""
        try:
            described = await admin.describe_consumer_groups(group_ids)
        except Exception:  # noqa: BLE001 — fall back to treating all as active
            return {g: "Unknown" for g in group_ids}

        states: dict[str, str] = {}
        for info in described:
            group_id = getattr(info, "group", None)
            state = getattr(info, "state", None)
            if group_id is None and isinstance(info, (list, tuple)) and len(info) >= 3:
                # Fallback positional parse: (error_code, group, state, ...)
                group_id, state = info[1], info[2]
            if group_id is not None:
                states[group_id] = state or "Unknown"
        return states

    async def _group_lag(
        self, consumer: AIOKafkaConsumer, offsets: dict[Any, Any]
    ) -> tuple[list[dict[str, Any]], int, set[str]]:
        tps = list(offsets.keys())
        end_offsets = await consumer.end_offsets(tps)

        partitions: list[dict[str, Any]] = []
        total_lag = 0
        topics: set[str] = set()
        for tp in tps:
            consumer_offset = offsets[tp].offset
            log_end_offset = end_offsets.get(tp, consumer_offset)
            lag = max(0, log_end_offset - consumer_offset) if consumer_offset >= 0 else 0
            total_lag += lag
            topics.add(tp.topic)
            partitions.append(
                {
                    "topic": tp.topic,
                    "partition": tp.partition,
                    "lag": lag,
                    "log_end_offset": log_end_offset,
                    "consumer_offset": consumer_offset,
                }
            )
        return partitions, total_lag, topics

    @staticmethod
    def _empty_group(group_id: str, state: str) -> dict[str, Any]:
        return {
            "group_id": group_id,
            "state": state,
            "total_lag": 0,
            "topic_count": 0,
            "lag_trend": "stable",
            "lag_rate_per_min": 0.0,
            "partitions": [],
        }
