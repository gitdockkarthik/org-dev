"""RealKafkaCollector — collect live cluster state from a real Kafka/Redpanda broker.

Uses kafka-python-ng's synchronous admin client (and a short-lived consumer for
end offsets) to build the same snapshot dict the synthetic collector produces,
so the rest of the pipeline (kafka_store, anomaly_detector) is unchanged.

kafka-python is synchronous, so all blocking client work runs inside asyncio's
default executor to stay compatible with the otherwise-async pipeline.

Only depends on kafka (kafka-python-ng), asyncio, ssl, and tools/base.py.
"""
from __future__ import annotations

import asyncio
import ssl
from typing import Any

from kafka import KafkaAdminClient, KafkaConsumer

from tools.base import KafkaCollector

# Consumer-group states that have no live members / offsets worth querying.
_INACTIVE_GROUP_STATES = {"Dead", "Empty"}


def _is_internal_topic(name: str) -> bool:
    """Internal/system topics that should never surface on the dashboard."""
    return name.startswith("__") or name == "_schemas"


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
        self.jmx_port: int | None = config.get("jmx_port")

    # ------------------------------------------------------------------ #
    # Config helpers                                                      #
    # ------------------------------------------------------------------ #
    @property
    def _bootstrap_list(self) -> list[str]:
        """kafka-python accepts a list of host:port entries."""
        return [s.strip() for s in str(self.bootstrap_servers).split(",") if s.strip()]

    @property
    def _source_type(self) -> str:
        return "kafka_sasl" if self.auth_type == "sasl" else "kafka_internal"

    def _security_kwargs(self) -> dict[str, Any]:
        """Build the kafka-python security kwargs for the configured auth mode."""
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
                kwargs["ssl_context"] = ssl.create_default_context()
            return kwargs

        raise RuntimeError(
            f"Unknown auth_type {self.auth_type!r} (expected 'none' or 'sasl') "
            f"for bootstrap_servers={self.bootstrap_servers!r}"
        )

    # ------------------------------------------------------------------ #
    # Collection (async wrapper over the synchronous client)             #
    # ------------------------------------------------------------------ #
    async def collect(self) -> dict[str, Any]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._collect_sync)

    async def ping(self) -> dict:
        """Lightweight connection test — broker list only, no topic/group/JMX fetch."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._ping_sync)

    def _ping_sync(self) -> dict:
        security = self._security_kwargs()
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=self._bootstrap_list,
                **security,
                request_timeout_ms=10000,
            )
        except Exception as exc:
            raise RuntimeError(
                f"Failed to connect to Kafka at bootstrap_servers="
                f"{self.bootstrap_servers!r} (auth_type={self.auth_type!r}): {exc}"
            ) from exc
        try:
            cluster_info = admin.describe_cluster()
            brokers = self._build_brokers(cluster_info)
            cluster_id = cluster_info.get('cluster_id', '') if isinstance(cluster_info, dict) else (getattr(cluster_info, 'cluster_id', '') or '')
            return {
                "ok": True,
                "broker_count": len(brokers),
                "cluster_id": str(cluster_id),
                "topic_count": None,
            }
        finally:
            try:
                admin.close()
            except Exception:
                pass

    def _collect_sync(self) -> dict[str, Any]:
        security = self._security_kwargs()

        try:
            admin = KafkaAdminClient(
                bootstrap_servers=self._bootstrap_list,
                **security,
            )
        except Exception as exc:  # noqa: BLE001 — surface any connect failure clearly
            raise RuntimeError(
                f"Failed to connect to Kafka at bootstrap_servers="
                f"{self.bootstrap_servers!r} (auth_type={self.auth_type!r}): {exc}"
            ) from exc

        try:
            try:
                cluster_info = admin.describe_cluster()
                brokers = self._build_brokers(cluster_info)

                topic_jmx = None
                if self.jmx_port:
                    broker_host = cluster_info.get("brokers", [{}])[0].get("host", "")
                    if broker_host:
                        topic_names = [n for n in admin.list_topics() if not _is_internal_topic(n)]
                        topic_jmx = self._query_topic_jmx(broker_host, self.jmx_port, topic_names)
                topics, total_urp = self._build_topics(admin, topic_jmx)
                consumer_groups = self._build_consumer_groups(admin, security)

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
                admin.close()
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

    def _query_jmx(self, host: str, port: int) -> dict[str, Any]:
        """Query JMX MBeans from a single broker. Returns metric dict or defaults on failure."""
        defaults = {"heap_pct": 0.0, "cpu_pct": 0.0, "disk_pct": 0.0, "gc_pause_ms": 0,
                     "messages_in_per_sec": 0.0, "request_handler_idle_pct": 100.0,
                     "bytes_in_per_sec": 0.0, "bytes_out_per_sec": 0.0,
                     "isr_shrinks_per_sec": 0.0, "isr_expands_per_sec": 0.0,
                     "produce_latency_ms": 0.0, "fetch_latency_ms": 0.0}
        try:
            import jpype
            from jpype import javax
            if not jpype.isJVMStarted():
                jpype.startJVM(jpype.getDefaultJVMPath(), convertStrings=True)
            url = javax.management.remote.JMXServiceURL(
                f"service:jmx:rmi:///jndi/rmi://{host}:{port}/jmxrmi")
            connector = javax.management.remote.JMXConnectorFactory.connect(url)
            mbean = connector.getMBeanServerConnection()
            # Heap
            heap = mbean.getAttribute(
                javax.management.ObjectName("java.lang:type=Memory"), "HeapMemoryUsage")
            heap_used = float(heap.get("used"))
            heap_max = float(heap.get("max"))
            heap_pct = round((heap_used / heap_max) * 100, 1) if heap_max > 0 else 0.0
            # CPU
            try:
                cpu_raw = mbean.getAttribute(
                    javax.management.ObjectName("java.lang:type=OperatingSystem"), "ProcessCpuLoad")
                cpu_pct = round(float(cpu_raw) * 100, 1)
            except Exception:
                cpu_pct = 0.0
            # GC pause count
            try:
                gc_names = mbean.queryNames(
                    javax.management.ObjectName("java.lang:type=GarbageCollector,name=*"), None)
                gc_total = 0
                for gc_name in gc_names:
                    gc_total += int(mbean.getAttribute(gc_name, "CollectionCount"))
                gc_pause_ms = gc_total
            except Exception:
                gc_pause_ms = 0
            # Messages in/sec
            try:
                msgs_obj = mbean.getAttribute(
                    javax.management.ObjectName(
                        "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec"), "OneMinuteRate")
                messages_in = round(float(msgs_obj), 2)
            except Exception:
                messages_in = 0.0
            # Request handler idle %
            try:
                idle_obj = mbean.getAttribute(
                    javax.management.ObjectName(
                        "kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent"),
                    "OneMinuteRate")
                req_idle = round(float(idle_obj) * 100, 1)
            except Exception:
                req_idle = 100.0
            # Bytes in/out per sec (broker aggregate)
            bytes_in = 0.0
            bytes_out = 0.0
            try:
                bytes_in = round(float(mbean.getAttribute(
                    javax.management.ObjectName(
                        "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec"), "OneMinuteRate")), 2)
            except Exception:
                pass
            try:
                bytes_out = round(float(mbean.getAttribute(
                    javax.management.ObjectName(
                        "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec"), "OneMinuteRate")), 2)
            except Exception:
                pass
            # ISR shrink/expand rate
            isr_shrinks = 0.0
            isr_expands = 0.0
            try:
                isr_shrinks = round(float(mbean.getAttribute(
                    javax.management.ObjectName(
                        "kafka.server:type=ReplicaManager,name=IsrShrinksPerSec"), "OneMinuteRate")), 4)
            except Exception:
                pass
            try:
                isr_expands = round(float(mbean.getAttribute(
                    javax.management.ObjectName(
                        "kafka.server:type=ReplicaManager,name=IsrExpandsPerSec"), "OneMinuteRate")), 4)
            except Exception:
                pass
            # Produce/Fetch request latency (mean ms)
            produce_latency_ms = 0.0
            fetch_latency_ms = 0.0
            try:
                produce_latency_ms = round(float(mbean.getAttribute(
                    javax.management.ObjectName(
                        "kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce"), "Mean")), 2)
            except Exception:
                pass
            try:
                fetch_latency_ms = round(float(mbean.getAttribute(
                    javax.management.ObjectName(
                        "kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer"), "Mean")), 2)
            except Exception:
                pass
            connector.close()
            return {"heap_pct": heap_pct, "cpu_pct": cpu_pct, "disk_pct": 0.0,
                    "gc_pause_ms": gc_pause_ms, "messages_in_per_sec": messages_in,
                    "request_handler_idle_pct": req_idle,
                    "bytes_in_per_sec": bytes_in, "bytes_out_per_sec": bytes_out,
                    "isr_shrinks_per_sec": isr_shrinks, "isr_expands_per_sec": isr_expands,
                    "produce_latency_ms": produce_latency_ms, "fetch_latency_ms": fetch_latency_ms}
        except Exception as exc:
            import logging
            logging.getLogger("kafka-analyser").warning(f"JMX query failed for {host}:{port}: {exc}")
            return defaults

    def _query_topic_jmx(self, host: str, port: int, topic_names: list[str]) -> dict[str, dict]:
        """Query per-topic JMX: log size, msgs/sec, bytes/sec."""
        result: dict[str, dict] = {}
        try:
            import jpype
            from jpype import javax
            if not jpype.isJVMStarted():
                jpype.startJVM(jpype.getDefaultJVMPath(), convertStrings=True)
            url = javax.management.remote.JMXServiceURL(
                f"service:jmx:rmi:///jndi/rmi://{host}:{port}/jmxrmi")
            connector = javax.management.remote.JMXConnectorFactory.connect(url)
            mbean = connector.getMBeanServerConnection()
            try:
                log_names = mbean.queryNames(
                    javax.management.ObjectName("kafka.log:type=Log,name=Size,topic=*,partition=*"), None)
                topic_sizes: dict[str, int] = {}
                for obj_name in log_names:
                    topic = str(obj_name.getKeyProperty("topic"))
                    size = int(mbean.getAttribute(obj_name, "Value"))
                    topic_sizes[topic] = topic_sizes.get(topic, 0) + size
                for t in topic_names:
                    if t not in result:
                        result[t] = {}
                    result[t]["size_bytes"] = topic_sizes.get(t, 0)
            except Exception:
                pass
            for t in topic_names:
                if t not in result:
                    result[t] = {}
                for metric, key in [("MessagesInPerSec", "messages_in_per_sec"),
                                     ("BytesInPerSec", "bytes_in_per_sec"),
                                     ("BytesOutPerSec", "bytes_out_per_sec")]:
                    try:
                        val = mbean.getAttribute(
                            javax.management.ObjectName(
                                f"kafka.server:type=BrokerTopicMetrics,name={metric},topic={t}"),
                            "OneMinuteRate")
                        result[t][key] = round(float(val), 2)
                    except Exception:
                        pass
            connector.close()
        except Exception as exc:
            import logging
            logging.getLogger("kafka-analyser").warning(f"Topic JMX query failed for {host}:{port}: {exc}")
        return result

    def _build_brokers(self, cluster_info: dict[str, Any]) -> list[dict[str, Any]]:
        brokers: list[dict[str, Any]] = []
        for node in cluster_info.get("brokers", []) or []:
            host = node.get("host", "")
            metrics = self._query_jmx(host, self.jmx_port) if self.jmx_port else {
                "heap_pct": 0.0, "cpu_pct": 0.0, "disk_pct": 0.0, "gc_pause_ms": 0,
                "messages_in_per_sec": 0.0, "request_handler_idle_pct": 100.0,
                "bytes_in_per_sec": 0.0, "bytes_out_per_sec": 0.0,
                "isr_shrinks_per_sec": 0.0, "isr_expands_per_sec": 0.0,
                "produce_latency_ms": 0.0, "fetch_latency_ms": 0.0}
            brokers.append({
                "id": str(node.get("node_id")),
                "host": host,
                "port": int(node.get("port", 0)),
                **metrics,
                "urp_count": 0,
                "status": "healthy",
            })
        return brokers

    def _build_topics(
        self, admin: KafkaAdminClient, topic_jmx: dict[str, dict] | None = None
    ) -> tuple[list[dict[str, Any]], int]:
        """Return (topics, total_under_replicated_partition_count)."""
        names = [n for n in admin.list_topics() if not _is_internal_topic(n)]
        if not names:
            return [], 0

        # Cap at 5000 topics for performance on large clusters
        if len(names) > 5000:
            names = sorted(names)[:5000]

        # Batch describe_topics in chunks of 500 to avoid single large blocking call
        described = []
        _BATCH = 500
        for i in range(0, len(names), _BATCH):
            described.extend(admin.describe_topics(names[i:i + _BATCH]))

        topics: list[dict[str, Any]] = []
        total_urp = 0
        for meta in described:
            name = meta.get("topic", "")
            if not name or _is_internal_topic(name):
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

            jmx = (topic_jmx or {}).get(name, {})
            topics.append(
                {
                    "name": name,
                    "partition_count": partition_count,
                    "replication_factor": replication_factor,
                    "messages_in_per_sec": jmx.get("messages_in_per_sec", 0.0),
                    "bytes_in_per_sec": jmx.get("bytes_in_per_sec", 0.0),
                    "bytes_out_per_sec": jmx.get("bytes_out_per_sec", 0.0),
                    "total_messages": 0,
                    "size_bytes": jmx.get("size_bytes", 0),
                    "retention_bytes": -1,
                    "retention_pct": 0.0,
                    "status": "degraded" if urp else "healthy",
                }
            )
        return topics, total_urp

    def _build_consumer_groups(
        self, admin: KafkaAdminClient, security: dict[str, Any]
    ) -> list[dict[str, Any]]:
        listed = admin.list_consumer_groups()
        group_ids = [entry[0] for entry in listed]
        if not group_ids:
            return []

        states = self._describe_group_states(admin, group_ids)

        # Only active groups need an offset/lag round-trip.
        active = [g for g in group_ids if states.get(g, "Unknown") not in _INACTIVE_GROUP_STATES]
        # Cap active group lag fetch at 100 to avoid per-group round trips on large clusters
        # Groups beyond cap are reported with zero lag (state preserved)
        active = active[:100]

        consumer: KafkaConsumer | None = None
        try:
            groups: list[dict[str, Any]] = []
            for group_id in group_ids:
                state = states.get(group_id, "Unknown")
                if group_id not in active:
                    groups.append(self._empty_group(group_id, state))
                    continue

                offsets = admin.list_consumer_group_offsets(group_id)
                if not offsets:
                    groups.append(self._empty_group(group_id, state))
                    continue

                if consumer is None:
                    consumer = KafkaConsumer(
                        bootstrap_servers=self._bootstrap_list,
                        enable_auto_commit=False,
                        group_id=None,
                        **security,
                    )

                partitions, total_lag, topics = self._group_lag(consumer, offsets)
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
                    consumer.close()
                except Exception:  # noqa: BLE001 — best-effort cleanup
                    pass

    def _describe_group_states(
        self, admin: KafkaAdminClient, group_ids: list[str]
    ) -> dict[str, str]:
        """Map group_id -> state, tolerating describe failures."""
        try:
            described = admin.describe_consumer_groups(group_ids)
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

    def _group_lag(
        self, consumer: KafkaConsumer, offsets: dict[Any, Any]
    ) -> tuple[list[dict[str, Any]], int, set[str]]:
        tps = list(offsets.keys())
        end_offsets = consumer.end_offsets(tps)

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
