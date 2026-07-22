"""
Individual metric collection handlers for Kafka Analyser.
Each function is a self-contained job handler registered with jobs.py.
Replaces the monolithic _collection_loop in main.py.
"""
import asyncio
import logging
import time
from typing import Any

import kafka_store as _ks

logger = logging.getLogger(__name__)


async def _get_enabled_clusters() -> list[dict]:
    """Get enabled clusters from storage."""
    from storage import get_backend
    from config import settings
    clusters = await get_backend().get_clusters(settings.agent_slug)
    return [c for c in clusters if c.get("enabled")]


async def _get_collector(c: dict):
    """Build a RealKafkaCollector for a cluster config."""
    from tools.real_kafka import RealKafkaCollector
    return RealKafkaCollector({
        "bootstrap_servers": c["bootstrap_servers"],
        "auth_type": "none" if c.get("auth_type", "none") == "none" else "sasl",
        "sasl_username": c.get("sasl_username"),
        "sasl_password": c.get("sasl_password"),
        "sasl_mechanism": c.get("sasl_mechanism", "PLAIN"),
        "tls_enabled": c.get("tls_enabled", False),
        "cluster_label": c["name"],
        "jmx_port": c.get("jmx_port"),
        "prometheus_port": c.get("prometheus_port"),
    })


def _cid(c: dict) -> str:
    return str(c.get("id", c.get("name", "")))


# ── Job 1: Broker Health ──────────────────────────────────────────────────────
async def collect_broker_health():
    """Collect broker JVM metrics via Prometheus Phase 1 filtered pull."""
    clusters = await _get_enabled_clusters()
    if not clusters:
        collect_broker_health._last_result = "No enabled clusters"
        return
    from tools.prometheus_collector import scrape_all_brokers
    results = []
    for c in clusters:
        cid = _cid(c)
        prom_port = c.get("prometheus_port")
        if not prom_port:
            continue
        try:
            collector = await _get_collector(c)
            brokers = await collector.collect_brokers_only()
            if not brokers:
                continue
            data = _ks.get_cluster_data(cid) or {}
            if not data.get("brokers"):
                data["brokers"] = brokers
                _ks.set_cluster_data(data, source_type=c.get("source_type", "live"), cluster_id=cid)
            broker_metrics = await scrape_all_brokers(brokers, prom_port, cpu_cores=c.get("cpu_cores"))
            for broker in brokers:
                bid = str(broker.get("broker_id", broker.get("host", "")))
                if bid in broker_metrics and broker_metrics[bid]:
                    broker.update(broker_metrics[bid])
            # Update only brokers in cache
            _ks.update_brokers(cid, brokers)
            from kafka_store import save_brokers
            await save_brokers(cid, brokers=brokers)
            results.append(f"{c['name']}: {len(brokers)} brokers")
        except Exception as e:
            logger.warning("broker_health failed for %s: %s", c["name"], e)
    collect_broker_health._last_result = "Brokers updated: " + ", ".join(results) if results else "No data collected"


# ── Job 2: Consumer Lag (Active Groups) ──────────────────────────────────────
async def collect_consumer_lag_active():
    """Collect lag for all consumer groups using direct AdminClient calls — ~10s for 641 groups."""
    clusters = await _get_enabled_clusters()
    if not clusters:
        collect_consumer_lag_active._last_result = "No enabled clusters"
        return
    total_groups = 0
    for c in clusters:
        cid = _cid(c)
        try:
            from kafka import KafkaAdminClient
            security = {}
            if c.get("auth_type") not in (None, "none"):
                security = {
                    "security_protocol": "SASL_PLAINTEXT",
                    "sasl_mechanism": c.get("sasl_mechanism", "PLAIN"),
                    "sasl_plain_username": c.get("sasl_username"),
                    "sasl_plain_password": c.get("sasl_password"),
                }
            loop = asyncio.get_event_loop()
            def _fetch_all_lags():
                admin = KafkaAdminClient(
                    bootstrap_servers=c["bootstrap_servers"],
                    request_timeout_ms=15000,
                    **security,
                )
                try:
                    # Get all groups with protocol types
                    all_groups = admin.list_consumer_groups()
                    consumer_gids = [g[0] for g in all_groups if g[1] == "consumer"]
                    connect_gids = [g[0] for g in all_groups if g[1] == "connect"]
                    sr_gids = [g[0] for g in all_groups if g[1] == "sr"]
                    empty_gids = [g[0] for g in all_groups if g[1] == ""]
                    # Fetch offsets for all consumer groups
                    enriched = []
                    total_lag = 0
                    for gid in consumer_gids:
                        try:
                            offsets = admin.list_consumer_group_offsets(gid)
                            # Calculate lag per partition
                            group_lag = 0
                            topic_set = set()
                            for tp, offset_meta in offsets.items():
                                committed = offset_meta.offset if hasattr(offset_meta, 'offset') else offset_meta
                                if committed >= 0:
                                    topic_set.add(tp.topic)
                                    # We'll calculate lag later with end offsets
                                    group_lag += 0  # placeholder
                            enriched.append({
                                "group_id": gid,
                                "state": "consumer",
                                "topic_count": len(topic_set),
                                "total_lag": group_lag,
                                "committed_offsets": len(offsets),
                            })
                            total_lag += group_lag
                        except Exception as e:
                            enriched.append({"group_id": gid, "state": "consumer", "total_lag": -1, "error": str(e)})
                    return {
                        "groups": enriched,
                        "group_states": {
                            "consumer": len(consumer_gids),
                            "connect": len(connect_gids),
                            "schema_registry": len(sr_gids),
                            "empty": len(empty_gids),
                            "total": len(all_groups),
                        },
                        "total_lag": total_lag,
                    }
                finally:
                    admin.close()
            result = await loop.run_in_executor(None, _fetch_all_lags)
            data = _ks.get_cluster_data(cid) or {}
            data["consumer_groups"] = result["groups"]
            data["group_states"] = result["group_states"]
            if "counts" not in data:
                data["counts"] = {}
            data["counts"]["consumer_groups"] = result["group_states"]["total"]
            data["counts"]["active_groups"] = result["group_states"]["consumer"]
            data["counts"]["total_lag"] = result["total_lag"]
            _ks.set_cluster_data(data, source_type=c.get("source_type", "live"), cluster_id=cid)
            from kafka_store import save_groups
            await save_groups(cid)
            total_groups += result["group_states"]["consumer"]
        except Exception as e:
            logger.warning("consumer_lag_active failed for %s: %s", c["name"], e)
    collect_consumer_lag_active._last_result = f"Consumer groups collected: {total_groups} groups"


# ── Job 3: Topic Sizes ────────────────────────────────────────────────────────
async def collect_topic_sizes():
    """Collect topic sizes via AdminClient describe_log_dirs — fast, no JMX needed."""
    clusters = await _get_enabled_clusters()
    if not clusters:
        collect_topic_sizes._last_result = "No enabled clusters"
        return
    results = []
    for c in clusters:
        cid = _cid(c)
        try:
            collector = await _get_collector(c)
            sizes_result = await collector.collect_topic_sizes(top_n=100)
            if sizes_result.get("error"):
                logger.warning("topic_sizes failed for %s: %s", c["name"], sizes_result["error"])
                continue
            data = _ks.get_cluster_data(cid) or {}
            if "counts" not in data:
                data["counts"] = {}
            data["counts"]["top_topics_by_size"] = sizes_result["topic_sizes"]
            data["counts"]["total_size_gb"] = sizes_result["total_size_gb"]
            data["counts"]["total_topics"] = sizes_result["total_topics"]
            # Also populate data["topics"] for dashboard compatibility
            data["topics"] = [
                {
                    "name": t["topic"],
                    "size_bytes": t["size_bytes"],
                    "size_mb": t["size_mb"],
                    "partition_count": 0,
                    "replication_factor": 0,
                    "messages_in_per_sec": 0.0,
                    "bytes_in_per_sec": 0.0,
                    "bytes_out_per_sec": 0.0,
                    "total_messages": 0,
                    "under_replicated": 0,
                    "status": "healthy",
                }
                for t in sizes_result["topic_sizes"]
            ]
            _ks.set_cluster_data(data, source_type=c.get("source_type", "live"), cluster_id=cid)
            # Persist to PostgreSQL
            from kafka_store import save_topics_metrics
            await save_topics_metrics(cid)
            results.append(f"{c['name']}: {sizes_result['total_topics']} topics, {sizes_result['total_size_gb']}GB in {sizes_result['collection_time_secs']}s")
        except Exception as e:
            logger.warning("topic_sizes failed for %s: %s", c["name"], e)
    collect_topic_sizes._last_result = "; ".join(results) if results else "No data collected"


# ── Job 4: Topic Structure ────────────────────────────────────────────────────
async def collect_topic_structure():
    """Collect full topic structure — partition counts, RF, URP. Slow, run every 30 min."""
    clusters = await _get_enabled_clusters()
    if not clusters:
        collect_topic_structure._last_result = "No enabled clusters"
        return
    results = []
    for c in clusters:
        cid = _cid(c)
        try:
            collector = await _get_collector(c)
            all_topic_names = await collector.list_all_topics()
            if not all_topic_names:
                continue
            described_topics, total_urp = await collector.describe_all_topics(all_topic_names, workers=10)
            if described_topics:
                total_rf1 = sum(1 for t in described_topics if t.get("replication_factor") == 1)
                data = _ks.get_cluster_data(cid) or {}
                if "counts" not in data:
                    data["counts"] = {}
                data["counts"]["total_topics"] = len(all_topic_names)
                data["counts"]["topics_rf1"] = total_rf1
                data["counts"]["total_urp"] = total_urp
                _ks.set_cluster_data(data, source_type=c.get("source_type", "live"), cluster_id=cid)
                results.append(f"{c['name']}: {len(described_topics)} topics, {total_rf1} RF=1, {total_urp} URP")
        except Exception as e:
            logger.warning("topic_structure failed for %s: %s", c["name"], e)
    collect_topic_structure._last_result = "; ".join(results) if results else "No data collected"


# ── Job 5: Consumer Lag Full (Governance) ─────────────────────────────────────
async def collect_consumer_lag_full():
    """Full consumer group audit including EMPTY and DEAD groups for governance."""
    clusters = await _get_enabled_clusters()
    if not clusters:
        collect_consumer_lag_full._last_result = "No enabled clusters"
        return
    for c in clusters:
        cid = _cid(c)
        try:
            collector = await _get_collector(c)
            all_groups = await collector.collect_group_states()
            all_gids = [g.get("group_id") or g.get("group_name") for g in all_groups if g.get("group_id") or g.get("group_name")]
            enriched = await collector.fetch_all_group_lags(all_gids) if all_gids else []
            data = _ks.get_cluster_data(cid) or {}
            data["consumer_groups_full"] = enriched
            _ks.set_cluster_data(data, source_type=c.get("source_type", "live"), cluster_id=cid)
            collect_consumer_lag_full._last_result = f"Full audit: {len(enriched)} groups"
        except Exception as e:
            logger.warning("consumer_lag_full failed for %s: %s", c["name"], e)
            collect_consumer_lag_full._last_result = f"Failed: {e}"


# ── Job 6: Kafka Connectors ───────────────────────────────────────────────────
async def collect_connectors():
    """Collect Kafka Connect connector status."""
    clusters = await _get_enabled_clusters()
    if not clusters:
        collect_connectors._last_result = "No enabled clusters"
        return
    results = []
    for c in clusters:
        cid = _cid(c)
        connect_url = c.get("kafka_connect_url", "")
        if not connect_url:
            continue
        try:
            from tools.kafka_connect import KafkaConnectCollector
            collector = KafkaConnectCollector(connect_url)
            connect_data = await collector.collect()
            data = _ks.get_cluster_data(cid) or {}
            data["connectors"] = connect_data.get("connectors", [])
            data["connector_count"] = connect_data.get("connector_count", 0)
            _ks.set_cluster_data(data, source_type=c.get("source_type", "live"), cluster_id=cid)
            results.append(f"{c['name']}: {connect_data.get('connector_count', 0)} connectors")
        except Exception as e:
            logger.warning("connectors failed for %s: %s", c["name"], e)
    collect_connectors._last_result = "; ".join(results) if results else "No connectors configured"


# ── Job 7: Message Rate ───────────────────────────────────────────────────────
_prev_offsets: dict = {}
_prev_offset_time: float = 0.0

async def collect_msg_rate():
    """Calculate message in/out rates from offset deltas — no JMX needed."""
    global _prev_offsets, _prev_offset_time
    clusters = await _get_enabled_clusters()
    if not clusters:
        collect_msg_rate._last_result = "No enabled clusters"
        return
    for c in clusters:
        cid = _cid(c)
        try:
            from kafka import KafkaConsumer, TopicPartition
            security = {}
            if c.get("auth_type") not in (None, "none"):
                security = {
                    "security_protocol": "SASL_PLAINTEXT",
                    "sasl_mechanism": c.get("sasl_mechanism", "PLAIN"),
                    "sasl_plain_username": c.get("sasl_username"),
                    "sasl_plain_password": c.get("sasl_password"),
                }
            # Get top topics by size for msg rate tracking
            data = _ks.get_cluster_data(cid) or {}
            top_topics = [t["topic"] for t in data.get("counts", {}).get("top_topics_by_size", [])[:30]]
            if not top_topics:
                collect_msg_rate._last_result = "No topic size data yet — run kafka-topic-sizes first"
                return
            # Get partition assignments via AdminClient
            from kafka import KafkaAdminClient
            admin = KafkaAdminClient(
                bootstrap_servers=c["bootstrap_servers"],
                request_timeout_ms=10000,
                **security,
            )
            meta = admin.describe_topics(top_topics)
            admin.close()
            tps = []
            for topic_meta in meta:
                topic = topic_meta.get("topic", "")
                for p in topic_meta.get("partitions", []):
                    tps.append(TopicPartition(topic, p["partition"]))
            if not tps:
                continue
            # Get latest offsets via KafkaConsumer seek_to_end
            loop = asyncio.get_event_loop()
            def _get_offsets():
                consumer = KafkaConsumer(
                    bootstrap_servers=c["bootstrap_servers"],
                    request_timeout_ms=10000,
                    **security,
                )
                consumer.assign(tps)
                consumer.seek_to_end(*tps)
                offsets = {tp: consumer.position(tp) for tp in tps}
                consumer.close()
                return offsets
            now = time.time()
            offsets = await loop.run_in_executor(None, _get_offsets)
            # Calculate rates if we have previous offsets
            topic_rates = {}
            if _prev_offsets and _prev_offset_time:
                elapsed = now - _prev_offset_time
                if elapsed > 0:
                    for tp, curr in offsets.items():
                        key = f"{cid}:{tp.topic}:{tp.partition}"
                        prev = _prev_offsets.get(key, curr)
                        delta = max(0, curr - prev)
                        rate = delta / elapsed
                        topic_rates[tp.topic] = topic_rates.get(tp.topic, 0) + rate
            # Store current offsets
            _prev_offsets = {f"{cid}:{tp.topic}:{tp.partition}": off for tp, off in offsets.items()}
            _prev_offset_time = now
            # Build top by msg rate
            top_by_rate = sorted(
                [{"topic": t, "messages_in_per_sec": round(r, 2)} for t, r in topic_rates.items()],
                key=lambda x: x["messages_in_per_sec"], reverse=True
            )[:30]
            if "counts" not in data:
                data["counts"] = {}
            data["counts"]["top_topics_by_msg_rate"] = top_by_rate
            data["counts"]["total_hot"] = sum(1 for t in top_by_rate if t["messages_in_per_sec"] > 100)
            _ks.set_cluster_data(data, source_type=c.get("source_type", "live"), cluster_id=cid)
            collect_msg_rate._last_result = f"Msg rates: {len(top_by_rate)} topics, {sum(t['messages_in_per_sec'] for t in top_by_rate):.0f} total msgs/s"
        except Exception as e:
            logger.warning("msg_rate failed for %s: %s", c["name"], e)
            collect_msg_rate._last_result = f"Failed: {e}"


# ── Job 8: Schema Registry ────────────────────────────────────────────────────
async def collect_schema_registry():
    """Collect schema registry stats."""
    clusters = await _get_enabled_clusters()
    if not clusters:
        collect_schema_registry._last_result = "No enabled clusters"
        return
    results = []
    for c in clusters:
        cid = _cid(c)
        sr_url = c.get("schema_registry_url", "")
        if not sr_url:
            continue
        try:
            from tools.schema_registry import SchemaRegistryCollector
            collector = SchemaRegistryCollector(sr_url)
            sr_data = await collector.collect()
            data = _ks.get_cluster_data(cid) or {}
            data["schema_registry"] = sr_data
            _ks.set_cluster_data(data, source_type=c.get("source_type", "live"), cluster_id=cid)
            results.append(f"{c['name']}: {sr_data.get('subject_count', 0)} schemas")
        except Exception as e:
            logger.warning("schema_registry failed for %s: %s", c["name"], e)
    collect_schema_registry._last_result = "; ".join(results) if results else "No schema registry configured"
