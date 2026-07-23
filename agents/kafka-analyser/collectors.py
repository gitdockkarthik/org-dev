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
            # Update cache
            _ks.update_brokers(cid, brokers)
            # Upsert to postgres — one row per broker per cluster (latest only)
            try:
                from database import SessionLocal
                from sqlalchemy import text
                async with SessionLocal() as sess:
                    for broker in brokers:
                        bid = broker.get("broker_id") or broker.get("id", "")
                        await sess.execute(text("""
                            INSERT INTO kafka_broker_metrics
                            (time, cluster_id, broker_id, heap_pct, gc_pause_ms,
                             request_handler_idle_pct, urp_count, messages_in_per_sec,
                             cpu_pct, disk_pct)
                            VALUES (now(), :cid, :bid, :heap, :gc, :idle, :urp, :msgs, :cpu, :disk)
                            ON CONFLICT (cluster_id, broker_id)
                            DO UPDATE SET
                                time = now(),
                                heap_pct = EXCLUDED.heap_pct,
                                gc_pause_ms = EXCLUDED.gc_pause_ms,
                                request_handler_idle_pct = EXCLUDED.request_handler_idle_pct,
                                urp_count = EXCLUDED.urp_count,
                                messages_in_per_sec = EXCLUDED.messages_in_per_sec,
                                cpu_pct = EXCLUDED.cpu_pct,
                                disk_pct = EXCLUDED.disk_pct
                        """), {
                            "cid": int(cid),
                            "bid": bid,
                            "heap": broker.get("heap_pct", 0.0),
                            "gc": int(broker.get("gc_pause_ms", 0)),
                            "idle": broker.get("request_handler_idle_pct", 100.0),
                            "urp": int(broker.get("urp_count", 0)),
                            "msgs": broker.get("messages_in_per_sec", 0.0),
                            "cpu": broker.get("cpu_pct", 0.0),
                            "disk": broker.get("disk_pct", 0.0),
                        })
                    await sess.commit()
            except Exception as db_exc:
                logger.warning("broker upsert failed: %s", db_exc)
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
                    # Batched lag: 100 groups per batch (optimal from testing — ~31s for 642 groups)
                    from kafka import KafkaConsumer
                    BATCH = 100
                    group_committed = {}
                    end_offsets = {}
                    for batch_start in range(0, len(consumer_gids), BATCH):
                        batch_gids = consumer_gids[batch_start:batch_start + BATCH]
                        batch_committed = {}
                        for gid in batch_gids:
                            try:
                                offsets = admin.list_consumer_group_offsets(gid)
                                batch_committed[gid] = {
                                    tp: (meta.offset if hasattr(meta, 'offset') else meta)
                                    for tp, meta in offsets.items()
                                    if (meta.offset if hasattr(meta, 'offset') else meta) >= 0
                                }
                            except Exception:
                                batch_committed[gid] = {}
                        group_committed.update(batch_committed)
                        batch_tps = list(set(tp for committed in batch_committed.values() for tp in committed.keys()))
                        if batch_tps:
                            try:
                                _consumer = KafkaConsumer(
                                    bootstrap_servers=c["bootstrap_servers"],
                                    request_timeout_ms=10000,
                                    **security,
                                )
                                _consumer.assign(batch_tps)
                                _consumer.seek_to_end(*batch_tps)
                                end_offsets.update({tp: _consumer.position(tp) for tp in batch_tps})
                                _consumer.close()
                            except Exception as e:
                                logger.warning("seek_to_end batch failed: %s", e)
                    # Calculate lag per group
                    for gid in consumer_gids:
                        committed = group_committed.get(gid, {})
                        group_lag = sum(
                            max(0, end_offsets.get(tp, committed_off) - committed_off)
                            for tp, committed_off in committed.items()
                        )
                        enriched.append({
                            "group_id": gid,
                            "state": "consumer",
                            "topic_count": len(set(tp.topic for tp in committed.keys())),
                            "total_lag": group_lag,
                            "committed_offsets": len(committed),
                        })
                        total_lag += group_lag
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
    """Collect ALL topic sizes via AdminClient describe_log_dirs and upsert to postgres."""
    clusters = await _get_enabled_clusters()
    if not clusters:
        collect_topic_sizes._last_result = "No enabled clusters"
        return
    results = []
    for c in clusters:
        cid = _cid(c)
        try:
            collector = await _get_collector(c)
            # Collect ALL topics — no top_n limit
            sizes_result = await collector.collect_topic_sizes(top_n=99999)
            if sizes_result.get("error"):
                logger.warning("topic_sizes failed for %s: %s", c["name"], sizes_result["error"])
                continue
            all_topics = sizes_result["topic_sizes"]
            total_size_gb = sizes_result["total_size_gb"]
            total_topics = sizes_result["total_topics"]
            # Bulk upsert all topics in single SQL statement — fast (~0.65s for 16k rows)
            from database import SessionLocal
            from sqlalchemy import text
            async with SessionLocal() as sess:
                if all_topics:
                    values = ",".join(
                        f"({int(cid)}, '{t['topic'].replace(chr(39), chr(39)*2)}', {t['size_bytes']})"
                        for t in all_topics
                    )
                    await sess.execute(text(f"""
                        INSERT INTO kafka_topic_metrics
                        (cluster_id, topic, size_bytes, time, partition_count, replication_factor,
                         messages_in_per_sec, bytes_in_per_sec, bytes_out_per_sec,
                         total_messages, retention_bytes, retention_pct, last_seen)
                        SELECT c, t, s, now(), 0, 0, 0, 0, 0, 0, -1, 0, now()
                        FROM (VALUES {values}) AS v(c, t, s)
                        ON CONFLICT (cluster_id, topic) DO UPDATE SET
                            size_bytes = EXCLUDED.size_bytes,
                            last_seen = now(),
                            time = now()
                    """))
                # Cleanup stale topics not seen in last 2 sync cycles
                await sess.execute(text("""
                    DELETE FROM kafka_topic_metrics
                    WHERE cluster_id = :cid
                    AND last_seen < now() - interval '35 minutes'
                """), {"cid": int(cid)})
                await sess.commit()
            # Update kafka_store cache with top 100 for dashboard
            top_100 = all_topics[:100]
            data = _ks.get_cluster_data(cid) or {}
            if "counts" not in data:
                data["counts"] = {}
            data["counts"]["top_topics_by_size"] = top_100
            data["counts"]["total_size_gb"] = total_size_gb
            data["counts"]["total_topics"] = total_topics
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
                    "status": "healthy" if t["size_bytes"] < 10*1024**3 else "retention-warning" if t["size_bytes"] < 50*1024**3 else "retention-critical",
                }
                for t in top_100
            ]
            _ks.set_cluster_data(data, source_type=c.get("source_type", "live"), cluster_id=cid)
            results.append(f"{c['name']}: {total_topics} topics, {total_size_gb}GB in {sizes_result['collection_time_secs']}s")
        except Exception as e:
            logger.warning("topic_sizes failed for %s: %s", c["name"], e)
            import traceback; traceback.print_exc()
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
    """Calculate bytes/sec ingestion rate using describe_log_dirs delta between cycles.
    Upserts bytes_in_per_sec to kafka_topic_metrics in postgres."""
    global _prev_offsets, _prev_offset_time
    clusters = await _get_enabled_clusters()
    if not clusters:
        collect_msg_rate._last_result = "No enabled clusters"
        return
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
            def _get_partition_sizes():
                admin = KafkaAdminClient(
                    bootstrap_servers=c["bootstrap_servers"],
                    request_timeout_ms=15000,
                    **security,
                )
                try:
                    result = admin.describe_log_dirs()
                    sizes = {}
                    for log_dir in result.log_dirs:
                        if log_dir[0] != 0: continue
                        for topic_entry in log_dir[2]:
                            topic = topic_entry[0]
                            if topic.startswith('_'): continue
                            for partition in topic_entry[1]:
                                key = f"{topic}:{partition[0]}"
                                sizes[key] = partition[1]
                    return sizes
                finally:
                    admin.close()
            now = time.time()
            current_sizes = await loop.run_in_executor(None, _get_partition_sizes)
            prev_key = f"{cid}_sizes"
            prev_sizes = _prev_offsets.get(prev_key, {})
            prev_time = _prev_offset_time if _prev_offset_time else now
            elapsed = max(1, now - prev_time)
            _prev_offsets[prev_key] = current_sizes
            _prev_offset_time = now
            if not prev_sizes:
                collect_msg_rate._last_result = "Baseline stored — rates available next cycle"
                return
            # Calculate bytes/sec per topic
            topic_rates = {}
            for key, size2 in current_sizes.items():
                size1 = prev_sizes.get(key, size2)
                delta = max(0, size2 - size1)
                topic = key.rsplit(':', 1)[0]
                topic_rates[topic] = topic_rates.get(topic, 0) + (delta / elapsed)
            # Upsert bytes_in_per_sec to postgres for active topics
            active_topics = {t: r for t, r in topic_rates.items() if r > 0}
            if active_topics:
                from database import SessionLocal
                from sqlalchemy import text
                async with SessionLocal() as sess:
                    for topic, rate in active_topics.items():
                        await sess.execute(text("""
                            UPDATE kafka_topic_metrics
                            SET bytes_in_per_sec = :rate, time = now()
                            WHERE cluster_id = :cid AND topic = :topic
                        """), {"cid": int(cid), "rate": round(rate, 2), "topic": topic})
                    await sess.commit()
            # Also write to kafka_topic_metrics_hourly for trend chart
            if active_topics:
                from database import SessionLocal as _SL2
                from sqlalchemy import text as _t2
                async with _SL2() as sess2:
                    for topic, rate in list(active_topics.items())[:50]:
                        await sess2.execute(_t2("""
                            INSERT INTO kafka_topic_metrics_hourly
                            (cluster_id, topic, hour_bucket, avg_msgs, max_msgs, sample_count)
                            VALUES (:cid, :topic, date_trunc('hour', now()), :rate, :rate, 1)
                            ON CONFLICT (cluster_id, topic, hour_bucket)
                            DO UPDATE SET
                                avg_msgs = (kafka_topic_metrics_hourly.avg_msgs * kafka_topic_metrics_hourly.sample_count + EXCLUDED.avg_msgs) / (kafka_topic_metrics_hourly.sample_count + 1),
                                max_msgs = GREATEST(kafka_topic_metrics_hourly.max_msgs, EXCLUDED.max_msgs),
                                sample_count = kafka_topic_metrics_hourly.sample_count + 1
                        """), {"cid": int(cid), "topic": topic, "rate": round(rate/1024, 2)})
                    await sess2.commit()
            hot_count = sum(1 for r in active_topics.values() if r > 100*1024)
            collect_msg_rate._last_result = f"Rates updated: {len(active_topics)} active topics, {hot_count} hot (>100KB/s)"
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
