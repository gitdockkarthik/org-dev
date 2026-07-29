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


async def _get_cluster(cluster_id: str) -> dict | None:
    """Get a single enabled cluster by ID."""
    clusters = await _get_enabled_clusters()
    return next((c for c in clusters if str(c.get("id", "")) == str(cluster_id)), None)


# ── Job 1: Broker Health ──────────────────────────────────────────────────────
async def collect_broker_health(cluster_id: str = ""):
    """Collect broker JVM metrics via Prometheus Phase 1 filtered pull."""
    c = await _get_cluster(cluster_id)
    if not c:
        collect_broker_health._last_result = f"Cluster {cluster_id} not found or disabled"
        return
    from tools.prometheus_collector import scrape_all_brokers
    results = []
    cid = _cid(c)
    prom_port = c.get("prometheus_port")
    if not prom_port:
        collect_broker_health._last_result = "No Prometheus port configured"
        return
    try:
        collector = await _get_collector(c)
        brokers = await collector.collect_brokers_only()
        if not brokers:
            collect_broker_health._last_result = "No brokers found"
            return
        # Collect true per-broker log directory sizes
        log_dir_result = {"broker_sizes_gb": {}}
        try:
            log_dir_result = await collector.collect_broker_log_dir_sizes()
        except Exception as log_exc:
            logger.warning("collect_broker_log_dir_sizes failed: %s", log_exc)
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
                        node_id = int(bid) if bid.isdigit() else None
                        data_gb_true = None
                        if node_id is not None:
                            data_gb_true = log_dir_result.get("broker_sizes_gb", {}).get(node_id)
                        await sess.execute(text("""
                            INSERT INTO kafka_broker_metrics
                            (time, cluster_id, broker_id, heap_pct, gc_pause_ms,
                             request_handler_idle_pct, urp_count, messages_in_per_sec,
                             cpu_pct, disk_pct, bytes_in_per_sec, bytes_out_per_sec,
                             produce_latency_ms, fetch_latency_ms,
                             isr_shrinks_per_sec, isr_expands_per_sec, data_gb_true)
                            VALUES (now(), :cid, :bid, :heap, :gc, :idle, :urp, :msgs, :cpu, :disk,
                                    :bin, :bout, :plat, :flat, :isrs, :isre, :data_gb_true)
                            ON CONFLICT (cluster_id, broker_id)
                            DO UPDATE SET
                                time = now(),
                                heap_pct = EXCLUDED.heap_pct,
                                gc_pause_ms = EXCLUDED.gc_pause_ms,
                                request_handler_idle_pct = EXCLUDED.request_handler_idle_pct,
                                urp_count = EXCLUDED.urp_count,
                                messages_in_per_sec = EXCLUDED.messages_in_per_sec,
                                cpu_pct = EXCLUDED.cpu_pct,
                                disk_pct = EXCLUDED.disk_pct,
                                bytes_in_per_sec = EXCLUDED.bytes_in_per_sec,
                                bytes_out_per_sec = EXCLUDED.bytes_out_per_sec,
                                produce_latency_ms = EXCLUDED.produce_latency_ms,
                                fetch_latency_ms = EXCLUDED.fetch_latency_ms,
                                isr_shrinks_per_sec = EXCLUDED.isr_shrinks_per_sec,
                                isr_expands_per_sec = EXCLUDED.isr_expands_per_sec,
                                data_gb_true = EXCLUDED.data_gb_true
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
                            "bin": broker.get("bytes_in_per_sec", 0.0),
                            "bout": broker.get("bytes_out_per_sec", 0.0),
                            "plat": broker.get("produce_latency_ms", 0.0),
                            "flat": broker.get("fetch_latency_ms", 0.0),
                            "isrs": broker.get("isr_shrinks_per_sec", 0.0),
                            "isre": broker.get("isr_expands_per_sec", 0.0),
                            "data_gb_true": data_gb_true,
                        })
                    await sess.commit()
        except Exception as db_exc:
            logger.warning("broker upsert failed: %s", db_exc)
        results.append(f"{c['name']}: {len(brokers)} brokers")
    except Exception as e:
        logger.warning("broker_health failed for %s: %s", c["name"], e)
    collect_broker_health._last_result = results[0] if results else "No data collected"


# ── Job 2: Consumer Lag (Active Groups) ──────────────────────────────────────
async def collect_consumer_lag_active(cluster_id: str = ""):
    """Collect lag for all consumer groups using direct AdminClient calls — ~10s for 641 groups."""
    c = await _get_cluster(cluster_id)
    if not c:
        collect_consumer_lag_active._last_result = f"Cluster {cluster_id} not found or disabled"
        return
    total_groups = 0
    cid = _cid(c)
    try:
        from kafka import KafkaAdminClient
        security = {}
        if c.get("auth_type") not in (None, "none"):
            import ssl as _ssl
            tls = c.get("tls_enabled", False)
            security = {
                "security_protocol": "SASL_SSL" if tls else "SASL_PLAINTEXT",
                "sasl_mechanism": c.get("sasl_mechanism", "PLAIN"),
                "sasl_plain_username": c.get("sasl_username"),
                "sasl_plain_password": c.get("sasl_password"),
            }
            if tls:
                ssl_ctx = _ssl.create_default_context()
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = _ssl.CERT_NONE
                security["ssl_context"] = ssl_ctx
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
                    # Collect ALL committed offsets first, then fetch end offsets in one consumer session
                    from kafka import KafkaConsumer
                    BATCH = 100
                    group_committed = {}
                    end_offsets = {}
                    # Step 1: collect all committed offsets (fast — AdminClient)
                    for batch_start in range(0, len(consumer_gids), BATCH):
                        batch_gids = consumer_gids[batch_start:batch_start + BATCH]
                        for gid in batch_gids:
                            try:
                                offsets = admin.list_consumer_group_offsets(gid)
                                group_committed[gid] = {
                                    tp: (meta.offset if hasattr(meta, 'offset') else meta)
                                    for tp, meta in offsets.items()
                                    if (meta.offset if hasattr(meta, 'offset') else meta) > 0
                                }
                            except Exception:
                                group_committed[gid] = {}
                    # Step 2: get ALL end offsets in one consumer session (avoid repeated connections)
                    all_tps = list(set(tp for committed in group_committed.values() for tp in committed.keys()))
                    if all_tps:
                        try:
                            _consumer = KafkaConsumer(
                                bootstrap_servers=c["bootstrap_servers"],
                                request_timeout_ms=10000,
                                **security,
                            )
                            # Process in batches within single connection
                            SEEK_BATCH = 500
                            for i in range(0, len(all_tps), SEEK_BATCH):
                                batch_tps = all_tps[i:i + SEEK_BATCH]
                                _consumer.assign(batch_tps)
                                _consumer.seek_to_end(*batch_tps)
                                end_offsets.update({tp: _consumer.position(tp) for tp in batch_tps})
                            _consumer.close()
                        except Exception as e:
                            logger.warning("seek_to_end failed: %s", e)
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
        # Upsert consumer group lag to postgres
        try:
            from database import SessionLocal
            from sqlalchemy import text as _cgt
            enriched = result["groups"]
            if enriched:
                async with SessionLocal() as sess:
                    values = ",".join(
                        f"({int(cid)}, '{g['group_id'].replace(chr(39), chr(39)*2)}', "
                        f"'{g.get('state','consumer')}', {g.get('total_lag',0)}, "
                        f"{g.get('topic_count',0)}, {g.get('committed_offsets',0)}, now())"
                        for g in enriched if g.get('group_id')
                    )
                    if values:
                        await sess.execute(_cgt(f"""
                            INSERT INTO kafka_consumer_group_lag
                            (cluster_id, group_id, state, total_lag, topic_count, committed_offsets, updated_at)
                            VALUES {values}
                            ON CONFLICT (cluster_id, group_id) DO UPDATE SET
                                state = EXCLUDED.state,
                                total_lag = EXCLUDED.total_lag,
                                topic_count = EXCLUDED.topic_count,
                                committed_offsets = EXCLUDED.committed_offsets,
                                updated_at = now()
                        """))
                        await sess.commit()
        except Exception as _ge:
            logger.warning("consumer group lag upsert failed: %s", _ge)
        # Insert lag snapshot for trend chart
        try:
            from database import SessionLocal
            from sqlalchemy import text as _lst
            async with SessionLocal() as sess:
                await sess.execute(_lst("""
                    INSERT INTO kafka_lag_snapshots (cluster_id, total_lag, group_count, collected_at)
                    VALUES (:cid, :lag, :cnt, NOW())
                """), {"cid": str(cid), "lag": int(result["total_lag"]),
                       "cnt": result["group_states"]["consumer"]})
                await sess.commit()
        except Exception as _lse:
            logger.warning("lag snapshot insert failed: %s", _lse)
        total_groups += result["group_states"]["consumer"]
    except Exception as e:
        logger.warning("consumer_lag_active failed for %s: %s", c["name"], e)
    collect_consumer_lag_active._last_result = f"Consumer groups collected: {total_groups} groups"


# ── Job 3: Topic Sizes ────────────────────────────────────────────────────────
async def collect_topic_sizes(cluster_id: str = ""):
    """Collect ALL topic sizes via AdminClient describe_log_dirs and upsert to postgres."""
    c = await _get_cluster(cluster_id)
    if not c:
        collect_topic_sizes._last_result = f"Cluster {cluster_id} not found or disabled"
        return
    results = []
    cid = _cid(c)
    try:
        collector = await _get_collector(c)
        # Collect ALL topics — no top_n limit
        sizes_result = await collector.collect_topic_sizes(top_n=99999)
        if sizes_result.get("error"):
            logger.warning("topic_sizes failed for %s: %s", c["name"], sizes_result["error"])
            collect_topic_sizes._last_result = f"Error: {sizes_result['error']}"
            return
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
    collect_topic_sizes._last_result = results[0] if results else "No data collected"


# ── Job 4: Topic Structure ────────────────────────────────────────────────────
async def collect_topic_structure(cluster_id: str = ""):
    """Collect full topic structure — partition counts, RF, URP. Slow, run every 30 min."""
    c = await _get_cluster(cluster_id)
    if not c:
        collect_topic_structure._last_result = f"Cluster {cluster_id} not found or disabled"
        return
    results = []
    cid = _cid(c)
    try:
        collector = await _get_collector(c)
        all_topic_names = await collector.list_all_topics()
        if not all_topic_names:
            collect_topic_structure._last_result = "No topics found"
            return
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
            # Bulk update partition_count and replication_factor in kafka_topic_metrics
            try:
                from database import SessionLocal
                from sqlalchemy import text as _pct
                async with SessionLocal() as sess:
                    values = ",".join(
                        f"('{t['name'].replace(chr(39), chr(39)*2)}', {t.get('partition_count',0)}, {t.get('replication_factor',0)})"
                        for t in described_topics
                    )
                    if values:
                        await sess.execute(_pct(f"""
                            UPDATE kafka_topic_metrics SET
                                partition_count = v.pc,
                                replication_factor = v.rf
                            FROM (VALUES {values}) AS v(topic, pc, rf)
                            WHERE kafka_topic_metrics.cluster_id = {int(cid)}
                            AND kafka_topic_metrics.topic = v.topic
                        """))
                        await sess.commit()
                logger.info("Updated partition counts for %d topics in %s", len(described_topics), c["name"])
            except Exception as _pe:
                logger.warning("partition count update failed: %s", _pe)
            results.append(f"{c['name']}: {len(described_topics)} topics, {total_rf1} RF=1, {total_urp} URP")
            # Collect broker leader distribution
            try:
                from kafka import KafkaAdminClient
                import collections as _col
                security = {}
                if c.get("auth_type") not in (None, "none"):
                    import ssl as _ssl2
                    _tls2 = c.get("tls_enabled", False)
                    security = {
                        "security_protocol": "SASL_SSL" if _tls2 else "SASL_PLAINTEXT",
                        "sasl_mechanism": c.get("sasl_mechanism", "PLAIN"),
                        "sasl_plain_username": c.get("sasl_username"),
                        "sasl_plain_password": c.get("sasl_password"),
                    }
                    if _tls2:
                        _ssl_ctx2 = _ssl2.create_default_context()
                        _ssl_ctx2.check_hostname = False
                        _ssl_ctx2.verify_mode = _ssl2.CERT_NONE
                        security["ssl_context"] = _ssl_ctx2
                _admin = KafkaAdminClient(
                    bootstrap_servers=c["bootstrap_servers"],
                    request_timeout_ms=15000,
                    **security,
                )
                _all_topics = [t for t in _admin.list_topics() if not t.startswith('_')]
                leader_counts = _col.defaultdict(int)
                replica_counts = _col.defaultdict(int)
                # topic -> leader_broker_id mapping for partition leaders table
                partition_leaders = []
                BATCH = 500
                for _i in range(0, len(_all_topics), BATCH):
                    _meta = _admin.describe_topics(_all_topics[_i:_i+BATCH])
                    for _tm in _meta:
                        topic_name = _tm.get('topic', '')
                        for _p in _tm.get('partitions', []):
                            leader_id = str(_p['leader'])
                            leader_counts[leader_id] += 1
                            partition_leaders.append({
                                'topic': topic_name,
                                'partition': _p['partition'],
                                'leader': leader_id,
                            })
                            for _r in _p.get('replicas', []):
                                replica_counts[str(_r)] += 1
                _admin.close()
                from database import SessionLocal
                from sqlalchemy import text as _st
                async with SessionLocal() as _sess:
                    # Bulk upsert partition leaders — same pattern as kafka_topic_metrics
                    now_ts = __import__('datetime').datetime.now(__import__('datetime').timezone.utc)
                    BULK = 1000
                    for _bi in range(0, len(partition_leaders), BULK):
                        batch = partition_leaders[_bi:_bi+BULK]
                        pl_values = ", ".join(
                            f"({int(cid)}, '{pl['topic'].replace(chr(39), chr(39)*2)}', {pl['partition']}, '{pl['leader']}')"
                            for pl in batch
                        )
                        await _sess.execute(_st(f"""
                            INSERT INTO kafka_partition_leaders
                            (cluster_id, topic, partition, leader_broker_id, updated_at)
                            SELECT c, t, p, l, now()
                            FROM (VALUES {pl_values}) AS v(c, t, p, l)
                            ON CONFLICT (cluster_id, topic, partition) DO UPDATE SET
                                leader_broker_id = EXCLUDED.leader_broker_id,
                                updated_at = now()
                        """))
                    # Aggregate data_gb per broker from kafka_topic_metrics
                    for broker_id, lcount in leader_counts.items():
                        data_gb_row = await _sess.execute(_st("""
                            SELECT COALESCE(SUM(tm.size_bytes), 0) / 1073741824.0
                            FROM kafka_topic_metrics tm
                            JOIN kafka_partition_leaders pl ON pl.cluster_id=tm.cluster_id AND pl.topic=tm.topic
                            WHERE tm.cluster_id=:cid AND pl.leader_broker_id=:bid
                        """), {"cid": int(cid), "bid": broker_id})
                        data_gb = round(float(data_gb_row.scalar() or 0), 2)
                        await _sess.execute(_st("""
                            INSERT INTO kafka_broker_distribution
                            (cluster_id, broker_id, leader_partition_count, replica_partition_count, data_gb, updated_at)
                            VALUES (:cid, :bid, :lcount, :rcount, :data_gb, now())
                            ON CONFLICT (cluster_id, broker_id) DO UPDATE SET
                                leader_partition_count = EXCLUDED.leader_partition_count,
                                replica_partition_count = EXCLUDED.replica_partition_count,
                                data_gb = EXCLUDED.data_gb,
                                updated_at = now()
                        """), {"cid": int(cid), "bid": broker_id,
                               "lcount": lcount, "rcount": replica_counts.get(broker_id, 0),
                               "data_gb": data_gb})
                    await _sess.commit()
                logger.info("Broker distribution updated for %s: %s brokers, %s partition leaders", c["name"], len(leader_counts), len(partition_leaders))
            except Exception as _be:
                logger.warning("Broker distribution failed for %s: %s", c["name"], _be)
    except Exception as e:
        logger.warning("topic_structure failed for %s: %s", c["name"], e)
    collect_topic_structure._last_result = results[0] if results else "No data collected"


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


# ── Job 7: Connector Snapshots for SLO Tracking ───────────────────────────────
async def collect_connector_snapshots(cluster_id: str = ""):
    """Collect connector states and save snapshots for SLO tracking."""
    c = await _get_cluster(cluster_id)
    if not c:
        collect_connector_snapshots._last_result = f"Cluster {cluster_id} not found"
        return
    connect_url = c.get("kafka_connect_url", "")
    if not connect_url:
        collect_connector_snapshots._last_result = "No Kafka Connect URL configured"
        return
    try:
        from tools.kafka_connect import KafkaConnectCollector
        from database import SessionLocal
        from sqlalchemy import text as _ct
        import asyncio as _aio
        # Collect from all workers
        urls = [u.strip() for u in connect_url.split(",") if u.strip()]
        all_connectors = {}
        async def _collect_one(url):
            try:
                r = await KafkaConnectCollector(url).collect()
                for conn in r.get("connectors", []):
                    name = conn["name"]
                    if name not in all_connectors:
                        all_connectors[name] = conn
            except Exception:
                pass
        await _aio.gather(*[_collect_one(u) for u in urls])
        connectors = list(all_connectors.values())
        if not connectors:
            collect_connector_snapshots._last_result = "No connectors found"
            return
        # Save snapshots to postgres
        cid = _cid(c)
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        async with SessionLocal() as sess:
            values = ",".join(
                f"({int(cid)}, '{conn['name'].replace(chr(39), chr(39)*2)}', "
                f"'{conn.get('type','unknown')}', '{conn.get('state','UNKNOWN')}', "
                f"{conn.get('total_tasks',0)}, {conn.get('running_tasks',0)}, "
                f"{conn.get('failed_tasks',0)}, '{now.isoformat()}')"
                for conn in connectors
            )
            await sess.execute(_ct(f"""
                INSERT INTO kafka_connector_snapshots
                (cluster_id, connector_name, connector_type, state, total_tasks, running_tasks, failed_tasks, collected_at)
                VALUES {values}
            """))
            await sess.commit()
        collect_connector_snapshots._last_result = f"Saved {len(connectors)} connector snapshots"
    except Exception as e:
        logger.error("collect_connector_snapshots failed: %s", e)
        collect_connector_snapshots._last_result = f"Error: {e}"


# ── Job 7b: SLO Compliance Computation ────────────────────────────────────────
async def compute_slo_compliance(cluster_id: str = ""):
    """Compute hourly SLO compliance and save to kafka_slo_compliance."""
    c = await _get_cluster(cluster_id)
    if not c:
        compute_slo_compliance._last_result = f"Cluster {cluster_id} not found"
        return
    cid = _cid(c)
    try:
        from database import SessionLocal
        from sqlalchemy import text as _slo
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        hour_bucket = now.replace(minute=0, second=0, microsecond=0)
        prev_hour = hour_bucket - timedelta(hours=1)
        async with SessionLocal() as sess:
            # Get SLO targets
            tgt = await sess.execute(_slo(
                "SELECT * FROM kafka_slo_targets WHERE cluster_id=:cid LIMIT 1"
            ), {"cid": int(cid)})
            target = tgt.fetchone()
            lag_target = target.consumer_lag_target if target else 10000
            conn_target = target.connector_availability_target if target else 99.0
            urp_target = target.urp_target if target else 0
            # Connector availability % in last hour
            # SLI: RUNNING / (RUNNING + FAILED) — excludes PAUSED and UNASSIGNED
            conn_stats = await sess.execute(_slo("""
                SELECT SUM(CASE WHEN state='RUNNING' THEN 1 ELSE 0 END) as running,
                       SUM(CASE WHEN state='FAILED' THEN 1 ELSE 0 END) as failed
                FROM kafka_connector_snapshots
                WHERE cluster_id=:cid AND collected_at >= :prev AND collected_at < :now
                AND state IN ('RUNNING', 'FAILED')
            """), {"cid": int(cid), "prev": prev_hour, "now": hour_bucket})
            cs = conn_stats.fetchone()
            conn_running = cs.running or 0
            conn_failed = cs.failed or 0
            conn_total = conn_running + conn_failed
            conn_avail_pct = (conn_running / conn_total * 100) if conn_total > 0 else None
            # Consumer lag compliance % in last hour
            lag_stats = await sess.execute(_slo("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN total_lag <= :target THEN 1 ELSE 0 END) as compliant
                FROM kafka_lag_snapshots
                WHERE cluster_id=:cid AND collected_at >= :prev AND collected_at < :now
            """), {"cid": str(cid), "target": lag_target, "prev": prev_hour, "now": hour_bucket})
            ls = lag_stats.fetchone()
            lag_compliance_pct = (ls.compliant / ls.total * 100) if ls and ls.total > 0 else None
            # Broker + URP compliance % in last hour
            broker_stats = await sess.execute(_slo("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN urp_count <= :urp THEN 1 ELSE 0 END) as urp_ok,
                       AVG(CASE WHEN cpu_pct IS NOT NULL THEN 1.0 ELSE 0 END) as broker_online_ratio
                FROM kafka_broker_metrics
                WHERE cluster_id=:cid AND time >= :prev AND time < :now
            """), {"cid": int(cid), "urp": urp_target, "prev": prev_hour, "now": hour_bucket})
            bs = broker_stats.fetchone()
            # Broker availability: ratio of brokers reporting metrics (proxy for online)
            # Get expected broker count from max ever seen for this cluster
            max_brokers = await sess.execute(_slo(
                "SELECT COUNT(DISTINCT broker_id) FROM kafka_broker_metrics WHERE cluster_id=:cid"
            ), {"cid": int(cid)})
            expected_brokers = max_brokers.scalar() or 1
            actual_brokers = await sess.execute(_slo(
                "SELECT COUNT(DISTINCT broker_id) FROM kafka_broker_metrics WHERE cluster_id=:cid AND time >= :prev AND time < :now"
            ), {"cid": int(cid), "prev": prev_hour, "now": hour_bucket})
            ab = actual_brokers.scalar() or 0
            broker_avail_pct = round(ab / expected_brokers * 100, 1) if expected_brokers > 0 else None
            urp_compliance_pct = (bs.urp_ok / bs.total * 100) if bs and bs.total > 0 else None
            # Broker CPU/Heap compliance in last hour
            br_stats = await sess.execute(_slo("""
                SELECT AVG(cpu_pct) as avg_cpu, AVG(heap_pct) as avg_heap
                FROM kafka_broker_metrics
                WHERE cluster_id=:cid AND time >= :prev AND time < :now
            """), {"cid": int(cid), "prev": prev_hour, "now": hour_bucket})
            br = br_stats.fetchone()
            # Get targets
            tgt_row = await sess.execute(_slo(
                "SELECT max_broker_cpu_pct, max_broker_heap_pct, min_task_health_pct FROM kafka_slo_targets WHERE cluster_id=:cid LIMIT 1"
            ), {"cid": int(cid)})
            tgt = tgt_row.fetchone()
            cpu_target = float(tgt.max_broker_cpu_pct) if tgt and tgt.max_broker_cpu_pct else 85.0
            heap_target = float(tgt.max_broker_heap_pct) if tgt and tgt.max_broker_heap_pct else 80.0
            avg_cpu = br.avg_cpu or 0 if br else 0
            avg_heap = br.avg_heap or 0 if br else 0
            cpu_compliance_pct = 100.0 if avg_cpu <= cpu_target else max(0, round((1 - (avg_cpu - cpu_target)/cpu_target) * 100, 1))
            heap_compliance_pct = 100.0 if avg_heap <= heap_target else max(0, round((1 - (avg_heap - heap_target)/heap_target) * 100, 1))
            # Task health compliance from connector snapshots
            # Use latest snapshot only to avoid counting multiple snapshots per connector
            task_stats = await sess.execute(_slo("""
                WITH latest AS (
                    SELECT DISTINCT ON (connector_name) connector_name, failed_tasks, state
                    FROM kafka_connector_snapshots
                    WHERE cluster_id=:cid AND collected_at >= :prev AND collected_at < :now
                    AND state IN ('RUNNING', 'FAILED')
                    ORDER BY connector_name, collected_at DESC
                )
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN failed_tasks = 0 AND state = 'RUNNING' THEN 1 ELSE 0 END) as healthy
                FROM latest
            """), {"cid": int(cid), "prev": prev_hour, "now": hour_bucket})
            ts = task_stats.fetchone()
            task_health_pct = round(ts.healthy / ts.total * 100, 1) if ts and ts.total > 0 else None
            # Overall compliance — all metrics
            metrics = [m for m in [conn_avail_pct, lag_compliance_pct, urp_compliance_pct,
                                   cpu_compliance_pct, heap_compliance_pct, task_health_pct] if m is not None]
            overall = sum(metrics) / len(metrics) if metrics else None
            # Upsert compliance snapshot
            await sess.execute(_slo("""
                INSERT INTO kafka_slo_compliance
                (cluster_id, hour_bucket, connector_availability_pct, consumer_lag_compliance_pct,
                 broker_availability_pct, urp_compliance_pct, overall_compliance_pct,
                 connector_total, connector_running, connector_failed,
                 broker_cpu_compliance_pct, broker_heap_compliance_pct, task_health_compliance_pct)
                VALUES (:cid, :hb, :ca, :lc, :ba, :uc, :oa, :ct, :cr, :cf, :cc, :hc, :tc)
                ON CONFLICT (cluster_id, hour_bucket) DO UPDATE SET
                    connector_availability_pct = EXCLUDED.connector_availability_pct,
                    consumer_lag_compliance_pct = EXCLUDED.consumer_lag_compliance_pct,
                    broker_availability_pct = EXCLUDED.broker_availability_pct,
                    urp_compliance_pct = EXCLUDED.urp_compliance_pct,
                    overall_compliance_pct = EXCLUDED.overall_compliance_pct,
                    connector_total = EXCLUDED.connector_total,
                    connector_running = EXCLUDED.connector_running,
                    connector_failed = EXCLUDED.connector_failed,
                    broker_cpu_compliance_pct = EXCLUDED.broker_cpu_compliance_pct,
                    broker_heap_compliance_pct = EXCLUDED.broker_heap_compliance_pct,
                    task_health_compliance_pct = EXCLUDED.task_health_compliance_pct
            """), {"cid": int(cid), "hb": hour_bucket, "ca": conn_avail_pct, "lc": lag_compliance_pct,
                  "ba": broker_avail_pct, "uc": urp_compliance_pct, "oa": overall,
                  "ct": conn_total, "cr": conn_running, "cf": conn_failed,
                  "cc": cpu_compliance_pct, "hc": heap_compliance_pct, "tc": task_health_pct})
            await sess.commit()
        compute_slo_compliance._last_result = f"SLO computed: overall={overall:.1f}% conn={conn_avail_pct:.1f}% cpu={cpu_compliance_pct:.1f}%" if overall else "SLO computed (no data)"
    except Exception as e:
        logger.error("compute_slo_compliance failed: %s", e)
        compute_slo_compliance._last_result = f"Error: {e}"


# ── Job 8: Schema Registry Subjects ───────────────────────────────────────────

async def collect_sr_subjects(cluster_id: str = ""):
    """Collect SR subjects for restricted clusters — stores in kafka_sr_subjects table.
    Only runs for clusters where sr_restricted=True or first-time detection."""
    c = await _get_cluster(cluster_id)
    if not c:
        collect_sr_subjects._last_result = f"Cluster {cluster_id} not found"
        return
    sr_url = c.get("schema_registry_url", "")
    if not sr_url:
        collect_sr_subjects._last_result = "No SR URL configured"
        return
    cid = _cid(c)
    try:
        import httpx
        from tools.schema_registry import SchemaRegistryCollector
        sr_username = c.get("schema_registry_username")
        sr_password = c.get("schema_registry_password")
        auth = httpx.BasicAuth(sr_username, sr_password) if sr_username and sr_password else None

        # Step 1: Try /subjects — if works, cluster is not restricted, clear flag
        async with httpx.AsyncClient(timeout=10.0, auth=auth) as client:
            resp = await client.get(f"{sr_url}/subjects")
            if resp.status_code == 200:
                # Not restricted — clear sr_restricted flag and exit
                from database import SessionLocal
                from sqlalchemy import text as _t
                if SessionLocal:
                    async with SessionLocal() as sess:
                        await sess.execute(_t(
                            "UPDATE kafka_clusters SET sr_restricted=false WHERE id=:cid"
                        ), {"cid": int(cid)})
                        await sess.commit()
                collect_sr_subjects._last_result = "SR not restricted — standard collection applies"
                return

            if resp.status_code != 422:
                collect_sr_subjects._last_result = f"SR returned {resp.status_code}"
                return

        # Step 2: Restricted — get topics from postgres
        from database import SessionLocal
        from sqlalchemy import text as _t
        topics = []
        if SessionLocal:
            async with SessionLocal() as sess:
                tr = await sess.execute(_t(
                    "SELECT topic FROM kafka_topic_names WHERE cluster_id=:cid"
                ), {"cid": int(cid)})
                topics = [r.topic for r in tr.fetchall()]

        if not topics:
            collect_sr_subjects._last_result = "No topics in DB yet — run topic-structure job first"
            return

        # Step 3: Derive subjects from topic names
        collector = SchemaRegistryCollector(sr_url, username=sr_username, password=sr_password, topics=topics)
        async with httpx.AsyncClient(timeout=300.0, auth=auth) as client:
            subjects = await collector._get_subjects_from_topics(client)

        # Step 4: Upsert subjects to postgres
        if SessionLocal:
            async with SessionLocal() as sess:
                # Mark cluster as restricted
                await sess.execute(_t(
                    "UPDATE kafka_clusters SET sr_restricted=true WHERE id=:cid"
                ), {"cid": int(cid)})
                # Upsert subjects
                now = __import__('datetime').datetime.now(__import__('datetime').timezone.utc)
                for subject in subjects:
                    await sess.execute(_t("""
                        INSERT INTO kafka_sr_subjects (cluster_id, subject, collected_at)
                        VALUES (:cid, :subject, :now)
                        ON CONFLICT (cluster_id, subject) DO UPDATE SET collected_at=:now
                    """), {"cid": int(cid), "subject": subject, "now": now})
                await sess.commit()

        collect_sr_subjects._last_result = f"Collected {len(subjects)} subjects from {len(topics)} topics"
        logger.info("collect_sr_subjects: cluster %s — %d subjects stored", cid, len(subjects))

    except Exception as e:
        logger.warning("collect_sr_subjects failed for %s: %s", c.get("name"), e)
        collect_sr_subjects._last_result = f"Failed: {e}"

collect_sr_subjects._last_result = "Not yet run"


# ── Job 9: Message Rate ───────────────────────────────────────────────────────
_prev_offsets: dict = {}
_prev_offset_time: float = 0.0

async def collect_msg_rate(cluster_id: str = ""):
    """Calculate bytes/sec ingestion rate using describe_log_dirs delta between cycles.
    Upserts bytes_in_per_sec to kafka_topic_metrics in postgres."""
    global _prev_offsets, _prev_offset_time
    c = await _get_cluster(cluster_id)
    if not c:
        collect_msg_rate._last_result = f"Cluster {cluster_id} not found or disabled"
        return
    cid = _cid(c)
    try:
        from kafka import KafkaAdminClient
        security = {}
        if c.get("auth_type") not in (None, "none"):
            import ssl as _ssl
            tls = c.get("tls_enabled", False)
            security = {
                "security_protocol": "SASL_SSL" if tls else "SASL_PLAINTEXT",
                "sasl_mechanism": c.get("sasl_mechanism", "PLAIN"),
                "sasl_plain_username": c.get("sasl_username"),
                "sasl_plain_password": c.get("sasl_password"),
            }
            if tls:
                ssl_ctx = _ssl.create_default_context()
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = _ssl.CERT_NONE
                security["ssl_context"] = ssl_ctx
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
        # Update kafka_counts_metrics with top topics by msg rate
        if active_topics:
            top_by_rate = sorted(active_topics.items(), key=lambda x: -x[1])[:20]
            top_msg_rate = [{"name": t, "bytes_in_per_sec": round(r, 2)} for t, r in top_by_rate]
            try:
                from routes_settings import _upsert
                import json as _jm
                existing_raw = None
                from database import SessionLocal as _SLm
                from sqlalchemy import text as _tm
                async with _SLm() as _sm:
                    _er = await _sm.execute(_tm(
                        "SELECT value FROM agent_config WHERE agent_slug='kafka-analyser' AND key=:key"
                    ), {"key": f"kafka_counts_metrics_{cid}"})
                    _er_row = _er.fetchone()
                    if _er_row:
                        existing_raw = _er_row.value
                existing = _jm.loads(existing_raw) if existing_raw else {}
                if isinstance(existing, str):
                    existing = _jm.loads(existing)
                existing["top_topics_by_msg_rate"] = top_msg_rate
                await _upsert(f"kafka_counts_metrics_{cid}", _jm.dumps(existing))
            except Exception as _me:
                logger.warning("msg_rate counts update failed: %s", _me)
        collect_msg_rate._last_result = f"Rates updated: {len(active_topics)} active topics, {hot_count} hot (>100KB/s)"
    except Exception as e:
        logger.warning("msg_rate failed for %s: %s", c["name"], e)
        collect_msg_rate._last_result = f"Failed: {e}"


# ── Job 9: Schema Registry ────────────────────────────────────────────────────
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
            sr_username = c.get("schema_registry_username")
            sr_password = c.get("schema_registry_password")
            # Get topic names from postgres for topic-derived subject fallback
            topics = []
            try:
                from database import SessionLocal
                from sqlalchemy import text as _t
                if SessionLocal:
                    async with SessionLocal() as sess:
                        tr = await sess.execute(_t(
                            "SELECT topic FROM kafka_topic_names WHERE cluster_id=:cid LIMIT 100"
                        ), {"cid": int(cid)})
                        topics = [r.topic for r in tr.fetchall()]
            except Exception:
                pass
            collector = SchemaRegistryCollector(sr_url, username=sr_username, password=sr_password, topics=topics)
            sr_data = await collector.collect()
            data = _ks.get_cluster_data(cid) or {}
            data["schema_registry"] = sr_data
            _ks.set_cluster_data(data, source_type=c.get("source_type", "live"), cluster_id=cid)
            results.append(f"{c['name']}: {sr_data.get('subject_count', 0)} schemas")
        except Exception as e:
            logger.warning("schema_registry failed for %s: %s", c["name"], e)
    collect_schema_registry._last_result = "; ".join(results) if results else "No schema registry configured"
