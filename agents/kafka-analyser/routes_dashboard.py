from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse
from tools.real_kafka import RealKafkaCollector
from storage import get_backend
import json
import asyncio
import time

import httpx
import os

import kafka_store

from shared.llm import stream_message as _llm_stream

import logging
logger = logging.getLogger(__name__)

_lag_trend_cache: dict = {}
_LAG_TREND_CACHE_TTL_SECS = 180  # 3 minutes — matches consumer-lag's collection cadence


def _get_lag_trend_cached(key: str):
    entry = _lag_trend_cache.get(key)
    if entry is None:
        return None
    data, cached_at = entry
    if (time.time() - cached_at) > _LAG_TREND_CACHE_TTL_SECS:
        del _lag_trend_cache[key]
        return None
    return data


def _set_lag_trend_cached(key: str, data: dict):
    _lag_trend_cache[key] = (data, time.time())


router = APIRouter(tags=["dashboard"])


async def _collector_for_cluster(cluster_id: str) -> RealKafkaCollector:
    """Build a RealKafkaCollector from a cluster_id in DB."""
    cluster = await get_backend().get_cluster(int(cluster_id))
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    return RealKafkaCollector({
        "bootstrap_servers": cluster["bootstrap_servers"],
        "auth_type": "none" if cluster["auth_type"] == "none" else "sasl",
        "sasl_username": cluster.get("sasl_username"),
        "sasl_password": cluster.get("sasl_password"),
        "sasl_mechanism": cluster.get("sasl_mechanism", "PLAIN"),
        "tls_enabled": cluster.get("tls_enabled", False),
        "cluster_label": cluster["name"],
        "jmx_port": cluster.get("jmx_port"),
    })


@router.get("/dashboard/overview")
async def get_overview(cluster_id: str | None = None, hours: int | None = None) -> dict:
    """Cluster health, broker status, anomaly summary."""
    # Read all data from postgres — no kafka_store dependency
    if not cluster_id:
        # Try to get first enabled cluster
        try:
            from storage import get_backend as _gb2
            _cls = await _gb2().get_clusters("kafka-analyser")
            _en = [c for c in _cls if c.get("enabled")]
            cluster_id = str(_en[0]["id"]) if _en else None
        except Exception:
            pass
    if not cluster_id:
        return {"empty": True}
    data = kafka_store.get_cluster_data(cluster_id, hours=hours) or {}
    # Topic count from postgres
    topic_count_pg = 0
    try:
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text as _ovt
        if SessionLocal:
            async with SessionLocal() as _ovs:
                _ovr = await _ovs.execute(_ovt(
                    "SELECT COUNT(*) FROM kafka_topic_metrics WHERE cluster_id=:cid"
                ), {"cid": int(cluster_id)})
                topic_count_pg = _ovr.scalar() or 0
    except Exception as _tce:
        logger.warning("topic count query failed: %s", _tce)
    topics = data.get("topics", [])
    # Consumer groups from postgres
    consumer_groups = []
    try:
        from database import DashboardSessionLocal as _SL
        from sqlalchemy import text as _cgt2
        if _SL:
            async with _SL() as _cgs:
                _cgr = await _cgs.execute(_cgt2(
                    "SELECT group_id, total_lag FROM kafka_consumer_group_lag "
                    "WHERE cluster_id=:cid AND updated_at >= NOW() - INTERVAL '20 minutes' ORDER BY total_lag DESC"
                ), {"cid": int(cluster_id)})
                consumer_groups = [{"group_id": r.group_id, "total_lag": r.total_lag} for r in _cgr.fetchall()]
    except Exception:
        consumer_groups = data.get("consumer_groups", [])
    # Read brokers from postgres (authoritative source)
    brokers = data.get("brokers", [])
    if cluster_id:
        try:
            from database import DashboardSessionLocal as SessionLocal
            from sqlalchemy import text as _bt
            if SessionLocal:
                async with SessionLocal() as _bs:
                    _br = await _bs.execute(_bt(
                        "SELECT broker_id, heap_pct, cpu_pct, gc_pause_ms, "
                        "request_handler_idle_pct, urp_count, messages_in_per_sec, disk_pct, "
                        "time, data_gb_true "
                        "FROM kafka_broker_metrics WHERE cluster_id=:cid AND time=(SELECT MAX(time) FROM kafka_broker_metrics bm2 WHERE bm2.cluster_id=:cid AND bm2.broker_id = kafka_broker_metrics.broker_id) ORDER BY broker_id"
                    ), {"cid": int(cluster_id)})
                    _rows = _br.fetchall()
                    if _rows:
                        from datetime import datetime as _dt_ov, timezone as _tz_ov, timedelta as _td_ov
                        _now_ov = _dt_ov.now(_tz_ov.utc)
                        def _broker_status_ov(r):
                            _row_time = r.time
                            if _row_time is not None and _row_time.tzinfo is None:
                                _row_time = _row_time.replace(tzinfo=_tz_ov.utc)
                            _stale = _row_time is None or (_now_ov - _row_time) > _td_ov(minutes=6)
                            _unreachable = r.data_gb_true is None or _stale
                            if _unreachable:
                                return "unreachable", False
                            if r.urp_count and r.urp_count > 0:
                                return "degraded", True
                            return "healthy", True
                        brokers = []
                        for r in _rows:
                            _status_ov, _reachable_ov = _broker_status_ov(r)
                            brokers.append({"broker_id": r.broker_id, "id": r.broker_id,
                                    "heap_pct": r.heap_pct, "cpu_pct": r.cpu_pct,
                                    "gc_pause_ms": r.gc_pause_ms, "urp_count": r.urp_count,
                                    "request_handler_idle_pct": r.request_handler_idle_pct,
                                    "messages_in_per_sec": r.messages_in_per_sec,
                                    "status": _status_ov,
                                    "reachable": _reachable_ov,
                                    "cpu_cores_configured": True})
        except Exception:
            pass
    # Compute health score from real metrics
    score = 100
    # URP and RF=1 deductions -- sourced directly from kafka_topic_metrics (the
    # accurate, database-backed source also used by /dashboard/counts), not the
    # in-memory kafka_store cache, which is reset on every restart and can go stale.
    total_urp = 0
    rf1 = 0
    if cluster_id:
        try:
            from database import DashboardSessionLocal as _hsSL
            from sqlalchemy import text as _hst
            if _hsSL:
                async with _hsSL() as _hss:
                    _hsr = await _hss.execute(_hst(
                        "SELECT COALESCE(SUM(urp_count), 0) as urp, "
                        "COUNT(*) FILTER (WHERE replication_factor = 1 AND partition_count > 0) as rf1 "
                        "FROM kafka_topic_metrics WHERE cluster_id=:cid"
                    ), {"cid": int(cluster_id)})
                    _hsrow = _hsr.fetchone()
                    total_urp = int(_hsrow.urp or 0) if _hsrow else 0
                    rf1 = int(_hsrow.rf1 or 0) if _hsrow else 0
        except Exception as _hse:
            logger.warning("health_score URP/RF1 query failed: %s", _hse)
    score -= total_urp * 5
    # High heap deduction
    for b in brokers:
        heap = b.get("heap_pct", 0)
        if heap >= 85:
            score -= 15
        elif heap >= 70:
            score -= 5
    # Critical consumer groups deduction
    critical_groups = [g for g in consumer_groups if g.get("total_lag", 0) > 10000]
    score -= len(critical_groups) * 2
    # RF=1 topics deduction
    if rf1 > 100:
        score -= 10
    elif rf1 > 0:
        score -= 5
    health_score = max(0, min(100, score))
    cluster = {**data.get("cluster", {}), "health_score": health_score}
    # Determine status
    if health_score >= 80:
        cluster["status"] = "healthy"
    elif health_score >= 50:
        cluster["status"] = "degraded"
    else:
        cluster["status"] = "critical"
    return {
        "cluster": {
            **cluster,
            "topic_count": topic_count_pg or len(topics) or data.get("counts", {}).get("total_topics", 0),
            "consumer_group_count": len(consumer_groups),
            "critical_count": len(critical_groups),
        },
        "brokers": brokers,
        "anomalies": data.get("anomalies", []),
        "topic_count": topic_count_pg or len(topics) or data.get("counts", {}).get("total_topics", 0),
        "consumer_group_count": len(consumer_groups),
        "health_score": health_score,
        "critical_count": len(critical_groups),
    }


@router.get("/dashboard/counts")
async def get_counts(cluster_id: str | None = None) -> dict:
    """Cluster counts — reads from DB directly, not cache."""
    if not cluster_id:
        return {"empty": True}
    total_groups_count = 0
    total_rf1_count = 0
    total_partitions_count = 0
    total_urp_count = 0
    large_topics_count = 0
    total_hot_count = 0
    total_stale_count = 0
    try:
        from storage import get_backend
        import json as _json
        all_cfg = await get_backend().get_all()

        # Structure counts live in kafka_metrics_history (scan_type='topics_structure')
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text as _text
        structure = {}
        if SessionLocal:
            async with SessionLocal() as _sess:
                _row = await _sess.execute(
                    _text("""SELECT data_json FROM kafka_metrics_history
                             WHERE cluster_id = :cid AND scan_type = 'topics_structure'
                             ORDER BY collected_at DESC LIMIT 1"""),
                    {"cid": cluster_id}
                )
                _r = _row.fetchone()
                if _r:
                    _d = _json.loads(_r.data_json)
                    structure = _d.get("counts", _d) if isinstance(_d, dict) else {}

        metrics_raw = all_cfg.get(f"kafka_counts_metrics_{cluster_id}")
        metrics_str = _json.loads(metrics_raw) if metrics_raw else {}
        metrics = _json.loads(metrics_str) if isinstance(metrics_str, str) else metrics_str

        # Read broker count from kafka_metrics_history (same source as get_brokers)
        brokers = []
        if SessionLocal:
            async with SessionLocal() as _sess2:
                _br = await _sess2.execute(
                    _text("""SELECT data_json FROM kafka_metrics_history
                             WHERE cluster_id = :cid AND scan_type = 'brokers'
                             ORDER BY collected_at DESC LIMIT 1"""),
                    {"cid": cluster_id}
                )
                _br_row = _br.fetchone()
                if _br_row:
                    brokers = _json.loads(_br_row.data_json) or []

        # Read top topics by size directly from kafka_topic_metrics
        top_topics_by_size = []
        total_topics_count = 0
        if SessionLocal:
            async with SessionLocal() as _sess3:
                _tr = await _sess3.execute(_text("""
                    SELECT topic, size_bytes
                    FROM kafka_topic_metrics
                    WHERE cluster_id = :cid
                    ORDER BY size_bytes DESC LIMIT 100
                """), {"cid": int(cluster_id)})
                _topic_rows = _tr.fetchall()
                top_topics_by_size = [
                    {"name": r.topic, "size_bytes": r.size_bytes,
                     "size_mb": round(r.size_bytes / 1024 / 1024, 1)}
                    for r in _topic_rows
                ]
                _cnt = await _sess3.execute(_text(
                    "SELECT COUNT(*) FROM kafka_topic_metrics WHERE cluster_id = :cid"
                ), {"cid": int(cluster_id)})
                total_topics_count = _cnt.scalar() or 0
                _gcnt = await _sess3.execute(_text(
                    "SELECT COUNT(*) FROM kafka_consumer_group_lag WHERE cluster_id = :cid "
                    "AND updated_at >= NOW() - INTERVAL '20 minutes'"
                ), {"cid": int(cluster_id)})
                total_groups_count = _gcnt.scalar() or 0
                _rf1 = await _sess3.execute(_text(
                    "SELECT COUNT(*) FROM kafka_topic_metrics WHERE cluster_id=:cid AND replication_factor=1 AND partition_count>0"
                ), {"cid": int(cluster_id)})
                total_rf1_count = _rf1.scalar() or 0
                _parts = await _sess3.execute(_text(
                    "SELECT COALESCE(SUM(partition_count),0) FROM kafka_topic_metrics WHERE cluster_id=:cid"
                ), {"cid": int(cluster_id)})
                total_partitions_count = _parts.scalar() or 0
                _urp = await _sess3.execute(_text(
                    "SELECT COALESCE(SUM(urp_count),0) FROM kafka_topic_metrics WHERE cluster_id=:cid"
                ), {"cid": int(cluster_id)})
                total_urp_count = _urp.scalar() or 0
                _large = await _sess3.execute(_text(
                    "SELECT COUNT(*) FROM kafka_topic_metrics WHERE cluster_id=:cid AND size_bytes > 10737418240"
                ), {"cid": int(cluster_id)})
                large_topics_count = _large.scalar() or 0
                _hot = await _sess3.execute(_text(
                    "SELECT COUNT(*) FROM kafka_topic_metrics WHERE cluster_id=:cid AND bytes_in_per_sec > 102400"
                ), {"cid": int(cluster_id)})
                total_hot_count = _hot.scalar() or 0
                _stale = await _sess3.execute(_text(
                    "SELECT COUNT(*) FROM kafka_topic_metrics WHERE cluster_id=:cid AND bytes_in_per_sec = 0 AND size_bytes > 0"
                ), {"cid": int(cluster_id)})
                total_stale_count = _stale.scalar() or 0
                _conn = await _sess3.execute(_text("""
                    SELECT COUNT(DISTINCT connector_name)
                    FROM kafka_connector_snapshots
                    WHERE cluster_id=:cid AND collected_at >= NOW() - INTERVAL '15 minutes'
                """), {"cid": int(cluster_id)})
                total_connectors_count = _conn.scalar() or 0
        return {
            "total_topics": total_topics_count or structure.get("total_topics", 0),
            "total_groups": total_groups_count,
            "total_brokers": structure.get("total_brokers", len(brokers)),
            "total_connectors": total_connectors_count,
            "total_rf1": total_rf1_count,
            "total_urp": total_urp_count,
            "total_partitions": total_partitions_count,
            "top_topics_by_size": top_topics_by_size,
            "top_topics_by_msg_rate": metrics.get("top_topics_by_msg_rate", []),
            "total_hot": total_hot_count,
            "large_topics_count": large_topics_count,
            "total_stale": total_stale_count,
        }
    except Exception as _e:
        return {"empty": True, "error": str(_e)}


@router.get("/dashboard/topics/detail")
async def get_topic_detail(name: str, cluster_id: str | None = None) -> dict:
    """Topic detail — reads entirely from postgres (no live Kafka call)."""
    if not cluster_id:
        return {"error": "cluster_id required"}
    try:
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text as _t
        if SessionLocal is None:
            return {"error": "Database not available"}
        async with SessionLocal() as sess:
            row = await sess.execute(_t("""
                SELECT topic, size_bytes, partition_count, replication_factor,
                       bytes_in_per_sec, messages_in_per_sec, total_messages, last_seen,
                       urp_count
                FROM kafka_topic_metrics
                WHERE cluster_id=:cid AND topic=:topic
                LIMIT 1
            """), {"cid": int(cluster_id), "topic": name})
            r = row.fetchone()
        if not r:
            return {"error": "Topic not found"}
        return {
            "name": r.topic,
            "partition_count": r.partition_count or 0,
            "replication_factor": r.replication_factor or 0,
            "under_replicated_partitions": r.urp_count or 0,
            "bytes_in_per_sec": r.bytes_in_per_sec or 0.0,
            "messages_in_per_sec": r.messages_in_per_sec or 0.0,
            "size_bytes": r.size_bytes or 0,
            "total_messages": r.total_messages or 0,
            "last_seen": r.last_seen.isoformat() if r.last_seen else None,
            "partitions": [],
        }
    except Exception as exc:
        return {"error": str(exc)}


@router.get("/dashboard/consumer-groups")
async def get_consumer_groups(cluster_id: str | None = None, hours: int | None = None) -> dict:
    """Consumer group lag leaderboard sorted worst-first — reads from postgres."""
    if not cluster_id:
        return {"empty": True}
    try:
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text as _t
        if SessionLocal is None:
            return {"empty": True}
        async with SessionLocal() as sess:
            rows = await sess.execute(_t("""
                SELECT group_id, state, total_lag, topic_count, committed_offsets, updated_at
                FROM kafka_consumer_group_lag
                WHERE cluster_id = :cid AND updated_at >= NOW() - INTERVAL '20 minutes'
                ORDER BY total_lag DESC
            """), {"cid": int(cluster_id)})
            _rows = rows.fetchall()
            # Trend/rate -- aggregate the already-tracked per-partition inflow/consumed
            # deltas (from the same cycle that also powers the topic-lag popup) to
            # compute a genuine net lag change per group, rather than leaving
            # lag_trend/lag_rate_per_min entirely unset (a real, previously-silent gap:
            # every group fell through to "stable" with an undefined rate).
            _trend_by_group: dict[str, dict] = {}
            try:
                _tr = await sess.execute(_t("""
                    SELECT group_id,
                           COALESCE(SUM(inflow_since_last), 0) as total_inflow,
                           COALESCE(SUM(consumed_since_last), 0) as total_consumed,
                           AVG(interval_seconds) as avg_interval
                    FROM kafka_consumer_group_partition_lag
                    WHERE cluster_id = :cid AND updated_at >= NOW() - INTERVAL '20 minutes'
                    AND inflow_since_last IS NOT NULL AND consumed_since_last IS NOT NULL
                    GROUP BY group_id
                """), {"cid": int(cluster_id)})
                for tr in _tr.fetchall():
                    net_change = int(tr.total_inflow) - int(tr.total_consumed)
                    interval = float(tr.avg_interval) if tr.avg_interval else 0.0
                    rate_per_min = round(net_change / (interval / 60), 1) if interval > 0 else 0.0
                    if net_change > 1000:
                        trend = "growing"
                    elif net_change < -1000:
                        trend = "shrinking"
                    else:
                        trend = "stable"
                    _trend_by_group[tr.group_id] = {"lag_trend": trend, "lag_rate_per_min": rate_per_min}
            except Exception as _tre:
                logger.warning("consumer-groups trend aggregation failed: %s", _tre)
            groups = [
                {
                    "group_id": r.group_id,
                    "state": r.state,
                    "total_lag": r.total_lag,
                    "topic_count": r.topic_count,
                    "committed_offsets": r.committed_offsets,
                    "updated_at": r.updated_at.isoformat() if r.updated_at else None,
                    "lag_trend": _trend_by_group.get(r.group_id, {}).get("lag_trend", "stable"),
                    "lag_rate_per_min": _trend_by_group.get(r.group_id, {}).get("lag_rate_per_min", 0.0),
                }
                for r in _rows
            ]
        if not groups:
            return {"empty": True}
        return {"consumer_groups": groups, "total": len(groups)}
    except Exception as e:
        logger.error("get_consumer_groups failed: %s", e)
        return {"empty": True}


@router.get("/dashboard/consumer-groups/{group_id}/topics")
async def get_consumer_group_topics(group_id: str, cluster_id: str | None = None) -> dict:
    """Per-topic lag breakdown for a consumer group — reads entirely from postgres."""
    if not cluster_id:
        return {"error": "cluster_id required"}
    try:
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text as _t
        if SessionLocal is None:
            return {"error": "Database not available"}
        async with SessionLocal() as sess:
            rows = await sess.execute(_t("""
                SELECT topic, partition_count, lag, updated_at
                FROM kafka_consumer_group_topic_lag
                WHERE cluster_id = :cid AND group_id = :gid AND updated_at >= NOW() - INTERVAL '20 minutes'
                ORDER BY lag DESC
            """), {"cid": int(cluster_id), "gid": group_id})
            results = rows.fetchall()
            partition_rows = await sess.execute(_t("""
                SELECT topic, partition, lag, inflow_since_last, consumed_since_last, interval_seconds
                FROM kafka_consumer_group_partition_lag
                WHERE cluster_id = :cid AND group_id = :gid AND updated_at >= NOW() - INTERVAL '20 minutes'
                ORDER BY topic, lag DESC
            """), {"cid": int(cluster_id), "gid": group_id})
            partition_results = partition_rows.fetchall()
        partitions_by_topic: dict[str, list[dict]] = {}
        for pr in partition_results:
            partitions_by_topic.setdefault(pr.topic, []).append({
                "partition": pr.partition, "lag": pr.lag,
                "inflow_since_last": pr.inflow_since_last,
                "consumed_since_last": pr.consumed_since_last,
                "interval_seconds": pr.interval_seconds,
            })
        if not results:
            return {"group_id": group_id, "topics": [], "total_lag": 0}
        topics = [
            {
                "topic": r.topic,
                "partition_count": r.partition_count,
                "lag": r.lag,
                "partitions": partitions_by_topic.get(r.topic, []),
            }
            for r in results
        ]
        return {
            "group_id": group_id,
            "topics": topics,
            "total_lag": sum(t["lag"] for t in topics),
            "updated_at": results[0].updated_at.isoformat() if results[0].updated_at else None,
        }
    except Exception as exc:
        return {"error": str(exc)}


@router.get("/dashboard/topics")
async def get_topics(cluster_id: str | None = None, hours: int | None = None,
                     limit: int = 50, offset: int = 0, search: str = "") -> dict:
    """Topic metrics from postgres — sorted by size descending, with pagination and search."""
    from database import DashboardSessionLocal as SessionLocal
    from sqlalchemy import text
    import logging
    logger = logging.getLogger(__name__)
    if SessionLocal is None:
        return {"empty": True}
    if not cluster_id:
        return {"topics": [], "total": 0, "limit": limit, "offset": offset}
    cid = cluster_id
    try:
        async with SessionLocal() as sess:
            # Total count
            count_result = await sess.execute(text(
                "SELECT COUNT(*) FROM kafka_topic_metrics WHERE cluster_id = :cid" +
                (" AND topic ILIKE :search" if search else "")
            ), {"cid": int(cid), "search": f"%{search}%"} if search else {"cid": int(cid)})
            total = count_result.scalar()
            # Paginated topics
            query = """
                SELECT topic, size_bytes, partition_count, replication_factor,
                       messages_in_per_sec, bytes_in_per_sec, bytes_out_per_sec,
                       total_messages, last_seen
                FROM kafka_topic_metrics
                WHERE cluster_id = :cid
            """
            params = {"cid": int(cid), "limit": limit, "offset": offset}
            if search:
                query += " AND topic ILIKE :search"
                params["search"] = f"%{search}%"
            query += " ORDER BY size_bytes DESC LIMIT :limit OFFSET :offset"
            result = await sess.execute(text(query), params)
            rows = result.fetchall()
        topics = []
        for r in rows:
            size = r.size_bytes or 0
            status = "retention-critical" if size > 50*1024**3 else "retention-warning" if size > 10*1024**3 else "healthy"
            topics.append({
                "name": r.topic,
                "topic": r.topic,
                "size_bytes": size,
                "size_mb": round(size / 1024 / 1024, 1),
                "partition_count": r.partition_count or 0,
                "replication_factor": r.replication_factor or 0,
                "messages_in_per_sec": r.messages_in_per_sec or 0.0,
                "bytes_in_per_sec": r.bytes_in_per_sec or 0.0,
                "bytes_out_per_sec": r.bytes_out_per_sec or 0.0,
                "total_messages": r.total_messages or 0,
                "under_replicated": 0,
                "status": status,
                "last_seen": r.last_seen.isoformat() if r.last_seen else None,
            })
        return {"topics": topics, "total": total, "limit": limit, "offset": offset}
    except Exception as e:
        logger.error("get_topics failed: %s", e)
        return {"empty": True}


@router.get("/dashboard/brokers")
async def get_brokers(cluster_id: str | None = None, hours: int | None = None) -> dict:
    """Per-broker CPU, heap, GC, and URP metrics — reads from kafka_broker_metrics."""
    if not cluster_id:
        return {"empty": True}
    try:
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text as _text
        if SessionLocal is None:
            return {"empty": True}
        async with SessionLocal() as _sess:
            rows = await _sess.execute(
                _text("""SELECT broker_id, heap_pct, cpu_pct, gc_pause_ms,
                                request_handler_idle_pct, urp_count, messages_in_per_sec,
                                disk_pct, bytes_in_per_sec, bytes_out_per_sec,
                                produce_latency_ms, fetch_latency_ms,
                                isr_shrinks_per_sec, isr_expands_per_sec, time, data_gb_true, bytes_in_per_sec_true
                         FROM kafka_broker_metrics
                         WHERE cluster_id = :cid
                         AND time = (SELECT MAX(time) FROM kafka_broker_metrics bm2 WHERE bm2.cluster_id = :cid AND bm2.broker_id = kafka_broker_metrics.broker_id)
                         ORDER BY broker_id"""),
                {"cid": int(cluster_id)}
            )
            # Fetch broker rows first before executing next query
            _broker_rows = rows.fetchall()
            # Aggregate bytes_in per broker from partition leaders
            bytes_by_broker = {}
            data_gb_by_broker = {}
            try:
                _br2 = await _sess.execute(_text("""
                    SELECT pl.leader_broker_id,
                           COALESCE(SUM(tm.bytes_in_per_sec), 0) as bytes_in,
                           COALESCE(SUM(tm.size_bytes), 0) / 1073741824.0 as data_gb
                    FROM kafka_partition_leaders pl
                    JOIN kafka_topic_metrics tm ON tm.cluster_id=pl.cluster_id AND tm.topic=pl.topic
                    WHERE pl.cluster_id=:cid
                    GROUP BY pl.leader_broker_id
                """), {"cid": int(cluster_id)})
                for _r2 in _br2.fetchall():
                    bytes_by_broker[str(_r2.leader_broker_id)] = round(float(_r2.bytes_in or 0), 2)
                    data_gb_by_broker[str(_r2.leader_broker_id)] = round(float(_r2.data_gb or 0), 2)
            except Exception as _bex:
                pass
            brokers = []
            for r in _broker_rows:
                bid = str(r.broker_id)
                from datetime import datetime, timezone, timedelta as _td_reach
                _now_reach = datetime.now(timezone.utc)
                _row_time = r.time
                if _row_time is not None and _row_time.tzinfo is None:
                    _row_time = _row_time.replace(tzinfo=timezone.utc)
                _is_stale = _row_time is None or (_now_reach - _row_time) > _td_reach(minutes=6)
                _is_unreachable = r.data_gb_true is None or _is_stale
                if _is_unreachable:
                    _status = "unreachable"
                elif r.urp_count and r.urp_count > 0:
                    _status = "degraded"
                else:
                    _status = "healthy"
                brokers.append({
                    "broker_id": r.broker_id,
                    "id": r.broker_id,
                    "heap_pct": r.heap_pct,
                    "cpu_pct": r.cpu_pct,
                    "gc_pause_ms": r.gc_pause_ms,
                    "bytes_in_per_sec": r.bytes_in_per_sec_true if r.bytes_in_per_sec_true is not None else bytes_by_broker.get(bid, 0.0),
                    "data_gb": r.data_gb_true if r.data_gb_true is not None else data_gb_by_broker.get(bid, 0.0),
                    "request_handler_idle_pct": r.request_handler_idle_pct,
                    "urp_count": r.urp_count,
                    "messages_in_per_sec": r.messages_in_per_sec,
                    "disk_pct": r.disk_pct,
                    "status": _status,
                    "reachable": not _is_unreachable,
                    "cpu_cores_configured": True,
                    "bytes_out_per_sec": r.bytes_out_per_sec or 0.0,
                    "isr_shrinks_per_sec": r.isr_shrinks_per_sec or 0.0,
                    "isr_expands_per_sec": r.isr_expands_per_sec or 0.0,
                    "produce_latency_ms": r.produce_latency_ms or 0.0,
                    "fetch_latency_ms": r.fetch_latency_ms or 0.0,
                })
        if not brokers:
            return {"empty": True}
        return {"brokers": brokers}
    except Exception as _e:
        return {"empty": True, "error": str(_e)}


@router.get("/dashboard/broker-distribution")
async def get_broker_distribution(cluster_id: str | None = None) -> dict:
    """Broker leader/replica partition distribution and data volume."""
    if not cluster_id:
        return {"empty": True}
    try:
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text as _text
        if SessionLocal is None:
            return {"empty": True}
        async with SessionLocal() as sess:
            rows = await sess.execute(_text("""
                SELECT bd.broker_id, bd.leader_partition_count, bd.replica_partition_count,
                       bm.cpu_pct, bm.heap_pct
                FROM kafka_broker_distribution bd
                LEFT JOIN kafka_broker_metrics bm
                    ON bm.cluster_id = bd.cluster_id AND bm.broker_id = bd.broker_id
                WHERE bd.cluster_id = :cid
                ORDER BY bd.broker_id
            """), {"cid": int(cluster_id)})
            brokers = [
                {
                    "broker_id": r.broker_id,
                    "leader_partition_count": r.leader_partition_count,
                    "replica_partition_count": r.replica_partition_count,
                    "cpu_pct": r.cpu_pct or 0.0,
                    "heap_pct": r.heap_pct or 0.0,
                }
                for r in rows.fetchall()
            ]
        if not brokers:
            return {"empty": True}
        return {"brokers": brokers}
    except Exception as e:
        return {"empty": True, "error": str(e)}


@router.get("/dashboard/connectors")
async def get_connectors(cluster_id: str | None = None, hours: int | None = None) -> dict:
    """Connector state — reads from live Kafka Connect REST API."""
    if not cluster_id:
        return {"connectors": []}
    try:
        kc = await get_kafka_connect(cluster_id=cluster_id)
        return {"connectors": kc.get("connectors", [])}
    except Exception:
        return {"connectors": []}


@router.get("/dashboard/insights")
async def get_insights(cluster_id: str | None = None, hours: int | None = None) -> dict:
    """Active anomalies with severity, root cause, and recommendations."""
    data = kafka_store.get_cluster_data(cluster_id, hours=hours)
    if data is None:
        return {"empty": True}
    return {"anomalies": data["anomalies"]}


@router.get("/dashboard/schema-registry")
async def get_schema_registry(cluster_id: str | None = None) -> dict:
    """Fetch Schema Registry subjects, versions and compatibility."""
    from storage import get_backend

    # Get cluster's schema registry URL
    sr_url = None
    if cluster_id:
        try:
            cluster = await get_backend().get_cluster(int(cluster_id))
            if cluster:
                sr_url = cluster.get("schema_registry_url", "")
        except Exception:
            pass

    if not sr_url:
        return {
            "status": "not_configured",
            "message": "No Schema Registry URL configured for this cluster. Edit the cluster in Settings to add one.",
            "subjects": [],
            "subject_count": 0,
        }

    from tools.schema_registry import SchemaRegistryCollector
    sr_username = cluster.get("schema_registry_username")
    sr_password = cluster.get("schema_registry_password")
    # Get topics for restricted SR fallback
    _sr_topics = []
    try:
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text as _t2
        if SessionLocal:
            async with SessionLocal() as _sess:
                _tr = await _sess.execute(_t2(
                    "SELECT topic FROM kafka_topic_names WHERE cluster_id=:cid LIMIT 100"
                ), {"cid": int(cluster_id)})
                _sr_topics = [r.topic for r in _tr.fetchall()]
    except Exception:
        pass
    collector = SchemaRegistryCollector(
        sr_url,
        username=sr_username,
        password=sr_password,
        topics=_sr_topics,
        sr_restricted=cluster.get("sr_restricted"),
        cluster_id=int(cluster_id) if cluster_id else None,
    )
    return await collector.collect()


@router.get("/dashboard/zookeeper")
async def get_zookeeper(cluster_id: str | None = None) -> dict:
    """Fetch ZooKeeper stats or detect KRaft mode."""
    from storage import get_backend

    zk_url = None
    if cluster_id:
        try:
            cluster = await get_backend().get_cluster(int(cluster_id))
            if cluster:
                zk_url = cluster.get("zookeeper_url", "")
        except Exception:
            pass

    if not zk_url:
        return {
            "mode": "kraft",
            "status": "not_configured",
            "message": "No ZooKeeper URL configured. This cluster may be running in KRaft mode (no ZooKeeper needed), or add a ZooKeeper URL in cluster Settings.",
        }

    from tools.zookeeper import ZooKeeperCollector
    zk_nodes = [u.strip() for u in zk_url.split(",") if u.strip()]
    last_result = None
    for zk_node in zk_nodes:
        collector = ZooKeeperCollector(zk_node)
        result = await collector.collect()
        if result.get("status") != "unreachable":
            return result
        last_result = result
    return last_result or {"status": "unreachable", "url": zk_url}


@router.get("/dashboard/kafka-connect")
async def get_kafka_connect(cluster_id: str | None = None) -> dict:
    """Fetch Kafka Connect cluster status and connector details."""
    from storage import get_backend

    connect_url = None
    if cluster_id:
        try:
            cluster = await get_backend().get_cluster(int(cluster_id))
            if cluster:
                connect_url = cluster.get("kafka_connect_url", "")
        except Exception:
            pass

    if not connect_url:
        if cluster_id:
            return {"status": "not_configured", "message": "No Kafka Connect URL configured for this cluster.", "connector_count": 0, "connectors": []}
        try:
            all_clusters = await get_backend().get_clusters("kafka-analyser")
            enabled = [c for c in all_clusters if c.get("enabled") and c.get("kafka_connect_url")]
            if enabled:
                connect_url = enabled[0].get("kafka_connect_url", "")
            else:
                return {"status": "not_configured", "message": "No Kafka Connect URL configured.", "connector_count": 0, "connectors": []}
        except Exception:
            return {"status": "not_configured", "connector_count": 0, "connectors": []}

    from tools.kafka_connect import KafkaConnectCollector
    import asyncio as _asyncio, socket as _socket
    urls = [u.strip() for u in connect_url.split(",") if u.strip()]

    async def _try_worker(url):
        hostname = url.split("//")[-1].split(":")[0]
        try:
            result = await KafkaConnectCollector(url).collect()
            for c in result.get("connectors", []):
                c["cluster"] = hostname
            result["cluster"] = hostname
            result["url"] = url
            if result.get("connector_count", 0) == 0:
                result["status"] = "empty"
            return result
        except Exception as e:
            return {"status": "unreachable", "url": url, "cluster": hostname,
                    "connector_count": 0, "connectors": [], "error": str(e)}

    worker_results = await _asyncio.gather(*[_try_worker(u) for u in urls])

    # Build IP→hostname map for worker_id resolution
    ip_to_host = {}
    for wr in worker_results:
        try:
            ip = _socket.gethostbyname(wr.get("cluster", ""))
            port = wr["url"].split(":")[-1].rstrip("/")
            ip_to_host[f"{ip}:{port}"] = wr["cluster"]
        except Exception:
            pass

    reachable = [r for r in worker_results if r.get("status") not in ("unreachable", "empty", "error") and r.get("connector_count", 0) > 0]
    unreachable = [r for r in worker_results if r.get("status") == "unreachable"]

    # Assign each worker a cluster_group label based on its connector-set fingerprint --
    # workers sharing the same fingerprint genuinely belong to the same distributed
    # Kafka Connect cluster (confirmed: several configured worker URLs are actually
    # separate clusters, not multiple nodes of one). Labeled A, B, C... in order of
    # first appearance for a stable, human-readable grouping in the UI.
    fp_to_label: dict[int, str] = {}
    _group_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    for wr in worker_results:
        fp = hash(frozenset(c.get("name","") for c in wr.get("connectors", [])))
        wr["_fp"] = fp
        if fp not in fp_to_label:
            _idx = len(fp_to_label)
            fp_to_label[fp] = f"Cluster {_group_letters[_idx]}" if _idx < len(_group_letters) else f"Cluster {_idx+1}"
        wr["cluster_group"] = fp_to_label[fp]

    # Deduplicate workers by connector fingerprint
    seen_fps = set()
    unique_clusters = []
    for wr in reachable:
        fp = wr.get("_fp", hash(frozenset(c.get("name","") for c in wr.get("connectors", []))))
        if fp not in seen_fps:
            seen_fps.add(fp)
            unique_clusters.append(wr)

    # Deduplicate connectors by name
    seen_names = {}
    all_connectors = []
    for wr in unique_clusters:
        for c in wr.get("connectors", []):
            if c.get("name","") not in seen_names:
                seen_names[c["name"]] = True
                wid = c.get("worker_id", "")
                if wid in ip_to_host:
                    c["worker_id"] = ip_to_host[wid]
                all_connectors.append(c)

    running = sum(1 for c in all_connectors if c.get("state") == "RUNNING")
    failed  = sum(1 for c in all_connectors if c.get("state") == "FAILED")
    paused  = sum(1 for c in all_connectors if c.get("state") == "PAUSED")

    # Attach lag data for sink connectors with dedicated consumer groups
    try:
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text as _cct
        if all_connectors and cluster_id:
            async with SessionLocal() as _csess:
                connector_names = [c.get("name", "") for c in all_connectors if c.get("name")]
                if connector_names:
                    # Use each connector's real discovered group ID (from its config's
                    # consumer.override.group.id) when present, falling back to the connect-{name}
                    # default pattern only when no override is configured.
                    group_ids = [
                        c.get("group_id_override") or f"connect-{c.get('name', '')}"
                        for c in all_connectors if c.get("name")
                    ]
                    _lag_rows = await _csess.execute(_cct("""
                        SELECT group_id, total_lag, topic_count FROM kafka_consumer_group_lag
                        WHERE cluster_id = :cid AND group_id = ANY(:gids) AND updated_at >= NOW() - INTERVAL '20 minutes'
                    """), {"cid": int(cluster_id), "gids": group_ids})
                    lag_by_group = {r.group_id: {"lag": r.total_lag, "topic_count": r.topic_count} for r in _lag_rows.fetchall()}
                    # PAUSED connectors' consumer groups genuinely stop updating (nothing
                    # actively consuming), so the 20-min freshness filter above -- correct
                    # for RUNNING connectors, where stale means "untrustworthy" -- would
                    # otherwise make a paused connector's real, often-worst-case lag
                    # silently invisible. Separate, unfiltered lookup for paused connectors
                    # only, with the actual age surfaced so the UI can show it's not live.
                    paused_group_ids = [
                        c.get("group_id_override") or f"connect-{c.get('name', '')}"
                        for c in all_connectors if c.get("name") and c.get("state") == "PAUSED"
                    ]
                    stale_lag_by_group: dict[str, dict] = {}
                    if paused_group_ids:
                        _stale_rows = await _csess.execute(_cct("""
                            SELECT group_id, total_lag, topic_count, updated_at FROM kafka_consumer_group_lag
                            WHERE cluster_id = :cid AND group_id = ANY(:gids)
                        """), {"cid": int(cluster_id), "gids": paused_group_ids})
                        for r in _stale_rows.fetchall():
                            if r.group_id not in lag_by_group:
                                stale_lag_by_group[r.group_id] = {
                                    "lag": r.total_lag, "topic_count": r.topic_count,
                                    "lag_stale_since": r.updated_at.isoformat() if r.updated_at else None,
                                }
                    # Trend/rate -- same aggregation already built and validated for the
                    # Consumer Groups tab, reused here to power the new bubble chart's
                    # y-axis (lag rate) without duplicating the underlying data source.
                    trend_by_group: dict[str, dict] = {}
                    try:
                        _ctr = await _csess.execute(_cct("""
                            SELECT group_id,
                                   COALESCE(SUM(inflow_since_last), 0) as total_inflow,
                                   COALESCE(SUM(consumed_since_last), 0) as total_consumed,
                                   AVG(interval_seconds) as avg_interval
                            FROM kafka_consumer_group_partition_lag
                            WHERE cluster_id = :cid AND group_id = ANY(:gids) AND updated_at >= NOW() - INTERVAL '20 minutes'
                            AND inflow_since_last IS NOT NULL AND consumed_since_last IS NOT NULL
                            GROUP BY group_id
                        """), {"cid": int(cluster_id), "gids": group_ids})
                        for tr in _ctr.fetchall():
                            _net = int(tr.total_inflow) - int(tr.total_consumed)
                            _interval = float(tr.avg_interval) if tr.avg_interval else 0.0
                            _rate = round(_net / (_interval / 60), 1) if _interval > 0 else 0.0
                            trend_by_group[tr.group_id] = _rate
                    except Exception as _ctre:
                        logger.warning("connector trend lookup failed: %s", _ctre)
                    for c in all_connectors:
                        gid = c.get("group_id_override") or f"connect-{c.get('name', '')}"
                        _lg = lag_by_group.get(gid) or stale_lag_by_group.get(gid, {})
                        c["lag"] = _lg.get("lag")
                        c["topic_count"] = _lg.get("topic_count")
                        c["lag_stale_since"] = _lg.get("lag_stale_since")
                        c["lag_rate_per_min"] = trend_by_group.get(gid, 0.0)
    except Exception as _lag_exc:
        logger.warning("connector lag lookup failed: %s", _lag_exc)
        for c in all_connectors:
            c.setdefault("lag", None)

    worker_nodes = [{
        "hostname": wr["cluster"],
        "url": wr["url"],
        "status": "up" if wr.get("status") not in ("unreachable","error","empty") else "down",
        "connector_count": wr.get("connector_count", 0),
        "error": wr.get("error","") if wr.get("status") == "unreachable" else "",
        "cluster_group": wr.get("cluster_group", ""),
    } for wr in worker_results]

    return {
        "status": "healthy" if reachable else "error",
        "connector_count": len(all_connectors),
        "connectors": all_connectors,
        "clusters": [{"cluster": r["cluster"], "url": r["url"],
                      "connector_count": r.get("connector_count",0),
                      "running": r.get("summary",{}).get("running",0),
                      "failed": r.get("summary",{}).get("failed",0),
                      "paused": r.get("summary",{}).get("paused",0)}
                     for r in unique_clusters],
        "worker_nodes": worker_nodes,
        "unreachable": [{"cluster": r["cluster"], "url": r["url"], "error": r.get("error","")} for r in unreachable],
        "summary": {"running": running, "failed": failed, "paused": paused,
                    "unassigned": len(all_connectors) - running - failed - paused},
    }


@router.get("/dashboard/connectors/topic-breakdown")
async def get_connector_topic_breakdown(cluster_id: str | None = None, top_n: int = 5) -> dict:
    """Top-N connectors by total lag, each with its own per-topic lag breakdown.
    Powers the Connector Anomalies row chart (replaces the bubble chart) -- each
    connector gets a fixed-width row, segmented by the topics contributing to its
    lag, so no single outlier can visually dominate or hide the others."""
    if not cluster_id:
        return {"connectors": []}
    try:
        kc_data = await get_kafka_connect(cluster_id)
        all_connectors = kc_data.get("connectors", [])
        with_lag = [c for c in all_connectors if (c.get("lag") or 0) > 0]
        top = sorted(with_lag, key=lambda c: -(c.get("lag") or 0))[:top_n]
        if not top:
            return {"connectors": []}

        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text as _t
        if SessionLocal is None:
            return {"connectors": []}

        group_ids = [c.get("group_id_override") or f"connect-{c.get('name', '')}" for c in top]
        async with SessionLocal() as sess:
            rows = await sess.execute(_t("""
                SELECT group_id, topic, lag FROM kafka_consumer_group_topic_lag
                WHERE cluster_id = :cid AND group_id = ANY(:gids) AND updated_at >= NOW() - INTERVAL '20 minutes'
                ORDER BY group_id, lag DESC
            """), {"cid": int(cluster_id), "gids": group_ids})
            topics_by_group: dict[str, list] = {}
            for r in rows.fetchall():
                topics_by_group.setdefault(r.group_id, []).append({"topic": r.topic, "lag": r.lag})

        result = []
        for c in top:
            gid = c.get("group_id_override") or f"connect-{c.get('name', '')}"
            result.append({
                "name": c.get("name"),
                "state": c.get("state"),
                "total_lag": c.get("lag") or 0,
                "topics": topics_by_group.get(gid, []),
            })
        return {"connectors": result}
    except Exception as exc:
        logger.warning("get_connector_topic_breakdown failed: %s", exc)
        return {"connectors": [], "error": str(exc)}


@router.get("/dashboard/mirrormaker")
async def get_mirrormaker(cluster_id: str | None = None, hours: int | None = None) -> dict:
    """Detect MirrorMaker replication and compare source/target lag."""
    # Build cluster data from postgres instead of kafka_store
    data = {}
    if cluster_id:
        try:
            from database import DashboardSessionLocal as SessionLocal
            from sqlalchemy import text as _mmt
            if SessionLocal:
                async with SessionLocal() as _mms:
                    # Consumer groups
                    _cg = await _mms.execute(_mmt(
                        "SELECT group_id FROM kafka_consumer_group_lag WHERE cluster_id=:cid"
                    ), {"cid": int(cluster_id)})
                    data["consumer_groups"] = [{"group_id": r.group_id} for r in _cg.fetchall()]
                    # Topics
                    _tn = await _mms.execute(_mmt(
                        "SELECT topic FROM kafka_topic_names WHERE cluster_id=:cid"
                    ), {"cid": int(cluster_id)})
                    data["topics"] = [{"name": r.topic} for r in _tn.fetchall()]
        except Exception as _mme:
            logger.warning("MM data fetch failed: %s", _mme)
    if not data:
        return {
            "detected": False,
            "mode": "none",
            "message": "No cluster data available. Run a sync first.",
        }

    from tools.mirrormaker import detect_mirrormaker
    result = detect_mirrormaker(data)

    # If cluster has a configured mirror source, add cross-cluster comparison
    if cluster_id:
        try:
            from storage import get_backend
            cluster = await get_backend().get_cluster(int(cluster_id))
            if cluster:
                mirror_mode = cluster.get("mirror_mode", "none")
                # Override detection with explicit mirror_mode config
                if mirror_mode and mirror_mode != "none" and not result.get("detected"):
                    result["detected"] = True
                    result["mode"] = mirror_mode
                    result["message"] = f"MirrorMaker {mirror_mode.upper()} configured for this cluster."
                # For MM1: fetch mirror consumer groups from postgres
                if mirror_mode == "mm1" and cluster_id:
                    try:
                        from database import DashboardSessionLocal as SessionLocal
                        from sqlalchemy import text as _mm1t
                        if SessionLocal:
                            async with SessionLocal() as _mm1s:
                                _mm1cg = await _mm1s.execute(_mm1t("""
                                    SELECT group_id, total_lag FROM kafka_consumer_group_lag
                                    WHERE cluster_id=:cid AND group_id ILIKE '%mirror%' AND updated_at >= NOW() - INTERVAL '20 minutes'
                                    ORDER BY total_lag DESC
                                """), {"cid": int(cluster_id)})
                                mm1_groups = [{"group_id": r.group_id, "total_lag": r.total_lag} for r in _mm1cg.fetchall()]
                                total_lag = sum(g["total_lag"] for g in mm1_groups)
                                result["mm1"] = {
                                    "consumer_groups": mm1_groups,
                                    "group_count": len(mm1_groups),
                                    "total_lag": total_lag,
                                    "health": "healthy" if total_lag == 0 else ("warning" if total_lag < 10000 else "critical"),
                                }
                    except Exception as _mm1e:
                        logger.warning("MM1 group fetch failed: %s", _mm1e)
                source_id = cluster.get("mirror_source_cluster_id")

                if mirror_mode != "none" and source_id:
                    source_data = kafka_store.get_cluster_data(str(source_id))
                    if source_data:
                        # Compare topic lag between source and target
                        source_topics = {t.get("name"): t for t in source_data.get("topics", [])}
                        target_topics = {t.get("name"): t for t in data.get("topics", [])}

                        replication_lag = []
                        for topic, src in source_topics.items():
                            if topic in target_topics:
                                src_msgs = src.get("total_messages", 0)
                                tgt_msgs = target_topics[topic].get("total_messages", 0)
                                lag = max(0, src_msgs - tgt_msgs)
                                replication_lag.append({
                                    "topic": topic,
                                    "source_messages": src_msgs,
                                    "target_messages": tgt_msgs,
                                    "lag": lag,
                                    "status": "healthy" if lag < 1000 else "lagging",
                                })

                        result["detected"] = True
                        result["mode"] = mirror_mode
                        result["cross_cluster"] = {
                            "source_cluster": source_data.get("cluster", {}).get("name", "unknown"),
                            "target_cluster": data.get("cluster", {}).get("name", "unknown"),
                            "topic_replication": replication_lag,
                            "total_topics_replicated": len(replication_lag),
                        }
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning("MirrorMaker cross-cluster comparison failed: %s", exc)

    return result


@router.post("/dashboard/insights/narrative")
async def get_insights_narrative(request: Request, cluster_id: str | None = None) -> dict:
    """Generate AI narrative summary of current cluster health."""
    data = kafka_store.get_cluster_data(cluster_id)
    if data is None:
        return {"narrative": "No cluster data available. Run a sync first."}

    from tools.anomaly_detector import detect_anomalies
    anomalies = detect_anomalies(data)

    cluster = data.get("cluster", {})
    brokers = data.get("brokers", [])
    groups = data.get("consumer_groups", [])
    topics = data.get("topics", [])

    top_lag_groups = sorted(groups, key=lambda g: g.get("total_lag", 0), reverse=True)[:5]
    growing_groups = [g for g in groups if g.get("lag_trend") == "growing"]
    critical_anomalies = [a for a in anomalies if a["severity"] == "critical"]
    warning_anomalies = [a for a in anomalies if a["severity"] == "warning"]

    summary = f"""Kafka Cluster: {cluster.get("name", "unknown")}
Health Score: {cluster.get("health_score", 100)}/100
Brokers: {len(brokers)} online
Topics: {len(topics)}
Consumer Groups: {len(groups)}
Active Anomalies: {len(critical_anomalies)} critical, {len(warning_anomalies)} warning

Top 5 Consumer Groups by Lag:
{chr(10).join(f"- {g.get('group_id') or g.get('group_name', 'unknown')}: {g.get('total_lag',0):,} msgs ({g.get('lag_trend','stable')})" for g in top_lag_groups)}

Growing Groups: {len(growing_groups)}
{chr(10).join(f"- {g.get('group_id') or g.get('group_name','unknown')}: +{g.get('lag_rate_per_min',0):,.0f}/min" for g in growing_groups) if growing_groups else "None"}

Critical Anomalies:
{chr(10).join(f"- {a['category']}: {a['description']}" for a in critical_anomalies) if critical_anomalies else "None"}
"""

    prompt = f"""You are a Kafka cluster intelligence agent. Analyze this cluster state and provide a concise executive summary (2-3 sentences) followed by the top 3 actionable recommendations.

{summary}

Format your response as:
**Cluster Health Summary**
[2-3 sentence summary of overall health, highlighting the most important issues]

**Top Recommendations**
1. [Most urgent action]
2. [Second priority]
3. [Third priority]

Be specific — use actual group names, numbers, and timeframes from the data."""

    api_key = request.headers.get("x-anthropic-key", "") or os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return {
            "narrative": "Anthropic API key not configured. Add your key in Settings.",
            "anomaly_count": len(anomalies),
            "critical_count": len(critical_anomalies),
        }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-6",
                    "max_tokens": 500,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            resp.raise_for_status()
            result = resp.json()
            narrative = result["content"][0]["text"]
            return {
                "narrative": narrative,
                "anomaly_count": len(anomalies),
                "critical_count": len(critical_anomalies),
            }
    except Exception as exc:
        return {
            "narrative": f"AI analysis unavailable: {str(exc)}",
            "anomaly_count": len(anomalies),
            "critical_count": len(critical_anomalies),
        }


@router.get("/dashboard/overview/lag-trend")
async def get_lag_trend(cluster_id: str | None = None, minutes: float = 1440.0) -> dict:
    """Return total consumer lag trend over time, bucketed by time interval."""
    if not cluster_id:
        return {"empty": True, "points": []}

    _cache_key = f"{cluster_id}:{minutes}"
    _cached = _get_lag_trend_cached(_cache_key)
    if _cached is not None:
        return _cached

    # Determine bucket size based on time range
    if minutes <= 60:
        bucket_interval = '5 minutes'
    elif minutes <= 360:
        bucket_interval = '15 minutes'
    elif minutes <= 1440:
        bucket_interval = '1 hour'
    elif minutes <= 10080:
        bucket_interval = '6 hours'
    else:
        bucket_interval = '1 day'

    try:
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text
        if SessionLocal is None:
            return {"empty": True, "points": []}

        async with SessionLocal() as session:
            sql = f"""
                SELECT
                    date_bin(
                        '{bucket_interval}'::INTERVAL,
                        collected_at,
                        TIMESTAMP '2001-01-01'
                    ) AS bucket_time,
                    AVG(total_lag)::bigint AS avg_lag
                FROM kafka_lag_snapshots
                WHERE cluster_id = :cluster_id
                AND collected_at >= NOW() - ((:minutes) * INTERVAL '1 minute')
                AND total_lag >= 0
                GROUP BY date_bin(
                    '{bucket_interval}'::INTERVAL,
                    collected_at,
                    TIMESTAMP '2001-01-01'
                )
                ORDER BY bucket_time ASC
            """
            result = await session.execute(
                text(sql),
                {
                    "cluster_id": str(cluster_id),
                    "minutes": float(minutes)
                }
            )
            rows = result.fetchall()

        if not rows:
            return {"empty": True, "points": []}

        points = [
            {"time": str(row.bucket_time)[:16].replace(' ', 'T'), "total_lag": int(row.avg_lag)}
            for row in rows
        ]

        result = {"empty": False, "points": points}
        _set_lag_trend_cached(_cache_key, result)
        return result

    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"lag_trend error: {e}")
        return {"empty": True, "points": []}


@router.get("/dashboard/topics/message-rate")
async def get_topic_message_rate(
    cluster_id: str | None = None, minutes: float = 1440.0, topic: str | None = None
) -> dict:
    """Message in/out rate over time — blends kafka_topic_message_rate_snapshots (raw,
    <= 6h) and kafka_topic_message_rate_hourly_rollup (aggregated, > 6h). Ensures
    completeness for queries spanning the retention cutoff.
    No topic param: summed across ALL topics (cluster-wide view).
    topic param: single-topic series (for topic-lag popup / Topics tab use)."""
    if not cluster_id:
        return {"empty": True, "points": []}

    if minutes <= 60:
        bucket_interval = '5 minutes'
    elif minutes <= 360:
        bucket_interval = '15 minutes'
    elif minutes <= 1440:
        bucket_interval = '1 hour'
    elif minutes <= 10080:
        bucket_interval = '6 hours'
    else:
        bucket_interval = '1 day'

    bucket_seconds = {
        '5 minutes': 300, '15 minutes': 900, '1 hour': 3600,
        '6 hours': 21600, '1 day': 86400,
    }[bucket_interval]

    try:
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text
        from datetime import datetime, timezone, timedelta
        if SessionLocal is None:
            return {"empty": True, "points": []}

        async with SessionLocal() as session:
            now = datetime.now(timezone.utc)
            range_start = now - timedelta(minutes=minutes)

            # Determine the REAL boundary between rolled-up and not-yet-rolled-up data, rather
            # than assuming a fixed cutoff — eliminates any gap since both queries agree on the
            # same verified fact. Safe because the raw table stays small (bounded to roughly one
            # rollup cycle) regardless of the exact boundary.
            max_rollup_row = await session.execute(text("""
                SELECT MAX(hour_bucket) as max_bucket FROM kafka_topic_message_rate_hourly_rollup
                WHERE cluster_id = :cluster_id
            """), {"cluster_id": int(cluster_id)})
            max_bucket_row = max_rollup_row.fetchone()
            if max_bucket_row and max_bucket_row.max_bucket:
                cutoff = max_bucket_row.max_bucket + timedelta(hours=1)
            else:
                # No rollup data exists yet for this cluster (e.g. a brand-new cluster before
                # the hourly rollup job has processed it). Setting cutoff to `now` here would
                # be backwards -- it would incorrectly route recent-window queries to the
                # blended raw+rollup path (which then finds nothing in either half). Set it
                # well before range_start instead, so the raw-only path is used for the full
                # requested range, correctly using all available raw data.
                cutoff = range_start - timedelta(days=1)

            # Same dynamic-boundary approach, one tier further down: the real edge
            # between "still in hourly_rollup" and "aged into daily_rollup" (rollup_hourly_to_daily
            # deletes hourly rows once rolled up). MIN(hour_bucket) of what's left in
            # hourly_rollup IS this boundary -- more direct than querying daily_rollup's max,
            # since it can't drift out of sync with what hourly actually still holds.
            min_hourly_row = await session.execute(text("""
                SELECT MIN(hour_bucket) as min_bucket FROM kafka_topic_message_rate_hourly_rollup
                WHERE cluster_id = :cluster_id
            """), {"cluster_id": int(cluster_id)})
            min_hourly = min_hourly_row.fetchone()
            daily_cutoff = min_hourly.min_bucket if min_hourly and min_hourly.min_bucket else (range_start - timedelta(days=1))

            topic_filter_sql = "AND topic = :topic" if topic else ""

            # If the requested range doesn't reach past the cutoff, use raw table only
            # (covers 1h, 6h views with zero added cost)
            if range_start >= cutoff:
                sql = f"""
                    WITH bucketed AS (
                        SELECT
                            date_bin(
                                '{bucket_interval}'::INTERVAL,
                                collected_at,
                                TIMESTAMP '2001-01-01'
                            ) AS bucket_time,
                            SUM(inflow) AS raw_inflow,
                            COUNT(inflow) AS inflow_cnt,
                            SUM(outflow) AS raw_outflow,
                            COUNT(outflow) AS outflow_cnt
                        FROM kafka_topic_message_rate_snapshots
                        WHERE cluster_id = :cluster_id
                        AND collected_at >= NOW() - ((:minutes) * INTERVAL '1 minute')
                        {topic_filter_sql}
                        GROUP BY date_bin(
                            '{bucket_interval}'::INTERVAL,
                            collected_at,
                            TIMESTAMP '2001-01-01'
                        )
                    ),
                    grouped AS (
                        SELECT *,
                            COUNT(*) FILTER (WHERE inflow_cnt > 0) OVER (ORDER BY bucket_time ROWS UNBOUNDED PRECEDING) AS inflow_grp,
                            COUNT(*) FILTER (WHERE outflow_cnt > 0) OVER (ORDER BY bucket_time ROWS UNBOUNDED PRECEDING) AS outflow_grp
                        FROM bucketed
                    )
                    SELECT
                        bucket_time,
                        COALESCE(FIRST_VALUE(raw_inflow) OVER (PARTITION BY inflow_grp ORDER BY bucket_time), 0)::bigint AS total_inflow,
                        COALESCE(FIRST_VALUE(raw_outflow) OVER (PARTITION BY outflow_grp ORDER BY bucket_time), 0)::bigint AS total_outflow
                    FROM grouped
                    ORDER BY bucket_time ASC
                """
                params = {"cluster_id": int(cluster_id), "minutes": minutes}
                if topic:
                    params["topic"] = topic
                rows = await session.execute(text(sql), params)
            else:
                # Range spans the cutoff — query both raw (cutoff to now) and rollup (range_start to cutoff)
                sql_raw = f"""
                    WITH bucketed AS (
                        SELECT
                            date_bin(
                                '{bucket_interval}'::INTERVAL,
                                collected_at,
                                TIMESTAMP '2001-01-01'
                            ) AS bucket_time,
                            SUM(inflow) AS raw_inflow,
                            COUNT(inflow) AS inflow_cnt,
                            SUM(outflow) AS raw_outflow,
                            COUNT(outflow) AS outflow_cnt
                        FROM kafka_topic_message_rate_snapshots
                        WHERE cluster_id = :cluster_id
                        AND collected_at >= :cutoff
                        {topic_filter_sql}
                        GROUP BY date_bin(
                            '{bucket_interval}'::INTERVAL,
                            collected_at,
                            TIMESTAMP '2001-01-01'
                        )
                    ),
                    grouped AS (
                        SELECT *,
                            COUNT(*) FILTER (WHERE inflow_cnt > 0) OVER (ORDER BY bucket_time ROWS UNBOUNDED PRECEDING) AS inflow_grp,
                            COUNT(*) FILTER (WHERE outflow_cnt > 0) OVER (ORDER BY bucket_time ROWS UNBOUNDED PRECEDING) AS outflow_grp
                        FROM bucketed
                    )
                    SELECT
                        bucket_time,
                        COALESCE(FIRST_VALUE(raw_inflow) OVER (PARTITION BY inflow_grp ORDER BY bucket_time), 0)::bigint AS total_inflow,
                        COALESCE(FIRST_VALUE(raw_outflow) OVER (PARTITION BY outflow_grp ORDER BY bucket_time), 0)::bigint AS total_outflow
                    FROM grouped
                """

                # hourly_rollup only actually holds rows from daily_cutoff forward (anything
                # older was already deleted once rolled into daily_rollup) -- bound the query
                # accordingly rather than querying a range it can no longer satisfy.
                hourly_query_start = max(range_start, daily_cutoff)

                sql_rollup = f"""
                    SELECT
                        date_bin(
                            '{bucket_interval}'::INTERVAL,
                            hour_bucket,
                            TIMESTAMP '2001-01-01'
                        ) AS bucket_time,
                        COALESCE(SUM(total_inflow), 0)::bigint AS total_inflow,
                        COALESCE(SUM(total_outflow), 0)::bigint AS total_outflow
                    FROM kafka_topic_message_rate_hourly_rollup
                    WHERE cluster_id = :cluster_id
                    AND hour_bucket >= :hourly_query_start
                    AND hour_bucket < :cutoff
                    {topic_filter_sql}
                    GROUP BY date_bin(
                        '{bucket_interval}'::INTERVAL,
                        hour_bucket,
                        TIMESTAMP '2001-01-01'
                    )
                """

                sql_daily = f"""
                    SELECT
                        date_bin(
                            '{bucket_interval}'::INTERVAL,
                            day_bucket,
                            TIMESTAMP '2001-01-01'
                        ) AS bucket_time,
                        COALESCE(SUM(total_inflow), 0)::bigint AS total_inflow,
                        COALESCE(SUM(total_outflow), 0)::bigint AS total_outflow
                    FROM kafka_topic_message_rate_daily_rollup
                    WHERE cluster_id = :cluster_id
                    AND day_bucket >= :range_start
                    AND day_bucket < :daily_cutoff
                    {topic_filter_sql}
                    GROUP BY date_bin(
                        '{bucket_interval}'::INTERVAL,
                        day_bucket,
                        TIMESTAMP '2001-01-01'
                    )
                """

                params = {
                    "cluster_id": int(cluster_id), "cutoff": cutoff, "range_start": range_start,
                    "hourly_query_start": hourly_query_start, "daily_cutoff": daily_cutoff,
                }
                if topic:
                    params["topic"] = topic

                # Execute all three tiers -- daily only actually needed when the range reaches
                # that far back, but the query is cheap/empty-returning otherwise so no need
                # to conditionally skip it.
                raw_rows = await session.execute(text(sql_raw), params)
                rollup_rows = await session.execute(text(sql_rollup), params)
                daily_rows = await session.execute(text(sql_daily), params)

                # Merge results: combine into one dict keyed by bucket_time, summing duplicates
                merged = {}
                for r in raw_rows.fetchall():
                    key = r.bucket_time
                    if key not in merged:
                        merged[key] = {"inflow": 0, "outflow": 0}
                    merged[key]["inflow"] += r.total_inflow
                    merged[key]["outflow"] += r.total_outflow

                for r in rollup_rows.fetchall():
                    key = r.bucket_time
                    if key not in merged:
                        merged[key] = {"inflow": 0, "outflow": 0}
                    merged[key]["inflow"] += r.total_inflow
                    merged[key]["outflow"] += r.total_outflow

                for r in daily_rows.fetchall():
                    key = r.bucket_time
                    if key not in merged:
                        merged[key] = {"inflow": 0, "outflow": 0}
                    merged[key]["inflow"] += r.total_inflow
                    merged[key]["outflow"] += r.total_outflow

                # Build rows-like structure from merged dict, sorted by time
                class MergedRow:
                    def __init__(self, bucket_time, total_inflow, total_outflow):
                        self.bucket_time = bucket_time
                        self.total_inflow = total_inflow
                        self.total_outflow = total_outflow

                rows = [MergedRow(k, v["inflow"], v["outflow"]) for k, v in sorted(merged.items())]

            points = [
                {
                    "time": r.bucket_time.isoformat(),
                    "inflow": r.total_inflow,
                    "outflow": r.total_outflow,
                    "inflow_rate": round(r.total_inflow / bucket_seconds, 2),
                    "outflow_rate": round(r.total_outflow / bucket_seconds, 2),
                }
                for r in rows
            ]
        if not points:
            return {"empty": True, "points": []}
        return {"points": points}
    except Exception as exc:
        return {"empty": True, "points": [], "error": str(exc)}


@router.get("/dashboard/topics/message-rate/archive")
async def list_message_rate_archive(cluster_id: str | None = None) -> dict:
    """List available cold-archive CSV files (hourly message-rate detail, aged out
    of Postgres beyond 7 days) for a cluster -- lets the dashboard offer browse/
    download without giving anyone direct MinIO access/credentials."""
    if not cluster_id:
        return {"files": []}
    try:
        from collectors import _get_minio_client, _KAFKA_ARCHIVE_BUCKET
        s3 = _get_minio_client()
        prefix = f"cluster-{cluster_id}/"
        try:
            resp = s3.list_objects_v2(Bucket=_KAFKA_ARCHIVE_BUCKET, Prefix=prefix)
        except Exception as e:
            logger.warning("list_message_rate_archive: bucket/list failed: %s", e)
            return {"files": []}
        files = [
            {
                "filename": obj["Key"].removeprefix(prefix),
                "size_bytes": obj["Size"],
                "last_modified": obj["LastModified"].isoformat(),
            }
            for obj in resp.get("Contents", [])
        ]
        files.sort(key=lambda f: f["filename"], reverse=True)
        return {"files": files}
    except Exception as exc:
        return {"files": [], "error": str(exc)}


@router.get("/dashboard/topics/message-rate/archive/download")
async def download_message_rate_archive(cluster_id: str, filename: str):
    """Stream a single archived CSV file back to the browser. filename is
    validated against a strict pattern (no path traversal) before use."""
    import re
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}\.csv", filename):
        raise HTTPException(status_code=400, detail="Invalid filename")
    try:
        from collectors import _get_minio_client, _KAFKA_ARCHIVE_BUCKET
        s3 = _get_minio_client()
        key = f"cluster-{cluster_id}/{filename}"
        obj = s3.get_object(Bucket=_KAFKA_ARCHIVE_BUCKET, Key=key)
        body = obj["Body"].read()
        return StreamingResponse(
            iter([body]),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="cluster-{cluster_id}-{filename}"'},
        )
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"File not found: {exc}")


@router.get("/dashboard/consumer-groups/{group_id}/message-rate")
async def get_group_message_rate(
    group_id: str, cluster_id: str | None = None, minutes: float = 1440.0
) -> dict:
    """Message in/out rate over time for a SPECIFIC consumer group — from
    kafka_consumer_group_rate_snapshots. Correctly group-attributed (unlike the
    topic-level table, which aggregates outflow across all groups reading a topic)."""
    if not cluster_id:
        return {"empty": True, "points": []}

    if minutes <= 60:
        bucket_interval = '5 minutes'
    elif minutes <= 360:
        bucket_interval = '15 minutes'
    elif minutes <= 1440:
        bucket_interval = '1 hour'
    elif minutes <= 10080:
        bucket_interval = '6 hours'
    else:
        bucket_interval = '1 day'

    bucket_seconds = {
        '5 minutes': 300, '15 minutes': 900, '1 hour': 3600,
        '6 hours': 21600, '1 day': 86400,
    }[bucket_interval]

    try:
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text
        if SessionLocal is None:
            return {"empty": True, "points": []}

        async with SessionLocal() as session:
            sql = f"""
                SELECT
                    date_bin(
                        '{bucket_interval}'::INTERVAL,
                        collected_at,
                        TIMESTAMP '2001-01-01'
                    ) AS bucket_time,
                    COALESCE(SUM(inflow), 0)::bigint AS total_inflow,
                    COALESCE(SUM(outflow), 0)::bigint AS total_outflow
                FROM kafka_consumer_group_rate_snapshots
                WHERE cluster_id = :cluster_id
                AND group_id = :group_id
                AND collected_at >= NOW() - ((:minutes) * INTERVAL '1 minute')
                GROUP BY date_bin(
                    '{bucket_interval}'::INTERVAL,
                    collected_at,
                    TIMESTAMP '2001-01-01'
                )
                ORDER BY bucket_time ASC
            """
            rows = await session.execute(text(sql), {
                "cluster_id": int(cluster_id), "group_id": group_id, "minutes": minutes
            })
            points = [
                {
                    "time": r.bucket_time.isoformat(),
                    "inflow": r.total_inflow,
                    "outflow": r.total_outflow,
                    "inflow_rate": round(r.total_inflow / bucket_seconds, 2),
                    "outflow_rate": round(r.total_outflow / bucket_seconds, 2),
                }
                for r in rows.fetchall()
            ]
        if not points:
            return {"empty": True, "points": []}
        return {"points": points}
    except Exception as exc:
        return {"empty": True, "points": [], "error": str(exc)}


@router.get("/dashboard/topics/history")
async def get_topics_history(
    cluster_id: str | None = None,
    minutes: float = 1440.0,
    topics: str | None = None,
) -> dict:
    """Return per-topic msgs/sec trend from kafka_topic_metrics_hourly.
    topics param: comma-separated list of up to 5 topic names (custom compare mode).
    No topics param: top 10 by max rate in window (default mode).
    Time filters: 60=1hr, 360=6hr, 1440=24hr, 10080=7d, 43200=30d.
    """
    from storage import get_backend
    from collections import defaultdict
    from datetime import datetime, timedelta, timezone

    if not cluster_id:
        return {"empty": True, "series": []}

    # Parse optional topic filter (up to 5)
    topic_filter: list[str] | None = None
    if topics:
        topic_filter = [t.strip() for t in topics.split(",") if t.strip()][:5]

    # 1-hour view: use raw 5-min-granularity snapshots instead of the hourly-average
    # table, which only has 2 data points for this range (flat-line appearance).
    if minutes <= 60:
        from database import DashboardSessionLocal as _BytesSessionLocal
        from sqlalchemy import text as _bytes_text
        if _BytesSessionLocal is None:
            return {"empty": True, "series": []}
        try:
            async with _BytesSessionLocal() as _bsess:
                _topic_clause = ""
                _params: dict = {"cid": int(cluster_id), "minutes": minutes}
                if topic_filter:
                    _topic_clause = "AND topic = ANY(:topics)"
                    _params["topics"] = topic_filter
                _sql = f"""
                    SELECT
                        date_bin('5 minutes'::interval, collected_at, TIMESTAMP '2001-01-01') AS bucket_time,
                        topic,
                        AVG(bytes_in_per_sec) AS avg_rate
                    FROM kafka_topic_bytes_rate_snapshots
                    WHERE cluster_id = :cid
                    AND collected_at >= NOW() - (:minutes * INTERVAL '1 minute')
                    AND topic NOT LIKE '\\_%'
                    {_topic_clause}
                    GROUP BY bucket_time, topic
                    ORDER BY bucket_time ASC
                """
                _rows = (await _bsess.execute(_bytes_text(_sql), _params)).fetchall()
        except Exception:
            return {"empty": True, "series": []}

        if not _rows:
            if topic_filter:
                return {"labels": [], "series": [{"name": t, "values": []} for t in topic_filter], "snapshot_count": 0}
            return {"empty": True, "series": []}

        _bucket_set = sorted({r.bucket_time for r in _rows})
        _labels = [b.isoformat() for b in _bucket_set]
        _bucket_idx = {b: i for i, b in enumerate(_bucket_set)}
        _topic_values: dict[str, list[float]] = {}
        for r in _rows:
            arr = _topic_values.setdefault(r.topic, [0.0] * len(_bucket_set))
            arr[_bucket_idx[r.bucket_time]] = round((r.avg_rate or 0.0) / 1024, 4)

        if topic_filter:
            _series = [{"name": t, "values": _topic_values.get(t, [0.0] * len(_bucket_set))} for t in topic_filter]
        else:
            _topic_maxes = {t: max(v) for t, v in _topic_values.items()}
            _top_topics = sorted(_topic_maxes, key=lambda n: _topic_maxes[n], reverse=True)[:10]
            _series = [{"name": t, "values": _topic_values[t]} for t in _top_topics]

        return {"labels": _labels, "series": _series, "snapshot_count": len(_bucket_set)}

    # Determine hour buckets for selected window
    now = datetime.now(timezone.utc)
    if minutes <= 60:
        total_buckets = 2
        delta_hours = 2
        bucket_hours = 1
    elif minutes <= 360:
        total_buckets = 6
        delta_hours = 6
        bucket_hours = 1
    elif minutes <= 1440:
        total_buckets = 24
        delta_hours = 24
        bucket_hours = 1
    elif minutes <= 10080:
        total_buckets = 28
        delta_hours = 168
        bucket_hours = 6
    else:
        total_buckets = 30
        delta_hours = 720
        bucket_hours = 24

    # Generate all expected hour bucket labels (zero-filled)
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    # Align to bucket boundary
    bucket_offset = current_hour.hour % bucket_hours
    current_bucket = current_hour - timedelta(hours=bucket_offset)
    all_buckets = []
    bucket_dts = []
    for i in range(total_buckets - 1, -1, -1):
        b = current_bucket - timedelta(hours=i * bucket_hours)
        all_buckets.append(b.isoformat())
        bucket_dts.append(b)

    try:
        rows = await get_backend().get_topic_history_hourly(
            int(cluster_id),
            minutes=float(delta_hours * 60),
            topic_filter=topic_filter,
        )
    except Exception:
        return {"empty": True, "series": []}

    if not rows:
        if topic_filter:
            # Return empty series for each requested topic
            series = [{"name": t, "values": [0.0] * total_buckets} for t in topic_filter]
            return {"labels": all_buckets, "series": series, "snapshot_count": total_buckets, "empty_reason": "no_data"}
        return {"empty": True, "series": []}

    from collections import defaultdict
    bucket_sum = defaultdict(lambda: defaultdict(float))
    bucket_cnt = defaultdict(lambda: defaultdict(int))
    for r in rows:
        if r["topic"].startswith("_"):
            continue
        rt = datetime.fromisoformat(r["time"])
        if rt.tzinfo is None:
            rt = rt.replace(tzinfo=timezone.utc)
        idx = min(range(len(bucket_dts)),
                  key=lambda j: abs((bucket_dts[j] - rt).total_seconds()))
        b = all_buckets[idx]
        bucket_sum[r["topic"]][b] += float(r["avg_msgs"] or 0)
        bucket_cnt[r["topic"]][b] += 1

    topic_data: dict[str, dict[str, float]] = defaultdict(dict)
    for t, buckets in bucket_sum.items():
        for b, total in buckets.items():
            topic_data[t][b] = total / bucket_cnt[t][b]

    if topic_filter:
        # Custom mode — return requested topics in order (even if no data)
        series = []
        for name in topic_filter:
            vals = [round(topic_data[name].get(b, 0.0), 4) for b in all_buckets]
            series.append({"name": name, "values": vals})
    else:
        # Default mode — top 10 by max rate in window
        if topic_data:
            topic_maxes = {t: max(v.values()) for t, v in topic_data.items()}
            top_topics = sorted(topic_maxes, key=lambda n: topic_maxes[n], reverse=True)[:10]
        else:
            top_topics = []
        series = []
        for name in top_topics:
            vals = [round(topic_data[name].get(b, 0.0), 4) for b in all_buckets]
            series.append({"name": name, "values": vals})

    # Drop the current/still-forming bucket if it has no data yet anywhere — avoids
    # misleadingly showing "0" for a bucket that simply hasn't landed data yet (the
    # hourly collector runs on a ~2-4 min cadence after each UTC hour boundary).
    if all_buckets and series and all(
        (s["values"][-1] if s["values"] else 0.0) == 0.0 for s in series
    ):
        all_buckets = all_buckets[:-1]
        for s in series:
            s["values"] = s["values"][:-1]

    if not series:
        return {"labels": all_buckets, "series": [], "snapshot_count": total_buckets}
    return {"labels": all_buckets, "series": series, "snapshot_count": total_buckets}


@router.get("/dashboard/topics/name-search")
async def search_topic_names(cluster_id: str | None = None, q: str = "") -> dict:
    """Autocomplete topic name search from kafka_topic_names table (DB-backed)."""
    from storage import get_backend
    if not cluster_id or not q or len(q) < 2:
        return {"results": []}
    try:
        results = await get_backend().topic_search(int(cluster_id), q.strip())
        return {"results": results}
    except Exception:
        return {"results": []}


@router.post("/dashboard/insights/narrative/stream")
async def stream_insights_narrative(
    request: Request,
    cluster_id: str | None = None
):
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    continuation_of = body.get("continuation_of", None)
    from fastapi.responses import StreamingResponse

    api_key = request.headers.get("x-anthropic-key", "") or \
              os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        async def nokey():
            yield 'data: Anthropic API key not configured.\n\n'
            yield 'data: [DONE]\n\n'
        return StreamingResponse(nokey(), media_type="text/event-stream")

    # Read all data from DB directly — no cache dependency
    import json as _json
    from database import DashboardSessionLocal as SessionLocal
    from sqlalchemy import text as _text
    from storage import get_backend as _gb

    try:
        _all_cfg = await _gb().get_all()

        # Structure counts (total topics, RF=1, partitions etc)
        _struct_raw = None
        _groups_raw = None
        _brokers_raw = None
        if SessionLocal:
            async with SessionLocal() as _sess:
                # Latest broker data
                _br = await _sess.execute(_text(
                    "SELECT data_json FROM kafka_metrics_history WHERE cluster_id=:cid AND scan_type='brokers' ORDER BY collected_at DESC LIMIT 1"
                ), {"cid": cluster_id})
                _br_row = _br.fetchone()
                _brokers_raw = _json.loads(_br_row.data_json) if _br_row else []

                # Latest groups data
                _gr = await _sess.execute(_text(
                    "SELECT data_json FROM kafka_metrics_history WHERE cluster_id=:cid AND scan_type='groups' ORDER BY collected_at DESC LIMIT 1"
                ), {"cid": cluster_id})
                _gr_row = _gr.fetchone()
                _groups_raw = _json.loads(_gr_row.data_json) if _gr_row else []

                # Structure counts
                _st = await _sess.execute(_text(
                    "SELECT data_json FROM kafka_metrics_history WHERE cluster_id=:cid AND scan_type='topics_structure' ORDER BY collected_at DESC LIMIT 1"
                ), {"cid": cluster_id})
                _st_row = _st.fetchone()
                if _st_row:
                    _st_data = _json.loads(_st_row.data_json)
                    _struct_raw = _st_data.get("counts", {})

                # Lag trend (last 24h)
                _lt = await _sess.execute(_text(
                    """SELECT date_trunc('hour', collected_at) as bucket, data_json
                       FROM kafka_metrics_history WHERE cluster_id=:cid AND scan_type='groups'
                       AND collected_at >= NOW() - INTERVAL '24 hours'
                       ORDER BY bucket ASC"""
                ), {"cid": cluster_id})
                _lt_rows = _lt.fetchall()
                lag_trend_points = []
                for _lr in _lt_rows:
                    try:
                        _lgroups = _json.loads(_lr.data_json)
                        _total = sum(g.get("total_lag", 0) for g in _lgroups if isinstance(g, dict))
                        lag_trend_points.append({"time": str(_lr.bucket)[:16], "total_lag": _total})
                    except Exception:
                        pass

        brokers = _brokers_raw or []
        groups = _groups_raw or []
        structure = _struct_raw or {}

        # Metrics counts (top by size, msg rate)
        _metrics_raw = _all_cfg.get(f"kafka_counts_metrics_{cluster_id}")
        _metrics_str = _json.loads(_metrics_raw) if _metrics_raw else {}
        metrics_counts = _json.loads(_metrics_str) if isinstance(_metrics_str, str) else _metrics_str

        # Phase2 broker status
        broker_phase2 = {}
        for b in brokers:
            host = b.get("host", "")
            _p2 = _all_cfg.get(f"phase2_{host}:7071") or _all_cfg.get(f"phase2_{host}:{_all_cfg.get('prometheus_port', 7071)}")
            if _p2:
                try:
                    _p2d = _json.loads(_p2)
                    broker_phase2[host] = _p2d
                except Exception:
                    pass

        # Anomalies from cache (still valid)
        data = kafka_store.get_cluster_data(cluster_id) if cluster_id else None
        anomalies = (data or {}).get("anomalies", [])

    except Exception as _de:
        brokers, groups, structure, metrics_counts, broker_phase2, lag_trend_points, anomalies = [], [], {}, {}, {}, [], []

    # Build rich context
    total_topics = structure.get("total_topics", 0)
    total_rf1 = structure.get("total_rf1", 0)
    total_urp = structure.get("total_urp", 0)
    total_partitions = structure.get("total_partitions", 0)
    total_groups = structure.get("total_groups", 0)

    top_by_size = metrics_counts.get("top_topics_by_size", [])
    top_by_msg = metrics_counts.get("top_topics_by_msg_rate", [])

    # Broker health
    active_brokers = [b for b in brokers if b.get("heap_pct", 0) > 0]
    degraded_brokers = [b for b in brokers if b.get("heap_pct", 0) == 0 and b.get("cpu_pct", 0) == 0]
    avg_heap = round(sum(b.get("heap_pct", 0) for b in active_brokers) / max(len(active_brokers), 1), 1)
    avg_cpu = round(sum(b.get("cpu_pct", 0) for b in active_brokers) / max(len(active_brokers), 1), 1)
    total_urps = sum(b.get("urp_count", 0) for b in brokers)

    broker_details = []
    for b in brokers:
        host = b.get("host", "")
        p2 = broker_phase2.get(host, {})
        broker_details.append({
            "id": b.get("broker_id", b.get("id")),
            "host": host,
            "heap_pct": b.get("heap_pct", 0),
            "cpu_pct": b.get("cpu_pct", 0),
            "produce_latency_ms": b.get("produce_latency_ms", 0),
            "fetch_latency_ms": b.get("fetch_latency_ms", 0),
            "urp": b.get("urp_count", 0),
            "status": "DEGRADED - metrics unavailable" if b.get("heap_pct", 0) == 0 and b.get("cpu_pct", 0) == 0 else "healthy",
            "phase2_fails": p2.get("phase2_fail_count", 0),
            "throughput_available": p2.get("throughput_available", True),
        })

    # Consumer group health
    critical_groups = [{"name": g.get("group_id"), "lag": g.get("total_lag", 0),
                        "trend": g.get("lag_trend"), "state": g.get("state")}
                       for g in groups if g.get("total_lag", 0) > 10000][:10]
    warning_groups = [{"name": g.get("group_id"), "lag": g.get("total_lag", 0)}
                      for g in groups if 1000 < g.get("total_lag", 0) <= 10000][:5]
    healthy_groups = len([g for g in groups if g.get("total_lag", 0) <= 1000])

    # Lag trend summary
    if lag_trend_points:
        lag_start = lag_trend_points[0]["total_lag"]
        lag_end = lag_trend_points[-1]["total_lag"]
        lag_change = lag_end - lag_start
        lag_trend_summary = f"24h trend: {lag_start:,} → {lag_end:,} ({'+' if lag_change > 0 else ''}{lag_change:,} msgs, {'GROWING ⚠️' if lag_change > 100000 else 'STABLE ✅' if abs(lag_change) < 50000 else 'DECLINING ✅'})"
    else:
        lag_trend_summary = "No trend data available"

    anomaly_details = [{"severity": a.get("severity"), "category": a.get("category"),
                        "description": a.get("description")} for a in anomalies[:10]]

    prompt = f"""You are a senior Kafka platform intelligence agent providing an executive-level cluster analysis report.

Keep the report concise and actionable. Use short paragraphs, not large tables. Where tables are needed, limit to 5 rows maximum showing only the most critical items. Focus on insights and recommendations, not raw data enumeration.
Target total length: 600-800 words.

Analyse this Kafka cluster data and produce a detailed markdown report with these sections:

## Executive Summary
One paragraph overall health grade (A/B/C/D/F) with key numbers and the single most important finding.

## Broker Analysis
Assess broker health, CPU/heap pressure, ISR stability, request handling capacity. Flag any broker at risk.

## Topic Intelligence
Identify high-traffic topics, stale topics (data but no traffic), under-replicated topics, and partition imbalance.

## Consumer Lag Analysis
Which consumer groups are falling behind? What is the business impact of growing lag? Risk of data loss if lag exceeds retention?

## Anomaly Assessment
Evaluate detected anomalies. Severity, likely root cause, and recommended response.

## Capacity & Performance Outlook
Broker headroom, partition growth trend, resource utilisation trajectory.

## Recommended Actions
Prioritised list with effort estimate (quick win / medium / large). Each action should have a clear "why" and "what happens if ignored".

---
CLUSTER DATA:

Brokers ({len(active_brokers)} active, {len(degraded_brokers)} degraded):
- Average Heap: {avg_heap}%
- Average CPU: {avg_cpu}%
- Under-replicated Partitions: {total_urps}
- Broker details: {broker_details}

Topics ({total_topics} total, {total_partitions} partitions):
- Top 10 by message rate: {top_by_msg}
- Top 10 by size: {top_by_size}
- Low replication (RF=1): {total_rf1} topics
- Under-replicated (URP): {total_urp}

Consumer Groups ({total_groups} total):
- Critical (lag >10k): {critical_groups}
- Warning (lag 1k-10k): {warning_groups}
- Healthy (lag <1k): {healthy_groups} groups
- {lag_trend_summary}

Anomalies ({len(anomalies)} detected):
{anomaly_details}
"""

    async def event_stream():
        try:
            async for chunk in _llm_stream(
                model=settings.model,
                max_tokens=8192,
                messages=(
                    [
                        {"role": "user", "content": prompt},
                        {"role": "assistant", "content": continuation_of},
                        {"role": "user", "content": "Please continue the analysis from where you left off. Do not repeat what was already written."},
                    ]
                    if continuation_of
                    else [{"role": "user", "content": prompt}]
                ),
                api_key=api_key,
            ):
                if chunk.startswith("[STOP_REASON]"):
                    yield f"data: {chunk}\n\n"
                else:
                    escaped = chunk.replace("\n","\\n")
                    yield f"data: {escaped}\n\n"
            yield "data: [DONE]\n\n"
        except Exception as e:
            yield f"data: [ERROR] {str(e)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control":"no-cache",
                 "X-Accel-Buffering":"no"},
    )


@router.post("/dashboard/insights/tab-stream")
async def stream_tab_insights(
    request: Request,
    cluster_id: str | None = None
):
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    tab = body.get("tab", "")
    continuation_of = body.get("continuation_of", None)
    from fastapi.responses import StreamingResponse

    api_key = request.headers.get("x-anthropic-key", "") or \
              os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        async def nokey():
            yield 'data: Anthropic API key not configured.\n\n'
            yield 'data: [DONE]\n\n'
        return StreamingResponse(nokey(), media_type="text/event-stream")

    # Keep cache data for tabs not yet migrated to DB (connect/zk/schema/mirror)
    data = kafka_store.get_cluster_data(cluster_id) if cluster_id else (
        kafka_store.get_cluster_data(kafka_store.get_all_cluster_ids()[0])
        if kafka_store.get_all_cluster_ids() else None
    )
    data = data or {}

    # Read all data from DB directly
    import json as _json2
    from database import DashboardSessionLocal as _SL2
    from sqlalchemy import text as _text2
    from storage import get_backend as _gb2

    try:
        _all_cfg2 = await _gb2().get_all()
        _brokers2, _groups2, _struct2 = [], [], {}
        if _SL2:
            async with _SL2() as _s2:
                _r = await _s2.execute(_text2(
                    "SELECT data_json FROM kafka_metrics_history WHERE cluster_id=:cid AND scan_type='brokers' ORDER BY collected_at DESC LIMIT 1"
                ), {"cid": cluster_id})
                _rr = _r.fetchone()
                if _rr: _brokers2 = _json2.loads(_rr.data_json)

                _r = await _s2.execute(_text2(
                    "SELECT data_json FROM kafka_metrics_history WHERE cluster_id=:cid AND scan_type='groups' ORDER BY collected_at DESC LIMIT 1"
                ), {"cid": cluster_id})
                _rr = _r.fetchone()
                if _rr: _groups2 = _json2.loads(_rr.data_json)

                _r = await _s2.execute(_text2(
                    "SELECT data_json FROM kafka_metrics_history WHERE cluster_id=:cid AND scan_type='topics_structure' ORDER BY collected_at DESC LIMIT 1"
                ), {"cid": cluster_id})
                _rr = _r.fetchone()
                if _rr:
                    _d = _json2.loads(_rr.data_json)
                    _struct2 = _d.get("counts", {})

        _metrics2_raw = _all_cfg2.get(f"kafka_counts_metrics_{cluster_id}")
        _metrics2_str = _json2.loads(_metrics2_raw) if _metrics2_raw else {}
        _metrics2 = _json2.loads(_metrics2_str) if isinstance(_metrics2_str, str) else _metrics2_str

        # Phase2 status per broker
        _b_phase2 = {}
        for _b in _brokers2:
            _h = _b.get("host", "")
            _p = _all_cfg2.get(f"phase2_{_h}:7071")
            if _p:
                try: _b_phase2[_h] = _json2.loads(_p)
                except: pass

    except Exception:
        _brokers2, _groups2, _struct2, _metrics2, _b_phase2 = [], [], {}, {}, {}

    concise = "Keep it concise, 200-400 words. Be self-explanatory — assume the reader has no Kafka expertise. Focus on business impact and actionable recommendations."

    if tab == "brokers":
        _active = [b for b in _brokers2 if b.get("heap_pct", 0) > 0]
        _degraded = [b for b in _brokers2 if b.get("heap_pct", 0) == 0 and b.get("cpu_pct", 0) == 0]
        broker_details = [{"id": b.get("broker_id", b.get("id")), "host": b.get("host",""),
            "heap_pct": b.get("heap_pct"), "cpu_pct": b.get("cpu_pct"),
            "produce_latency_ms": b.get("produce_latency_ms"),
            "fetch_latency_ms": b.get("fetch_latency_ms"),
            "urp": b.get("urp_count"),
            "status": "DEGRADED" if b.get("heap_pct",0)==0 and b.get("cpu_pct",0)==0 else "healthy",
            "phase2_fails": _b_phase2.get(b.get("host",""), {}).get("phase2_fail_count", 0)
        } for b in _brokers2]
        prompt = f"""You are a Kafka broker health specialist. Explain findings in plain language — assume the reader is not a Kafka expert. Analyse these broker metrics and provide:
## Health Assessment
Overall broker health. Which brokers are healthy, which are degraded and why. Explain what "degraded" means for the cluster.
## Key Concerns
CPU/heap pressure, fetch latency (explain what 500ms+ fetch latency means for consumers), ISR stability.
## Risk Assessment
If the degraded broker stays down, what happens? What is the single point of failure risk?
## Recommendations
Prioritised actions — what to fix first and why.
{concise}

BROKER DATA ({len(_brokers2)} brokers, {len(_degraded)} degraded):
{broker_details}
"""

    elif tab == "topics":
        _top_size = _metrics2.get("top_topics_by_size", [])
        _top_msg = _metrics2.get("top_topics_by_msg_rate", [])
        total_rf1 = _struct2.get("total_rf1", 0)
        total_topics = _struct2.get("total_topics", 0)
        total_partitions = _struct2.get("total_partitions", 0)
        low_rep_count = total_rf1
        top_by_size = [{"name": t["name"], "size_gb": round(t.get("size_bytes",0)/1024**3,1)} for t in _top_size[:10]]
        top_by_msg = [{"name": t["name"], "msgs_per_sec": t.get("messages_in_per_sec",0)} for t in _top_msg[:10]]
        prompt = f"""You are a Kafka topic intelligence specialist. Explain findings in plain language for a non-expert audience. Analyse these topics and provide:
## Storage Risk Assessment
Largest topics by disk usage. Explain what happens when topics grow unbounded — disk pressure, broker instability.
## Traffic Analysis
Active vs idle topics. Large topics with zero traffic — are they orphaned? What should be done?
## Data Loss Risk
{low_rep_count} topics have RF=1 (only 1 copy). Explain what this means — if one broker fails, this data is permanently lost.
## Recommendations
Prioritised actions with clear business justification.
{concise}

TOPIC DATA (Total: {total_topics:,}, Partitions: {total_partitions:,}, RF=1 risk: {low_rep_count:,}):
- Top topics by storage: {top_by_size}
- Active topics by message rate: {top_by_msg}
"""

    elif tab == "consumer-groups":
        groups = _groups2
        critical_groups = [{"name": g.get("group_id"), "lag": g.get("total_lag", 0),
                            "trend": g.get("lag_trend"), "state": g.get("state")}
                           for g in groups if g.get("total_lag", 0) > 10000][:10]
        warning_groups = [{"name": g.get("group_id"), "lag": g.get("total_lag", 0)}
                          for g in groups if 1000 < g.get("total_lag", 0) <= 10000][:5]
        healthy_groups = len([g for g in groups if g.get("total_lag", 0) <= 1000])
        prompt = f"""You are a Kafka consumer lag analyst. Explain in plain language for non-experts. Analyse these consumer groups and provide:
## Lag Situation
What is consumer lag and why does it matter? Which groups are critically behind?
## Business Impact
What does millions of messages of lag mean for the business? Which downstream systems are affected? What data freshness issues exist?
## Trend Analysis
Is lag growing (producers faster than consumers = worsening), stable, or declining (recovering)?
## Recommendations
Which groups to fix first, how to investigate dead consumer groups, and what happens if ignored.
{concise}

CONSUMER GROUP DATA ({len(groups)} total):
- Critical lag (>10k msgs): {len(critical_groups)} groups — {critical_groups[:5]}
- Warning lag (1k-10k): {len(warning_groups)} groups — {warning_groups[:3]}
- Healthy (<1k): {healthy_groups} groups
"""

    elif tab == "kafka-connect":
        cdata = data.get("connectors", {})
        connectors_list = cdata.get("connectors", []) if isinstance(cdata, dict) else (cdata or [])
        connector_details = [{"name": c.get("name"), "state": c.get("state"), "type": c.get("type"),
                              "running_tasks": c.get("running_tasks"), "total_tasks": c.get("total_tasks"),
                              "failed_tasks": c.get("failed_tasks")} for c in connectors_list]
        prompt = f"""You are a Kafka Connect pipeline specialist. Analyse these connectors and provide:
## Pipeline Health — running/failed/paused breakdown
## Risk Areas — connectors with failed tasks, type imbalance
## Recommendations — which connectors need attention
{concise}

CONNECTOR DATA ({len(connector_details)} connectors):
{connector_details}
"""

    elif tab == "zookeeper":
        zk = data.get("zookeeper", {})
        zk_metrics = zk.get("metrics", {}) if isinstance(zk, dict) else {}
        zk_summary = {"status": zk.get("status") if isinstance(zk, dict) else None,
                      "server_mode": zk.get("server_mode") if isinstance(zk, dict) else None,
                      "metrics": zk_metrics}
        prompt = f"""You are a ZooKeeper operations specialist. Analyse these metrics and provide:
## ZooKeeper Health — latency, connections, znode pressure
## Concerns — any metrics outside normal range
## Recommendations — tuning or migration suggestions
{concise}

ZOOKEEPER DATA:
{zk_summary}
"""

    elif tab == "schema-registry":
        sr = data.get("schema_registry", {})
        sr_summary = {"status": sr.get("status") if isinstance(sr, dict) else None,
                      "subject_count": sr.get("subject_count") if isinstance(sr, dict) else None,
                      "total_versions": sr.get("total_versions") if isinstance(sr, dict) else None,
                      "global_compatibility": sr.get("global_compatibility") if isinstance(sr, dict) else None,
                      "schema_types": sr.get("schema_types") if isinstance(sr, dict) else None}
        prompt = f"""You are a Schema Registry governance specialist. Analyse and provide:
## Registry Health — subject count, version sprawl, compatibility policy
## Concerns — missing schemas, compatibility risks
## Recommendations — governance improvements
{concise}

SCHEMA REGISTRY DATA:
{sr_summary}
"""

    elif tab == "mirrormaker":
        mm = data.get("mirrormaker", {})
        mm_summary = {"detected": mm.get("detected") if isinstance(mm, dict) else None,
                      "mode": mm.get("mode") if isinstance(mm, dict) else None,
                      "mm1": mm.get("mm1") if isinstance(mm, dict) else None,
                      "mm2": mm.get("mm2") if isinstance(mm, dict) else None}
        prompt = f"""You are a Kafka replication specialist. Analyse MirrorMaker and provide:
## Replication Health — mode, lag, group status
## Risk Areas — lag growth, replication delay impact
## Recommendations — lag reduction, monitoring suggestions
{concise}

MIRRORMAKER DATA:
{mm_summary}
"""

    else:
        prompt = f"""You are a Kafka platform specialist. Provide a concise analysis with:
## Health Assessment
## Concerns
## Recommendations
{concise}

Unknown tab "{tab}" — no specific data available.
"""

    async def event_stream():
        try:
            async for chunk in _llm_stream(
                model=settings.model,
                max_tokens=4096,
                messages=(
                    [
                        {"role": "user", "content": prompt},
                        {"role": "assistant", "content": continuation_of},
                        {"role": "user", "content": "Please continue the analysis from where you left off. Do not repeat what was already written."},
                    ]
                    if continuation_of
                    else [{"role": "user", "content": prompt}]
                ),
                api_key=api_key,
            ):
                if chunk.startswith("[STOP_REASON]"):
                    yield f"data: {chunk}\n\n"
                else:
                    escaped = chunk.replace("\n","\\n")
                    yield f"data: {escaped}\n\n"
            yield "data: [DONE]\n\n"
        except Exception as e:
            yield f"data: [ERROR] {str(e)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control":"no-cache",
                 "X-Accel-Buffering":"no"},
    )


# ─── On-demand streaming endpoints ──────────────────────────────────

@router.get("/dashboard/topics/stream")
async def stream_topic_details(cluster_id: str, limit: int = 500):
    """Stream topic details on-demand — top N topics, rest available via search."""
    collector = await _collector_for_cluster(cluster_id)
    # Read topic names from postgres
    all_names = []
    total_count = 0
    try:
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text as _tst
        if SessionLocal:
            async with SessionLocal() as _tss:
                _tsr = await _tss.execute(_tst(
                    "SELECT topic FROM kafka_topic_metrics WHERE cluster_id=:cid ORDER BY size_bytes DESC LIMIT :lim"
                ), {"cid": int(cluster_id), "lim": limit})
                all_names = [r.topic for r in _tsr.fetchall()]
                _tc = await _tss.execute(_tst("SELECT COUNT(*) FROM kafka_topic_metrics WHERE cluster_id=:cid"), {"cid": int(cluster_id)})
                total_count = _tc.scalar() or 0
    except Exception:
        pass
    if not all_names:
        return {"topics": [], "total": 0, "total_topics": total_count}
    names_to_describe = all_names[:limit]
    # Alphabetical from summary — frontend re-sorts by anomaly after describe

    async def generate():
        _BATCH = 50
        sent = 0
        total = len(names_to_describe)
        for i in range(0, total, _BATCH):
            batch_names = names_to_describe[i:i + _BATCH]
            try:
                details = await collector.fetch_topic_details(batch_names)
                for t in details:
                    yield f"data: {json.dumps(t)}\n\n"
                    sent += 1
            except Exception as exc:
                yield f"data: {json.dumps({'error': str(exc)})}\n\n"
        yield f"data: {json.dumps({'done': True, 'total': sent, 'total_topics': total_count})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.get("/dashboard/groups/stream")
async def stream_group_lags(cluster_id: str):
    """Stream consumer group lag on-demand — fetches lag in batches."""
    collector = await _collector_for_cluster(cluster_id)
    # Read groups from postgres sorted by lag
    group_ids = []
    try:
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text as _gst
        if SessionLocal:
            async with SessionLocal() as _gss:
                _gsr = await _gss.execute(_gst(
                    "SELECT group_id FROM kafka_consumer_group_lag WHERE cluster_id=:cid ORDER BY total_lag DESC LIMIT 2000"
                ), {"cid": int(cluster_id)})
                group_ids = [r.group_id for r in _gsr.fetchall()]
    except Exception:
        pass
    if not group_ids:
        return {"groups": [], "total": 0}

    async def generate():
        _BATCH = 20
        sent = 0
        total = len(group_ids)
        for i in range(0, total, _BATCH):
            batch = group_ids[i:i + _BATCH]
            try:
                lags = await collector.fetch_group_lags(batch)
                for g in lags:
                    yield f"data: {json.dumps(g)}\n\n"
                    sent += 1
            except Exception as exc:
                yield f"data: {json.dumps({'error': str(exc)})}\n\n"
        total_groups = len((data or {}).get("consumer_groups", []))
        yield f"data: {json.dumps({'done': True, 'total': sent, 'total_groups': total_groups})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.get("/dashboard/topics/search")
async def search_topics(cluster_id: str, q: str = ""):
    """Live search across all topics on the cluster."""
    if not q or len(q) < 2:
        return {"topics": [], "query": q}
    try:
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text as _tq
        if SessionLocal:
            async with SessionLocal() as sess:
                rows = await sess.execute(_tq("""
                    SELECT topic, size_bytes, partition_count, replication_factor, bytes_in_per_sec
                    FROM kafka_topic_metrics
                    WHERE cluster_id=:cid AND topic ILIKE :q
                    ORDER BY size_bytes DESC LIMIT 50
                """), {"cid": int(cluster_id), "q": f"%{q}%"})
                topics = [{"name": r.topic, "topic": r.topic, "size_bytes": r.size_bytes,
                           "partition_count": r.partition_count, "replication_factor": r.replication_factor,
                           "bytes_in_per_sec": r.bytes_in_per_sec or 0.0}
                          for r in rows.fetchall()]
                cnt = await sess.execute(_tq(
                    "SELECT COUNT(*) FROM kafka_topic_metrics WHERE cluster_id=:cid AND topic ILIKE :q"
                ), {"cid": int(cluster_id), "q": f"%{q}%"})
                total = cnt.scalar() or 0
            return {"topics": topics, "query": q, "total_matches": total}
    except Exception as exc:
        return {"topics": [], "query": q, "error": str(exc)}


@router.get("/dashboard/groups/search")
async def search_groups(cluster_id: str, q: str = ""):
    """Search consumer groups by name."""
    if not q or len(q) < 2:
        return {"groups": [], "query": q}
    # Search from postgres
    try:
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text as _gst
        if SessionLocal:
            async with SessionLocal() as sess:
                rows = await sess.execute(_gst("""
                    SELECT group_id, state, total_lag, topic_count
                    FROM kafka_consumer_group_lag
                    WHERE cluster_id=:cid AND group_id ILIKE :q
                    ORDER BY total_lag DESC LIMIT 100
                """), {"cid": int(cluster_id), "q": f"%{q}%"})
                matched = [{"group_id": r.group_id, "state": r.state,
                            "total_lag": r.total_lag, "topic_count": r.topic_count}
                           for r in rows.fetchall()]
    except Exception:
        matched = []
    if not matched:
        return {"groups": matched, "query": q}
    # Fetch real lag for matched groups
    collector = await _collector_for_cluster(cluster_id)
    try:
        lags = await collector.fetch_group_lags([g["group_id"] for g in matched[:50]])
        lag_map = {g["group_id"]: g for g in lags}
        for g in matched:
            lag_data = lag_map.get(g["group_id"])
            if lag_data:
                g["total_lag"] = lag_data["total_lag"]
                g["topic_count"] = lag_data["topic_count"]
                g["partitions"] = lag_data.get("partitions", [])
        return {"groups": matched, "query": q}
    except Exception as exc:
        return {"groups": matched, "query": q, "error": str(exc)}


@router.get("/dashboard/schemas/stream")
async def stream_schema_details(cluster_id: str, limit: int = 50):
    """Stream schema registry subject details — top N subjects, rest via search."""
    from tools.schema_registry import SchemaRegistryCollector
    cluster = await get_backend().get_cluster(int(cluster_id))
    if not cluster or not cluster.get("schema_registry_url"):
        return {"subjects": [], "status": "not_configured"}
    sr = SchemaRegistryCollector(cluster["schema_registry_url"],
        username=cluster.get("schema_registry_username"),
        password=cluster.get("schema_registry_password"),
        topics=[])
    try:
        result = await sr.collect()
        total_subjects = result.get("subject_count", len(result.get("subjects", [])))
        if result.get("subjects") and len(result["subjects"]) > limit:
            result["subjects"] = result["subjects"][:limit]
        result["total_subjects"] = total_subjects
        return result
    except Exception as exc:
        return {"subjects": [], "error": str(exc)}


@router.get("/dashboard/connectors/search")
async def search_connectors(cluster_id: str, q: str = ""):
    """Search connectors by name."""
    if not q or len(q) < 2:
        return {"connectors": [], "query": q}
    try:
        kc = await get_kafka_connect(cluster_id=cluster_id)
        all_connectors = kc.get("connectors", [])
        ql = q.lower()
        matched = [c for c in all_connectors if ql in c.get("name", "").lower()][:50]
    except Exception:
        matched = []
    return {"connectors": matched, "query": q}

@router.get("/dashboard/schemas/search")
async def search_schemas(cluster_id: str, q: str = ""):
    """Search schema subjects by name."""
    if not q or len(q) < 2:
        return {"subjects": [], "query": q}
    from tools.schema_registry import SchemaRegistryCollector
    cluster = await get_backend().get_cluster(int(cluster_id))
    if not cluster or not cluster.get("schema_registry_url"):
        return {"subjects": [], "query": q}
    sr = SchemaRegistryCollector(cluster["schema_registry_url"],
        username=cluster.get("schema_registry_username"),
        password=cluster.get("schema_registry_password"),
        topics=[])
    try:
        result = await sr.collect()
        ql = q.lower()
        matched = [s for s in result.get("subjects", []) if ql in s.get("subject", "").lower()][:50]
        return {"subjects": matched, "query": q}
    except Exception as exc:
        return {"subjects": [], "query": q, "error": str(exc)}


@router.get("/dashboard/prometheus-debug")
async def prometheus_debug() -> dict:
    """Debug endpoint — shows Prometheus broker state with sample values."""
    try:
        from tools.prometheus_collector import _broker_state
        details = {}
        for host, state in _broker_state.items():
            metrics = state.get('metrics', {})
            details[host] = {
                "scrape_time": round(state.get('time', 0), 1),
                "metric_names": sorted(metrics.keys())[:30],
                "total_metric_count": len(metrics),
                "msgs_total": [e['value'] for e in metrics.get('kafka_server_brokertopicmetrics_messagesin_total', [])],
                "bytesin_total": [e['value'] for e in metrics.get('kafka_server_brokertopicmetrics_bytesin_total', [])],
                "heap_used": [e for e in metrics.get('jvm_memory_bytes_used', [])],
                "produce_latency": [e for e in metrics.get('kafka_network_requestmetrics_totaltimems', []) if e.get('labels', {}).get('request') == 'Produce'][:3],
            }
        return {"broker_count": len(_broker_state), "details": details}
    except Exception as exc:
        return {"error": str(exc)}
