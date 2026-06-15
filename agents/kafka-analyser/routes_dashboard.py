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

_lag_trend_cache: dict = {}
_LAG_TREND_CACHE_TTL_SECS = 300  # 5 minutes — matches collection interval


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
    data = kafka_store.get_cluster_data(cluster_id, hours=hours)
    if data is None:
        return {"empty": True}
    topics = data["topics"]
    consumer_groups = data["consumer_groups"]
    brokers = data.get("brokers", [])
    # Compute health score from real metrics
    score = 100
    # URP deduction
    total_urp = data.get("counts", {}).get("total_urp", 0)
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
    rf1 = data.get("counts", {}).get("total_rf1", 0)
    if rf1 > 100:
        score -= 10
    elif rf1 > 0:
        score -= 5
    health_score = max(0, min(100, score))
    cluster = {**data["cluster"], "health_score": health_score}
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
            "topic_count": data.get("counts", {}).get("total_topics") or len(topics),
            "consumer_group_count": data.get("counts", {}).get("total_groups") or len(consumer_groups),
            "critical_count": len(critical_groups),
        },
        "brokers": brokers,
        "anomalies": data.get("anomalies", []),
        "topic_count": data.get("counts", {}).get("total_topics") or len(topics),
        "consumer_group_count": data.get("counts", {}).get("total_groups") or len(consumer_groups),
        "health_score": health_score,
        "critical_count": len(critical_groups),
    }


@router.get("/dashboard/counts")
async def get_counts(cluster_id: str | None = None) -> dict:
    """Cluster counts — reads from DB directly, not cache."""
    if not cluster_id:
        return {"empty": True}
    try:
        from storage import get_backend
        import json as _json
        all_cfg = await get_backend().get_all()

        # Structure counts live in kafka_metrics_history (scan_type='topics_structure')
        from database import SessionLocal
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

        return {
            "total_topics": structure.get("total_topics", 0),
            "total_groups": structure.get("total_groups", 0),
            "total_brokers": structure.get("total_brokers", len(brokers)),
            "total_connectors": 0,
            "total_rf1": structure.get("total_rf1", 0),
            "total_urp": structure.get("total_urp", 0),
            "total_partitions": structure.get("total_partitions", 0),
            "top_topics_by_size": metrics.get("top_topics_by_size", []),
            "top_topics_by_msg_rate": metrics.get("top_topics_by_msg_rate", []),
            "total_hot": metrics.get("total_hot", 0),
        }
    except Exception as _e:
        return {"empty": True, "error": str(_e)}


@router.get("/dashboard/topics/detail")
async def get_topic_detail(name: str, cluster_id: str | None = None) -> dict:
    """Describe a single topic live from Kafka — partitions, RF, leaders, ISR."""
    from storage import get_backend
    clusters = await get_backend().get_clusters("kafka-analyser")
    c = next((c for c in clusters if str(c.get("id")) == str(cluster_id) and c.get("enabled")), None)
    if not c:
        return {"error": "Cluster not found"}
    try:
        from tools.real_kafka import RealKafkaCollector
        collector = RealKafkaCollector({
            "bootstrap_servers": c["bootstrap_servers"],
            "auth_type": "none" if c["auth_type"] == "none" else "sasl",
            "sasl_username": c.get("sasl_username"),
            "sasl_password": c.get("sasl_password"),
            "sasl_mechanism": c.get("sasl_mechanism", "PLAIN"),
            "tls_enabled": c.get("tls_enabled", False),
            "cluster_label": c["name"],
        })
        # Describe single topic
        described, _ = await collector.describe_all_topics([name], workers=1)
        if not described:
            return {"error": "Topic not found"}
        topic = described[0]
        # Get topic metrics from cache
        import kafka_store
        data = kafka_store.get_cluster_data(cluster_id)
        cached_topic = next((t for t in data.get("topics", []) if t.get("name") == name), {}) if data else {}
        return {
            "name": name,
            "partition_count": topic.get("partition_count", 0),
            "replication_factor": topic.get("replication_factor", 0),
            "under_replicated_partitions": topic.get("under_replicated_partitions", 0),
            "messages_in_per_sec": cached_topic.get("messages_in_per_sec", 0.0),
            "bytes_in_per_sec": cached_topic.get("bytes_in_per_sec", 0.0),
            "size_bytes": cached_topic.get("size_bytes", 0),
            "partitions": topic.get("partitions", []),
        }
    except Exception as exc:
        return {"error": str(exc)}


@router.get("/dashboard/consumer-groups")
async def get_consumer_groups(cluster_id: str | None = None, hours: int | None = None) -> dict:
    """Consumer group lag leaderboard sorted worst-first."""
    data = kafka_store.get_cluster_data(cluster_id, hours=hours)
    if data is None:
        return {"empty": True}
    groups = sorted(data["consumer_groups"], key=lambda g: g["total_lag"], reverse=True)
    return {"consumer_groups": groups}


@router.get("/dashboard/topics")
async def get_topics(cluster_id: str | None = None, hours: int | None = None) -> dict:
    """Topic metrics sorted by message rate descending."""
    data = kafka_store.get_cluster_data(cluster_id, hours=hours)
    if data is None:
        return {"empty": True}
    topics = sorted(data["topics"], key=lambda t: t["messages_in_per_sec"], reverse=True)
    return {"topics": topics}


@router.get("/dashboard/brokers")
async def get_brokers(cluster_id: str | None = None, hours: int | None = None) -> dict:
    """Per-broker CPU, heap, GC, and URP metrics — reads from DB directly."""
    if not cluster_id:
        return {"empty": True}
    try:
        from database import SessionLocal
        from sqlalchemy import text as _text
        import json as _json
        if SessionLocal is None:
            return {"empty": True}
        async with SessionLocal() as _sess:
            _row = await _sess.execute(
                _text("""SELECT data_json FROM kafka_metrics_history
                         WHERE cluster_id = :cid AND scan_type = 'brokers'
                         ORDER BY collected_at DESC LIMIT 1"""),
                {"cid": cluster_id}
            )
            _r = _row.fetchone()
        if not _r:
            return {"empty": True}
        brokers = _json.loads(_r.data_json)
        if not brokers:
            return {"empty": True}
        return {"brokers": brokers}
    except Exception as _e:
        return {"empty": True, "error": str(_e)}


@router.get("/dashboard/connectors")
async def get_connectors(cluster_id: str | None = None, hours: int | None = None) -> dict:
    """Connector state and per-task health."""
    data = kafka_store.get_cluster_data(cluster_id, hours=hours)
    if data is None:
        return {"empty": True}
    return {"connectors": data["connectors"]}


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
    collector = SchemaRegistryCollector(sr_url)
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
    collector = ZooKeeperCollector(zk_url)
    return await collector.collect()


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
        return {
            "status": "not_configured",
            "message": "No Kafka Connect URL configured for this cluster. Edit the cluster in Settings to add one.",
            "connector_count": 0,
            "connectors": [],
        }

    from tools.kafka_connect import KafkaConnectCollector
    collector = KafkaConnectCollector(connect_url)
    return await collector.collect()


@router.get("/dashboard/mirrormaker")
async def get_mirrormaker(cluster_id: str | None = None, hours: int | None = None) -> dict:
    """Detect MirrorMaker replication and compare source/target lag."""
    data = kafka_store.get_cluster_data(cluster_id, hours=hours)
    if data is None:
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
        from database import SessionLocal
        from sqlalchemy import text
        if SessionLocal is None:
            return {"empty": True, "points": []}

        async with SessionLocal() as session:
            sql = f"""
                WITH buckets AS (
                    SELECT generate_series(
                        date_bin(
                            '{bucket_interval}'::INTERVAL,
                            NOW() - ((:minutes) * INTERVAL '1 minute'),
                            TIMESTAMP '2001-01-01'
                        ),
                        date_bin(
                            '{bucket_interval}'::INTERVAL,
                            NOW(),
                            TIMESTAMP '2001-01-01'
                        ),
                        '{bucket_interval}'::INTERVAL
                    ) AS bucket_time
                ),
                actuals AS (
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
                    GROUP BY date_bin(
                        '{bucket_interval}'::INTERVAL,
                        collected_at,
                        TIMESTAMP '2001-01-01'
                    )
                )
                SELECT
                    b.bucket_time,
                    COALESCE(a.avg_lag, 0) AS avg_lag
                FROM buckets b
                LEFT JOIN actuals a ON b.bucket_time = a.bucket_time
                ORDER BY b.bucket_time ASC
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
            {"time": str(row.bucket_time)[:16], "total_lag": int(row.avg_lag)}
            for row in rows
        ]

        result = {"empty": False, "points": points}
        _set_lag_trend_cached(_cache_key, result)
        return result

    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"lag_trend error: {e}")
        return {"empty": True, "points": []}


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

    # Determine hour buckets for selected window
    now = datetime.now(timezone.utc)
    if minutes <= 60:
        total_buckets = 2       # current hour + previous hour
        delta_hours = 2
    elif minutes <= 360:
        total_buckets = 6
        delta_hours = 6
    elif minutes <= 1440:
        total_buckets = 24
        delta_hours = 24
    elif minutes <= 10080:
        total_buckets = 168
        delta_hours = 168
    else:
        total_buckets = 720
        delta_hours = 720

    # Generate all expected hour bucket labels (zero-filled)
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    all_buckets = []
    bucket_dts = []
    for i in range(total_buckets - 1, -1, -1):
        b = current_hour - timedelta(hours=i)
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

    # Group rows by topic — snap to nearest hour bucket
    topic_data: dict[str, dict[str, float]] = defaultdict(dict)
    for r in rows:
        if r["topic"].startswith("_"):
            continue
        rt = datetime.fromisoformat(r["time"])
        if rt.tzinfo is None:
            rt = rt.replace(tzinfo=timezone.utc)
        # Snap to nearest bucket
        idx = min(
            range(len(bucket_dts)),
            key=lambda j: abs((bucket_dts[j] - rt).total_seconds()),
        )
        topic_data[r["topic"]][all_buckets[idx]] = r["avg_msgs"]

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
    import anthropic as _anthropic

    api_key = request.headers.get("x-anthropic-key", "") or \
              os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        async def nokey():
            yield 'data: Anthropic API key not configured.\n\n'
            yield 'data: [DONE]\n\n'
        return StreamingResponse(nokey(), media_type="text/event-stream")

    # Read all data from DB directly — no cache dependency
    import json as _json
    from database import SessionLocal
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
            client = _anthropic.AsyncAnthropic(api_key=api_key)
            async with client.messages.stream(
                model="claude-sonnet-4-6",
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
            ) as stream:
                async for text in stream.text_stream:
                    escaped = text.replace("\n","\\n")
                    yield f"data: {escaped}\n\n"
                try:
                    final_msg = await stream.get_final_message()
                    stop_reason = final_msg.stop_reason
                except Exception:
                    stop_reason = "end_turn"
                yield f"data: [STOP_REASON] {stop_reason}\n\n"
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
    import anthropic as _anthropic

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
    from database import SessionLocal as _SL2
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
            client = _anthropic.AsyncAnthropic(api_key=api_key)
            async with client.messages.stream(
                model="claude-sonnet-4-6",
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
            ) as stream:
                async for text in stream.text_stream:
                    escaped = text.replace("\n","\\n")
                    yield f"data: {escaped}\n\n"
                try:
                    final_msg = await stream.get_final_message()
                    stop_reason = final_msg.stop_reason
                except Exception:
                    stop_reason = "end_turn"
                yield f"data: [STOP_REASON] {stop_reason}\n\n"
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
    data = kafka_store.get_cluster_data(cluster_id)
    all_names = [t["name"] for t in (data or {}).get("topics", [])]
    total_count = (data or {}).get("counts", {}).get("total_topics", len(all_names))
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
    data = kafka_store.get_cluster_data(cluster_id)
    all_groups = [g for g in (data or {}).get("consumer_groups", [])
                  if g.get("state", "Unknown") not in ("Empty", "Dead")]
    if not all_groups:
        return {"groups": [], "total": 0}

    # Sort by state — active groups first, limit to 200
    group_ids = [g["group_id"] for g in all_groups][:2000]

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
    collector = await _collector_for_cluster(cluster_id)
    try:
        matched_names = await collector.search_topics(q)
        if not matched_names:
            return {"topics": [], "query": q}
        details = await collector.fetch_topic_details(matched_names[:50])
        return {"topics": details, "query": q, "total_matches": len(matched_names)}
    except Exception as exc:
        return {"topics": [], "query": q, "error": str(exc)}


@router.get("/dashboard/groups/search")
async def search_groups(cluster_id: str, q: str = ""):
    """Search consumer groups by name."""
    if not q or len(q) < 2:
        return {"groups": [], "query": q}
    data = kafka_store.get_cluster_data(cluster_id)
    all_groups = (data or {}).get("consumer_groups", [])
    ql = q.lower()
    matched = [g for g in all_groups if ql in g["group_id"].lower()][:100]
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
    sr = SchemaRegistryCollector(cluster["schema_registry_url"])
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
    data = kafka_store.get_cluster_data(cluster_id)
    all_connectors = (data or {}).get("connectors", [])
    ql = q.lower()
    matched = [c for c in all_connectors if ql in c.get("name", "").lower()][:50]
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
    sr = SchemaRegistryCollector(cluster["schema_registry_url"])
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
