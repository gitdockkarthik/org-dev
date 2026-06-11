from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse
from tools.real_kafka import RealKafkaCollector
from storage import get_backend
import json
import asyncio

import httpx
import os

import kafka_store

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
    return {
        "cluster": {
            **data["cluster"],
            "topic_count": len(topics),
            "consumer_group_count": len(consumer_groups),
        },
        "brokers": data["brokers"],
        "anomalies": data["anomalies"],
        "topic_count": len(topics),
        "consumer_group_count": len(consumer_groups),
    }


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
    """Per-broker CPU, heap, GC, and URP metrics."""
    data = kafka_store.get_cluster_data(cluster_id, hours=hours)
    if data is None:
        return {"empty": True}
    return {"brokers": data["brokers"]}


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


@router.get("/dashboard/topics/history")
async def get_topics_history(cluster_id: str | None = None, minutes: float = 1440.0, hours: float | None = None) -> dict:
    """Return per-topic msgs/sec trend from PostgreSQL for timeline chart."""
    from storage import get_backend
    if not cluster_id:
        return {"empty": True, "series": []}
    # Support both minutes and legacy hours parameter
    effective_minutes = minutes
    if hours is not None:
        effective_minutes = hours * 60

    # Use daily aggregation for 7-day view (10080 minutes)
    if effective_minutes >= 10080:
        try:
            rows = await get_backend().get_topic_history_daily(int(cluster_id), days=7)
        except Exception:
            return {"empty": True, "series": []}
        if not rows:
            return {"empty": True, "series": []}
        from collections import defaultdict
        from datetime import datetime, timedelta, timezone
        # Always generate all 7 days regardless of data availability
        today = datetime.now(timezone.utc).date()
        all_days = [(today - timedelta(days=6-i)).isoformat() for i in range(7)]
        topic_data: dict[str, dict[str, float]] = defaultdict(dict)
        for r in rows:
            if not r["topic"].startswith("_"):
                topic_data[r["topic"]][r["day"]] = r["avg_msgs"]
        if topic_data:
            topic_maxes = {t: max(v.values()) for t, v in topic_data.items()}
            top_topics = sorted(topic_maxes, key=lambda n: topic_maxes[n], reverse=True)[:5]
        else:
            top_topics = []
        series = []
        for name in top_topics:
            vals = [round(topic_data[name].get(d, 0.0), 3) for d in all_days]
            series.append({"topic": name, "values": vals})
        # If no data at all, return empty series with 7-day labels
        if not series:
            return {"labels": all_days, "series": [], "snapshot_count": 7}
        return {"labels": all_days, "series": series, "snapshot_count": 7}

    # Determine bucket size based on time range
    from datetime import datetime, timedelta, timezone
    if effective_minutes <= 5:
        bucket_minutes = 1
        total_buckets = 5
    elif effective_minutes <= 60:
        bucket_minutes = 10
        total_buckets = 6
    elif effective_minutes <= 360:
        bucket_minutes = 30
        total_buckets = 12
    else:
        bucket_minutes = 60
        total_buckets = 24

    try:
        rows = await get_backend().get_topic_history_bucketed(
            int(cluster_id), minutes=effective_minutes, bucket_minutes=bucket_minutes
        )
    except Exception:
        return {"empty": True, "series": []}

    # Generate all expected bucket timestamps (zero-filled). Keep parsed
    # datetimes alongside the ISO labels so the snap loop below doesn't
    # re-parse on every row.
    now = datetime.now(timezone.utc)
    # Round now down to current bucket boundary
    now_minute = (now.minute // bucket_minutes) * bucket_minutes
    bucket_end = now.replace(minute=now_minute, second=0, microsecond=0)
    all_buckets = []          # ISO labels (returned to the client)
    bucket_dts = []           # parsed datetimes (used for snapping)
    for i in range(total_buckets - 1, -1, -1):
        b = bucket_end - timedelta(minutes=i * bucket_minutes)
        all_buckets.append(b.isoformat())
        bucket_dts.append(b)

    from collections import defaultdict
    topic_data: dict[str, dict[str, float]] = defaultdict(dict)
    for r in rows:
        if not r["topic"].startswith("_"):
            # Find closest bucket. Coerce naive timestamps to UTC so the
            # subtraction never hits an aware/naive mismatch (the column is
            # timestamptz, but a non-UTC session or naive backend could
            # otherwise break this).
            rt = datetime.fromisoformat(r["time"])
            if rt.tzinfo is None:
                rt = rt.replace(tzinfo=timezone.utc)
            idx = min(
                range(len(bucket_dts)),
                key=lambda j: abs((bucket_dts[j] - rt).total_seconds()),
            )
            topic_data[r["topic"]][all_buckets[idx]] = r["avg_msgs"]

    if topic_data:
        topic_maxes = {t: max(v.values()) for t, v in topic_data.items()}
        top_topics = sorted(topic_maxes, key=lambda n: topic_maxes[n], reverse=True)[:5]
    else:
        top_topics = []

    series = []
    for name in top_topics:
        vals = [round(topic_data[name].get(b, 0.0), 3) for b in all_buckets]
        series.append({"topic": name, "values": vals})

    if not series:
        return {"labels": all_buckets, "series": [], "snapshot_count": total_buckets}
    return {"labels": all_buckets, "series": series, "snapshot_count": total_buckets}


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

    data = kafka_store.get_cluster_data(cluster_id) if cluster_id else (
        kafka_store.get_cluster_data(kafka_store.get_all_cluster_ids()[0])
        if kafka_store.get_all_cluster_ids() else None
    )
    if not data:
        async def empty():
            yield 'data: No cluster data available. Run a sync first.\n\n'
            yield 'data: [DONE]\n\n'
        return StreamingResponse(empty(),
            media_type="text/event-stream")

    api_key = request.headers.get("x-anthropic-key", "") or \
              os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        async def nokey():
            yield 'data: Anthropic API key not configured.\n\n'
            yield 'data: [DONE]\n\n'
        return StreamingResponse(nokey(),
            media_type="text/event-stream")

    brokers = data.get("brokers", [])
    topics = data.get("topics", {})
    groups = data.get("consumer_groups", {})
    anomalies = data.get("anomalies", [])

    # Compute aggregates
    total_partitions = sum(t.get("partition_count", 0) for t in topics)
    total_urps = sum(b.get("urp_count", 0) for b in brokers)
    avg_heap = round(sum(b.get("heap_pct", 0) for b in brokers) / max(len(brokers), 1), 1)
    avg_cpu = round(sum(b.get("cpu_pct", 0) for b in brokers) / max(len(brokers), 1), 1)
    avg_req_idle = round(sum(b.get("request_handler_idle_pct", 0) for b in brokers) / max(len(brokers), 1), 1)

    # Top topics by traffic
    sorted_by_msgs = sorted(topics, key=lambda t: t.get("messages_in_per_sec", 0), reverse=True)[:10]
    top_topics = [{"name": t.get("name"), "msgs_sec": t.get("messages_in_per_sec", 0),
                   "size_bytes": t.get("size_bytes", 0), "partitions": t.get("partition_count", 0),
                   "rf": t.get("replication_factor", 0)} for t in sorted_by_msgs]

    # Stale topics
    stale_topics = [t.get("name") for t in topics if t.get("messages_in_per_sec", 0) == 0 and t.get("size_bytes", 0) > 1000]

    # Consumer group health
    critical_groups = [{"name": g.get("group_id") or g.get("name"), "lag": g.get("total_lag", 0),
                        "trend": g.get("lag_trend")} for g in groups if g.get("total_lag", 0) > 10000]
    warning_groups = [{"name": g.get("group_id") or g.get("name"), "lag": g.get("total_lag", 0)}
                      for g in groups if 1000 < g.get("total_lag", 0) <= 10000]
    healthy_groups = len([g for g in groups if g.get("total_lag", 0) <= 1000])

    # Pre-build structures (avoid dict literals inside f-string expressions)
    broker_details = [{"id": b.get("id"), "heap": b.get("heap_pct"),
        "cpu": b.get("cpu_pct"), "urp": b.get("urp_count"),
        "gc_pause_ms": b.get("gc_pause_ms"),
        "produce_latency_ms": b.get("produce_latency_ms"),
        "fetch_latency_ms": b.get("fetch_latency_ms"),
        "bytes_in": b.get("bytes_in_per_sec"),
        "bytes_out": b.get("bytes_out_per_sec"),
        "isr_shrinks": b.get("isr_shrinks_per_sec"),
        "req_idle": b.get("request_handler_idle_pct")} for b in brokers]

    anomaly_details = [{"severity": a.get("severity"),
        "category": a.get("category"),
        "description": a.get("description")} for a in anomalies[:10]]

    low_rep_count = len([t for t in topics if t.get("replication_factor", 0) == 1])

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

Brokers ({len(brokers)} active):
- Average Heap: {avg_heap}%
- Average CPU: {avg_cpu}%
- Average Request Handler Idle: {avg_req_idle}%
- Under-replicated Partitions: {total_urps}
- Broker details: {broker_details}

Topics ({len(topics)} total, {total_partitions} partitions):
- Top 10 by traffic: {top_topics}
- Stale topics (data but no traffic): {stale_topics[:20]}
- Low replication (RF=1): {low_rep_count} topics

Consumer Groups ({len(groups)} total):
- Critical (lag >10k): {critical_groups}
- Warning (lag 1k-10k): {warning_groups}
- Healthy (lag <1k): {healthy_groups} groups

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

    data = kafka_store.get_cluster_data(cluster_id) if cluster_id else (
        kafka_store.get_cluster_data(kafka_store.get_all_cluster_ids()[0])
        if kafka_store.get_all_cluster_ids() else None
    )
    if not data:
        async def empty():
            yield 'data: No cluster data available. Run a sync first.\n\n'
            yield 'data: [DONE]\n\n'
        return StreamingResponse(empty(),
            media_type="text/event-stream")

    api_key = request.headers.get("x-anthropic-key", "") or \
              os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        async def nokey():
            yield 'data: Anthropic API key not configured.\n\n'
            yield 'data: [DONE]\n\n'
        return StreamingResponse(nokey(),
            media_type="text/event-stream")

    concise = "Keep it concise, 200-400 words. No tables. Short paragraphs."

    if tab == "brokers":
        brokers = data.get("brokers", [])
        broker_details = [{"id": b.get("id"), "heap": b.get("heap_pct"),
            "cpu": b.get("cpu_pct"), "urp": b.get("urp_count"),
            "gc_pause_ms": b.get("gc_pause_ms"),
            "produce_latency_ms": b.get("produce_latency_ms"),
            "fetch_latency_ms": b.get("fetch_latency_ms"),
            "bytes_in": b.get("bytes_in_per_sec"),
            "bytes_out": b.get("bytes_out_per_sec"),
            "isr_shrinks": b.get("isr_shrinks_per_sec"),
            "req_idle": b.get("request_handler_idle_pct")} for b in brokers]
        prompt = f"""You are a Kafka broker health specialist. Analyse these broker metrics and provide:
## Health Assessment — overall broker health, any at risk
## Key Concerns — CPU/heap pressure, GC issues, latency anomalies, ISR instability
## Recommendations — prioritised actions for broker health
{concise}

BROKER DATA ({len(brokers)} brokers):
{broker_details}
"""

    elif tab == "topics":
        topics = data.get("topics", [])
        total_partitions = sum(t.get("partition_count", 0) for t in topics)
        sorted_by_msgs = sorted(topics, key=lambda t: t.get("messages_in_per_sec", 0), reverse=True)[:10]
        top_topics = [{"name": t.get("name"), "msgs_sec": t.get("messages_in_per_sec", 0),
                       "size_bytes": t.get("size_bytes", 0), "partitions": t.get("partition_count", 0),
                       "rf": t.get("replication_factor", 0)} for t in sorted_by_msgs]
        stale_topics = [t.get("name") for t in topics if t.get("messages_in_per_sec", 0) == 0 and t.get("size_bytes", 0) > 1000]
        low_rep_count = len([t for t in topics if t.get("replication_factor", 0) == 1])
        prompt = f"""You are a Kafka topic intelligence specialist. Analyse these topics and provide:
## Topic Health Overview — traffic patterns, stale topics, partition balance
## Risk Areas — under-replicated, orphaned, anomalous growth
## Recommendations — cleanup candidates, replication fixes, partition rebalancing
{concise}

TOPIC DATA ({len(topics)} total, {total_partitions} partitions):
- Top 10 by traffic: {top_topics}
- Stale topics (data but no traffic): {stale_topics[:20]}
- Low replication (RF=1): {low_rep_count} topics
"""

    elif tab == "consumer-groups":
        groups = data.get("consumer_groups", [])
        critical_groups = [{"name": g.get("group_id") or g.get("name"), "lag": g.get("total_lag", 0),
                            "trend": g.get("lag_trend")} for g in groups if g.get("total_lag", 0) > 10000]
        warning_groups = [{"name": g.get("group_id") or g.get("name"), "lag": g.get("total_lag", 0)}
                          for g in groups if 1000 < g.get("total_lag", 0) <= 10000]
        healthy_groups = len([g for g in groups if g.get("total_lag", 0) <= 1000])
        prompt = f"""You are a Kafka consumer lag analyst. Analyse these consumer groups and provide:
## Lag Assessment — which groups are critical, trend direction
## Business Impact — what does this lag mean for data freshness and processing
## Recommendations — how to reduce lag, which groups to prioritise
{concise}

CONSUMER GROUP DATA ({len(groups)} total):
- Critical (lag >10k): {critical_groups}
- Warning (lag 1k-10k): {warning_groups}
- Healthy (lag <1k): {healthy_groups} groups
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
async def stream_topic_details(cluster_id: str):
    """Stream topic details on-demand — fetches partition/URP for batches of topics."""
    collector = await _collector_for_cluster(cluster_id)
    data = kafka_store.get_cluster_data(cluster_id)
    all_names = [t["name"] for t in (data or {}).get("topics", [])]
    if not all_names:
        return {"topics": [], "total": 0}

    async def generate():
        _BATCH = 100
        sent = 0
        total = len(all_names)
        for i in range(0, total, _BATCH):
            batch_names = all_names[i:i + _BATCH]
            try:
                details = await collector.fetch_topic_details(batch_names)
                for t in details:
                    yield f"data: {json.dumps(t)}\n\n"
                    sent += 1
            except Exception as exc:
                yield f"data: {json.dumps({'error': str(exc)})}\n\n"
        yield f"data: {json.dumps({'done': True, 'total': sent})}\n\n"

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
    group_ids = [g["group_id"] for g in all_groups][:200]

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
        yield f"data: {json.dumps({'done': True, 'total': sent})}\n\n"

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
async def stream_schema_details(cluster_id: str):
    """Stream schema registry subject details on-demand."""
    from tools.schema_registry import SchemaRegistryCollector
    cluster = await get_backend().get_cluster(int(cluster_id))
    if not cluster or not cluster.get("schema_registry_url"):
        return {"subjects": [], "status": "not_configured"}
    sr = SchemaRegistryCollector(cluster["schema_registry_url"])
    try:
        result = await sr.collect()
        return result
    except Exception as exc:
        return {"subjects": [], "error": str(exc)}
