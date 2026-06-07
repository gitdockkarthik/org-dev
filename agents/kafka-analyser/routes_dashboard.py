from fastapi import APIRouter, Request

import httpx
import os

import kafka_store

router = APIRouter(tags=["dashboard"])


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

    try:
        rows = await get_backend().get_topic_history(int(cluster_id), minutes=effective_minutes)
    except Exception:
        return {"empty": True, "series": []}
    if not rows:
        return {"empty": True, "series": []}

    # Group by time then topic
    from collections import defaultdict
    times: list[str] = sorted(set(r["time"] for r in rows))
    topic_data: dict[str, dict[str, float]] = defaultdict(dict)
    for r in rows:
        if not r["topic"].startswith("_"):
            topic_data[r["topic"]][r["time"]] = r["messages_in_per_sec"]

    # Top 5 topics by max msgs/sec
    topic_maxes = {t: max(v.values()) for t, v in topic_data.items()}
    top_topics = sorted(topic_maxes, key=lambda n: topic_maxes[n], reverse=True)[:5]

    labels = [t for t in times]
    series = []
    for name in top_topics:
        vals = [round(topic_data[name].get(ts, 0.0), 3) for ts in times]
        series.append({"topic": name, "values": vals})

    return {"labels": labels, "series": series, "snapshot_count": len(times)}


@router.post("/dashboard/insights/narrative/stream")
async def stream_insights_narrative(
    request: Request,
    cluster_id: str | None = None
):
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

    # Build same prompt as existing narrative endpoint
    # Reuse the prompt building logic from get_insights_narrative
    brokers = data.get("brokers", [])
    topics  = data.get("topics", {})
    groups  = data.get("consumer_groups", {})
    anomalies = data.get("anomalies", [])

    prompt = f"""You are a senior Kafka platform engineer.
Analyse this Kafka cluster and produce a concise markdown report with:
## Cluster Health Summary
## Top 3 Issues & Recommendations
## Consumer Group Health
## Capacity & Performance Observations

Cluster data:
- Brokers: {len(brokers)} active
- Topics: {len(topics)}
- Consumer groups: {len(groups)}
- Anomalies: {[a.get('description','') for a in anomalies[:5]]}
- Broker details: {[{'id':b.get('id'),'heap':b.get('heap_pct'),'cpu':b.get('cpu_pct'),'urp':b.get('urp_count')} for b in brokers[:3]]}
"""

    async def event_stream():
        try:
            client = _anthropic.AsyncAnthropic(api_key=api_key)
            async with client.messages.stream(
                model="claude-sonnet-4-6",
                max_tokens=2000,
                messages=[{"role":"user","content":prompt}],
            ) as stream:
                async for text in stream.text_stream:
                    escaped = text.replace("\n","\\n")
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
