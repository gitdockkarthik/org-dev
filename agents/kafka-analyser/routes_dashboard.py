from fastapi import APIRouter, Request

import httpx
import os

import kafka_store

router = APIRouter(tags=["dashboard"])


@router.get("/dashboard/overview")
async def get_overview() -> dict:
    """Cluster health, broker status, anomaly summary."""
    data = kafka_store.get_cluster_data()
    if data is None:
        return {"empty": True}
    return {
        "cluster": data["cluster"],
        "brokers": data["brokers"],
        "anomalies": data["anomalies"],
    }


@router.get("/dashboard/consumer-groups")
async def get_consumer_groups() -> dict:
    """Consumer group lag leaderboard sorted worst-first."""
    data = kafka_store.get_cluster_data()
    if data is None:
        return {"empty": True}
    groups = sorted(data["consumer_groups"], key=lambda g: g["total_lag"], reverse=True)
    return {"consumer_groups": groups}


@router.get("/dashboard/topics")
async def get_topics() -> dict:
    """Topic metrics sorted by message rate descending."""
    data = kafka_store.get_cluster_data()
    if data is None:
        return {"empty": True}
    topics = sorted(data["topics"], key=lambda t: t["messages_in_per_sec"], reverse=True)
    return {"topics": topics}


@router.get("/dashboard/brokers")
async def get_brokers() -> dict:
    """Per-broker CPU, heap, GC, and URP metrics."""
    data = kafka_store.get_cluster_data()
    if data is None:
        return {"empty": True}
    return {"brokers": data["brokers"]}


@router.get("/dashboard/connectors")
async def get_connectors() -> dict:
    """Connector state and per-task health."""
    data = kafka_store.get_cluster_data()
    if data is None:
        return {"empty": True}
    return {"connectors": data["connectors"]}


@router.get("/dashboard/insights")
async def get_insights() -> dict:
    """Active anomalies with severity, root cause, and recommendations."""
    data = kafka_store.get_cluster_data()
    if data is None:
        return {"empty": True}
    return {"anomalies": data["anomalies"]}


@router.post("/dashboard/insights/narrative")
async def get_insights_narrative(request: Request) -> dict:
    """Generate AI narrative summary of current cluster health."""
    data = kafka_store.get_cluster_data()
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
