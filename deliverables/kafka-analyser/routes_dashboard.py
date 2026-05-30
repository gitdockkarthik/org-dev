from fastapi import APIRouter

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
