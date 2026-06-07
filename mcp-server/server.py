"""Operative Intelligence MCP Server.

A Model Context Protocol gateway that exposes all agents
(Kafka Analyser, Alert Analyser, CUR Analyser) as MCP tools.

Translates MCP tool calls → REST API calls to existing agent endpoints.
Zero changes to agent code required.
"""

import os
import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

# ── Config ──────────────────────────────────────────────────────────────
KAFKA_URL = os.environ.get("KAFKA_ANALYSER_URL", "http://kafka-analyser:8003")
ALERT_URL = os.environ.get("ALERT_ANALYSER_URL", "http://alert-analyser:8001")
CUR_URL = os.environ.get("CUR_ANALYSER_URL", "http://cur-analyser:8002")

mcp = FastMCP(
    "Operative Intelligence",
    host="0.0.0.0",
    port=8005,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)

_client = httpx.AsyncClient(timeout=30.0)


async def _get(base_url: str, path: str, params: dict | None = None) -> dict:
    resp = await _client.get(f"{base_url}{path}", params=params)
    resp.raise_for_status()
    return resp.json()


async def _post(base_url: str, path: str, json: dict | None = None) -> dict:
    resp = await _client.post(f"{base_url}{path}", json=json)
    resp.raise_for_status()
    return resp.json()


@mcp.tool()
async def kafka_cluster_overview(cluster_id: str | None = None) -> dict:
    """Get Kafka cluster health overview — health score, broker count, anomalies, topic and consumer group counts."""
    params = {}
    if cluster_id:
        params["cluster_id"] = cluster_id
    return await _get(KAFKA_URL, "/dashboard/overview", params)


@mcp.tool()
async def kafka_brokers(cluster_id: str | None = None) -> dict:
    """Get per-broker metrics — heap %, CPU %, GC pauses, msgs/sec, bytes in/out, ISR shrink/expand rates, produce/fetch latency."""
    params = {}
    if cluster_id:
        params["cluster_id"] = cluster_id
    return await _get(KAFKA_URL, "/dashboard/brokers", params)


@mcp.tool()
async def kafka_topics(cluster_id: str | None = None) -> dict:
    """Get all topics with partition count, replication factor, msgs/sec, bytes/sec, log size, and health status."""
    params = {}
    if cluster_id:
        params["cluster_id"] = cluster_id
    return await _get(KAFKA_URL, "/dashboard/topics", params)


@mcp.tool()
async def kafka_topic_history(cluster_id: str, hours: float = 24.0) -> dict:
    """Get per-topic message rate history over time from PostgreSQL. Returns time-series data for trend charts."""
    return await _get(KAFKA_URL, "/dashboard/topics/history", {
        "cluster_id": cluster_id,
        "hours": hours,
    })


@mcp.tool()
async def kafka_consumer_groups(cluster_id: str | None = None) -> dict:
    """Get all consumer groups with state (Stable/Empty/Dead), topic assignments, and consumer lag per partition."""
    params = {}
    if cluster_id:
        params["cluster_id"] = cluster_id
    return await _get(KAFKA_URL, "/dashboard/consumer-groups", params)


@mcp.tool()
async def kafka_schema_registry(cluster_id: str | None = None) -> dict:
    """Get Schema Registry subjects, versions, and compatibility config for the selected cluster."""
    params = {}
    if cluster_id:
        params["cluster_id"] = cluster_id
    return await _get(KAFKA_URL, "/dashboard/schema-registry", params)


@mcp.tool()
async def kafka_zookeeper(cluster_id: str | None = None) -> dict:
    """Get ZooKeeper status — mode, version, latency, connections. Shows KRaft fallback if no ZK."""
    params = {}
    if cluster_id:
        params["cluster_id"] = cluster_id
    return await _get(KAFKA_URL, "/dashboard/zookeeper", params)


@mcp.tool()
async def kafka_connect(cluster_id: str | None = None) -> dict:
    """Get Kafka Connect connector status and task health. Supports 250+ connectors."""
    params = {}
    if cluster_id:
        params["cluster_id"] = cluster_id
    return await _get(KAFKA_URL, "/dashboard/kafka-connect", params)


@mcp.tool()
async def kafka_mirrormaker(cluster_id: str | None = None) -> dict:
    """Get MirrorMaker status — MM1/MM2 detection, replication topology, cross-cluster lag."""
    params = {}
    if cluster_id:
        params["cluster_id"] = cluster_id
    return await _get(KAFKA_URL, "/dashboard/mirrormaker", params)


@mcp.tool()
async def kafka_clusters() -> dict:
    """List all registered Kafka clusters with bootstrap servers, auth type, JMX port, environment, and enabled status."""
    return await _get(KAFKA_URL, "/settings/clusters")


@mcp.tool()
async def kafka_sync() -> dict:
    """Trigger immediate sync of all enabled Kafka clusters."""
    return await _post(KAFKA_URL, "/settings/sync")


@mcp.tool()
async def alert_dashboard() -> dict:
    """Get Alert Analyser dashboard — total alerts, genuine/noise/suspect counts, team breakdown, top noise sources."""
    return await _get(ALERT_URL, "/dashboard")


@mcp.tool()
async def alert_settings() -> dict:
    """Get Alert Analyser settings — OpsGenie config, noise thresholds, priority weights."""
    return await _get(ALERT_URL, "/settings")


@mcp.tool()
async def alert_sync() -> dict:
    """Trigger OpsGenie alert sync — fetches alerts, classifies noise, stores report."""
    return await _post(ALERT_URL, "/settings/sync")


@mcp.tool()
async def cur_dashboard(report_id: int | None = None) -> dict:
    """Get CUR dashboard — cost breakdown by service, account, environment, tags."""
    params = {}
    if report_id:
        params["report_id"] = report_id
    return await _get(CUR_URL, "/dashboard", params)


@mcp.tool()
async def cur_reports() -> dict:
    """List all uploaded CUR reports with upload date, row count, and date range."""
    return await _get(CUR_URL, "/reports")


@mcp.tool()
async def cur_generate_sample() -> dict:
    """Generate a 31-column, 2000-row sample CUR dataset for demonstration."""
    return await _post(CUR_URL, "/reports/generate-sample")


@mcp.tool()
async def cur_compare_reports(report_ids: str) -> dict:
    """Compare multiple CUR reports side-by-side. Pass comma-separated IDs (e.g. '1,2,3')."""
    return await _get(CUR_URL, "/reports/compare", {"ids": report_ids})


@mcp.tool()
async def platform_health() -> dict:
    """Check health of all agents in the platform."""
    results = {}
    for name, url in [("kafka-analyser", KAFKA_URL), ("alert-analyser", ALERT_URL), ("cur-analyser", CUR_URL)]:
        try:
            resp = await _client.get(f"{url}/health", timeout=5.0)
            results[name] = resp.json() if resp.status_code == 200 else {"status": "error", "code": resp.status_code}
        except Exception as e:
            results[name] = {"status": "unreachable", "error": str(e)}
    return results


if __name__ == "__main__":
    mcp.run(transport="sse")
