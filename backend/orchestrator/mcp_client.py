"""MCP Client — routes agent calls through the MCP Server (policy hub).

Maps REST proxy paths to MCP tool names. Maintains a persistent
SSE session with the MCP server.
"""

import asyncio
import json
import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)

MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://mcp-server:8005")

# ── Path → MCP tool mapping ────────────────────────────────────────────
# Maps: (agent_slug, http_method, path_prefix) → mcp_tool_name
_TOOL_MAP: dict[tuple[str, str, str], str] = {
    # Kafka Analyser
    ("kafka-analyser", "GET", "dashboard/overview"): "kafka_cluster_overview",
    ("kafka-analyser", "GET", "dashboard/brokers"): "kafka_brokers",
    ("kafka-analyser", "GET", "dashboard/topics"): "kafka_topics",
    ("kafka-analyser", "GET", "dashboard/topics/detail"): "kafka_topic_detail",
    ("kafka-analyser", "GET", "dashboard/topics/history"): "kafka_topic_history",
    ("kafka-analyser", "GET", "dashboard/topics/name-search"): "kafka_topic_name_search",
    ("kafka-analyser", "GET", "dashboard/overview/lag-trend"): "kafka_lag_trend",
    ("kafka-analyser", "GET", "dashboard/consumer-groups"): "kafka_consumer_groups",
    ("kafka-analyser", "GET", "dashboard/schema-registry"): "kafka_schema_registry",
    ("kafka-analyser", "GET", "dashboard/zookeeper"): "kafka_zookeeper",
    ("kafka-analyser", "GET", "dashboard/kafka-connect"): "kafka_connect",
    ("kafka-analyser", "GET", "dashboard/mirrormaker"): "kafka_mirrormaker",
    ("kafka-analyser", "GET", "settings/clusters"): "kafka_clusters",
    ("kafka-analyser", "POST", "settings/sync"): "kafka_sync",
    # Alert Analyser
    ("alert-analyser", "GET", "dashboard"): "alert_dashboard",
    ("alert-analyser", "GET", "settings"): "alert_settings",
    ("alert-analyser", "POST", "settings/sync"): "alert_sync",
    # CUR Analyser
    ("cur-analyser", "GET", "dashboard"): "cur_dashboard",
    ("cur-analyser", "GET", "reports"): "cur_reports",
    ("cur-analyser", "POST", "reports/generate-sample"): "cur_generate_sample",
    ("cur-analyser", "GET", "reports/compare"): "cur_compare_reports",
    # Cross-agent
    ("_platform", "GET", "health"): "platform_health",
}


def _find_tool(slug: str, method: str, path: str) -> str | None:
    """Find MCP tool name for a given agent slug, HTTP method, and path."""
    method = method.upper()
    # Try exact match first
    key = (slug, method, path)
    if key in _TOOL_MAP:
        return _TOOL_MAP[key]
    # Try prefix match (e.g. reports/123/data matches reports)
    for (s, m, p), tool in _TOOL_MAP.items():
        if s == slug and m == method and path.startswith(p):
            return tool
    return None


def _extract_arguments(path: str, query_params: dict) -> dict:
    """Convert query params to MCP tool arguments."""
    args = {}
    for key, value in query_params.items():
        if key == "cluster_id":
            args["cluster_id"] = value
        elif key == "minutes":
            args["minutes"] = float(value)
        elif key == "hours":
            args["hours"] = float(value)
        elif key == "report_id":
            args["report_id"] = int(value)
        elif key == "ids":
            args["report_ids"] = value
        else:
            args[key] = value
    return args


class MCPClient:
    """Async MCP client that maintains an SSE session."""

    def __init__(self):
        self._session_url: str | None = None
        self._results: dict[int, asyncio.Future] = {}
        self._next_id = 1
        self._lock = asyncio.Lock()
        self._connected = False
        self._sse_task: asyncio.Task | None = None
        self._http = httpx.AsyncClient(timeout=30.0)

    async def connect(self):
        """Establish SSE connection and initialize MCP session."""
        if self._connected:
            return
        try:
            self._sse_http = httpx.AsyncClient(timeout=None)
            self._post_http = httpx.AsyncClient(timeout=30.0)

            # Start SSE listener — it will set session_url
            self._session_event = asyncio.Event()
            self._sse_task = asyncio.create_task(self._listen_sse())

            # Wait for session URL
            await asyncio.wait_for(self._session_event.wait(), timeout=10.0)

            if not self._session_url:
                raise RuntimeError("Failed to get MCP session URL")

            # Initialize MCP protocol
            await self._send("initialize", {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "operative-backend", "version": "1.0"},
            })
            self._connected = True
            logger.info("MCP client connected to %s (session: %s)", MCP_SERVER_URL, self._session_url)
        except Exception as exc:
            logger.warning("MCP client connection failed: %s", exc)
            self._connected = False
            raise

    async def _listen_sse(self):
        """Background task: keep SSE stream open and resolve futures."""
        try:
            async with self._sse_http.stream("GET", f"{MCP_SERVER_URL}/sse") as resp:
                _buf = ""
                async for line in resp.aiter_lines():
                    if not line.startswith("data:"):
                        # Empty line = end of SSE event — try to parse accumulated buffer
                        if _buf:
                            raw = _buf.strip()
                            _buf = ""
                            if not self._session_url and raw.startswith("/"):
                                self._session_url = raw.strip()
                                self._session_event.set()
                                continue
                            if "jsonrpc" in raw:
                                try:
                                    data = json.loads(raw)
                                    msg_id = data.get("id")
                                    if msg_id and msg_id in self._results:
                                        self._results[msg_id].set_result(data)
                                except json.JSONDecodeError:
                                    pass
                        continue
                    raw = line[len("data: "):]
                    # Session URL handshake — single-line, handle immediately
                    if not self._session_url and raw.startswith("/"):
                        self._session_url = raw.strip()
                        self._session_event.set()
                        continue
                    # Accumulate data lines — large JSON payloads span multiple lines
                    _buf += raw
        except Exception as exc:
            logger.warning("MCP SSE listener error: %s", exc)
            self._connected = False

    async def _send(self, method: str, params: dict) -> dict:
        """Send JSON-RPC request and wait for response."""
        async with self._lock:
            msg_id = self._next_id
            self._next_id += 1

        loop = asyncio.get_event_loop()
        future = loop.create_future()
        self._results[msg_id] = future

        await self._post_http.post(
            f"{MCP_SERVER_URL}{self._session_url}",
            json={"jsonrpc": "2.0", "id": msg_id, "method": method, "params": params},
        )

        try:
            result = await asyncio.wait_for(future, timeout=25.0)
        finally:
            self._results.pop(msg_id, None)

        return result

    async def call_tool(self, tool_name: str, arguments: dict) -> dict:
        """Call an MCP tool and return the result."""
        if not self._connected:
            await self.connect()
        try:
            response = await self._send("tools/call", {
                "name": tool_name,
                "arguments": arguments,
            })
        except Exception:
            # Reconnect and retry once
            self._connected = False
            await self.connect()
            response = await self._send("tools/call", {
                "name": tool_name,
                "arguments": arguments,
            })

        if "error" in response:
            raise RuntimeError(f"MCP tool error: {response['error']}")

        # Extract text content from MCP response
        content = response.get("result", {}).get("content", [])
        for block in content:
            if block.get("type") == "text":
                try:
                    parsed = json.loads(block["text"])
                    # Unwrap list that was wrapped for MCP transport
                    if isinstance(parsed, dict) and "items" in parsed and "count" in parsed:
                        return parsed["items"]
                    return parsed
                except json.JSONDecodeError:
                    return {"text": block["text"]}
        return {}

    async def close(self):
        if self._sse_task:
            self._sse_task.cancel()
        if hasattr(self, '_sse_http'):
            await self._sse_http.aclose()
        if hasattr(self, '_post_http'):
            await self._post_http.aclose()


# ── Module-level singleton ──────────────────────────────────────────────
_client: MCPClient | None = None


async def get_mcp_client() -> MCPClient:
    global _client
    if _client is None:
        _client = MCPClient()
    return _client


async def route_via_mcp(slug: str, method: str, path: str, query_params: dict) -> dict | None:
    """Attempt to route a request through MCP. Returns None if no tool mapping exists."""
    tool_name = _find_tool(slug, method, path)
    if not tool_name:
        return None

    arguments = _extract_arguments(path, query_params)
    client = await get_mcp_client()

    try:
        return await client.call_tool(tool_name, arguments)
    except Exception as exc:
        logger.warning("MCP route failed for %s/%s → %s: %s", slug, path, tool_name, exc)
        return None
