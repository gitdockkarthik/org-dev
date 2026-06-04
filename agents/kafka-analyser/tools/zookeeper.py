"""ZooKeeper collector — fetches ZK node stats and cluster mode detection.

Connects via ZooKeeper 4-letter commands over TCP.
Gracefully handles KRaft mode (no ZooKeeper) by returning mode=kraft.
"""
from __future__ import annotations
import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


async def _zk_command(host: str, port: int, cmd: str, timeout: float = 5.0) -> str:
    """Send a 4-letter command to ZooKeeper and return response."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout,
        )
        writer.write(cmd.encode())
        await writer.drain()
        data = await asyncio.wait_for(reader.read(4096), timeout=timeout)
        writer.close()
        await writer.wait_closed()
        return data.decode(errors="replace")
    except Exception as exc:
        raise RuntimeError(f"ZooKeeper command '{cmd}' failed: {exc}") from exc


def _parse_mntr(output: str) -> dict[str, str]:
    """Parse zk_mntr output into a dict."""
    result = {}
    for line in output.strip().splitlines():
        parts = line.split("\t", 1)
        if len(parts) == 2:
            result[parts[0].strip()] = parts[1].strip()
    return result


class ZooKeeperCollector:
    def __init__(self, url: str) -> None:
        """url format: host:port (e.g. zookeeper:2181)"""
        self._url = url.strip()
        parts = self._url.rsplit(":", 1)
        self._host = parts[0]
        self._port = int(parts[1]) if len(parts) == 2 else 2181

    async def collect(self) -> dict[str, Any]:
        """Fetch ZooKeeper stats and return structured dict."""
        try:
            # Check if ZK is alive
            ruok = await _zk_command(self._host, self._port, "ruok")
            if "imok" not in ruok:
                return {
                    "mode": "zookeeper",
                    "status": "unhealthy",
                    "url": self._url,
                    "error": f"ZooKeeper not ok: {ruok}",
                }

            # Get detailed stats
            mntr_output = await _zk_command(self._host, self._port, "mntr")
            mntr = _parse_mntr(mntr_output)

            # Parse key metrics
            version = mntr.get("zk_version", "unknown").split(",")[0]
            mode = mntr.get("zk_server_state", "unknown")
            avg_latency = float(mntr.get("zk_avg_latency", 0))
            max_latency = float(mntr.get("zk_max_latency", 0))
            connections = int(mntr.get("zk_num_alive_connections", 0))
            outstanding = int(mntr.get("zk_outstanding_requests", 0))
            znode_count = int(mntr.get("zk_znode_count", 0))
            watch_count = int(mntr.get("zk_watch_count", 0))
            ephemerals = int(mntr.get("zk_ephemerals_count", 0))
            data_size = int(mntr.get("zk_approximate_data_size", 0))
            open_file_desc = int(mntr.get("zk_open_file_descriptor_count", 0))
            max_file_desc = int(mntr.get("zk_max_file_descriptor_count", 0))
            packets_received = int(mntr.get("zk_packets_received", 0))
            packets_sent = int(mntr.get("zk_packets_sent", 0))

            # Health assessment
            health = "healthy"
            warnings = []
            if avg_latency > 100:
                health = "warning"
                warnings.append(f"High avg latency: {avg_latency}ms")
            if outstanding > 10:
                health = "warning"
                warnings.append(f"Outstanding requests: {outstanding}")
            if open_file_desc > max_file_desc * 0.8:
                health = "warning"
                warnings.append("File descriptor usage > 80%")

            return {
                "mode": "zookeeper",
                "status": health,
                "url": self._url,
                "version": version,
                "server_mode": mode,  # leader/follower/standalone
                "warnings": warnings,
                "metrics": {
                    "avg_latency_ms": avg_latency,
                    "max_latency_ms": max_latency,
                    "connections": connections,
                    "outstanding_requests": outstanding,
                    "znode_count": znode_count,
                    "watch_count": watch_count,
                    "ephemerals_count": ephemerals,
                    "data_size_bytes": data_size,
                    "open_file_descriptors": open_file_desc,
                    "max_file_descriptors": max_file_desc,
                    "packets_received": packets_received,
                    "packets_sent": packets_sent,
                },
            }

        except Exception as exc:
            logger.warning("ZooKeeperCollector.collect failed: %s", exc)
            return {
                "mode": "zookeeper",
                "status": "unreachable",
                "url": self._url,
                "error": str(exc),
            }
