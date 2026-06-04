"""Kafka Connect collector — fetches connector status, tasks, and health.

Connects to Kafka Connect REST API.
Handles large numbers of connectors efficiently (250-300+).
"""
from __future__ import annotations
import asyncio
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_BATCH_SIZE = 50  # fetch connector details in batches


class KafkaConnectCollector:
    def __init__(self, url: str) -> None:
        self._url = url.rstrip("/")

    async def collect(self) -> dict[str, Any]:
        """Fetch all connector data and return structured dict."""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Check Connect cluster info
                info = await self._get_cluster_info(client)

                # Get all connector names
                connector_names = await self._get_connectors(client)

                # Fetch status in batches for efficiency
                connectors = await self._get_connector_statuses(client, connector_names)

                # Compute summary stats
                running = sum(1 for c in connectors if c["state"] == "RUNNING")
                failed = sum(1 for c in connectors if c["state"] == "FAILED")
                paused = sum(1 for c in connectors if c["state"] == "PAUSED")
                unassigned = sum(1 for c in connectors if c["state"] == "UNASSIGNED")
                total_tasks = sum(c["total_tasks"] for c in connectors)
                failed_tasks = sum(c["failed_tasks"] for c in connectors)

                return {
                    "status": "healthy" if failed == 0 else "degraded",
                    "url": self._url,
                    "cluster_info": info,
                    "connector_count": len(connectors),
                    "summary": {
                        "running": running,
                        "failed": failed,
                        "paused": paused,
                        "unassigned": unassigned,
                        "total_tasks": total_tasks,
                        "failed_tasks": failed_tasks,
                    },
                    "connectors": connectors,
                }
        except httpx.ConnectError:
            return {
                "status": "unreachable",
                "url": self._url,
                "connector_count": 0,
                "connectors": [],
            }
        except Exception as exc:
            logger.warning("KafkaConnectCollector.collect failed: %s", exc)
            return {
                "status": "error",
                "url": self._url,
                "error": str(exc),
                "connector_count": 0,
                "connectors": [],
            }

    async def _get_cluster_info(self, client: httpx.AsyncClient) -> dict:
        try:
            resp = await client.get(f"{self._url}/")
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return {}

    async def _get_connectors(self, client: httpx.AsyncClient) -> list[str]:
        try:
            resp = await client.get(f"{self._url}/connectors")
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return []

    async def _get_connector_statuses(
        self, client: httpx.AsyncClient, names: list[str]
    ) -> list[dict]:
        """Fetch connector statuses in batches."""
        results = []
        for i in range(0, len(names), _BATCH_SIZE):
            batch = names[i:i + _BATCH_SIZE]
            tasks = [self._get_connector_detail(client, name) for name in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in batch_results:
                if isinstance(r, dict):
                    results.append(r)
        return results

    async def _get_connector_detail(
        self, client: httpx.AsyncClient, name: str
    ) -> dict:
        try:
            # Get status
            status_resp = await client.get(f"{self._url}/connectors/{name}/status")
            status_resp.raise_for_status()
            status = status_resp.json()

            connector_status = status.get("connector", {})
            tasks = status.get("tasks", [])

            state = connector_status.get("state", "UNKNOWN")
            connector_type = status.get("type", "unknown")

            total_tasks = len(tasks)
            failed_tasks = sum(1 for t in tasks if t.get("state") == "FAILED")
            running_tasks = sum(1 for t in tasks if t.get("state") == "RUNNING")
            paused_tasks = sum(1 for t in tasks if t.get("state") == "PAUSED")

            # Get config for connector class
            connector_class = ""
            try:
                config_resp = await client.get(f"{self._url}/connectors/{name}/config")
                if config_resp.status_code == 200:
                    config = config_resp.json()
                    connector_class = config.get("connector.class", "").split(".")[-1]
            except Exception:
                pass

            task_details = [
                {
                    "task_id": t.get("id") if isinstance(t.get("id"), int) else (t.get("id", {}).get("task", i) if isinstance(t.get("id"), dict) else i),
                    "state": t.get("state", "UNKNOWN"),
                    "worker_id": t.get("worker_id", ""),
                    "trace": t.get("trace", "")[:200] if t.get("trace") else "",
                }
                for i, t in enumerate(tasks)
            ]

            return {
                "name": name,
                "state": state,
                "type": connector_type,
                "connector_class": connector_class,
                "total_tasks": total_tasks,
                "failed_tasks": failed_tasks,
                "running_tasks": running_tasks,
                "paused_tasks": paused_tasks,
                "tasks": task_details,
                "worker_id": connector_status.get("worker_id", ""),
                "trace": connector_status.get("trace", "")[:200] if connector_status.get("trace") else "",
            }
        except Exception as exc:
            return {
                "name": name,
                "state": "UNKNOWN",
                "type": "unknown",
                "connector_class": "",
                "total_tasks": 0,
                "failed_tasks": 0,
                "running_tasks": 0,
                "paused_tasks": 0,
                "tasks": [],
                "error": str(exc),
            }
