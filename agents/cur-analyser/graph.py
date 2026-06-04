"""Graph backend abstraction — Neo4j or None.

Configured via:
  GRAPH_BACKEND=neo4j|none     (default: none)
  GRAPH_URL=bolt://host:7687   (required when GRAPH_BACKEND=neo4j)
  GRAPH_USERNAME=neo4j         (default: neo4j)
  GRAPH_PASSWORD=...           (required when GRAPH_BACKEND=neo4j)
"""
from __future__ import annotations
import logging
import os
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)


class GraphBackend(ABC):
    @abstractmethod
    async def query(self, cypher: str, params: dict | None = None) -> list[dict]: ...
    @abstractmethod
    async def upsert_node(self, label: str, id: str, properties: dict) -> None: ...
    @abstractmethod
    async def upsert_edge(self, from_id: str, to_id: str, rel: str, properties: dict | None = None) -> None: ...


class NoGraphBackend(GraphBackend):
    """No-op graph store — all operations are silent no-ops."""
    async def query(self, cypher: str, params: dict | None = None) -> list[dict]:
        return []
    async def upsert_node(self, label: str, id: str, properties: dict) -> None:
        pass
    async def upsert_edge(self, from_id: str, to_id: str, rel: str, properties: dict | None = None) -> None:
        pass


class Neo4jGraphBackend(GraphBackend):
    """Neo4j graph backend using neo4j async driver."""
    def __init__(self, url: str, username: str, password: str) -> None:
        self._url = url
        self._username = username
        self._password = password
        self._driver: Any = None

    async def _ensure_driver(self) -> Any:
        if self._driver is None:
            try:
                from neo4j import AsyncGraphDatabase
                self._driver = AsyncGraphDatabase.driver(
                    self._url,
                    auth=(self._username, self._password),
                )
            except ImportError:
                raise RuntimeError(
                    "neo4j package not installed. Add 'neo4j>=5.0' to requirements.txt"
                )
        return self._driver

    async def query(self, cypher: str, params: dict | None = None) -> list[dict]:
        try:
            driver = await self._ensure_driver()
            async with driver.session() as session:
                result = await session.run(cypher, params or {})
                return [dict(record) async for record in result]
        except Exception as exc:
            logger.warning("Neo4jGraphBackend.query failed: %s", exc)
            return []

    async def upsert_node(self, label: str, id: str, properties: dict) -> None:
        try:
            props = {**properties, "id": id}
            cypher = f"MERGE (n:{label} {{id: $id}}) SET n += $props"
            await self.query(cypher, {"id": id, "props": props})
        except Exception as exc:
            logger.warning("Neo4jGraphBackend.upsert_node failed: %s", exc)

    async def upsert_edge(self, from_id: str, to_id: str, rel: str, properties: dict | None = None) -> None:
        try:
            cypher = (
                f"MATCH (a {{id: $from_id}}), (b {{id: $to_id}}) "
                f"MERGE (a)-[r:{rel}]->(b) SET r += $props"
            )
            await self.query(cypher, {"from_id": from_id, "to_id": to_id, "props": properties or {}})
        except Exception as exc:
            logger.warning("Neo4jGraphBackend.upsert_edge failed: %s", exc)

    async def close(self) -> None:
        if self._driver:
            await self._driver.close()
            self._driver = None


def get_graph_backend() -> GraphBackend:
    """Factory — reads GRAPH_BACKEND, GRAPH_URL, GRAPH_USERNAME, GRAPH_PASSWORD env vars."""
    backend = os.getenv("GRAPH_BACKEND", "none").lower()

    if backend == "neo4j":
        url = os.getenv("GRAPH_URL", "bolt://localhost:7687")
        username = os.getenv("GRAPH_USERNAME", "neo4j")
        password = os.getenv("GRAPH_PASSWORD", "")
        if not password:
            logger.warning("GraphBackend: GRAPH_PASSWORD not set — falling back to none")
            return NoGraphBackend()
        logger.info("GraphBackend: using Neo4j at %s", url)
        return Neo4jGraphBackend(url, username, password)
    else:
        logger.info("GraphBackend: disabled (GRAPH_BACKEND=none)")
        return NoGraphBackend()
