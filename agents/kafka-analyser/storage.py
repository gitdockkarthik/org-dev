"""Storage abstraction for the Kafka Analyser.

Two interchangeable backends, selected by the STORAGE_BACKEND env var
(default "postgres"):

  * PostgresBackend — durable, encrypted-at-rest config in the AgentConfig
    table (mirrors the original routes_settings _upsert / load_config_from_db).
  * MemoryBackend   — process-local dict; non-persistent, handy for tests and
    DB-less runs.

Usage:
    init_storage("kafka-analyser")        # once, on startup
    backend = get_backend()               # anywhere after init
    await backend.set("source_type", '"synthetic"')
    cfg = await backend.get_all()

Only stdlib + existing project deps (sqlalchemy, asyncpg) are used.
"""
from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone

from sqlalchemy import select, delete
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import settings
from encryption import encrypt, decrypt, is_secret_key

logger = logging.getLogger(__name__)


# ── Abstract base ───────────────────────────────────────────────────────────
class StorageBackend(ABC):
    """Async key→value config store."""

    @abstractmethod
    async def get(self, key: str) -> str | None:
        ...

    @abstractmethod
    async def set(self, key: str, value: str) -> None:
        ...

    @abstractmethod
    async def get_all(self) -> dict[str, str]:
        ...

    @abstractmethod
    async def delete(self, key: str) -> None:
        ...

    # ── Cluster CRUD ──────────────────────────────────────────────────────────
    @abstractmethod
    async def save_cluster(self, cluster: dict) -> dict:
        """Save or update a cluster. Returns saved cluster with id."""
        ...

    @abstractmethod
    async def get_clusters(self, agent_slug: str) -> list[dict]:
        """Return all clusters for agent_slug."""
        ...

    @abstractmethod
    async def get_cluster(self, cluster_id: int) -> dict | None:
        """Return single cluster by id."""
        ...

    @abstractmethod
    async def delete_cluster(self, cluster_id: int) -> bool:
        """Delete cluster by id. Returns True if deleted."""
        ...

    @abstractmethod
    async def update_cluster_status(self, cluster_id: int, status: str, last_tested_at=None) -> None:
        """Update cluster status and optionally last_tested_at."""
        ...


# ── In-memory backend ───────────────────────────────────────────────────────
class MemoryBackend(StorageBackend):
    """Process-local dict. Data is lost when the process exits."""

    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self._clusters: dict[int, dict] = {}
        self._next_id: int = 1
        self._lock = asyncio.Lock()
        logger.warning(
            "StorageBackend: using in-memory storage — data will not persist across restarts"
        )

    async def get(self, key: str) -> str | None:
        async with self._lock:
            return self._store.get(key)

    async def set(self, key: str, value: str) -> None:
        async with self._lock:
            self._store[key] = value

    async def get_all(self) -> dict[str, str]:
        async with self._lock:
            return dict(self._store)

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._store.pop(key, None)

    # ── Cluster CRUD ──────────────────────────────────────────────────────────
    async def save_cluster(self, cluster: dict) -> dict:
        async with self._lock:
            cid = cluster.get("id")
            if cid:
                # Update existing record (merge provided fields).
                rec = dict(self._clusters.get(cid, {}))
                rec.update(cluster)
                rec["id"] = cid
                self._clusters[cid] = rec
            else:
                # Insert with a freshly assigned id.
                cid = self._next_id
                self._next_id += 1
                rec = dict(cluster)
                rec["id"] = cid
                self._clusters[cid] = rec
            return dict(rec)

    async def get_clusters(self, agent_slug: str) -> list[dict]:
        async with self._lock:
            return [
                dict(c) for c in self._clusters.values()
                if c.get("agent_slug") == agent_slug
            ]

    async def get_cluster(self, cluster_id: int) -> dict | None:
        async with self._lock:
            c = self._clusters.get(cluster_id)
            return dict(c) if c is not None else None

    async def delete_cluster(self, cluster_id: int) -> bool:
        async with self._lock:
            return self._clusters.pop(cluster_id, None) is not None

    async def update_cluster_status(self, cluster_id: int, status: str, last_tested_at=None) -> None:
        async with self._lock:
            c = self._clusters.get(cluster_id)
            if c is None:
                return
            c["status"] = status
            if last_tested_at is not None:
                c["last_tested_at"] = last_tested_at


# ── Postgres backend ────────────────────────────────────────────────────────
class PostgresBackend(StorageBackend):
    """Durable config in the AgentConfig table, scoped by agent_slug.

    Secret keys (per is_secret_key) are encrypted at rest. All DB errors are
    caught and logged — methods never raise.
    """

    def __init__(self, agent_slug: str) -> None:
        self.agent_slug = agent_slug

    async def set(self, key: str, value: str) -> None:
        from database import SessionLocal
        from models import AgentConfig

        if SessionLocal is None:
            return
        try:
            now = datetime.now(timezone.utc)
            stored = value  # already encoded by caller
            async with SessionLocal() as session:
                stmt = (
                    pg_insert(AgentConfig)
                    .values(
                        agent_slug=self.agent_slug,
                        key=key,
                        value=stored,
                        updated_at=now,
                    )
                    .on_conflict_do_update(
                        index_elements=["agent_slug", "key"],
                        set_={"value": stored, "updated_at": now},
                    )
                )
                await session.execute(stmt)
                await session.commit()
        except Exception:
            logger.exception("PostgresBackend.set: failed to upsert key=%r", key)

    async def get_all(self) -> dict[str, str]:
        from database import SessionLocal
        from models import AgentConfig

        if SessionLocal is None:
            return {}
        try:
            async with SessionLocal() as session:
                rows = (
                    await session.execute(
                        select(AgentConfig).where(
                            AgentConfig.agent_slug == self.agent_slug
                        )
                    )
                ).scalars().all()

            # Return the raw stored strings verbatim — the caller owns decoding.
            return {r.key: r.value for r in rows}
        except Exception:
            logger.exception("PostgresBackend.get_all: DB query failed")
            return {}

    async def get(self, key: str) -> str | None:
        cfg = await self.get_all()
        return cfg.get(key)

    async def delete(self, key: str) -> None:
        from database import SessionLocal
        from models import AgentConfig

        if SessionLocal is None:
            return
        try:
            async with SessionLocal() as session:
                await session.execute(
                    delete(AgentConfig).where(
                        AgentConfig.agent_slug == self.agent_slug,
                        AgentConfig.key == key,
                    )
                )
                await session.commit()
        except Exception:
            logger.exception("PostgresBackend.delete: failed to delete key=%r", key)

    # ── Cluster CRUD ──────────────────────────────────────────────────────────
    # Columns accepted from an incoming cluster dict (sasl_password handled
    # separately so it can be encrypted at rest).
    _CLUSTER_FIELDS = (
        "agent_slug", "name", "environment", "source_type", "bootstrap_servers",
        "auth_type", "sasl_username", "sasl_mechanism", "tls_enabled", "enabled",
        "status",
        "schema_registry_url",
        "zookeeper_url",
        "kafka_connect_url",
        "jmx_port",
        "mirror_source_cluster_id",
        "mirror_mode",
    )

    @staticmethod
    def _row_to_dict(row) -> dict:
        """Convert a KafkaCluster ORM row to a plain dict (decrypt sasl_password)."""
        pw = row.sasl_password
        if pw:
            try:
                pw = decrypt(pw)
            except Exception:
                logger.error(
                    "PostgresBackend._row_to_dict: failed to decrypt sasl_password for id=%s",
                    row.id,
                )
                pw = None
        return {
            "id": row.id,
            "agent_slug": row.agent_slug,
            "name": row.name,
            "environment": row.environment,
            "source_type": row.source_type,
            "bootstrap_servers": row.bootstrap_servers,
            "auth_type": row.auth_type,
            "sasl_username": row.sasl_username,
            "sasl_password": pw,
            "sasl_mechanism": row.sasl_mechanism,
            "tls_enabled": row.tls_enabled,
            "enabled": row.enabled,
            "config_json": row.config_json,
            "status": row.status,
            "last_tested_at": row.last_tested_at,
            "created_at": row.created_at,
            "schema_registry_url": row.schema_registry_url or "",
            "zookeeper_url": row.zookeeper_url or "",
            "kafka_connect_url": row.kafka_connect_url or "",
            "jmx_port": row.jmx_port,
            "mirror_source_cluster_id": row.mirror_source_cluster_id,
            "mirror_mode": row.mirror_mode or "none",
        }

    async def save_cluster(self, cluster: dict) -> dict:
        from database import SessionLocal
        from models import KafkaCluster

        if SessionLocal is None:
            return dict(cluster)
        try:
            pw = cluster.get("sasl_password")
            stored_pw = encrypt(pw) if pw else pw  # keep None/"" as-is
            async with SessionLocal() as session:
                cid = cluster.get("id")
                row = await session.get(KafkaCluster, cid) if cid else None
                if row is None:
                    row = KafkaCluster()
                    session.add(row)
                for f in self._CLUSTER_FIELDS:
                    if f in cluster:
                        setattr(row, f, cluster[f])
                if "sasl_password" in cluster:
                    row.sasl_password = stored_pw
                await session.commit()
                await session.refresh(row)
                return self._row_to_dict(row)
        except Exception:
            logger.exception("PostgresBackend.save_cluster: failed")
            return dict(cluster)

    async def get_clusters(self, agent_slug: str) -> list[dict]:
        from database import SessionLocal
        from models import KafkaCluster

        if SessionLocal is None:
            return []
        try:
            async with SessionLocal() as session:
                rows = (
                    await session.execute(
                        select(KafkaCluster).where(KafkaCluster.agent_slug == agent_slug)
                    )
                ).scalars().all()
            return [self._row_to_dict(r) for r in rows]
        except Exception:
            logger.exception("PostgresBackend.get_clusters: failed for agent_slug=%r", agent_slug)
            return []

    async def get_cluster(self, cluster_id: int) -> dict | None:
        from database import SessionLocal
        from models import KafkaCluster

        if SessionLocal is None:
            return None
        try:
            async with SessionLocal() as session:
                row = await session.get(KafkaCluster, cluster_id)
                return self._row_to_dict(row) if row is not None else None
        except Exception:
            logger.exception("PostgresBackend.get_cluster: failed for id=%s", cluster_id)
            return None

    async def delete_cluster(self, cluster_id: int) -> bool:
        from database import SessionLocal
        from models import KafkaCluster

        if SessionLocal is None:
            return False
        try:
            async with SessionLocal() as session:
                row = await session.get(KafkaCluster, cluster_id)
                if row is None:
                    return False
                await session.delete(row)
                await session.commit()
                return True
        except Exception:
            logger.exception("PostgresBackend.delete_cluster: failed for id=%s", cluster_id)
            return False

    async def update_cluster_status(self, cluster_id: int, status: str, last_tested_at=None) -> None:
        from database import SessionLocal
        from models import KafkaCluster

        if SessionLocal is None:
            return
        try:
            async with SessionLocal() as session:
                row = await session.get(KafkaCluster, cluster_id)
                if row is None:
                    return
                row.status = status
                if last_tested_at is not None:
                    row.last_tested_at = last_tested_at
                await session.commit()
        except Exception:
            logger.exception("PostgresBackend.update_cluster_status: failed for id=%s", cluster_id)


# ── Factory ─────────────────────────────────────────────────────────────────
def get_storage(agent_slug: str) -> StorageBackend:
    """Build a backend from the STORAGE_BACKEND env var (default "postgres")."""
    backend = settings.storage_backend.lower().strip()

    if backend == "memory":
        logger.info("StorageBackend: using 'memory' backend")
        return MemoryBackend()
    if backend == "postgres":
        logger.info("StorageBackend: using 'postgres' backend (agent_slug=%r)", agent_slug)
        return PostgresBackend(agent_slug)

    logger.warning(
        "StorageBackend: unknown STORAGE_BACKEND=%r — falling back to in-memory storage",
        backend,
    )
    return MemoryBackend()


# ── Module-level singleton ──────────────────────────────────────────────────
_backend: StorageBackend | None = None


def init_storage(agent_slug: str) -> StorageBackend:
    global _backend
    _backend = get_storage(agent_slug)
    return _backend


def get_backend() -> StorageBackend:
    if _backend is None:
        raise RuntimeError("Storage not initialised — call init_storage() first")
    return _backend
