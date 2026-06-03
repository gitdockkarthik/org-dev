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


# ── In-memory backend ───────────────────────────────────────────────────────
class MemoryBackend(StorageBackend):
    """Process-local dict. Data is lost when the process exits."""

    def __init__(self) -> None:
        self._store: dict[str, str] = {}
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
