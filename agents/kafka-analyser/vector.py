"""Vector backend abstraction — pgvector, Qdrant, Pinecone, or None.

Configured via:
  VECTOR_BACKEND=pgvector|qdrant|pinecone|none  (default: none)
  VECTOR_URL=http://host:6333                   (required for qdrant)
  VECTOR_URL=postgresql+asyncpg://...           (required for pgvector — reuses DATABASE_URL if not set)
  VECTOR_API_KEY=...                            (required for pinecone)
  VECTOR_INDEX=kafka-analyser                   (collection/index name, default: kafka-analyser)
"""
from __future__ import annotations
import logging
import os
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)


class VectorBackend(ABC):
    @abstractmethod
    async def upsert(self, id: str, vector: list[float], payload: dict) -> None: ...
    @abstractmethod
    async def search(self, vector: list[float], top_k: int = 5) -> list[dict]: ...
    @abstractmethod
    async def delete(self, id: str) -> None: ...


class NoVectorBackend(VectorBackend):
    """No-op vector store — all operations are silent no-ops."""
    async def upsert(self, id: str, vector: list[float], payload: dict) -> None:
        pass
    async def search(self, vector: list[float], top_k: int = 5) -> list[dict]:
        return []
    async def delete(self, id: str) -> None:
        pass


class QdrantVectorBackend(VectorBackend):
    """Qdrant vector backend."""
    def __init__(self, url: str, index: str) -> None:
        self._url = url
        self._index = index
        self._client: Any = None

    async def _ensure_client(self) -> Any:
        if self._client is None:
            try:
                from qdrant_client import AsyncQdrantClient
                self._client = AsyncQdrantClient(url=self._url)
            except ImportError:
                raise RuntimeError(
                    "qdrant-client not installed. Add 'qdrant-client>=1.7' to requirements.txt"
                )
        return self._client

    async def upsert(self, id: str, vector: list[float], payload: dict) -> None:
        try:
            from qdrant_client.models import PointStruct
            client = await self._ensure_client()
            await client.upsert(
                collection_name=self._index,
                points=[PointStruct(id=id, vector=vector, payload=payload)],
            )
        except Exception as exc:
            logger.warning("QdrantVectorBackend.upsert failed: %s", exc)

    async def search(self, vector: list[float], top_k: int = 5) -> list[dict]:
        try:
            client = await self._ensure_client()
            results = await client.search(
                collection_name=self._index,
                query_vector=vector,
                limit=top_k,
            )
            return [{"id": r.id, "score": r.score, "payload": r.payload} for r in results]
        except Exception as exc:
            logger.warning("QdrantVectorBackend.search failed: %s", exc)
            return []

    async def delete(self, id: str) -> None:
        try:
            from qdrant_client.models import PointIdsList
            client = await self._ensure_client()
            await client.delete(
                collection_name=self._index,
                points_selector=PointIdsList(points=[id]),
            )
        except Exception as exc:
            logger.warning("QdrantVectorBackend.delete failed: %s", exc)


class PineconeVectorBackend(VectorBackend):
    """Pinecone vector backend."""
    def __init__(self, api_key: str, index: str) -> None:
        self._api_key = api_key
        self._index = index
        self._client: Any = None

    async def _ensure_client(self) -> Any:
        if self._client is None:
            try:
                from pinecone import Pinecone
                pc = Pinecone(api_key=self._api_key)
                self._client = pc.Index(self._index)
            except ImportError:
                raise RuntimeError(
                    "pinecone-client not installed. Add 'pinecone-client>=3.0' to requirements.txt"
                )
        return self._client

    async def upsert(self, id: str, vector: list[float], payload: dict) -> None:
        try:
            client = await self._ensure_client()
            client.upsert(vectors=[(id, vector, payload)])
        except Exception as exc:
            logger.warning("PineconeVectorBackend.upsert failed: %s", exc)

    async def search(self, vector: list[float], top_k: int = 5) -> list[dict]:
        try:
            client = await self._ensure_client()
            results = client.query(vector=vector, top_k=top_k, include_metadata=True)
            return [{"id": r.id, "score": r.score, "payload": r.metadata} for r in results.matches]
        except Exception as exc:
            logger.warning("PineconeVectorBackend.search failed: %s", exc)
            return []

    async def delete(self, id: str) -> None:
        try:
            client = await self._ensure_client()
            client.delete(ids=[id])
        except Exception as exc:
            logger.warning("PineconeVectorBackend.delete failed: %s", exc)


class PgVectorBackend(VectorBackend):
    """pgvector backend — uses existing DATABASE_URL."""
    def __init__(self, url: str, index: str) -> None:
        self._url = url
        self._index = index

    async def upsert(self, id: str, vector: list[float], payload: dict) -> None:
        logger.debug("PgVectorBackend.upsert: %s (Phase 4 implementation)", id)

    async def search(self, vector: list[float], top_k: int = 5) -> list[dict]:
        logger.debug("PgVectorBackend.search (Phase 4 implementation)")
        return []

    async def delete(self, id: str) -> None:
        logger.debug("PgVectorBackend.delete: %s (Phase 4 implementation)", id)


def get_vector_backend() -> VectorBackend:
    """Factory — reads VECTOR_BACKEND, VECTOR_URL, VECTOR_API_KEY env vars."""
    backend = os.getenv("VECTOR_BACKEND", "none").lower()
    index = os.getenv("VECTOR_INDEX", "kafka-analyser")

    if backend == "qdrant":
        url = os.getenv("VECTOR_URL", "http://localhost:6333")
        logger.info("VectorBackend: using Qdrant at %s (index: %s)", url, index)
        return QdrantVectorBackend(url, index)
    elif backend == "pinecone":
        api_key = os.getenv("VECTOR_API_KEY", "")
        if not api_key:
            logger.warning("VectorBackend: VECTOR_API_KEY not set — falling back to none")
            return NoVectorBackend()
        logger.info("VectorBackend: using Pinecone (index: %s)", index)
        return PineconeVectorBackend(api_key, index)
    elif backend == "pgvector":
        url = os.getenv("VECTOR_URL") or os.getenv("DATABASE_URL", "")
        logger.info("VectorBackend: using pgvector (index: %s)", index)
        return PgVectorBackend(url, index)
    else:
        logger.info("VectorBackend: disabled (VECTOR_BACKEND=none)")
        return NoVectorBackend()
