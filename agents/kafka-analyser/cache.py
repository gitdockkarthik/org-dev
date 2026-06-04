"""Cache backend abstraction — Redis, Memory, or None.

Configured via:
  CACHE_BACKEND=redis|memory|none  (default: none)
  CACHE_URL=redis://host:6379      (required when CACHE_BACKEND=redis)
"""
from __future__ import annotations
import logging
import os
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)


class CacheBackend(ABC):
    @abstractmethod
    async def get(self, key: str) -> Any | None: ...
    @abstractmethod
    async def set(self, key: str, value: Any, ttl_secs: int = 300) -> None: ...
    @abstractmethod
    async def delete(self, key: str) -> None: ...
    @abstractmethod
    async def flush(self) -> None: ...


class NoCacheBackend(CacheBackend):
    async def get(self, key: str) -> Any | None:
        return None
    async def set(self, key: str, value: Any, ttl_secs: int = 300) -> None:
        pass
    async def delete(self, key: str) -> None:
        pass
    async def flush(self) -> None:
        pass


class MemoryCacheBackend(CacheBackend):
    def __init__(self) -> None:
        self._store: dict[str, Any] = {}
    async def get(self, key: str) -> Any | None:
        return self._store.get(key)
    async def set(self, key: str, value: Any, ttl_secs: int = 300) -> None:
        self._store[key] = value
    async def delete(self, key: str) -> None:
        self._store.pop(key, None)
    async def flush(self) -> None:
        self._store.clear()


class RedisCacheBackend(CacheBackend):
    def __init__(self, url: str) -> None:
        self._url = url
        self._client: Any = None

    async def _ensure_client(self) -> Any:
        if self._client is None:
            try:
                import redis.asyncio as aioredis
                self._client = aioredis.from_url(self._url, decode_responses=False)
            except ImportError:
                raise RuntimeError(
                    "redis package not installed. Add 'redis>=5.0' to requirements.txt"
                )
        return self._client

    async def get(self, key: str) -> Any | None:
        try:
            client = await self._ensure_client()
            import json
            raw = await client.get(key)
            return json.loads(raw) if raw else None
        except Exception as exc:
            logger.warning("RedisCacheBackend.get failed: %s", exc)
            return None

    async def set(self, key: str, value: Any, ttl_secs: int = 300) -> None:
        try:
            client = await self._ensure_client()
            import json
            await client.setex(key, ttl_secs, json.dumps(value))
        except Exception as exc:
            logger.warning("RedisCacheBackend.set failed: %s", exc)

    async def delete(self, key: str) -> None:
        try:
            client = await self._ensure_client()
            await client.delete(key)
        except Exception as exc:
            logger.warning("RedisCacheBackend.delete failed: %s", exc)

    async def flush(self) -> None:
        try:
            client = await self._ensure_client()
            await client.flushdb()
        except Exception as exc:
            logger.warning("RedisCacheBackend.flush failed: %s", exc)


def get_cache_backend() -> CacheBackend:
    """Factory — reads CACHE_BACKEND and CACHE_URL env vars."""
    backend = os.getenv("CACHE_BACKEND", "none").lower()
    if backend == "redis":
        url = os.getenv("CACHE_URL", "redis://localhost:6379")
        logger.info("CacheBackend: using Redis at %s", url)
        return RedisCacheBackend(url)
    elif backend == "memory":
        logger.info("CacheBackend: using in-memory cache")
        return MemoryCacheBackend()
    else:
        logger.info("CacheBackend: disabled (CACHE_BACKEND=none)")
        return NoCacheBackend()
