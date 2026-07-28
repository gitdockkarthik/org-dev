"""Schema Registry collector — fetches subjects, versions, and compatibility.

Connects to Confluent Schema Registry REST API.
Works with any Schema Registry compatible implementation.
"""
from __future__ import annotations
import asyncio
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_MAX_SUBJECTS = 200  # Cap for large registries — detail fetch is slow


class SchemaRegistryCollector:
    def __init__(self, url: str, username: str | None = None, password: str | None = None, topics: list[str] | None = None) -> None:
        self._url = url.rstrip("/")
        self._auth = (username, password) if username and password else None
        self._topics = topics or []

    async def collect(self) -> dict[str, Any]:
        """Fetch schema registry data and return structured dict."""
        try:
            auth = httpx.BasicAuth(self._auth[0], self._auth[1]) if self._auth else None
            async with httpx.AsyncClient(timeout=10.0, auth=auth) as client:
                subjects = await self._get_subjects(client)
                total_subject_count = len(subjects)
                sr_restricted = False
                if not subjects:
                    try:
                        config_resp = await client.get(f"{self._url}/config")
                        if config_resp.status_code == 200:
                            sr_restricted = True
                    except Exception:
                        pass

                # Cap subjects for performance on large registries
                if len(subjects) > _MAX_SUBJECTS:
                    subjects = sorted(subjects)[:_MAX_SUBJECTS]

                # Fetch global compatibility first
                global_compat = await self._get_global_compatibility(client)

                # Fetch subject details in parallel batches of 100
                subject_details = []
                _BATCH = 100
                for i in range(0, len(subjects), _BATCH):
                    batch = subjects[i:i + _BATCH]
                    results = await asyncio.gather(
                        *[self._get_subject_detail(client, s) for s in batch],
                        return_exceptions=False
                    )
                    subject_details.extend([r for r in results if r])

                total_versions = sum(s.get("version_count", 0) for s in subject_details)
                avro_count = sum(1 for s in subject_details if s.get("schema_type") == "AVRO")
                json_count = sum(1 for s in subject_details if s.get("schema_type") == "JSON")
                proto_count = sum(1 for s in subject_details if s.get("schema_type") == "PROTOBUF")

                return {
                    "status": "restricted" if sr_restricted else "healthy",
                    "url": self._url,
                    "subject_count": total_subject_count,
                    "total_versions": total_versions,
                    "global_compatibility": global_compat,
                    "schema_types": {
                        "AVRO": avro_count,
                        "JSON": json_count,
                        "PROTOBUF": proto_count,
                    },
                    "subjects": subject_details,
                }
        except httpx.ConnectError:
            return {"status": "unreachable", "url": self._url, "subjects": [], "subject_count": 0}
        except Exception as exc:
            logger.warning("SchemaRegistryCollector.collect failed: %s", exc)
            return {"status": "error", "url": self._url, "error": str(exc), "subjects": [], "subject_count": 0}

    async def _get_subjects(self, client: httpx.AsyncClient) -> list[str]:
        try:
            resp = await client.get(f"{self._url}/subjects")
            if resp.status_code == 422:
                logger.warning("Schema Registry subject listing restricted (RBAC) at %s — trying topic-derived subjects", self._url)
                return await self._get_subjects_from_topics(client)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return []

    async def _get_subjects_from_topics(self, client: httpx.AsyncClient) -> list[str]:
        """Derive subject names from Kafka topic names when listing is restricted."""
        if not self._topics:
            return []
        subjects = []
        candidates = []
        for topic in self._topics[:100]:
            candidates.append(f"{topic}-key")
            candidates.append(f"{topic}-value")
        import asyncio as _aio
        async def check(name: str) -> str | None:
            try:
                r = await client.get(f"{self._url}/subjects/{name}/versions/latest")
                if r.status_code == 200:
                    return name
            except Exception:
                pass
            return None
        batch_size = 20
        for i in range(0, len(candidates), batch_size):
            batch = candidates[i:i+batch_size]
            results = await _aio.gather(*[check(c) for c in batch])
            subjects.extend([r for r in results if r])
        logger.info("Topic-derived SR subjects found: %d", len(subjects))
        return subjects

    async def _get_subject_detail(self, client: httpx.AsyncClient, subject: str) -> dict | None:
        try:
            latest_resp = await client.get(f"{self._url}/subjects/{subject}/versions/latest")
            latest_resp.raise_for_status()
            latest = latest_resp.json()
            schema_type = latest.get("schemaType", "AVRO")
            latest_version = latest.get("version", 0)

            return {
                "subject": subject,
                "version_count": latest_version,
                "latest_version": latest_version,
                "schema_type": schema_type,
                "compatibility": "GLOBAL",
                "schema_id": latest.get("id"),
            }
        except Exception as exc:
            logger.warning("Failed to get detail for subject %s: %s", subject, exc)
            return None

    async def _get_global_compatibility(self, client: httpx.AsyncClient) -> str:
        try:
            resp = await client.get(f"{self._url}/config")
            if resp.status_code == 200:
                return resp.json().get("compatibilityLevel", "BACKWARD")
        except Exception:
            pass
        return "BACKWARD"
