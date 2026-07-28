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
    def __init__(self, url: str, username: str | None = None, password: str | None = None, topics: list[str] | None = None, sr_restricted: bool | None = None, cluster_id: int | None = None) -> None:
        self._url = url.rstrip("/")
        self._auth = (username, password) if username and password else None
        self._topics = topics or []
        self._sr_restricted = sr_restricted
        self._cluster_id = cluster_id

    async def collect(self) -> dict[str, Any]:
        """Fetch schema registry data and return structured dict."""
        try:
            # Fast path: read from postgres for known restricted clusters
            if self._sr_restricted and self._cluster_id:
                return await self._collect_from_postgres()

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

    async def _collect_from_postgres(self) -> dict[str, Any]:
        """Read SR subjects from postgres for restricted clusters — instant load."""
        try:
            from database import SessionLocal
            from sqlalchemy import text as _t
            if not SessionLocal:
                return {"status": "restricted", "url": self._url, "subject_count": 0, "subjects": [], "total_versions": 0, "global_compatibility": "UNKNOWN", "schema_types": {}}
            async with SessionLocal() as sess:
                rows = await sess.execute(_t("""
                    SELECT subject, latest_version, schema_type, collected_at
                    FROM kafka_sr_subjects
                    WHERE cluster_id=:cid
                    ORDER BY subject
                """), {"cid": self._cluster_id})
                subjects = rows.fetchall()
            if not subjects:
                return {
                    "status": "restricted",
                    "url": self._url,
                    "subject_count": 0,
                    "subjects": [],
                    "total_versions": 0,
                    "global_compatibility": "UNKNOWN",
                    "schema_types": {},
                    "restricted_note": "Schema Registry RBAC enabled — background sync job collecting subjects. Check back shortly.",
                }
            subject_details = [
                {
                    "subject": r.subject,
                    "version_count": r.latest_version or 0,
                    "latest_version": r.latest_version or 0,
                    "schema_type": r.schema_type or "AVRO",
                    "compatibility": "UNKNOWN",
                    "collected_at": r.collected_at.isoformat() if r.collected_at else None,
                }
                for r in subjects
            ]
            schema_types = {}
            for s in subject_details:
                t = s["schema_type"]
                schema_types[t] = schema_types.get(t, 0) + 1
            last_collected = subjects[-1].collected_at.isoformat() if subjects[-1].collected_at else None
            return {
                "status": "restricted",
                "url": self._url,
                "subject_count": len(subjects),
                "subjects": subject_details,
                "total_versions": sum(s["latest_version"] for s in subject_details),
                "global_compatibility": "UNKNOWN",
                "schema_types": schema_types,
                "restricted_note": f"Schema Registry RBAC enabled — {len(subjects)} subjects collected from topic names. Last synced: {last_collected}",
            }
        except Exception as e:
            logger.warning("_collect_from_postgres failed: %s", e)
            return {"status": "error", "url": self._url, "subject_count": 0, "subjects": [], "error": str(e)}

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
        """Derive subject names from Kafka topic names when listing is restricted.
        Checks {topic}-key and {topic}-value for all topics in batches of 20 parallel requests."""
        if not self._topics:
            return []
        import asyncio as _aio
        candidates = []
        for topic in self._topics:
            candidates.append(f"{topic}-key")
            candidates.append(f"{topic}-value")
        async def check(name: str) -> str | None:
            try:
                r = await client.get(f"{self._url}/subjects/{name}/versions/latest")
                if r.status_code == 200:
                    return name
            except Exception:
                pass
            return None
        subjects = []
        batch_size = 20
        for i in range(0, len(candidates), batch_size):
            batch = candidates[i:i+batch_size]
            results = await _aio.gather(*[check(c) for c in batch])
            subjects.extend([r for r in results if r])
        logger.info("Topic-derived SR subjects found: %d of %d candidates checked", len(subjects), len(candidates))
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
