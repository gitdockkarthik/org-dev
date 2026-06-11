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

_MAX_SUBJECTS = 1000  # Cap for large registries


class SchemaRegistryCollector:
    def __init__(self, url: str) -> None:
        self._url = url.rstrip("/")

    async def collect(self) -> dict[str, Any]:
        """Fetch schema registry data and return structured dict."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                subjects = await self._get_subjects(client)
                total_subject_count = len(subjects)

                # Cap subjects for performance on large registries
                if len(subjects) > _MAX_SUBJECTS:
                    subjects = sorted(subjects)[:_MAX_SUBJECTS]

                # Fetch global compatibility first
                global_compat = await self._get_global_compatibility(client)

                # Fetch subject details in parallel batches of 50
                subject_details = []
                _BATCH = 50
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
                    "status": "healthy",
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
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return []

    async def _get_subject_detail(self, client: httpx.AsyncClient, subject: str) -> dict | None:
        try:
            # Get versions and latest in parallel
            versions_resp, latest_resp = await asyncio.gather(
                client.get(f"{self._url}/subjects/{subject}/versions"),
                client.get(f"{self._url}/subjects/{subject}/versions/latest"),
            )
            versions_resp.raise_for_status()
            latest_resp.raise_for_status()
            versions = versions_resp.json()
            latest = latest_resp.json()
            schema_type = latest.get("schemaType", "AVRO")

            return {
                "subject": subject,
                "version_count": len(versions),
                "latest_version": max(versions) if versions else 0,
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
