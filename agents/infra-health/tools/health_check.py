import json
import random
from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar

from tools.base import ToolExecutor

_SERVICES = {
    "api-server": {
        "status": "healthy",
        "uptime_pct": 99.97,
        "last_incident": "2026-05-14T03:12:00Z",
        "last_incident_summary": "Elevated 5xx rate for 4 min during deploy; auto-recovered",
        "response_time_ms": 42,
    },
    "database": {
        "status": "healthy",
        "uptime_pct": 99.99,
        "last_incident": "2026-04-28T11:45:00Z",
        "last_incident_summary": "Read replica failover; promoted standby in 18 s",
        "response_time_ms": 8,
    },
    "cache": {
        "status": "degraded",
        "uptime_pct": 98.61,
        "last_incident": "2026-05-26T19:30:00Z",
        "last_incident_summary": "Memory eviction spike; hit-rate dropped to 71% — monitoring",
        "response_time_ms": 3,
    },
    "queue": {
        "status": "healthy",
        "uptime_pct": 99.95,
        "last_incident": "2026-05-01T08:00:00Z",
        "last_incident_summary": "Consumer lag spike during batch import; resolved in 11 min",
        "response_time_ms": 6,
    },
    "cdn": {
        "status": "healthy",
        "uptime_pct": 100.0,
        "last_incident": "2026-03-19T22:00:00Z",
        "last_incident_summary": "Edge node degradation in AP-Southeast; rerouted automatically",
        "response_time_ms": 18,
    },
}


class HealthCheckTool(ToolExecutor):
    """Return current mock health status for all infrastructure services."""

    name: ClassVar[str] = "check_health"
    description: ClassVar[str] = (
        "Return the current health status for all infrastructure services. "
        "Each service reports status (healthy/degraded/down), uptime percentage, "
        "response time in milliseconds, and details of the last incident."
    )
    input_schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "properties": {
            "services": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Optional list of service names to check. "
                    "Leave empty to check all services: "
                    "api-server, database, cache, queue, cdn."
                ),
            }
        },
        "required": [],
    }

    async def execute(self, services: list[str] | None = None) -> str:  # type: ignore[override]
        targets = services if services else list(_SERVICES.keys())
        unknown = [s for s in targets if s not in _SERVICES]

        results = {}
        for name in targets:
            if name not in _SERVICES:
                continue
            svc = dict(_SERVICES[name])
            last = datetime.fromisoformat(svc["last_incident"].replace("Z", "+00:00"))
            svc["last_incident_age_hours"] = round(
                (datetime.now(timezone.utc) - last).total_seconds() / 3600, 1
            )
            results[name] = svc

        overall = (
            "down" if any(s["status"] == "down" for s in results.values())
            else "degraded" if any(s["status"] == "degraded" for s in results.values())
            else "healthy"
        )

        payload = {
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "overall_status": overall,
            "services": results,
        }
        if unknown:
            payload["unknown_services"] = unknown

        return json.dumps(payload, default=str)
