"""In-memory report store — shared between routes_reports and routes_dashboard.

State is process-scoped and resets on restart. Suitable for single-instance
Railway deployment. Upgrade to Redis or DB-backed store for multi-instance.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

from tools.noise_detector import compute_dashboard_stats

_lock = threading.Lock()
_reports: list[dict[str, Any]] = []
_counter = 0


def add_report(filename: str, alerts: list[dict], classified: list[dict]) -> dict[str, Any]:
    global _counter
    noise_count = sum(1 for a in classified if a["classification"] == "noise")
    genuine_count = len(classified) - noise_count
    stats = compute_dashboard_stats(classified)
    with _lock:
        _counter += 1
        report: dict[str, Any] = {
            "id": _counter,
            "filename": filename,
            "_alerts": alerts,
            "_classified": classified,
            "_stats": stats,
            "total_alerts": len(alerts),
            "genuine_count": genuine_count,
            "noise_count": noise_count,
            "status": "ready",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        _reports.insert(0, report)
    return _public(report)


def list_reports() -> list[dict[str, Any]]:
    with _lock:
        return [_public(r) for r in _reports]


def get_report_classified(report_id: int) -> list[dict] | None:
    with _lock:
        for r in _reports:
            if r["id"] == report_id:
                return r["_classified"]
        return None


def get_latest_classified() -> list[dict] | None:
    with _lock:
        return _reports[0]["_classified"] if _reports else None


def get_latest_stats() -> dict[str, Any] | None:
    with _lock:
        return _reports[0]["_stats"] if _reports else None


def get_latest_meta() -> dict[str, Any] | None:
    with _lock:
        return _public(_reports[0]) if _reports else None


def _public(r: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in r.items() if not k.startswith("_")}
