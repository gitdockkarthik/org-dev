"""Report store — shared between routes_reports and routes_dashboard.

The in-memory ``_reports`` list is the primary, fast read path (process-scoped,
resets on restart). On top of it, ``store_report``/``load_latest_from_db`` add an
optional PostgreSQL-backed cache so the latest report survives restarts. The
in-memory path is preferred; the DB cache is a fallback used when ``_reports`` is
empty (e.g. immediately after a fresh boot).
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select, text

from tools.dashboard_builder import compute_dashboard_stats

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_reports: list[dict[str, Any]] = []
_counter = 0

# DB-backed persistence cache (populated by store_report / load_latest_from_db).
# Used as a fallback by get_latest_stats / get_latest_meta when _reports is empty.
_stats_cache: dict | None = None
_meta_cache: dict | None = None
# Strong refs to in-flight background persist tasks so they aren't GC'd mid-run.
_persist_tasks: set = set()


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
        # Cap in-memory report list to avoid unbounded memory growth.
        # Only the latest 3 reports are kept; older entries are evicted.
        while len(_reports) > 3:
            _reports.pop()
    meta = _public(report)
    _schedule_persist(classified, meta)
    return meta


def _schedule_persist(classified: list[dict], meta: dict[str, Any]) -> None:
    """Fire-and-forget DB persist when an event loop is running.

    add_report stays synchronous (its callers depend on that); persistence runs as
    a background task. With no running loop (e.g. unit tests) we skip silently — the
    in-memory store still works either way.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    task = loop.create_task(store_report(classified, meta))
    _persist_tasks.add(task)
    task.add_done_callback(_persist_tasks.discard)


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
        if _reports:
            stats = _reports[0]["_stats"]
            classified = _reports[0].get("_classified")
        else:
            stats = _stats_cache
            classified = None

    # Cached stats may predate the dedup fix and lack the raw/duplicate fields
    # (genuine_count_raw etc.). Recompute from the stored classified alerts so the
    # new fields are always present without waiting for the next sync, and write
    # the result back so we only pay for the recompute once.
    if stats and "genuine_count_raw" not in stats and classified:
        stats = compute_dashboard_stats(classified)
        with _lock:
            if _reports and _reports[0].get("_classified") is classified:
                _reports[0]["_stats"] = stats
    return stats


def get_latest_meta() -> dict[str, Any] | None:
    with _lock:
        if _reports:
            return _public(_reports[0])
    return _meta_cache


def _public(r: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in r.items() if not k.startswith("_")}


# ── PostgreSQL-backed persistence (survives restarts) ─────────────────────────


async def store_report(classified: list[dict], meta: dict) -> None:
    """Persist a report to PostgreSQL and refresh the persistence cache."""
    from config import settings
    from database import SessionLocal
    from models import AlertReport

    stats = compute_dashboard_stats(classified)

    # Refresh cache first so the fallback reflects the latest even without a DB.
    global _stats_cache, _meta_cache
    _stats_cache = stats
    _meta_cache = meta

    if SessionLocal is None:
        logger.warning("store_report: no DB — cached in memory only")
        return

    try:
        async with SessionLocal() as session:
            report = AlertReport(
                agent_slug=settings.agent_slug,
                filename=meta.get("filename", ""),
                total_alerts=meta.get("total_alerts", 0),
                genuine_count=meta.get("genuine_count", 0),
                noise_count=meta.get("noise_count", 0),
                suspect_count=stats.get("suspect_count", 0),
                status="ready",
                report_data=json.dumps(classified),
                stats_data=json.dumps(stats),
            )
            session.add(report)
            await session.commit()

            # Keep only the 48 most recent reports (~12 hours) for this agent.
            result = await session.execute(
                select(AlertReport)
                .where(AlertReport.agent_slug == settings.agent_slug)
                .order_by(AlertReport.created_at.desc())
                .offset(48)
            )
            for old in result.scalars().all():
                await session.delete(old)
            await session.commit()

        # Insert lightweight summary row for 90-day trend analysis
        try:
            total = meta.get("total_alerts", 0)
            genuine = meta.get("genuine_count", 0)
            noise = meta.get("noise_count", 0)
            suspect = stats.get("suspect_count", 0)
            noise_pct = round((noise / total * 100), 2) if total else 0

            priority_counts = stats.get("priority_counts", {})
            p1 = priority_counts.get("P1", 0)
            p2 = priority_counts.get("P2", 0)
            p3 = priority_counts.get("P3", 0)
            p4 = priority_counts.get("P4", 0)
            p5 = priority_counts.get("P5", 0)

            _lc = stats.get("data_quality", {}).get("lifecycle", {})
            _ack = stats.get("data_quality", {}).get("acknowledgement", {})
            never_closed_pct = _lc.get("never_closed_pct")
            acknowledged_pct = _lc.get("acknowledged_pct")
            proper_cycle_pct = _lc.get("proper_cycle_pct")
            never_acked_pct = _ack.get("never_acked_pct")

            async with SessionLocal() as _sum_sess:
                # Get previous row to compute deltas
                _prev = await _sum_sess.execute(
                    text("""
                        SELECT total_alerts, genuine_count,
                               noise_count, suspect_count
                        FROM alert_report_summary
                        WHERE agent_slug = :slug
                        ORDER BY synced_at DESC LIMIT 1
                    """),
                    {"slug": settings.agent_slug}
                )
                _prev_row = _prev.fetchone()
                if _prev_row:
                    new_alerts  = max(0, total - (_prev_row.total_alerts or 0))
                    new_genuine = max(0, genuine - (_prev_row.genuine_count or 0))
                    new_noise   = max(0, noise - (_prev_row.noise_count or 0))
                    new_suspect = max(0, suspect - (_prev_row.suspect_count or 0))
                else:
                    new_alerts  = total
                    new_genuine = genuine
                    new_noise   = noise
                    new_suspect = suspect

                await _sum_sess.execute(
                    text("""
                        INSERT INTO alert_report_summary
                          (agent_slug, synced_at, total_alerts,
                           genuine_count, noise_count, suspect_count,
                           noise_pct, p1_count, p2_count, p3_count,
                           p4_count, p5_count,
                           new_alerts, new_genuine, new_noise, new_suspect,
                           never_closed_pct, acknowledged_pct,
                           proper_cycle_pct, never_acked_pct)
                        VALUES
                          (:slug, NOW(), :total, :genuine, :noise,
                           :suspect, :noise_pct, :p1, :p2, :p3, :p4, :p5,
                           :new_alerts, :new_genuine,
                           :new_noise, :new_suspect,
                           :never_closed_pct, :acknowledged_pct,
                           :proper_cycle_pct, :never_acked_pct)
                    """),
                    {
                        "slug": settings.agent_slug,
                        "total": total, "genuine": genuine,
                        "noise": noise, "suspect": suspect,
                        "noise_pct": noise_pct,
                        "p1": p1, "p2": p2, "p3": p3,
                        "p4": p4, "p5": p5,
                        "new_alerts": new_alerts,
                        "new_genuine": new_genuine,
                        "new_noise": new_noise,
                        "new_suspect": new_suspect,
                        "never_closed_pct": never_closed_pct,
                        "acknowledged_pct": acknowledged_pct,
                        "proper_cycle_pct": proper_cycle_pct,
                        "never_acked_pct": never_acked_pct,
                    }
                )
                await _sum_sess.commit()
        except Exception as _e:
            logger.warning("store_report: summary insert failed: %s", _e)

        logger.info("store_report: saved to DB, total=%d", meta.get("total_alerts", 0))
    except Exception as e:
        logger.error("store_report: DB save failed: %s", e)


async def load_latest_from_db() -> bool:
    """Load all persisted reports into cache on startup."""
    from config import settings
    from database import SessionLocal
    from models import AlertReport
    if SessionLocal is None:
        return False
    global _stats_cache, _meta_cache
    try:
        async with SessionLocal() as session:
            result = await session.execute(
                select(AlertReport)
                .where(AlertReport.agent_slug == settings.agent_slug)
                .order_by(AlertReport.created_at.desc())
                .limit(3)
            )
            reports = result.scalars().all()
            if not reports:
                return False
            _reports.clear()
            for report in reports:
                stats = json.loads(report.stats_data) if report.stats_data else None
                classified = json.loads(report.report_data) if report.report_data else []
                _reports.append({
                    "id": report.id,
                    "filename": report.filename,
                    "total_alerts": report.total_alerts,
                    "genuine_count": report.genuine_count,
                    "noise_count": report.noise_count,
                    "suspect_count": report.suspect_count,
                    "status": report.status,
                    "created_at": report.created_at.isoformat(),
                    "_classified": classified,
                    "_stats": stats,
                })
            # Use latest report for stats/meta cache
            latest = reports[0]
            _stats_cache = json.loads(latest.stats_data) if latest.stats_data else None
            _meta_cache = {
                "id": latest.id,
                "filename": latest.filename,
                "total_alerts": latest.total_alerts,
                "genuine_count": latest.genuine_count,
                "noise_count": latest.noise_count,
                "suspect_count": latest.suspect_count,
                "status": latest.status,
                "created_at": latest.created_at.isoformat(),
            }
            logger.info(
                "load_latest_from_db: restored %d reports, latest id=%d total=%d",
                len(reports), latest.id, latest.total_alerts,
            )
            return True
    except Exception as e:
        logger.error("load_latest_from_db: failed: %s", e)
        return False

def get_reports() -> list[dict]:
    if _reports:
        return list_reports()
    return [_meta_cache] if _meta_cache is not None else []


def get_latest_csv() -> None:
    return None
