from fastapi import APIRouter

from report_store import get_latest_stats, get_latest_meta

router = APIRouter(tags=["dashboard"])


def _ensure_dedup_fields(stats: dict) -> dict:
    """Guarantee the dedup fields are present in a stats dict.

    get_latest_stats() recomputes from classified alerts when it can; this is a
    final safety net for any stats that still predate the dedup fix (e.g. a stale
    DB stats_data blob used as a filtered-period fallback). Missing fields are
    backfilled treating the cached counts as already-deduplicated (zero dupes), so
    the dashboard always receives the keys it expects.
    """
    if stats and "genuine_count_raw" not in stats:
        stats = {
            **stats,
            "genuine_count_raw": stats.get("genuine_count", 0),
            "genuine_duplicates": 0,
            "noise_count_raw": stats.get("noise_count", 0),
            "noise_duplicates": 0,
            "suspect_count_raw": stats.get("suspect_count", 0),
        }
    return stats


def _filter_alerts_by_date(
    classified: list, from_date: str | None, to_date: str | None
) -> list:
    """Filter classified alerts by their OpsGenie ``createdAt`` timestamp.

    Shared by /dashboard and /dashboard/period-summary so every date-scoped view
    counts the same alerts (by when the alert was created, not when it was synced).
    Alerts with a missing or unparseable ``createdAt`` are kept (fail-open),
    matching the original /dashboard behaviour.
    """
    from datetime import datetime, timezone

    # Normalise datetime-local 'T' separator (e.g. "2026-06-24T06:46") so the
    # space-based strptime formats below parse it.
    from_date = from_date.replace('T', ' ') if from_date else from_date
    to_date = to_date.replace('T', ' ') if to_date else to_date

    dt_from = dt_to = None
    if from_date:
        try:
            dt_from = datetime.strptime(
                from_date, '%Y-%m-%d %H:%M'
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            dt_from = datetime.strptime(
                from_date, '%Y-%m-%d'
            ).replace(tzinfo=timezone.utc)
    if to_date:
        try:
            dt_to = datetime.strptime(
                to_date, '%Y-%m-%d %H:%M'
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            dt_to = datetime.strptime(
                to_date + ' 23:59:59', '%Y-%m-%d %H:%M:%S'
            ).replace(tzinfo=timezone.utc)

    filtered = []
    for alert in classified:
        created = alert.get('createdAt', '')
        if not created:
            filtered.append(alert)
            continue
        try:
            # Parse ISO timestamp from OpsGenie
            alert_dt = datetime.fromisoformat(created.replace('Z', '+00:00'))
            if dt_from and alert_dt < dt_from:
                continue
            if dt_to and alert_dt > dt_to:
                continue
            filtered.append(alert)
        except Exception:
            filtered.append(alert)
    return filtered


@router.get("/dashboard")
async def get_dashboard(
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict:
    """Return precomputed stats for the most recently uploaded/generated report."""
    if from_date or to_date:
        from database import SessionLocal
        from models import AlertReport
        from sqlalchemy import select
        import json as _json
        if SessionLocal is not None:
            try:
                async with SessionLocal() as sess:
                    # Always use the latest report regardless of when it was
                    # synced, then filter its alerts by createdAt. Filtering
                    # reports by report.created_at (sync time) wrongly dropped
                    # the report when the sync fell outside the selected window,
                    # falling back to all-time unfiltered stats.
                    q = select(AlertReport).where(
                        AlertReport.agent_slug == 'alert-analyser'
                    ).order_by(AlertReport.created_at.desc())
                    result = await sess.execute(q)
                    reports = result.scalars().all()
                if reports:
                    latest = reports[0]
                    import json as _json
                    from tools.dashboard_builder import compute_dashboard_stats

                    # Load classified alerts and filter by createdAt for true period stats
                    raw_classified = _json.loads(latest.report_data) \
                        if latest.report_data else []

                    # Filter alerts by createdAt within the selected period
                    filtered = _filter_alerts_by_date(
                        raw_classified, from_date, to_date
                    )

                    # Recompute stats from filtered alerts
                    if filtered:
                        stats = compute_dashboard_stats(filtered)
                    else:
                        stats = _json.loads(latest.stats_data) \
                            if latest.stats_data else {}
                    return {
                        "stats": _ensure_dedup_fields(stats),
                        "report": {
                            "total_alerts": stats.get("total", latest.total_alerts),
                            "genuine_count": stats.get("genuine_count", latest.genuine_count),
                            "noise_count": stats.get("noise_count", latest.noise_count),
                            "suspect_count": stats.get("suspect_count", latest.suspect_count),
                            "created_at": str(latest.created_at),
                            "filename": latest.filename,
                        },
                        "filtered": True,
                        "report_count": len(reports),
                    }
            except Exception as _e:
                import logging
                logging.getLogger(__name__).error(
                    f"dashboard filter error: {_e}"
                )
    stats = get_latest_stats()
    if stats is None:
        return {"empty": True}
    return {
        "stats": _ensure_dedup_fields(stats),
        "report": get_latest_meta(),
    }


@router.get("/dashboard/trend")
async def get_dashboard_trend(
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict:
    """Return time-series of genuine/noise/suspect counts
    from alert_report_summary for trend and compare views."""
    from database import SessionLocal
    from sqlalchemy import select, text
    import logging
    _log = logging.getLogger(__name__)
    if SessionLocal is None:
        return {"empty": True, "points": []}
    try:
        async with SessionLocal() as sess:
            sql = """
                SELECT synced_at, total_alerts, genuine_count,
                       noise_count, suspect_count, noise_pct,
                       p1_count, p2_count, p3_count, p4_count, p5_count
                FROM alert_report_summary
                WHERE agent_slug = 'alert-analyser'
            """
            params = {}
            from datetime import datetime, timezone
            if from_date:
                sql += " AND synced_at >= :from_date"
                try:
                    dt_from = datetime.strptime(
                        from_date, '%Y-%m-%d %H:%M'
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    dt_from = datetime.strptime(
                        from_date, '%Y-%m-%d'
                    ).replace(tzinfo=timezone.utc)
                params["from_date"] = dt_from
            if to_date:
                sql += " AND synced_at <= :to_date"
                try:
                    dt_to = datetime.strptime(
                        to_date, '%Y-%m-%d %H:%M'
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    dt_to = datetime.strptime(
                        to_date + ' 23:59:59', '%Y-%m-%d %H:%M:%S'
                    ).replace(tzinfo=timezone.utc)
                params["to_date"] = dt_to
            sql += " ORDER BY synced_at ASC"
            result = await sess.execute(text(sql), params)
            rows = result.fetchall()
        if not rows:
            return {"empty": True, "points": []}
        points = [
            {
                "time": str(r.synced_at)[:16].replace(' ', 'T'),
                "total": r.total_alerts or 0,
                "genuine": r.genuine_count or 0,
                "noise": r.noise_count or 0,
                "suspect": r.suspect_count or 0,
                "noise_pct": float(r.noise_pct or 0),
                "p1": r.p1_count or 0,
                "p2": r.p2_count or 0,
                "p3": r.p3_count or 0,
                "p4": r.p4_count or 0,
                "p5": r.p5_count or 0,
            }
            for r in rows
        ]
        return {"empty": False, "points": points}
    except Exception as e:
        _log.error(f"trend error: {e}")
        return {"empty": True, "points": []}


@router.get("/dashboard/period-summary")
async def get_period_summary(
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict:
    """Return summed delta alerts for a time period from
    alert_report_summary. Used for Row 2 KPI cards."""
    from database import SessionLocal
    from sqlalchemy import text
    from datetime import datetime, timezone
    import logging
    _log = logging.getLogger(__name__)
    if SessionLocal is None:
        return {"empty": True}

    # When an explicit date range is given, derive Row 2 from the alerts'
    # createdAt — exactly as /dashboard does — so the period KPIs match the
    # Genuine tab for the same range instead of filtering syncs by synced_at.
    if from_date and to_date:
        from models import AlertReport
        from sqlalchemy import select
        from tools.dashboard_builder import compute_dashboard_stats
        import json as _json
        try:
            async with SessionLocal() as sess:
                # Always use the latest report (same as /dashboard's reports[0]),
                # regardless of when it was synced, then filter its alerts by
                # createdAt. Filtering reports by report.created_at (sync time)
                # wrongly dropped the report when the sync fell outside the
                # selected window — which showed Row 2 as "NO SYNCS".
                q = select(AlertReport).where(
                    AlertReport.agent_slug == 'alert-analyser'
                ).order_by(AlertReport.created_at.desc())
                result = await sess.execute(q)
                reports = result.scalars().all()

            if reports:
                latest = reports[0]
                raw_classified = _json.loads(latest.report_data) \
                    if latest.report_data else []
                filtered = _filter_alerts_by_date(
                    raw_classified, from_date, to_date
                )
                stats = compute_dashboard_stats(filtered)
                return {
                    "empty": False,
                    "new_alerts": stats["total"],
                    "new_genuine": stats["genuine_count"],
                    "new_noise": stats["noise_count"],
                    "new_suspect": stats["suspect_count"],
                    "noise_rate": stats["noise_ratio"],
                    "period_from": from_date,
                    "period_to": to_date,
                    "sync_count": 0,
                    "duplicate_count": stats.get("duplicate_count", 0),
                    "genuine_duplicates": stats.get("genuine_duplicates", 0),
                    "noise_duplicates": stats.get("noise_duplicates", 0),
                }
            # No report ingested in range — fall through to the synced_at path.
        except Exception as _e:
            _log.error(f"period_summary createdAt filter error: {_e}")

    try:
        async with SessionLocal() as sess:
            # Get oldest sync date for Row 1 label
            oldest = await sess.execute(
                text("""
                    SELECT MIN(synced_at) as oldest,
                           MAX(synced_at) as newest
                    FROM alert_report_summary
                    WHERE agent_slug = 'alert-analyser'
                """)
            )
            oldest_row = oldest.fetchone()
            oldest_date = str(oldest_row.oldest)[:10] \
                if oldest_row and oldest_row.oldest else None
            newest_date = str(oldest_row.newest)[:16] \
                if oldest_row and oldest_row.newest else None

            # Build period filter
            sql = """
                SELECT
                    COUNT(*) as sync_count,
                    SUM(new_alerts) as new_alerts,
                    SUM(new_genuine) as new_genuine,
                    SUM(new_noise) as new_noise,
                    SUM(new_suspect) as new_suspect,
                    MIN(synced_at) as period_from,
                    MAX(synced_at) as period_to,
                    AVG(never_closed_pct) as avg_never_closed_pct,
                    AVG(acknowledged_pct) as avg_acknowledged_pct,
                    AVG(proper_cycle_pct) as avg_proper_cycle_pct,
                    AVG(never_acked_pct) as avg_never_acked_pct
                FROM alert_report_summary
                WHERE agent_slug = 'alert-analyser'
            """
            params = {}
            if from_date:
                sql += " AND synced_at >= :from_date"
                _fd = from_date.replace('T', ' ')
                try:
                    params["from_date"] = datetime.strptime(
                        _fd, '%Y-%m-%d %H:%M'
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    params["from_date"] = datetime.strptime(
                        _fd, '%Y-%m-%d'
                    ).replace(tzinfo=timezone.utc)
            if to_date:
                sql += " AND synced_at <= :to_date"
                _td = to_date.replace('T', ' ')
                try:
                    params["to_date"] = datetime.strptime(
                        _td, '%Y-%m-%d %H:%M'
                    ).replace(tzinfo=timezone.utc)
                except ValueError:
                    params["to_date"] = datetime.strptime(
                        _td + ' 23:59:59', '%Y-%m-%d %H:%M:%S'
                    ).replace(tzinfo=timezone.utc)

            result = await sess.execute(text(sql), params)
            row = result.fetchone()

        if not row or not row.sync_count:
            return {
                "empty": True,
                "sync_count": 0,
                "oldest_date": oldest_date,
                "newest_date": newest_date,
            }

        return {
            "empty": False,
            "sync_count": int(row.sync_count),
            "new_alerts": int(row.new_alerts or 0),
            "new_genuine": int(row.new_genuine or 0),
            "new_noise": int(row.new_noise or 0),
            "new_suspect": int(row.new_suspect or 0),
            "period_from": str(row.period_from)[:16] if row.period_from else None,
            "period_to": str(row.period_to)[:16] if row.period_to else None,
            "avg_never_closed_pct": round(float(row.avg_never_closed_pct), 1) if row.avg_never_closed_pct else None,
            "avg_acknowledged_pct": round(float(row.avg_acknowledged_pct), 1) if row.avg_acknowledged_pct else None,
            "avg_proper_cycle_pct": round(float(row.avg_proper_cycle_pct), 1) if row.avg_proper_cycle_pct else None,
            "avg_never_acked_pct": round(float(row.avg_never_acked_pct), 1) if row.avg_never_acked_pct else None,
            "oldest_date": oldest_date,
            "newest_date": newest_date,
        }
    except Exception as e:
        _log.error(f"period_summary error: {e}")
        return {"empty": True}
