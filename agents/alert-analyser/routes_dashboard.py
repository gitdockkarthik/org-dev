from fastapi import APIRouter

from report_store import get_latest_stats, get_latest_meta

router = APIRouter(tags=["dashboard"])


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
                    q = select(AlertReport).where(
                        AlertReport.agent_slug == 'alert-analyser'
                    ).order_by(AlertReport.created_at.desc())
                    from sqlalchemy import cast
                    from sqlalchemy.dialects.postgresql import TIMESTAMP
                    if from_date:
                        q = q.where(
                            AlertReport.created_at >= cast(from_date, TIMESTAMP(timezone=True))
                        )
                    if to_date:
                        q = q.where(
                            AlertReport.created_at <= cast(
                                to_date + ' 23:59:59', TIMESTAMP(timezone=True)
                            )
                        )
                    result = await sess.execute(q)
                    reports = result.scalars().all()
                if reports:
                    latest = reports[0]
                    stats = _json.loads(latest.stats_data) \
                        if latest.stats_data else {}
                    return {
                        "stats": stats,
                        "report": {
                            "total_alerts": latest.total_alerts,
                            "genuine_count": latest.genuine_count,
                            "noise_count": latest.noise_count,
                            "suspect_count": latest.suspect_count,
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
        "stats": stats,
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
            if from_date:
                sql += " AND synced_at >= :from_date"
                params["from_date"] = from_date
            if to_date:
                sql += " AND synced_at <= :to_date"
                params["to_date"] = to_date + " 23:59:59"
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
