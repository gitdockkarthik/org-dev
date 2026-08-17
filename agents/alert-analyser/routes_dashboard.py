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
    if stats:
        stats.setdefault(
            "suspect_duplicates",
            stats.get("suspect_count_raw", stats.get("suspect_count", 0))
            - stats.get("suspect_count", 0),
        )
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
                    ).order_by(AlertReport.created_at.desc()).limit(1)
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

                    # Query incidents for period
                    new_incidents_count = 0
                    recurring_incidents_count = 0
                    try:
                        from sqlalchemy import text
                        from datetime import datetime, timezone
                        _fd = from_date.replace('T', ' ') if from_date else None
                        _td = to_date.replace('T', ' ') if to_date else None
                        incident_params = {}

                        if _fd:
                            try:
                                incident_params["period_start"] = datetime.strptime(
                                    _fd, '%Y-%m-%d %H:%M'
                                ).replace(tzinfo=timezone.utc)
                            except ValueError:
                                incident_params["period_start"] = datetime.strptime(
                                    _fd, '%Y-%m-%d'
                                ).replace(tzinfo=timezone.utc)

                        if _td:
                            try:
                                incident_params["period_end"] = datetime.strptime(
                                    _td, '%Y-%m-%d %H:%M'
                                ).replace(tzinfo=timezone.utc)
                            except ValueError:
                                incident_params["period_end"] = datetime.strptime(
                                    _td + ' 23:59:59', '%Y-%m-%d %H:%M:%S'
                                ).replace(tzinfo=timezone.utc)

                        if incident_params.get("period_start") and incident_params.get("period_end"):
                            async with SessionLocal() as incident_sess:
                                incident_result = await incident_sess.execute(
                                    text("""
                                        SELECT COUNT(DISTINCT alert_id) as incident_count
                                        FROM incident_management.incidents
                                        WHERE (alert_payload->>'createdAt')::timestamptz BETWEEN :period_start AND :period_end
                                    """),
                                    incident_params
                                )
                                incident_row = incident_result.fetchone()
                                if incident_row:
                                    new_incidents_count = int(incident_row.incident_count or 0)
                    except Exception as _ie:
                        import logging
                        logging.getLogger(__name__).error(f"dashboard incident query error: {_ie}")

                    recurring_incidents_count = max(0, stats.get("genuine_count", 0) - new_incidents_count)
                    stats["new_incidents_count"] = new_incidents_count
                    stats["recurring_incidents_count"] = recurring_incidents_count

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

    # Query all incidents (no date filter) for all-time view
    new_incidents_count = 0
    recurring_incidents_count = 0
    try:
        from database import SessionLocal
        from sqlalchemy import text
        import logging
        _log = logging.getLogger(__name__)
        if SessionLocal is not None:
            async with SessionLocal() as sess:
                incident_result = await sess.execute(
                    text("""
                        SELECT COUNT(DISTINCT alert_id) as incident_count
                        FROM incident_management.incidents
                    """)
                )
                incident_row = incident_result.fetchone()
                if incident_row:
                    new_incidents_count = int(incident_row.incident_count or 0)
    except Exception as _ie:
        import logging
        logging.getLogger(__name__).error(f"dashboard all-time incident query error: {_ie}")

    recurring_incidents_count = max(0, stats.get("genuine_count", 0) - new_incidents_count)
    stats["new_incidents_count"] = new_incidents_count
    stats["recurring_incidents_count"] = recurring_incidents_count

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
                ).order_by(AlertReport.created_at.desc()).limit(1)
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

                # Query incident_management.incidents for period
                new_incidents_count = 0
                recurring_incidents_count = 0
                try:
                    async with SessionLocal() as incident_sess:
                        # Convert from_date/to_date to datetime for incident query
                        _fd = from_date.replace('T', ' ') if from_date else None
                        _td = to_date.replace('T', ' ') if to_date else None
                        incident_params = {}

                        if _fd:
                            try:
                                incident_params["period_start"] = datetime.strptime(
                                    _fd, '%Y-%m-%d %H:%M'
                                ).replace(tzinfo=timezone.utc)
                            except ValueError:
                                incident_params["period_start"] = datetime.strptime(
                                    _fd, '%Y-%m-%d'
                                ).replace(tzinfo=timezone.utc)

                        if _td:
                            try:
                                incident_params["period_end"] = datetime.strptime(
                                    _td, '%Y-%m-%d %H:%M'
                                ).replace(tzinfo=timezone.utc)
                            except ValueError:
                                incident_params["period_end"] = datetime.strptime(
                                    _td + ' 23:59:59', '%Y-%m-%d %H:%M:%S'
                                ).replace(tzinfo=timezone.utc)

                        if incident_params.get("period_start") and incident_params.get("period_end"):
                            incident_result = await incident_sess.execute(
                                text("""
                                    SELECT COUNT(DISTINCT alert_id) as incident_count
                                    FROM incident_management.incidents
                                    WHERE (alert_payload->>'createdAt')::timestamptz BETWEEN :period_start AND :period_end
                                """),
                                incident_params
                            )
                            incident_row = incident_result.fetchone()
                            if incident_row:
                                new_incidents_count = int(incident_row.incident_count or 0)
                except Exception as _ie:
                    _log.error(f"period_summary incident query error: {_ie}")

                recurring_incidents_count = max(0, stats["genuine_count"] - new_incidents_count)

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
                    "suspect_duplicates": stats.get("suspect_duplicates", 0),
                    "new_incidents_count": new_incidents_count,
                    "recurring_incidents_count": recurring_incidents_count,
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

            # Query incidents for period
            new_incidents_count = 0
            recurring_incidents_count = 0
            try:
                if params.get("from_date") and params.get("to_date"):
                    incident_result = await sess.execute(
                        text("""
                            SELECT COUNT(DISTINCT alert_id) as incident_count
                            FROM incident_management.incidents
                            WHERE created_at BETWEEN :period_start AND :period_end
                        """),
                        {"period_start": params["from_date"], "period_end": params["to_date"]}
                    )
                    incident_row = incident_result.fetchone()
                    if incident_row:
                        new_incidents_count = int(incident_row.incident_count or 0)
            except Exception as _ie:
                _log.error(f"period_summary incident query error: {_ie}")

            # Compute recurring as genuine_count - new_incidents (floor at 0)
            new_genuine = int(row.new_genuine or 0) if row else 0
            recurring_incidents_count = max(0, new_genuine - new_incidents_count)

        if not row or not row.sync_count:
            return {
                "empty": True,
                "sync_count": 0,
                "oldest_date": oldest_date,
                "newest_date": newest_date,
                "new_incidents_count": 0,
                "recurring_incidents_count": 0,
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
            "new_incidents_count": new_incidents_count,
            "recurring_incidents_count": recurring_incidents_count,
        }
    except Exception as e:
        _log.error(f"period_summary error: {e}")
        return {"empty": True}


@router.get("/dashboard/incidents")
async def get_incidents() -> dict:
    """Return incident pipeline flow, aging analysis, resolution metrics, and recurrence signal."""
    from database import SessionLocal
    from sqlalchemy import text
    from datetime import datetime, timezone
    from fastapi import HTTPException
    import logging
    _log = logging.getLogger(__name__)

    if SessionLocal is None:
        return {"empty": True}

    try:
        async with SessionLocal() as sess:
            # 1. Pipeline flow: count by status (all 6 statuses always present)
            result = await sess.execute(text("""
                SELECT status, COUNT(*) as cnt
                FROM incident_management.incidents
                GROUP BY status
            """))
            status_rows = result.fetchall()
            status_counts = {row.status: row.cnt for row in status_rows}
            pipeline_flow = [
                {"status": s, "count": status_counts.get(s, 0)}
                for s in ["ESCALATED", "INVESTIGATING", "RCA_COMPLETE", "REMEDIATING", "RESOLVED", "MANUAL"]
            ]

            # 2. Aging buckets: priority-based age bands
            result = await sess.execute(text("""
                SELECT priority,
                       EXTRACT(EPOCH FROM (NOW() - COALESCE(escalated_at, created_at)))/3600 as hours_waiting
                FROM incident_management.incidents
                WHERE status NOT IN ('RESOLVED', 'MANUAL', 'PURGED')
                AND priority IN ('P1', 'P2', 'P3', 'P4')
            """))
            aging_rows = result.fetchall()

            # Priority-based age bands (hours)
            # P1: <2h, 2-6h, 6-24h, 24h+
            # P2: <4h, 4-12h, 12-48h, 48h+
            # P3: <8h, 8-24h, 24-72h, 72h+
            # P4: <72h, 72-168h, 168-720h, 720h+
            priority_bands = {
                'P1': [('< 2h', 0, 2), ('2–6h', 2, 6), ('6–24h', 6, 24), ('24h+', 24, None)],
                'P2': [('< 4h', 0, 4), ('4–12h', 4, 12), ('12–48h', 12, 48), ('48h+', 48, None)],
                'P3': [('< 8h', 0, 8), ('8–24h', 8, 24), ('24–72h', 24, 72), ('72h+', 72, None)],
                'P4': [('< 3d', 0, 72), ('3–7d', 72, 168), ('7–30d', 168, 720), ('30d+', 720, None)],
            }
            priority_aging = {p: {band[0]: 0 for band in bands} for p, bands in priority_bands.items()}
            for row in aging_rows:
                p = row.priority
                h = row.hours_waiting or 0
                if p in priority_bands:
                    for label, low, high in priority_bands[p]:
                        if high is None and h >= low:
                            priority_aging[p][label] += 1
                            break
                        elif high is not None and low <= h < high:
                            priority_aging[p][label] += 1
                            break
            aging_buckets = [
                {"priority": p, "bands": [{"bucket": k, "count": v} for k, v in bands.items()]}
                for p, bands in priority_aging.items()
            ]

            # 2b. SLA breach detection: priority-based resolution time thresholds (minutes)
            sla_thresholds = {"P1": 120, "P2": 240, "P3": 480, "P4": 4320}  # resolution SLAs
            breached_count = 0
            within_sla_count = 0
            per_priority_breach = {}
            try:
                result = await sess.execute(text("""
                    SELECT priority,
                           EXTRACT(EPOCH FROM (NOW() - COALESCE(escalated_at, created_at)))/60 as minutes_waiting
                    FROM incident_management.incidents
                    WHERE status NOT IN ('RESOLVED', 'MANUAL', 'PURGED')
                    AND priority IN ('P1', 'P2', 'P3', 'P4')
                """))
                sla_rows = result.fetchall()
                priority_counts = {"P1": {"breached": 0, "within": 0}, "P2": {"breached": 0, "within": 0}, "P3": {"breached": 0, "within": 0}, "P4": {"breached": 0, "within": 0}}
                for row in sla_rows:
                    if row.minutes_waiting is not None and row.priority in priority_counts:
                        threshold = sla_thresholds.get(row.priority, 4320)
                        if row.minutes_waiting > threshold:
                            breached_count += 1
                            priority_counts[row.priority]["breached"] += 1
                        else:
                            within_sla_count += 1
                            priority_counts[row.priority]["within"] += 1
                for p, counts in priority_counts.items():
                    total_p = counts["breached"] + counts["within"]
                    per_priority_breach[p] = {
                        "breached": counts["breached"],
                        "within": counts["within"],
                        "breach_pct": round(counts["breached"] / total_p * 100, 1) if total_p > 0 else 0,
                        "sla_minutes": sla_thresholds[p],
                    }
            except Exception as _sla_e:
                _log.error(f"SLA breach calculation error: {_sla_e}")

            total_sla = breached_count + within_sla_count
            breach_pct = (breached_count / total_sla * 100) if total_sla > 0 else 0
            sla_breach = {
                "breached_count": breached_count,
                "within_sla_count": within_sla_count,
                "breach_pct": round(breach_pct, 1),
                "per_priority": per_priority_breach,
            }

            # 3. Aging longest wait: single ticket with max age (non-resolved/manual)
            result = await sess.execute(text("""
                SELECT id, alert_id, title,
                       EXTRACT(EPOCH FROM (NOW() - COALESCE(escalated_at, created_at)))/60 as minutes_waiting
                FROM incident_management.incidents
                WHERE status NOT IN ('RESOLVED', 'MANUAL', 'PURGED')
                ORDER BY minutes_waiting DESC
                LIMIT 1
            """))
            longest_row = result.fetchone()
            aging_longest_wait = None
            if longest_row:
                aging_longest_wait = {
                    "id": str(longest_row.id),
                    "alert_id": longest_row.alert_id,
                    "title": longest_row.title,
                    "minutes_waiting": int(longest_row.minutes_waiting) if longest_row.minutes_waiting else 0,
                }

            # 4. Resolution ring: auto-resolved vs action-resolved vs manual
            result = await sess.execute(text("""
                SELECT
                    SUM(CASE WHEN status = 'RESOLVED' AND (resolved_externally IS NULL OR resolved_externally = FALSE) THEN 1 ELSE 0 END) as auto_resolved_count,
                    SUM(CASE WHEN status = 'RESOLVED' AND resolved_externally = TRUE THEN 1 ELSE 0 END) as action_resolved_count,
                    SUM(CASE WHEN status = 'MANUAL' THEN 1 ELSE 0 END) as manual_count
                FROM incident_management.incidents
                WHERE status IN ('RESOLVED', 'MANUAL')
            """))
            ring_row = result.fetchone()
            auto_resolved_count = ring_row.auto_resolved_count or 0
            action_resolved_count = ring_row.action_resolved_count or 0
            manual_count = ring_row.manual_count or 0
            total_resolved = auto_resolved_count + action_resolved_count + manual_count
            auto_resolved_pct = (auto_resolved_count / total_resolved * 100) if total_resolved > 0 else 0
            action_resolved_pct = (action_resolved_count / total_resolved * 100) if total_resolved > 0 else 0
            manual_pct = (manual_count / total_resolved * 100) if total_resolved > 0 else 0
            resolution_ring = {
                "auto_resolved_pct": round(auto_resolved_pct, 1),
                "action_resolved_pct": round(action_resolved_pct, 1),
                "manual_pct": round(manual_pct, 1),
            }

            # 5. Recurrence signal: top 10 by recurrence_count > 1
            result = await sess.execute(text("""
                SELECT id, alert_id, title, recurrence_count, status
                FROM incident_management.incidents
                WHERE recurrence_count > 1
                ORDER BY recurrence_count DESC
                LIMIT 10
            """))
            recurrence_rows = result.fetchall()
            recurrence_signal = [
                {
                    "id": str(r.id),
                    "alert_id": r.alert_id,
                    "title": r.title,
                    "recurrence_count": r.recurrence_count,
                    "status": r.status,
                }
                for r in recurrence_rows
            ]

            return {
                "pipeline_flow": pipeline_flow,
                "aging_buckets": aging_buckets,
                "sla_breach": sla_breach,
                "aging_longest_wait": aging_longest_wait,
                "resolution_ring": resolution_ring,
                "recurrence_signal": recurrence_signal,
            }
    except Exception as e:
        _log.error(f"incidents error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard/incidents/list")
async def get_incidents_list(
    status: str | None = None,
    priority: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> dict:
    """Return filtered list of incident tickets, ordered oldest-first (longest-waiting first)."""
    from database import SessionLocal
    from sqlalchemy import text
    from fastapi import HTTPException
    import logging
    _log = logging.getLogger(__name__)

    if SessionLocal is None:
        return {"tickets": []}

    try:
        async with SessionLocal() as sess:
            sql = """
                SELECT id, alert_id, priority, status, title, escalated_at, created_at, recurrence_count, related_ticket_id, resolved_externally, resolved_at
                FROM incident_management.incidents
                WHERE status != 'PURGED'
            """
            params = {}

            if status:
                sql += " AND status = :status"
                params["status"] = status

            if priority:
                sql += " AND priority = :priority"
                params["priority"] = priority

            sql += " ORDER BY COALESCE(escalated_at, created_at) ASC"
            if limit is not None:
                sql += " LIMIT :limit OFFSET :offset"
                params["limit"] = limit
                params["offset"] = offset
            result = await sess.execute(text(sql), params)
            rows = result.fetchall()

        # Get total count
        count_sql = "SELECT COUNT(*) FROM incident_management.incidents WHERE status != 'PURGED'"
        count_params = {}
        if status:
            count_sql += " AND status = :status"
            count_params["status"] = status
        if priority:
            count_sql += " AND priority = :priority"
            count_params["priority"] = priority
        async with SessionLocal() as sess2:
            count_result = await sess2.execute(text(count_sql), count_params)
            total = count_result.scalar() or 0

        sla_thresholds = {"P1": 15, "P2": 30, "P3": 60, "P4": 240}
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        tickets = []
        for row in rows:
            sla_breached = False
            if row.status not in ('RESOLVED', 'MANUAL'):
                try:
                    waiting_since = row.escalated_at or row.created_at
                    if waiting_since:
                        if waiting_since.tzinfo is None:
                            waiting_since = waiting_since.replace(tzinfo=timezone.utc)
                        minutes_waiting = (now - waiting_since).total_seconds() / 60
                        sla_breached = minutes_waiting > sla_thresholds.get(row.priority, 240)
                except Exception:
                    pass

            tickets.append({
                "id": str(row.id),
                "alert_id": row.alert_id,
                "priority": row.priority,
                "status": row.status,
                "title": row.title,
                "escalated_at": str(row.escalated_at) if row.escalated_at else None,
                "created_at": str(row.created_at) if row.created_at else None,
                "recurrence_count": row.recurrence_count,
                "related_ticket_id": str(row.related_ticket_id) if row.related_ticket_id else None,
                "resolved_externally": row.resolved_externally or False,
                "resolved_at": str(row.resolved_at) if row.resolved_at else None,
                "sla_breached": sla_breached,
            })
        return {"tickets": tickets, "total": total, "offset": offset, "limit": limit}
    except Exception as e:
        _log.error(f"incidents/list error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard/incidents/purge-preview")
async def get_purge_preview() -> dict:
    """Return count of tickets that would be purged based on current purge settings."""
    from database import SessionLocal
    from sqlalchemy import text
    from routes_settings import _config
    import logging
    _log = logging.getLogger(__name__)
    if SessionLocal is None:
        return {"eligible_count": 0, "purge_days": 7, "enabled": False}
    purge_days = int(_config.get("incident_purge_days", 7))
    enabled = bool(_config.get("incident_purge_enabled", False))
    try:
        async with SessionLocal() as sess:
            result = await sess.execute(text("""
                SELECT COUNT(*), priority
                FROM incident_management.incidents
                WHERE status = 'ESCALATED'
                AND purged_at IS NULL
                AND COALESCE(escalated_at, created_at) < NOW() - INTERVAL '1 day' * :days
                GROUP BY priority
            """).bindparams(days=purge_days))
            rows = result.fetchall()
        by_priority = {row.priority: row.count for row in rows}
        total = sum(by_priority.values())
        return {
            "eligible_count": total,
            "by_priority": by_priority,
            "purge_days": purge_days,
            "enabled": enabled,
        }
    except Exception as e:
        _log.error(f"purge-preview error: {e}")
        return {"eligible_count": 0, "purge_days": purge_days, "enabled": enabled, "error": str(e)}


@router.post("/dashboard/incidents/purge")
async def run_purge(dry_run: bool = True) -> dict:
    """Purge stale ESCALATED tickets. dry_run=true shows what would be purged without making changes."""
    from database import SessionLocal
    from sqlalchemy import text
    from routes_settings import _config
    from datetime import datetime, timezone
    import logging
    _log = logging.getLogger(__name__)
    if SessionLocal is None:
        return {"purged_count": 0, "dry_run": dry_run}
    purge_days = int(_config.get("incident_purge_days", 7))
    enabled = bool(_config.get("incident_purge_enabled", False))
    if not enabled and not dry_run:
        return {"purged_count": 0, "dry_run": dry_run, "error": "Purge is disabled. Enable in Settings first."}
    purge_reason = f"SLA_BREACH_NO_ACTION_{purge_days}D"
    try:
        async with SessionLocal() as sess:
            if dry_run:
                result = await sess.execute(text("""
                    SELECT COUNT(*) FROM incident_management.incidents
                    WHERE status = 'ESCALATED'
                    AND purged_at IS NULL
                    AND COALESCE(escalated_at, created_at) < NOW() - INTERVAL '1 day' * :days
                """).bindparams(days=purge_days))
                count = result.scalar() or 0
                return {"purged_count": count, "dry_run": True, "purge_days": purge_days, "purge_reason": purge_reason}
            else:
                now = datetime.now(timezone.utc)
                result = await sess.execute(text("""
                    UPDATE incident_management.incidents
                    SET status = 'PURGED',
                        purged_at = :now,
                        purge_reason = :reason,
                        updated_at = :now
                    WHERE status = 'ESCALATED'
                    AND purged_at IS NULL
                    AND COALESCE(escalated_at, created_at) < NOW() - INTERVAL '1 day' * :days
                    RETURNING id
                """).bindparams(now=now, reason=purge_reason, days=purge_days))
                purged_ids = result.fetchall()
                await sess.commit()
                count = len(purged_ids)
                _log.info(f"Purged {count} stale ESCALATED tickets older than {purge_days} days")
                return {"purged_count": count, "dry_run": False, "purge_days": purge_days, "purge_reason": purge_reason}
    except Exception as e:
        _log.error(f"purge error: {e}")
        return {"purged_count": 0, "dry_run": dry_run, "error": str(e)}


@router.get("/dashboard/incidents/resolution-audit")
async def get_resolution_audit(sample_size: int = 50) -> dict:
    """Audit sample of historically-resolved tickets to verify reconciliation accuracy.

    Queries a random sample of RESOLVED tickets with resolution_type IS NULL (pre-live-check
    historical resolutions), makes live per-ticket status checks against OpsGenie, and reports
    how many were actually closed vs. still open (falsely resolved). Used to assess accuracy
    of the old bounded-window snapshot comparison logic that was replaced by live checks.

    READ-ONLY: no updates made to any ticket. This is an audit/reporting endpoint only.
    """
    from database import SessionLocal
    from sqlalchemy import text
    from routes_settings import _config
    from tools.source import JSMSource, StandaloneOpsgenieSource
    import logging
    import asyncio as _asyncio
    _log = logging.getLogger(__name__)

    if SessionLocal is None:
        return {"sampled": 0, "confirmed_closed": 0, "still_open_incorrectly_resolved": 0, "unknown": 0, "accuracy_pct": None}

    try:
        async with SessionLocal() as sess:
            # Query random sample of unverified historical resolutions
            result = await sess.execute(
                text("""
                    SELECT id, alert_id FROM incident_management.incidents
                    WHERE status = 'RESOLVED' AND resolution_type IS NULL
                    ORDER BY RANDOM()
                    LIMIT :sample_size
                """),
                {"sample_size": sample_size},
            )
            sampled = result.fetchall()

        if not sampled:
            return {
                "sampled": 0,
                "confirmed_closed": 0,
                "still_open_incorrectly_resolved": 0,
                "unknown": 0,
                "accuracy_pct": None,
                "note": "No unverified historical resolutions to audit - either none exist or all have been reconciled under the new live-check logic.",
            }

        # Build source object (same logic as _run_opsgenie_sync)
        source_type = _config.get("source_type", "opsgenie")
        opsgenie_type = _config.get("opsgenie_type", "standalone")
        use_jsm = source_type == "standalone" and opsgenie_type == "jsm"

        if use_jsm:
            source = JSMSource(
                cloud_id=_config.get("cloud_id", ""),
                email=_config.get("email", ""),
                api_token=_config.get("api_token", ""),
            )
        else:
            source = StandaloneOpsgenieSource(
                api_key=_config.get("api_token", ""),
                base_url=_config.get("opsgenie_base_url") or "https://api.opsgenie.com",
            )

        # Bounded concurrency: 10 concurrent checks
        sem = _asyncio.Semaphore(10)

        async def _check_one(ticket):
            async with sem:
                try:
                    status = await source.get_alert_status(ticket.alert_id)
                except Exception:
                    status = None
            return ticket, status

        checked = await _asyncio.gather(*[_check_one(t) for t in sampled])

        confirmed_closed = 0
        still_open_incorrectly_resolved = 0
        unknown = 0

        for ticket, live_status in checked:
            if live_status is None:
                unknown += 1
            elif live_status.lower() == "closed":
                confirmed_closed += 1
            else:
                still_open_incorrectly_resolved += 1

        # Accuracy: confirmed_closed / (confirmed_closed + still_open), excluding unknowns
        accuracy_pct = None
        if (confirmed_closed + still_open_incorrectly_resolved) > 0:
            accuracy_pct = round(confirmed_closed / (confirmed_closed + still_open_incorrectly_resolved) * 100, 1)

        return {
            "sampled": len(checked),
            "confirmed_closed": confirmed_closed,
            "still_open_incorrectly_resolved": still_open_incorrectly_resolved,
            "unknown": unknown,
            "accuracy_pct": accuracy_pct,
        }
    except Exception as e:
        _log.error(f"resolution-audit error: {e}")
        return {
            "sampled": 0,
            "confirmed_closed": 0,
            "still_open_incorrectly_resolved": 0,
            "unknown": 0,
            "accuracy_pct": None,
            "error": str(e),
        }


@router.post("/dashboard/incidents/resolution-audit/correct")
async def correct_false_resolutions(batch_size: int = 100, dry_run: bool = True) -> dict:
    """Correct tickets falsely resolved under old snapshot-comparison logic.

    Processes a deterministic FIFO batch (oldest-first) of unverified RESOLVED
    tickets, verifies each against live OpsGenie, and either:
    - Reopens tickets confirmed still open (status != closed)
    - Marks tickets verified-correct if they really are closed
    - Leaves unknown/failed-lookup tickets for retry in a future batch

    dry_run=true: preview what would change without committing. dry_run=false: apply corrections.

    This is the write counterpart to GET /dashboard/incidents/resolution-audit (read-only audit).
    """
    from database import SessionLocal
    from sqlalchemy import text
    from routes_settings import _config
    from tools.source import JSMSource, StandaloneOpsgenieSource
    import logging
    import asyncio as _asyncio
    from datetime import datetime, timezone
    _log = logging.getLogger(__name__)

    if SessionLocal is None:
        return {"processed": 0, "reopened": 0, "confirmed_correctly_closed": 0, "unknown": 0, "dry_run": dry_run}

    try:
        async with SessionLocal() as sess:
            # Query deterministic batch: oldest-first, so repeated calls make progress
            result = await sess.execute(
                text("""
                    SELECT id, alert_id FROM incident_management.incidents
                    WHERE status = 'RESOLVED' AND resolution_type IS NULL AND reopened_at IS NULL
                    ORDER BY resolved_at ASC
                    LIMIT :batch_size
                """),
                {"batch_size": batch_size},
            )
            batch = result.fetchall()

        if not batch:
            return {
                "processed": 0,
                "reopened": 0,
                "confirmed_correctly_closed": 0,
                "unknown": 0,
                "dry_run": dry_run,
                "note": "No more unverified historical resolutions remain - correction complete.",
            }

        # Build source object (same logic as resolution-audit endpoint)
        source_type = _config.get("source_type", "opsgenie")
        opsgenie_type = _config.get("opsgenie_type", "standalone")
        use_jsm = source_type == "standalone" and opsgenie_type == "jsm"

        if use_jsm:
            source = JSMSource(
                cloud_id=_config.get("cloud_id", ""),
                email=_config.get("email", ""),
                api_token=_config.get("api_token", ""),
            )
        else:
            source = StandaloneOpsgenieSource(
                api_key=_config.get("api_token", ""),
                base_url=_config.get("opsgenie_base_url") or "https://api.opsgenie.com",
            )

        # Bounded concurrency: 10 concurrent checks
        sem = _asyncio.Semaphore(10)

        async def _check_one(ticket):
            async with sem:
                try:
                    status = await source.get_alert_status(ticket.alert_id)
                except Exception:
                    status = None
            return ticket, status

        checked = await _asyncio.gather(*[_check_one(t) for t in batch])

        reopened = 0
        confirmed_correctly_closed = 0
        unknown = 0
        now = datetime.now(timezone.utc)

        # Apply corrections only if not dry_run
        if not dry_run:
            async with SessionLocal() as sess:
                for ticket, live_status in checked:
                    if live_status is None:
                        # Unknown/lookup failed - do nothing, leave for retry in next batch
                        unknown += 1
                    elif live_status.lower() == "closed":
                        # Resolution was correct - mark as verified
                        await sess.execute(
                            text("""
                                UPDATE incident_management.incidents
                                SET resolution_type = 'self_healed', updated_at = now()
                                WHERE id = :id
                            """),
                            {"id": ticket.id},
                        )
                        confirmed_correctly_closed += 1
                    else:
                        # Still open - reopen the ticket
                        await sess.execute(
                            text("""
                                UPDATE incident_management.incidents
                                SET status = 'ESCALATED', reopened_at = :now,
                                    reopen_reason = 'false_positive_reconciliation',
                                    resolved_at = NULL, updated_at = :now
                                WHERE id = :id
                            """),
                            {"id": ticket.id, "now": now},
                        )
                        reopened += 1
                await sess.commit()
        else:
            # Dry run: just classify without updating
            for ticket, live_status in checked:
                if live_status is None:
                    unknown += 1
                elif live_status.lower() == "closed":
                    confirmed_correctly_closed += 1
                else:
                    reopened += 1

        return {
            "processed": len(checked),
            "reopened": reopened,
            "confirmed_correctly_closed": confirmed_correctly_closed,
            "unknown": unknown,
            "dry_run": dry_run,
        }
    except Exception as e:
        _log.error(f"resolution-audit/correct error: {e}")
        return {
            "processed": 0,
            "reopened": 0,
            "confirmed_correctly_closed": 0,
            "unknown": 0,
            "dry_run": dry_run,
            "error": str(e),
        }


@router.get("/dashboard/incidents/{ticket_id}")
async def get_incident_detail(ticket_id: str) -> dict:
    """Return alert payload for a single incident ticket by ID."""
    from database import SessionLocal
    from sqlalchemy import text
    from fastapi import HTTPException
    import logging
    import json
    _log = logging.getLogger(__name__)

    if SessionLocal is None:
        raise HTTPException(status_code=500, detail="Database not available")

    try:
        async with SessionLocal() as sess:
            result = await sess.execute(
                text("""
                    SELECT id, alert_payload
                    FROM incident_management.incidents
                    WHERE id = :ticket_id
                """),
                {"ticket_id": ticket_id}
            )
            row = result.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Incident not found")

            # Parse alert_payload if it's stored as JSON string
            alert_payload = row.alert_payload
            if isinstance(alert_payload, str):
                try:
                    alert_payload = json.loads(alert_payload)
                except json.JSONDecodeError:
                    alert_payload = None

            return {
                "id": str(row.id),
                "alert_payload": alert_payload
            }
    except HTTPException:
        raise
    except Exception as e:
        _log.error(f"incident detail error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard/lifetime")
async def get_lifetime_totals() -> dict:
    """Return agent's cumulative alert lifetime totals (counts since start of tracking)."""
    from database import SessionLocal
    from sqlalchemy import text
    from config import settings
    import logging
    _log = logging.getLogger(__name__)

    if SessionLocal is None:
        return {
            "total_alerts": 0,
            "genuine_count": 0,
            "noise_count": 0,
            "suspect_count": 0,
            "total_alerts_raw": 0,
            "genuine_count_raw": 0,
            "noise_count_raw": 0,
            "suspect_count_raw": 0,
            "counting_since": None,
            "last_cleanup_at": None,
        }

    try:
        async with SessionLocal() as sess:
            result = await sess.execute(
                text("""
                    SELECT total_alerts, genuine_count, noise_count, suspect_count,
                           total_alerts_raw, genuine_count_raw, noise_count_raw, suspect_count_raw,
                           counting_since, last_cleanup_at
                    FROM alert_lifetime_totals
                    WHERE agent_slug = :slug
                """),
                {"slug": settings.agent_slug}
            )
            row = result.fetchone()

        if row:
            return {
                "total_alerts": row.total_alerts or 0,
                "genuine_count": row.genuine_count or 0,
                "noise_count": row.noise_count or 0,
                "suspect_count": row.suspect_count or 0,
                "total_alerts_raw": row.total_alerts_raw or 0,
                "genuine_count_raw": row.genuine_count_raw or 0,
                "noise_count_raw": row.noise_count_raw or 0,
                "suspect_count_raw": row.suspect_count_raw or 0,
                "counting_since": row.counting_since.isoformat() if row.counting_since else None,
                "last_cleanup_at": row.last_cleanup_at.isoformat() if row.last_cleanup_at else None,
            }
        else:
            # No row found; return zeros with null dates (fail-open)
            return {
                "total_alerts": 0,
                "genuine_count": 0,
                "noise_count": 0,
                "suspect_count": 0,
                "total_alerts_raw": 0,
                "genuine_count_raw": 0,
                "noise_count_raw": 0,
                "suspect_count_raw": 0,
                "counting_since": None,
                "last_cleanup_at": None,
            }
    except Exception as e:
        _log.error(f"lifetime totals error: {e}")
        return {
            "total_alerts": 0,
            "genuine_count": 0,
            "noise_count": 0,
            "suspect_count": 0,
            "total_alerts_raw": 0,
            "genuine_count_raw": 0,
            "noise_count_raw": 0,
            "suspect_count_raw": 0,
            "counting_since": None,
            "last_cleanup_at": None,
            "error": str(e),
        }


@router.get("/dashboard/escalations")
async def get_escalation_log(limit: int = 50) -> dict:
    """Return recent escalation log entries."""
    from database import SessionLocal
    from sqlalchemy import text
    import logging
    logger = logging.getLogger(__name__)
    if SessionLocal is None:
        return {"escalations": []}
    try:
        async with SessionLocal() as session:
            result = await session.execute(text("""
                SELECT id, channel, severity, alert_count, message_summary,
                       recipients, status, error_message, sent_at
                FROM alert_escalation_log
                WHERE agent_slug = 'alert-analyser'
                ORDER BY sent_at DESC
                LIMIT :limit
            """), {"limit": limit})
            rows = result.fetchall()
        return {"escalations": [
            {
                "id": r.id,
                "channel": r.channel,
                "severity": r.severity,
                "alert_count": r.alert_count,
                "message_summary": r.message_summary,
                "recipients": r.recipients,
                "status": r.status,
                "error_message": r.error_message,
                "sent_at": r.sent_at.isoformat() if r.sent_at else None,
            }
            for r in rows
        ]}
    except Exception as e:
        logger.warning("get_escalation_log failed: %s", e)
        return {"escalations": []}
