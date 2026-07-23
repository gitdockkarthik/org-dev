"""SLI/SLO endpoints for Kafka Analyser."""
import logging
from fastapi import APIRouter
from typing import Any

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/slo/targets")
async def get_slo_targets(cluster_id: str) -> dict:
    """Get SLO targets for a cluster."""
    try:
        from database import SessionLocal
        from sqlalchemy import text as _t
        if not SessionLocal:
            return {"error": "DB unavailable"}
        async with SessionLocal() as sess:
            row = await sess.execute(_t(
                "SELECT * FROM kafka_slo_targets WHERE cluster_id=:cid LIMIT 1"
            ), {"cid": int(cluster_id)})
            r = row.fetchone()
        if r:
            return {
                "cluster_id": cluster_id,
                "connector_availability_target": r.connector_availability_target,
                "consumer_lag_target": r.consumer_lag_target,
                "broker_availability_target": r.broker_availability_target,
                "urp_target": r.urp_target,
                "min_throughput_bytes": r.min_throughput_bytes,
            }
        return {
            "cluster_id": cluster_id,
            "connector_availability_target": 99.0,
            "consumer_lag_target": 10000,
            "broker_availability_target": 100.0,
            "urp_target": 0,
            "min_throughput_bytes": 0,
        }
    except Exception as e:
        return {"error": str(e)}


@router.post("/slo/targets")
async def save_slo_targets(cluster_id: str, payload: dict) -> dict:
    """Save SLO targets for a cluster."""
    try:
        from database import SessionLocal
        from sqlalchemy import text as _t
        async with SessionLocal() as sess:
            await sess.execute(_t("""
                INSERT INTO kafka_slo_targets
                (cluster_id, connector_availability_target, consumer_lag_target,
                 broker_availability_target, urp_target, min_throughput_bytes, updated_at)
                VALUES (:cid, :ca, :cl, :ba, :urp, :mt, now())
                ON CONFLICT (cluster_id) DO UPDATE SET
                    connector_availability_target = EXCLUDED.connector_availability_target,
                    consumer_lag_target = EXCLUDED.consumer_lag_target,
                    broker_availability_target = EXCLUDED.broker_availability_target,
                    urp_target = EXCLUDED.urp_target,
                    min_throughput_bytes = EXCLUDED.min_throughput_bytes,
                    updated_at = now()
            """), {
                "cid": int(cluster_id),
                "ca": payload.get("connector_availability_target", 99.0),
                "cl": payload.get("consumer_lag_target", 10000),
                "ba": payload.get("broker_availability_target", 100.0),
                "urp": payload.get("urp_target", 0),
                "mt": payload.get("min_throughput_bytes", 0),
            })
            await sess.commit()
        return {"ok": True}
    except Exception as e:
        return {"error": str(e)}


@router.get("/slo/dashboard")
async def get_slo_dashboard(cluster_id: str, hours: int = 24) -> dict:
    """Get SLO dashboard data — current state + compliance trend."""
    try:
        from database import SessionLocal
        from sqlalchemy import text as _t
        from datetime import datetime, timezone, timedelta
        if not SessionLocal:
            return {"error": "DB unavailable"}
        now = datetime.now(timezone.utc)
        since = now - timedelta(hours=hours)
        async with SessionLocal() as sess:
            # SLO targets
            tgt = await sess.execute(_t(
                "SELECT * FROM kafka_slo_targets WHERE cluster_id=:cid LIMIT 1"
            ), {"cid": int(cluster_id)})
            target = tgt.fetchone()
            lag_target = int(target.consumer_lag_target) if target else 10000
            conn_target = float(target.connector_availability_target) if target else 99.0
            urp_target = int(target.urp_target) if target else 0

            # Current connector state — SLI excludes PAUSED/UNASSIGNED
            conn_now = await sess.execute(_t("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN state='RUNNING' THEN 1 ELSE 0 END) as running,
                       SUM(CASE WHEN state='FAILED' THEN 1 ELSE 0 END) as failed,
                       SUM(CASE WHEN state='PAUSED' THEN 1 ELSE 0 END) as paused,
                       SUM(CASE WHEN state='UNASSIGNED' THEN 1 ELSE 0 END) as unassigned
                FROM kafka_connector_snapshots
                WHERE cluster_id=:cid
                AND collected_at = (SELECT MAX(collected_at) FROM kafka_connector_snapshots WHERE cluster_id=:cid)
            """), {"cid": int(cluster_id)})
            cn = conn_now.fetchone()
            conn_total_all = cn.total or 0
            conn_running = cn.running or 0
            conn_failed = cn.failed or 0
            conn_paused = cn.paused or 0
            conn_unassigned = cn.unassigned or 0
            conn_active = conn_running + conn_failed  # excludes paused/unassigned
            conn_avail_pct = round(conn_running / conn_active * 100, 2) if conn_active > 0 else 0

            # Current lag
            lag_now = await sess.execute(_t("""
                SELECT total_lag, group_count FROM kafka_lag_snapshots
                WHERE cluster_id=:cid ORDER BY collected_at DESC LIMIT 1
            """), {"cid": str(cluster_id)})
            ln = lag_now.fetchone()
            current_lag = int(ln.total_lag) if ln else 0

            # Current broker + URP
            broker_now = await sess.execute(_t("""
                SELECT COUNT(*) as total, SUM(urp_count) as total_urp
                FROM kafka_broker_metrics WHERE cluster_id=:cid
            """), {"cid": int(cluster_id)})
            bn = broker_now.fetchone()
            broker_count = int(bn.total) if bn else 0
            current_urp = int(bn.total_urp or 0) if bn else 0

            # Compliance trend (hourly)
            trend = await sess.execute(_t("""
                SELECT hour_bucket, connector_availability_pct, consumer_lag_compliance_pct,
                       urp_compliance_pct, overall_compliance_pct,
                       connector_total, connector_running, connector_failed
                FROM kafka_slo_compliance
                WHERE cluster_id=:cid AND hour_bucket >= :since
                ORDER BY hour_bucket ASC
            """), {"cid": int(cluster_id), "since": since})
            trend_rows = trend.fetchall()

            # Per-connector current state
            connectors = await sess.execute(_t("""
                SELECT connector_name, connector_type, state, total_tasks, running_tasks, failed_tasks, collected_at
                FROM kafka_connector_snapshots
                WHERE cluster_id=:cid
                AND collected_at = (SELECT MAX(collected_at) FROM kafka_connector_snapshots WHERE cluster_id=:cid)
                ORDER BY state, connector_name
            """), {"cid": int(cluster_id)})
            connector_rows = connectors.fetchall()

        # Compliance status helper
        def status(pct, target):
            if pct is None: return "unknown"
            if pct >= target: return "green"
            if pct >= target * 0.8: return "amber"
            return "red"

        return {
            "cluster_id": cluster_id,
            "hours": hours,
            "targets": {
                "connector_availability": conn_target,
                "consumer_lag": lag_target,
                "broker_availability": 100.0,
                "urp": urp_target,
            },
            "current": {
                "connector_availability_pct": conn_avail_pct,
                "connector_total": conn_total_all,
                "connector_running": conn_running,
                "connector_failed": conn_failed,
                "connector_paused": conn_paused,
                "connector_unassigned": conn_unassigned,
                "consumer_lag": current_lag,
                "broker_count": broker_count,
                "urp": current_urp,
                "connector_status": status(conn_avail_pct, conn_target),
                "lag_status": "green" if current_lag <= lag_target else ("amber" if current_lag <= lag_target * 2 else "red"),
                "urp_status": "green" if current_urp <= urp_target else "red",
                "broker_status": "green" if broker_count > 0 else "red",
            },
            "trend": [
                {
                    "time": r.hour_bucket.isoformat(),
                    "connector_pct": r.connector_availability_pct,
                    "lag_pct": r.consumer_lag_compliance_pct,
                    "urp_pct": r.urp_compliance_pct,
                    "overall_pct": r.overall_compliance_pct,
                    "connector_running": r.connector_running,
                    "connector_failed": r.connector_failed,
                }
                for r in trend_rows
            ],
            "connectors": [
                {
                    "name": r.connector_name,
                    "type": r.connector_type,
                    "state": r.state,
                    "total_tasks": r.total_tasks,
                    "running_tasks": r.running_tasks,
                    "failed_tasks": r.failed_tasks,
                }
                for r in connector_rows
            ],
        }
    except Exception as e:
        logger.error("get_slo_dashboard failed: %s", e)
        return {"error": str(e)}


@router.get("/slo/monthly")
async def get_slo_monthly(cluster_id: str, months: int = 3) -> dict:
    """Get monthly SLO compliance summary for trend comparison."""
    try:
        from database import SessionLocal
        from sqlalchemy import text as _t
        async with SessionLocal() as sess:
            rows = await sess.execute(_t("""
                SELECT DATE_TRUNC('month', hour_bucket) as month,
                       AVG(connector_availability_pct) as connector_avg,
                       AVG(consumer_lag_compliance_pct) as lag_avg,
                       AVG(urp_compliance_pct) as urp_avg,
                       AVG(overall_compliance_pct) as overall_avg,
                       COUNT(*) as data_points
                FROM kafka_slo_compliance
                WHERE cluster_id=:cid
                AND hour_bucket >= DATE_TRUNC('month', NOW()) - INTERVAL ':months months'
                GROUP BY DATE_TRUNC('month', hour_bucket)
                ORDER BY month ASC
            """), {"cid": int(cluster_id), "months": months})
            monthly = rows.fetchall()
        return {
            "months": [
                {
                    "month": r.month.strftime("%b %Y"),
                    "connector_pct": round(r.connector_avg, 1) if r.connector_avg else None,
                    "lag_pct": round(r.lag_avg, 1) if r.lag_avg else None,
                    "urp_pct": round(r.urp_avg, 1) if r.urp_avg else None,
                    "overall_pct": round(r.overall_avg, 1) if r.overall_avg else None,
                    "data_points": r.data_points,
                }
                for r in monthly
            ]
        }
    except Exception as e:
        return {"error": str(e)}
