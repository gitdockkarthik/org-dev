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
                "max_broker_cpu_pct": r.max_broker_cpu_pct or 85.0,
                "max_broker_heap_pct": r.max_broker_heap_pct or 80.0,
                "min_task_health_pct": r.min_task_health_pct or 95.0,
                "max_failed_tasks": r.max_failed_tasks or 0,
            }
        return {
            "cluster_id": cluster_id,
            "connector_availability_target": 99.0,
            "consumer_lag_target": 10000,
            "broker_availability_target": 100.0,
            "urp_target": 0,
            "min_throughput_bytes": 0,
            "max_broker_cpu_pct": 85.0,
            "max_broker_heap_pct": 80.0,
            "min_task_health_pct": 95.0,
            "max_failed_tasks": 0,
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
                 broker_availability_target, urp_target, min_throughput_bytes,
                 max_broker_cpu_pct, max_broker_heap_pct, min_task_health_pct, max_failed_tasks, updated_at)
                VALUES (:cid, :ca, :cl, :ba, :urp, :mt, :cpu, :heap, :task, :ft, now())
                ON CONFLICT (cluster_id) DO UPDATE SET
                    connector_availability_target = EXCLUDED.connector_availability_target,
                    consumer_lag_target = EXCLUDED.consumer_lag_target,
                    broker_availability_target = EXCLUDED.broker_availability_target,
                    urp_target = EXCLUDED.urp_target,
                    min_throughput_bytes = EXCLUDED.min_throughput_bytes,
                    max_broker_cpu_pct = EXCLUDED.max_broker_cpu_pct,
                    max_broker_heap_pct = EXCLUDED.max_broker_heap_pct,
                    min_task_health_pct = EXCLUDED.min_task_health_pct,
                    max_failed_tasks = EXCLUDED.max_failed_tasks,
                    updated_at = now()
            """), {
                "cid": int(cluster_id),
                "ca": payload.get("connector_availability_target", 99.0),
                "cl": payload.get("consumer_lag_target", 10000),
                "ba": payload.get("broker_availability_target", 100.0),
                "urp": payload.get("urp_target", 0),
                "mt": payload.get("min_throughput_bytes", 0),
                "cpu": payload.get("max_broker_cpu_pct", 85.0),
                "heap": payload.get("max_broker_heap_pct", 80.0),
                "task": payload.get("min_task_health_pct", 95.0),
                "ft": payload.get("max_failed_tasks", 0),
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
            cpu_target = float(target.max_broker_cpu_pct) if target and target.max_broker_cpu_pct else 85.0
            heap_target = float(target.max_broker_heap_pct) if target and target.max_broker_heap_pct else 80.0
            task_target = float(target.min_task_health_pct) if target and target.min_task_health_pct else 95.0
            max_failed_tasks = int(target.max_failed_tasks) if target and target.max_failed_tasks is not None else 0

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
            # Dynamic expected broker count
            max_br = await sess.execute(_t(
                "SELECT COUNT(DISTINCT broker_id) FROM kafka_broker_metrics WHERE cluster_id=:cid"
            ), {"cid": int(cluster_id)})
            expected_brokers = max_br.scalar() or 1
            broker_avail_pct = round(broker_count / expected_brokers * 100, 1)
            # Broker CPU and heap averages
            broker_metrics = await sess.execute(_t("""
                SELECT AVG(cpu_pct) as avg_cpu, AVG(heap_pct) as avg_heap
                FROM kafka_broker_metrics WHERE cluster_id=:cid
            """), {"cid": int(cluster_id)})
            bm = broker_metrics.fetchone()
            avg_cpu = round(bm.avg_cpu or 0, 1) if bm else 0.0
            avg_heap = round(bm.avg_heap or 0, 1) if bm else 0.0

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
            # Task health: connectors with all tasks healthy / total active connectors
            total_active = conn_running + conn_failed
            conn_with_failures = sum(1 for r in connector_rows if r.failed_tasks > 0 and r.state == 'RUNNING')
            task_health_pct = round((total_active - conn_with_failures) / total_active * 100, 1) if total_active > 0 else 100.0

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
                "max_broker_cpu_pct": cpu_target,
                "max_broker_heap_pct": heap_target,
                "min_task_health_pct": task_target,
                "max_failed_tasks": max_failed_tasks,
                "expected_brokers": expected_brokers,
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
                "avg_cpu": avg_cpu,
                "avg_heap": avg_heap,
                "task_health_pct": task_health_pct,
                "connector_status": status(conn_avail_pct, conn_target),
                "lag_status": "green" if current_lag <= lag_target else ("amber" if current_lag <= lag_target * 2 else "red"),
                "urp_status": "green" if current_urp <= urp_target else "red",
                "broker_count": broker_count,
                "expected_brokers": expected_brokers,
                "broker_avail_pct": broker_avail_pct,
                "broker_status": "green" if broker_avail_pct >= 100 else ("amber" if broker_avail_pct >= 66 else "red"),
                "cpu_status": "green" if avg_cpu <= cpu_target else ("amber" if avg_cpu <= cpu_target * 1.1 else "red") if avg_cpu > 0 else "unknown",
                "heap_status": "green" if avg_heap <= heap_target else ("amber" if avg_heap <= heap_target * 1.1 else "red") if avg_heap > 0 else "unknown",
                "task_status": ("green" if task_health_pct >= task_target else ("amber" if task_health_pct >= task_target * 0.9 else "red")) if task_health_pct is not None else "unknown",
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
                AND hour_bucket >= DATE_TRUNC('month', NOW()) - (:months * INTERVAL '1 month')
                GROUP BY DATE_TRUNC('month', hour_bucket)
                ORDER BY month ASC
            """), {"cid": int(cluster_id), "months": months})
            monthly = rows.fetchall()
        return {
            "months": [
                {
                    "month": r.month.strftime("%b %Y"),
                    "connector_pct": round(r.connector_avg, 1) if r.connector_avg else None,
                    "lag_pct": round(r.lag_avg, 1) if r.lag_avg is not None else None,
                    "urp_pct": round(r.urp_avg, 1) if r.urp_avg else None,
                    "overall_pct": round(r.overall_avg, 1) if r.overall_avg else None,
                    "data_points": r.data_points,
                }
                for r in monthly
            ]
        }
    except Exception as e:
        return {"error": str(e)}

@router.get("/slo/connector-trend")
async def get_connector_trend(cluster_id: str, hours: int = 24) -> dict:
    """Connector availability trend from snapshots (every 2 min, not hourly)."""
    try:
        from database import SessionLocal
        from sqlalchemy import text as _t
        from datetime import datetime, timezone, timedelta
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        bucket = '5 minutes' if hours <= 24 else '1 hour' if hours <= 168 else '6 hours'
        async with SessionLocal() as sess:
            rows = await sess.execute(_t(f"""
                SELECT date_bin('{bucket}'::interval, collected_at, TIMESTAMP '2001-01-01') as bucket_time,
                       SUM(CASE WHEN state='RUNNING' THEN 1 ELSE 0 END) as running,
                       SUM(CASE WHEN state='FAILED' THEN 1 ELSE 0 END) as failed,
                       SUM(CASE WHEN state='PAUSED' THEN 1 ELSE 0 END) as paused,
                       COUNT(*) as total
                FROM kafka_connector_snapshots
                WHERE cluster_id=:cid AND collected_at >= :since
                AND state IN ('RUNNING','FAILED','PAUSED')
                GROUP BY bucket_time
                ORDER BY bucket_time ASC
            """), {"cid": int(cluster_id), "since": since})
            points = []
            for r in rows.fetchall():
                active = (r.running or 0) + (r.failed or 0)
                avail = round(r.running / active * 100, 1) if active > 0 else None
                points.append({
                    "time": r.bucket_time.isoformat(),
                    "running": r.running or 0,
                    "failed": r.failed or 0,
                    "paused": r.paused or 0,
                    "availability_pct": avail,
                })
        return {"points": points}
    except Exception as e:
        return {"error": str(e), "points": []}
