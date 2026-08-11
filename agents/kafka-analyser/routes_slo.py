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
        from database import DashboardSessionLocal as SessionLocal
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
        from database import DashboardSessionLocal as SessionLocal
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
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text as _t
        from datetime import datetime, timezone, timedelta
        if not SessionLocal:
            return {"error": "DB unavailable"}
        now = datetime.now(timezone.utc)
        since = now - timedelta(hours=hours)

        # Fetch live connector data (same source as Kafka Connect tab)
        from routes_dashboard import get_kafka_connect
        connect_data = await get_kafka_connect(str(cluster_id))
        live_connectors = connect_data.get("connectors", [])

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

            # Current connector state — SLI excludes PAUSED/UNASSIGNED (live data)
            conn_total_all = len(live_connectors)
            conn_running = sum(1 for c in live_connectors if c.get("state") == "RUNNING")
            conn_failed = sum(1 for c in live_connectors if c.get("state") == "FAILED")
            conn_paused = sum(1 for c in live_connectors if c.get("state") == "PAUSED")
            conn_unassigned = sum(1 for c in live_connectors if c.get("state") == "UNASSIGNED")
            conn_active = conn_running + conn_failed  # excludes paused/unassigned
            conn_avail_pct = round(conn_running / conn_active * 100, 2) if conn_active > 0 else 0

            # Current lag
            lag_now = await sess.execute(_t("""
                SELECT total_lag, group_count FROM kafka_lag_snapshots
                WHERE cluster_id=:cid ORDER BY collected_at DESC LIMIT 1
            """), {"cid": str(cluster_id)})
            ln = lag_now.fetchone()
            current_lag = int(ln.total_lag) if ln else 0

            # Current broker + URP -- broker_count only counts brokers with a
            # genuinely successful data_gb_true connection attempt in the freshness
            # window, not just any row update (the main row is also updated via
            # Prometheus/JMX, a separate data source, so it can look "fresh" even
            # when that broker's own connection attempt failed). Live-validated
            # against a real broker outage on 2026-08-06.
            broker_now = await sess.execute(_t("""
                SELECT COUNT(*) FILTER (WHERE data_gb_true IS NOT NULL) as total
                FROM kafka_broker_metrics
                WHERE cluster_id=:cid AND time >= NOW() - INTERVAL '10 minutes'
            """), {"cid": int(cluster_id)})
            bn = broker_now.fetchone()
            broker_count = int(bn.total) if bn else 0
            # URP now sourced from kafka_topic_metrics.urp_count -- real, per-topic
            # data from describe_topics (collect_topic_structure), not the unreliable
            # Prometheus/JMX-derived broker metric. Confirmed as a real data-accuracy
            # fix during a live incident on 2026-08-06 (dashboard showed 0 while the
            # Kafka team's own CLI check showed 19,912 genuine URPs).
            urp_now = await sess.execute(_t("""
                SELECT COALESCE(SUM(urp_count), 0) as total_urp
                FROM kafka_topic_metrics
                WHERE cluster_id=:cid
            """), {"cid": int(cluster_id)})
            urp_row = urp_now.fetchone()
            current_urp = int(urp_row.total_urp or 0) if urp_row else 0
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

            # Per-connector current state (live data)
            # Task health: sum of running tasks / sum of total tasks across active connectors
            active_connectors = [c for c in live_connectors if c.get("state") in ("RUNNING", "FAILED")]
            total_tasks_sum = sum(c.get("total_tasks") or 0 for c in active_connectors)
            running_tasks_sum = sum(c.get("running_tasks") or 0 for c in active_connectors)
            task_health_pct = round(running_tasks_sum / total_tasks_sum * 100, 1) if total_tasks_sum > 0 else None

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
                "connector_status": "na" if conn_total_all == 0 else status(conn_avail_pct, conn_target),
                "lag_status": "green" if current_lag <= lag_target else ("amber" if current_lag <= lag_target * 2 else "red"),
                "urp_status": "green" if current_urp <= urp_target else "red",
                "broker_count": broker_count,
                "expected_brokers": expected_brokers,
                "broker_avail_pct": broker_avail_pct,
                "broker_status": "green" if broker_avail_pct >= 100 else ("amber" if broker_avail_pct >= 66 else "red"),
                "cpu_status": "green" if avg_cpu <= cpu_target else ("amber" if avg_cpu <= cpu_target * 1.1 else "red") if avg_cpu > 0 else "unknown",
                "heap_status": "green" if avg_heap <= heap_target else ("amber" if avg_heap <= heap_target * 1.1 else "red") if avg_heap > 0 else "unknown",
                "task_status": "na" if conn_total_all == 0 else (("green" if task_health_pct >= task_target else ("amber" if task_health_pct >= task_target * 0.9 else "red")) if task_health_pct is not None else "unknown"),
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
                    "name": c.get("name"),
                    "type": c.get("type"),
                    "state": c.get("state"),
                    "total_tasks": c.get("total_tasks"),
                    "running_tasks": c.get("running_tasks"),
                    "failed_tasks": c.get("failed_tasks"),
                }
                for c in live_connectors
            ],
        }
    except Exception as e:
        logger.error("get_slo_dashboard failed: %s", e)
        return {"error": str(e)}


@router.get("/slo/monthly")
async def get_slo_monthly(cluster_id: str, months: int = 3) -> dict:
    """Get monthly SLO compliance summary for trend comparison."""
    try:
        from database import DashboardSessionLocal as SessionLocal
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
        from database import DashboardSessionLocal as SessionLocal
        from sqlalchemy import text as _t
        from datetime import datetime, timezone, timedelta
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        bucket = '5 minutes' if hours <= 24 else '1 hour' if hours <= 168 else '6 hours'
        async with SessionLocal() as sess:
            # Fixed: was SUMming every snapshot within each bucket, so a bucket
            # spanning multiple snapshot cycles (job runs every ~2 min, buckets
            # are 5+ min) genuinely double/triple-counted every connector.
            # Correct approach: pick the single latest snapshot per bucket as
            # a clean point-in-time sample, not an accumulation.
            rows = await sess.execute(_t(f"""
                WITH bucketed AS (
                    SELECT *, date_bin('{bucket}'::interval, collected_at, TIMESTAMP '2001-01-01') as bucket_time
                    FROM kafka_connector_snapshots
                    WHERE cluster_id=:cid AND collected_at >= :since
                    AND state IN ('RUNNING','FAILED','PAUSED')
                ),
                latest_per_bucket AS (
                    SELECT bucket_time, MAX(collected_at) as latest_collected_at
                    FROM bucketed GROUP BY bucket_time
                )
                SELECT b.bucket_time,
                       SUM(CASE WHEN b.state='RUNNING' THEN 1 ELSE 0 END) as running,
                       SUM(CASE WHEN b.state='FAILED' THEN 1 ELSE 0 END) as failed,
                       SUM(CASE WHEN b.state='PAUSED' THEN 1 ELSE 0 END) as paused,
                       COUNT(*) as total
                FROM bucketed b
                JOIN latest_per_bucket l ON b.bucket_time = l.bucket_time AND b.collected_at = l.latest_collected_at
                GROUP BY b.bucket_time
                ORDER BY b.bucket_time ASC
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
