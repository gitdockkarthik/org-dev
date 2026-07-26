"""Audit log API routes."""
import logging
from fastapi import APIRouter, Depends
from sqlalchemy import text

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/api/audit/logs")
async def get_audit_logs(
    event_type: str | None = None,
    user_email: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list:
    try:
        from database import AsyncSessionLocal
        async with AsyncSessionLocal() as sess:
            where = "WHERE 1=1"
            params: dict = {"limit": limit, "offset": offset}
            if event_type:
                where += " AND event_type = :event_type"
                params["event_type"] = event_type
            if user_email:
                where += " AND user_email = :user_email"
                params["user_email"] = user_email
            rows = await sess.execute(text(f"""
                SELECT id, timestamp, event_type, agent_slug, user_email, user_role,
                       resource_type, resource_id, action, outcome, details, ip_address
                FROM audit_logs {where}
                ORDER BY timestamp DESC
                LIMIT :limit OFFSET :offset
            """), params)
            return [
                {
                    "id": r.id,
                    "timestamp": r.timestamp.isoformat(),
                    "event_type": r.event_type,
                    "agent_slug": r.agent_slug,
                    "user_email": r.user_email,
                    "user_role": r.user_role,
                    "resource_type": r.resource_type,
                    "resource_id": r.resource_id,
                    "action": r.action,
                    "outcome": r.outcome,
                    "details": r.details,
                    "ip_address": r.ip_address,
                }
                for r in rows.fetchall()
            ]
    except Exception as e:
        logger.error("get_audit_logs failed: %s", e)
        return []


@router.get("/api/audit/stats")
async def get_audit_stats() -> dict:
    """Summary stats for the audit dashboard."""
    try:
        from database import AsyncSessionLocal
        async with AsyncSessionLocal() as sess:
            rows = await sess.execute(text("""
                SELECT event_type, COUNT(*) as count
                FROM audit_logs
                WHERE timestamp >= NOW() - INTERVAL '30 days'
                GROUP BY event_type ORDER BY count DESC
            """))
            by_type = {r.event_type: r.count for r in rows.fetchall()}

            users = await sess.execute(text("""
                SELECT COUNT(DISTINCT user_email) as count
                FROM audit_logs WHERE timestamp >= NOW() - INTERVAL '30 days'
                AND user_email IS NOT NULL
            """))
            total_users = users.scalar() or 0

            total = await sess.execute(text(
                "SELECT COUNT(*) FROM audit_logs WHERE timestamp >= NOW() - INTERVAL '30 days'"
            ))
            total_events = total.scalar() or 0

        return {
            "total_events": total_events,
            "total_users": total_users,
            "by_type": by_type,
        }
    except Exception as e:
        logger.error("get_audit_stats failed: %s", e)
        return {"total_events": 0, "total_users": 0, "by_type": {}}
