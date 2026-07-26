"""Audit logging helper — writes to audit_logs table."""
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


async def log_audit_event(
    event_type: str,
    *,
    user_email: str | None = None,
    user_role: str | None = None,
    agent_slug: str | None = None,
    resource_type: str | None = None,
    resource_id: str | None = None,
    action: str | None = None,
    outcome: str = "success",
    details: dict[str, Any] | None = None,
    ip_address: str | None = None,
) -> None:
    """Write an audit event to the audit_logs table. Never raises."""
    try:
        from database import AsyncSessionLocal
        from sqlalchemy import text
        async with AsyncSessionLocal() as sess:
            await sess.execute(text("""
                INSERT INTO audit_logs
                (timestamp, event_type, agent_slug, user_email, user_role,
                 resource_type, resource_id, action, outcome, details, ip_address)
                VALUES
                (now(), :et, :ag, :ue, :ur, :rt, :ri, :ac, :oc, :dt::jsonb, :ip)
            """), {
                "et": event_type,
                "ag": agent_slug,
                "ue": user_email,
                "ur": user_role,
                "rt": resource_type,
                "ri": resource_id,
                "ac": action,
                "oc": outcome,
                "dt": __import__("json").dumps(details) if details else None,
                "ip": ip_address,
            })
            await sess.commit()
    except Exception as e:
        logger.warning("audit log failed: %s", e)
