"""Audit log API routes."""
import logging
from fastapi import APIRouter, Depends, Response
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
        from core.database import AsyncSessionLocal
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
        from core.database import AsyncSessionLocal
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


@router.get("/api/audit/llm-usage")
async def get_llm_usage(limit: int = 50, page: int = 1, hours: int = 168) -> dict:
    """Proxy Langfuse API for LLM usage data."""
    import httpx, os
    from datetime import datetime, timezone, timedelta
    langfuse_url = os.environ.get("LANGFUSE_INTERNAL_URL", "http://langfuse:3000")
    public_key = os.environ.get("LANGFUSE_PUBLIC_KEY", "")
    secret_key = os.environ.get("LANGFUSE_SECRET_KEY", "")
    if not public_key or not secret_key:
        return {"error": "Langfuse not configured", "data": [], "meta": {}}
    try:
        params = {"limit": limit, "page": page, "type": "GENERATION"}
        if hours > 0:
            since = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
            params["fromStartTime"] = since
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Fetch observations (LLM calls with token usage)
            resp = await client.get(
                f"{langfuse_url}/api/public/observations",
                params=params,
                auth=(public_key, secret_key),
            )
            resp.raise_for_status()
            data = resp.json()
        # Filter out zero-token observations server-side
        all_data = data.get("data", [])
        generations = [g for g in all_data if (g.get("usageDetails") or {}).get("total", 0) > 0 or g.get("model")]
        # Enrich with user data from audit_logs by matching session_id
        try:
            from core.database import AsyncSessionLocal
            from sqlalchemy import text as _t
            async with AsyncSessionLocal() as sess:
                rows = await sess.execute(_t("""
                    SELECT resource_id, user_email
                    FROM audit_logs
                    WHERE event_type='llm.invoke'
                    ORDER BY timestamp DESC
                    LIMIT 200
                """))
                audit_map = {r.resource_id: r.user_email for r in rows.fetchall()}
            for g in generations:
                session_id = (g.get("metadata") or {}).get("session_id")
                if session_id and session_id in audit_map:
                    if not g.get("metadata"):
                        g["metadata"] = {}
                    g["metadata"]["user_email"] = audit_map[session_id]
        except Exception:
            pass
        return {
            "generations": generations,
            "meta": data.get("meta", {}),
        }
    except Exception as e:
        logger.error("get_llm_usage failed: %s", e)
        return {"error": str(e), "generations": [], "meta": {}}


@router.get("/api/audit/llm-usage/stats")
async def get_llm_usage_stats(hours: int = 168) -> dict:
    """Fast, accurate aggregate stats queried directly from ClickHouse
    (Langfuse's backing store), bypassing pagination entirely — the
    paginated /api/audit/llm-usage endpoint is for browsing individual
    rows, not for computing true totals."""
    import httpx
    from datetime import datetime, timezone, timedelta

    where_clause = "WHERE type = 'GENERATION'"
    if hours > 0:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        where_clause += f" AND start_time >= '{since}'"

    query = f"""
    SELECT
      count() as total_calls,
      sum(usage_details['input']) as total_input,
      sum(usage_details['output']) as total_output,
      sum(usage_details['total']) as total_tokens,
      sum(total_cost) as total_cost
    FROM default.observations
    {where_clause}
    FORMAT JSON
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                "http://clickhouse:8123/",
                params={"user": "langfuse", "password": "langfuse"},
                content=query,
            )
            resp.raise_for_status()
            data = resp.json()
            row = data["data"][0] if data.get("data") else {}
            return {
                "total_calls": int(row.get("total_calls", 0)),
                "total_input": int(row.get("total_input", 0)),
                "total_output": int(row.get("total_output", 0)),
                "total_tokens": int(row.get("total_tokens", 0)),
                "total_cost": float(row.get("total_cost", 0) or 0),
            }
    except Exception as e:
        logger.error("get_llm_usage_stats failed: %s", e)
        return {"total_calls": 0, "total_input": 0, "total_output": 0, "total_tokens": 0, "total_cost": 0}


@router.get("/api/audit/llm-usage/export")
async def export_llm_usage_csv(hours: int = 168) -> Response:
    """CSV export of LLM usage grouped by agent, for cost review/optimization
    discussions with agent teams."""
    import httpx
    from datetime import datetime, timezone, timedelta
    import csv
    import io

    where_clause = "WHERE type = 'GENERATION'"
    if hours > 0:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        where_clause += f" AND start_time >= '{since}'"

    query = f"""
    SELECT
      name,
      count() as calls,
      sum(usage_details['input']) as input_tokens,
      sum(usage_details['output']) as output_tokens,
      sum(usage_details['total']) as total_tokens,
      sum(total_cost) as total_cost,
      round(sum(usage_details['input']) / count(), 1) as avg_input_per_call
    FROM default.observations
    {where_clause}
    GROUP BY name
    ORDER BY total_cost DESC
    FORMAT JSON
    """
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                "http://clickhouse:8123/",
                params={"user": "langfuse", "password": "langfuse"},
                content=query,
            )
            resp.raise_for_status()
            data = resp.json()
            rows = data.get("data", [])

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Agent", "Calls", "Input Tokens", "Output Tokens", "Total Tokens", "Total Cost (USD)", "Avg Input Tokens/Call"])
        for r in rows:
            agent_name = (r.get("name") or "unknown").replace(".llm_call", "")
            writer.writerow([
                agent_name,
                r.get("calls", 0),
                r.get("input_tokens", 0),
                r.get("output_tokens", 0),
                r.get("total_tokens", 0),
                f"{float(r.get('total_cost', 0) or 0):.4f}",
                r.get("avg_input_per_call", 0),
            ])

        csv_content = output.getvalue()
        filename = f"llm-usage-by-agent-{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv"
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    except Exception as e:
        logger.error("export_llm_usage_csv failed: %s", e)
        return Response(content=f"Export failed: {e}", media_type="text/plain", status_code=500)
