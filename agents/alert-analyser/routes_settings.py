import asyncio
import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import settings
from encryption import decrypt, encrypt, is_secret_key
from report_store import add_report
from tools.noise_detector import classify_alerts
from tools.source import AlertSource, JSMSource, OpsgenieAPISource, StandaloneOpsgenieSource

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/settings", tags=["settings"])

_sync_lock = False  # Prevent concurrent syncs

_DEFAULTS: dict = {
    "source_type": "file",
    "cloud_id": "",
    "email": "",
    "api_token": "",
    "last_synced": None,
    "alert_count": None,
    "sync_interval_minutes": 0,
    "noise_threshold_repeat": 3,
    "noise_threshold_window_mins": 60,
    "noise_threshold_close_secs": 300,
    "sync_window_days": 7,
    "priority_weights": {"P1": -3, "P2": -2, "P3": 0, "P4": 1, "P5": 2},
    "noise_classification_threshold": 0,
    "noise_suspect_threshold": -2,
    "opsgenie_base_url": "",
    "opsgenie_type": "standalone",
    "teams_enabled": False,
    "teams_webhook_url": "",
    "teams_severity_filter": ["critical", "warning"],
    "teams_cooldown_mins": 10,
    "esc_priorities": ["P1", "P2"],
}

# Write-through in-memory cache; populated from DB on startup.
_config: dict = dict(_DEFAULTS)

# Fired whenever settings are saved so the background sync loop re-evaluates immediately.
_sync_changed = asyncio.Event()


async def _upsert(key: str, value) -> None:
    from database import SessionLocal
    from models import AgentConfig

    if SessionLocal is None:
        return
    now = datetime.now(timezone.utc)
    raw = json.dumps(value)
    stored = encrypt(raw) if is_secret_key(key) else raw
    async with SessionLocal() as session:
        stmt = (
            pg_insert(AgentConfig)
            .values(agent_slug=settings.agent_slug, key=key, value=stored, updated_at=now)
            .on_conflict_do_update(
                index_elements=["agent_slug", "key"],
                set_={"value": stored, "updated_at": now},
            )
        )
        await session.execute(stmt)
        await session.commit()


async def load_config_from_db() -> dict:
    """Load all config rows from DB into _config. Returns the raw DB dict (empty if no DB)."""
    from database import SessionLocal
    from models import AgentConfig

    if SessionLocal is None:
        logger.warning("load_config_from_db: DATABASE_URL not set — no DB session available")
        return {}
    try:
        async with SessionLocal() as session:
            rows = (
                await session.execute(
                    select(AgentConfig).where(AgentConfig.agent_slug == settings.agent_slug)
                )
            ).scalars().all()

        logger.info(
            "load_config_from_db: found %d row(s) in agent_config — keys: %s",
            len(rows),
            [r.key for r in rows],
        )

        if not rows:
            return {}

        db_cfg: dict = {}
        for r in rows:
            secret = is_secret_key(r.key)
            try:
                raw = decrypt(r.value) if secret else r.value
                db_cfg[r.key] = json.loads(raw)
                logger.debug("load_config_from_db: loaded key=%r (secret=%s)", r.key, secret)
            except Exception as exc:
                logger.error(
                    "load_config_from_db: failed to decode key=%r (secret=%s, "
                    "stored_prefix=%r): %s",
                    r.key,
                    secret,
                    r.value[:20] if r.value else "",
                    exc,
                )

        _config.update(db_cfg)
        logger.info("load_config_from_db: successfully loaded keys: %s", list(db_cfg))

        # Re-encrypt any secrets stored with a previous key (or stored plaintext).
        # Idempotent: if already encrypted with the current key this is a no-op in terms of data.
        secret_keys = [k for k in db_cfg if is_secret_key(k) and db_cfg[k]]
        if secret_keys:
            for key in secret_keys:
                await _upsert(key, db_cfg[key])
            logger.info("load_config_from_db: re-encrypted %d secret key(s)", len(secret_keys))

        return db_cfg
    except Exception:
        logger.exception("load_config_from_db: DB query failed")
        return {}


async def _run_opsgenie_sync(full_sync: bool = False) -> dict:
    """Core sync logic callable from HTTP handler or lifespan startup."""
    global _sync_lock
    if _sync_lock:
        raise HTTPException(status_code=429, detail="Sync already in progress")
    _sync_lock = True
    try:
        source_type = _config.get("source_type", "opsgenie")
        opsgenie_type = _config.get("opsgenie_type", "standalone")
        use_jsm = source_type == "standalone" and opsgenie_type == "jsm"
        if use_jsm:
            source: AlertSource = JSMSource(
                cloud_id=_config["cloud_id"],
                email=_config["email"],
                api_token=_config["api_token"],
            )
        else:
            source = StandaloneOpsgenieSource(
                api_key=_config["api_token"],
                base_url=_config.get("opsgenie_base_url") or "https://api.opsgenie.com",
            )
        last_synced = _config.get("last_synced")
        if last_synced and not full_sync:
            alerts = await source.load_alerts(created_after=last_synced)
        else:
            sync_window_days = _config.get("sync_window_days", 7)
            alerts = await source.load_alerts(sync_window_days=sync_window_days)
        from datetime import datetime, timezone, timedelta
        filename = f"opsgenie-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}.json"
        from report_store import get_latest_classified
        existing = get_latest_classified()
        if existing and last_synced:
            window_mins = _config.get("noise_threshold_window_mins", 60)
            cutoff = datetime.now() - timedelta(minutes=window_mins * 4)
            def _alert_ts(a):
                try:
                    return datetime.fromisoformat(a["createdAt"].replace("Z", ""))
                except Exception:
                    return None
            all_alerts_raw = [
                a for a in existing
                if (_alert_ts(a) is None) or (_alert_ts(a) >= cutoff)
            ]
            existing_ids = {a.get("id") for a in all_alerts_raw}
            new_alerts = [a for a in alerts if a.get("id") not in existing_ids]
            combined_alerts = all_alerts_raw + new_alerts
            classified = classify_alerts(combined_alerts)
        else:
            classified = classify_alerts(alerts)
            combined_alerts = alerts
        report = add_report(filename, combined_alerts, classified)
        # Teams escalation for genuine P1/P2 unacknowledged alerts
        try:
            from tools.escalation_notifier import send_anomaly_summary
            import time

            teams_cfg = {
                "teams_enabled": _config.get("teams_enabled", False),
                "teams_webhook_url": _config.get("teams_webhook_url", ""),
                "teams_severity_filter": _config.get("teams_severity_filter",
                                                       ["critical", "warning"]),
                "teams_cooldown_mins": _config.get("teams_cooldown_mins", 10),
            }

            # Build anomalies from genuine P1/P2 unacknowledged alerts
            # Only escalate newly fetched alerts, not historical data
            if existing and last_synced:
                _new_ids = {a.get("id") for a in new_alerts}
                new_alerts_to_escalate = [a for a in classified if a.get("id") in _new_ids]
            else:
                new_alerts_to_escalate = classified
            escalation_anomalies = []
            for alert in new_alerts_to_escalate:
                if (alert.get("classification") == "genuine"
                        and alert.get("priority") in _config.get("esc_priorities", ["P1", "P2"])
                        ):  # temporarily removed: and not alert.get("acknowledged", False)
                    priority = alert.get("priority", "P3")
                    severity = "critical" if priority == "P1" else "warning"
                    escalation_anomalies.append({
                        "severity": severity,
                        "category": f"genuine_{priority.lower()}",
                        "description": (
                            f"{priority} alert: {alert.get('message', alert.get('alias', 'Unknown'))[:120]} "
                            f"— source: {alert.get('source', 'unknown')}, "
                            f"unacknowledged — immediate attention required."
                        ),
                        "recommended_action": "Acknowledge and investigate immediately.",
                    })

            # Deduplicate — keep top 8 by severity
            escalation_anomalies = escalation_anomalies[:8]

            try:
                from database import SessionLocal
                from sqlalchemy import text
                import json

                created_count = 0
                updated_count = 0

                # Deduplicate alerts by alias, keep newest createdAt
                _seen = {}
                for a in classified:
                    alias = a.get("alias") or a.get("id", "")
                    if not alias:
                        continue
                    existing = _seen.get(alias)
                    if existing is None or a.get("createdAt", "") > existing.get("createdAt", ""):
                        _seen[alias] = a
                deduped_alerts = list(_seen.values())

                async with SessionLocal() as session:
                    for alert in deduped_alerts:
                        try:
                            if alert.get("classification") != "genuine":
                                continue
                            if alert.get("status", "").lower() in ("closed", "resolved"):
                                continue
                            alert_id = alert.get("id")
                            priority = alert.get("priority", "P3")
                            title = alert.get("message", alert.get("alias", "Unknown"))[:200]
                            payload_json = json.dumps(alert)

                            # Derive source_tool
                            source_tool = alert.get("source", "unknown")

                            result = await session.execute(
                                text("""
                                    SELECT id, status FROM incident_management.incidents
                                    WHERE alert_id = :alert_id
                                    ORDER BY created_at DESC LIMIT 1
                                """),
                                {"alert_id": alert_id},
                            )
                            row = result.fetchone()

                            if row is None:
                                await session.execute(
                                    text("""
                                        INSERT INTO incident_management.incidents
                                        (alert_id, status, priority, title, alert_payload,
                                         recurrence_count, source_tool, created_at, updated_at)
                                        VALUES (:alert_id, 'ESCALATED', :priority, :title,
                                                :payload, 1, :source_tool, now(), now())
                                    """),
                                    {"alert_id": alert_id, "priority": priority,
                                     "title": title, "payload": payload_json,
                                     "source_tool": source_tool},
                                )
                                created_count += 1
                                _new_row = await session.execute(
                                    text("SELECT id FROM incident_management.incidents WHERE alert_id = :alert_id ORDER BY created_at DESC LIMIT 1"),
                                    {"alert_id": alert_id}
                                )
                                _new_ticket = _new_row.fetchone()
                                if _new_ticket:
                                    await session.execute(
                                        text("""
                                            INSERT INTO incident_management.incident_status_history
                                            (incident_id, from_status, to_status, changed_at)
                                            VALUES (:incident_id, NULL, 'ESCALATED', now())
                                        """),
                                        {"incident_id": _new_ticket.id}
                                    )
                            elif row.status not in ("RESOLVED", "MANUAL"):
                                await session.execute(
                                    text("""
                                        UPDATE incident_management.incidents
                                        SET updated_at = now(), recurrence_count = recurrence_count + 1,
                                            source_tool = :source_tool
                                        WHERE id = :id
                                    """),
                                    {"id": row.id,
                                     "source_tool": source_tool},
                                )
                                updated_count += 1
                            else:
                                await session.execute(
                                    text("""
                                        INSERT INTO incident_management.incidents
                                        (alert_id, status, priority, title, alert_payload,
                                         recurrence_count, related_ticket_id, source_tool,
                                         created_at, updated_at)
                                        VALUES (:alert_id, 'ESCALATED', :priority, :title,
                                                :payload, 1, :related_id, :source_tool,
                                                now(), now())
                                    """),
                                    {"alert_id": alert_id, "priority": priority, "title": title,
                                     "payload": payload_json, "related_id": row.id,
                                     "source_tool": source_tool},
                                )
                                created_count += 1
                                _new_row = await session.execute(
                                    text("SELECT id FROM incident_management.incidents WHERE alert_id = :alert_id ORDER BY created_at DESC LIMIT 1"),
                                    {"alert_id": alert_id}
                                )
                                _new_ticket = _new_row.fetchone()
                                if _new_ticket:
                                    await session.execute(
                                        text("""
                                            INSERT INTO incident_management.incident_status_history
                                            (incident_id, from_status, to_status, changed_at)
                                            VALUES (:incident_id, NULL, 'ESCALATED', now())
                                        """),
                                        {"incident_id": _new_ticket.id}
                                    )
                        except Exception as _alert_exc:
                            logger.error("Ticket creation/update failed for alert_id=%s: %s", alert.get("id"), _alert_exc)
                            # Persist failure record for debugging + retriggering
                            try:
                                # Safely parse ISO timestamp with Z suffix
                                alert_created_at = None
                                if alert.get("createdAt"):
                                    try:
                                        alert_created_at = datetime.fromisoformat(
                                            alert.get("createdAt", "").replace("Z", "+00:00")
                                        )
                                    except Exception:
                                        alert_created_at = None

                                await session.execute(
                                    text("""
                                        INSERT INTO incident_management.incident_creation_failures
                                        (alert_id, alert_title, alert_created_at, failure_reason, alert_payload)
                                        VALUES (:alert_id, :title, :created_at, :reason, :payload)
                                    """),
                                    {
                                        "alert_id": alert.get("id", ""),
                                        "title": alert.get("message", alert.get("alias", "Unknown"))[:500],
                                        "created_at": alert_created_at,
                                        "reason": str(_alert_exc)[:2000],
                                        "payload": json.dumps(alert),
                                    }
                                )
                                await session.commit()
                            except Exception as _log_exc:
                                logger.error("Failed to persist creation failure record: %s", _log_exc)
                    await session.commit()

                    # Reconciliation: live per-ticket OpsGenie status check.
                    # Replaces bounded-window snapshot comparison (was falsely
                    # auto-resolving incidents whose triggering alert aged past the
                    # ~4hr classification window, regardless of real OpsGenie state).
                    try:
                        import asyncio as _asyncio

                        RECONCILE_BATCH_SIZE = 100
                        RECONCILE_CONCURRENCY = 10

                        result = await session.execute(
                            text("""
                                SELECT id, alert_id, status, title
                                FROM incident_management.incidents
                                WHERE status NOT IN ('RESOLVED', 'MANUAL')
                                ORDER BY last_reconciliation_check_at ASC NULLS FIRST
                                LIMIT :batch_size
                            """),
                            {"batch_size": RECONCILE_BATCH_SIZE},
                        )
                        to_check = result.fetchall()

                        sem = _asyncio.Semaphore(RECONCILE_CONCURRENCY)
                        from tools.dashboard_builder import parse_message_status

                        async def _check_one(ticket):
                            msg_status = parse_message_status(ticket.title)
                            if msg_status == "resolved":
                                return ticket, "closed", "message_parse"
                            if msg_status == "open":
                                return ticket, "open", "message_parse"
                            async with sem:
                                try:
                                    status = await source.get_alert_status(ticket.alert_id)
                                except Exception:
                                    status = None
                            return ticket, status, "opsgenie_live"

                        checked = await _asyncio.gather(*[_check_one(t) for t in to_check])

                        auto_resolved_count = 0
                        resolved_externally_count = 0
                        still_open_count = 0
                        unknown_count = 0

                        for ticket, live_status, detected_via in checked:
                            now_ts = "now()"
                            if live_status is None:
                                # Lookup failed (rate limit, network error, not found) -
                                # fail-safe: just record the check attempt, leave ticket untouched.
                                await session.execute(
                                    text("""
                                        UPDATE incident_management.incidents
                                        SET last_reconciliation_check_at = now()
                                        WHERE id = :id
                                    """),
                                    {"id": ticket.id},
                                )
                                unknown_count += 1
                                continue

                            if live_status.lower() == "closed":
                                if ticket.status == "ESCALATED":
                                    await session.execute(
                                        text("""
                                            UPDATE incident_management.incidents
                                            SET status = 'RESOLVED', resolved_at = now(),
                                                resolution_type = 'self_healed',
                                                detected_via = :detected_via,
                                                last_reconciliation_check_at = now(),
                                                updated_at = now()
                                            WHERE id = :id
                                        """),
                                        {"id": ticket.id, "detected_via": detected_via},
                                    )
                                    auto_resolved_count += 1
                                    await session.execute(
                                        text("""
                                            INSERT INTO incident_management.incident_status_history
                                            (incident_id, from_status, to_status, changed_at)
                                            VALUES (:incident_id, 'ESCALATED', 'RESOLVED', now())
                                        """),
                                        {"incident_id": ticket.id}
                                    )
                                else:
                                    await session.execute(
                                        text("""
                                            UPDATE incident_management.incidents
                                            SET resolved_externally = TRUE,
                                                last_reconciliation_check_at = now(),
                                                updated_at = now()
                                            WHERE id = :id
                                        """),
                                        {"id": ticket.id},
                                    )
                                    resolved_externally_count += 1
                            else:
                                # Confirmed still open - just record the check, no status change.
                                await session.execute(
                                    text("""
                                        UPDATE incident_management.incidents
                                        SET last_reconciliation_check_at = now()
                                        WHERE id = :id
                                    """),
                                    {"id": ticket.id},
                                )
                                still_open_count += 1

                        await session.commit()
                        logger.info(
                            "Incident reconciliation: %d checked (batch=%d), %d auto-resolved, %d marked resolved_externally, %d confirmed still open, %d lookup failed/unknown",
                            len(checked), RECONCILE_BATCH_SIZE, auto_resolved_count, resolved_externally_count, still_open_count, unknown_count,
                        )
                    except Exception as recon_exc:
                        logger.warning("Incident reconciliation failed: %s", recon_exc)

                logger.info("Incident tickets: %d created, %d updated", created_count, updated_count)
            except Exception as exc:
                logger.warning("Incident ticket creation failed: %s", exc)

            # ── Escalation: new incidents (Teams concise) + open summary (Email detailed) ──
            cooldown_key = "alert_analyser_summary"
            cooldown_mins = int(teams_cfg.get("teams_cooldown_mins", 15))
            now_ts = time.time()
            if not hasattr(_run_opsgenie_sync, '_summary_cooldown'):
                _run_opsgenie_sync._summary_cooldown = {}
            last_ts = _run_opsgenie_sync._summary_cooldown.get(cooldown_key, 0)
            if now_ts - last_ts >= cooldown_mins * 60:
                from tools.escalation_notifier import send_incident_escalation, send_incident_email_report
                from database import SessionLocal as _SL
                from sqlalchemy import text as _text
                from datetime import datetime as _dt, timezone as _tz
                # Get new incidents since last escalation
                last_esc_time = _dt.fromtimestamp(last_ts, tz=_tz.utc) if last_ts else _dt.now(_tz.utc).replace(hour=0, minute=0, second=0)
                new_incidents = []
                open_incidents = []
                try:
                    async with _SL() as _sess:
                        # New incidents since last escalation
                        new_res = await _sess.execute(_text("""
                            SELECT id, priority, title, status,
                                   (alert_payload->>'createdAt')::timestamptz as created_at,
                                   recurrence_count
                            FROM incident_management.incidents
                            WHERE status = 'ESCALATED'
                            AND (alert_payload->>'createdAt')::timestamptz > :since
                            ORDER BY priority ASC, (alert_payload->>'createdAt')::timestamptz DESC
                            LIMIT 10
                        """), {"since": last_esc_time})
                        new_incidents = [dict(r._mapping) for r in new_res.fetchall()]
                        # All open incidents summary
                        open_res = await _sess.execute(_text("""
                            SELECT id, priority, title, status,
                                   (alert_payload->>'createdAt')::timestamptz as created_at,
                                   recurrence_count
                            FROM incident_management.incidents
                            WHERE status = 'ESCALATED'
                            ORDER BY priority ASC, (alert_payload->>'createdAt')::timestamptz DESC
                            LIMIT 100
                        """))
                        open_incidents = [dict(r._mapping) for r in open_res.fetchall()]
                except Exception as _ie:
                    logger.error("Incident query for escalation failed: %s", _ie)
                # Send Teams: top 3 new incidents + summary
                if new_incidents or escalation_anomalies:
                    sent = await send_incident_escalation(
                        new_incidents=new_incidents[:3],
                        open_count=len(open_incidents),
                        new_alert_count=len(escalation_anomalies),
                        config=_config,
                        dashboard_url="http://kpi-internal.cloud.operative.com:3000/agents/alert-analyser/dashboard",
                    )
                    # Send Email: full open incident report
                    await send_incident_email_report(
                        open_incidents=open_incidents,
                        new_incidents=new_incidents,
                        config=_config,
                    )
                    if sent:
                        _run_opsgenie_sync._summary_cooldown[cooldown_key] = now_ts
        except Exception as _esc_exc:
            import traceback
            logger.error("Alert escalation failed: %s\n%s", _esc_exc, traceback.format_exc())
        _config["last_synced"] = datetime.now(timezone.utc).isoformat()
        _config["alert_count"] = len(combined_alerts)
        await _upsert("last_synced", _config["last_synced"])
        await _upsert("alert_count", _config["alert_count"])
        return {
            "ok": True,
            "alert_count": len(combined_alerts),
            "last_synced": _config["last_synced"],
            "report": report,
        }
    finally:
        _sync_lock = False

class SettingsPayload(BaseModel):
    source_type: str = "file"
    cloud_id: str = ""
    email: str = ""
    api_token: str = ""
    sync_interval_minutes: int = 0
    noise_threshold_repeat: int = 3
    noise_threshold_window_mins: int = 60
    noise_threshold_close_secs: int = 300
    sync_window_days: int = 7
    incident_purge_days: int = 7
    incident_purge_enabled: bool = False
    priority_weights: dict = {"P1": -3, "P2": -2, "P3": 0, "P4": 1, "P5": 2}
    noise_classification_threshold: int = 0
    noise_suspect_threshold: int = -2
    opsgenie_base_url: str = ""
    opsgenie_type: str = "standalone"


@router.get("")
async def get_settings() -> dict:
    await load_config_from_db()
    result = {k: v for k, v in _config.items() if k != "api_token"}
    result["api_key_configured"] = bool(_config.get("api_key", ""))
    result["api_key_last4"] = (
        _config.get("api_key", "")[-4:]
        if _config.get("api_key") else ""
    )
    result["sync_window_days"] = _config.get("sync_window_days", 7)
    result["incident_purge_days"] = _config.get("incident_purge_days", 7)
    result["incident_purge_enabled"] = _config.get("incident_purge_enabled", False)
    result["priority_weights"] = _config.get(
        "priority_weights", {"P1": -3, "P2": -2, "P3": 0, "P4": 1, "P5": 2}
    )
    result["noise_classification_threshold"] = _config.get(
        "noise_classification_threshold", 0
    )
    result["api_token_configured"] = bool(_config.get("api_token", ""))
    result["genie_key_configured"] = bool(_config.get("api_token", "")) and _config.get("source_type") == "standalone" and _config.get("opsgenie_type") == "standalone"
    from config import settings as _settings
    result["model"] = _settings.model
    return result


@router.post("")
async def save_settings(request: Request) -> dict:
    data = await request.json()
    # Keep existing api_token if payload sends blank or mask
    if not data.get("api_token") or data.get("api_token") == "••••••••":
        data["api_token"] = _config.get("api_token", "")
    _config.update(data)
    for k, v in data.items():
        if k == "api_token" and not v:
            continue  # never upsert empty api_token — keep existing encrypted value
        await _upsert(k, v)
    _sync_changed.set()  # wake the background loop to re-evaluate interval immediately
    return {"ok": True}


@router.post("/test-email")
async def test_email(request: Request) -> dict:
    """Send a test email to verify Office 365 SMTP config."""
    body = await request.json()
    smtp_server = body.get("smtp_server", "smtp.office365.com").strip()
    smtp_port = int(body.get("smtp_port", 587))
    from_addr = body.get("from_address", "").strip()
    password = body.get("password", "").strip()
    to_addrs = [e.strip() for e in body.get("to", "").split(",") if e.strip()]
    subject_prefix = body.get("subject_prefix", "[Operative Alert]").strip()
    if not from_addr or not to_addrs or not password:
        raise HTTPException(status_code=400, detail="from_address, password and to are required")
    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"{subject_prefix} Test Notification"
        msg["From"] = from_addr
        msg["To"] = ", ".join(to_addrs)
        html_body = """<html><body>
        <h2>✅ Operative Intelligence — Test Email</h2>
        <p>This is a test notification from the Alert Analyser agent.</p>
        <p>Email escalation is configured correctly.</p>
        </body></html>"""
        msg.attach(MIMEText(html_body, "html"))
        import asyncio
        def _send():
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.ehlo()
                server.starttls()
                server.login(from_addr, password)
                server.sendmail(from_addr, to_addrs, msg.as_string())
        await asyncio.get_event_loop().run_in_executor(None, _send)
        return {"ok": True, "message": f"Test email sent to {', '.join(to_addrs)}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Email send failed: {str(e)}")


@router.post("/test-teams")
async def test_teams_webhook(request: Request) -> dict:
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    webhook_url = body.get("webhook_url", "").strip()
    if not webhook_url:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="webhook_url required")
    from tools.escalation_notifier import escalate as _escalate_teams
    test_anomaly = {
        "severity": "info",
        "category": "test",
        "description": "This is a test message from Operative Intelligence Alert Analyser. "
                       "Teams escalation is configured correctly.",
        "recommended_action": "No action required — this is a connectivity test.",
    }
    teams_cfg = {
        "teams_enabled": True,
        "teams_webhook_url": webhook_url,
        "teams_severity_filter": ["critical", "warning", "info"],
        "teams_cooldown_mins": 0,
    }
    success = await _escalate_teams(
        agent_name="Alert Analyser",
        cluster_name="Test",
        anomaly=test_anomaly,
        config=teams_cfg,
        dashboard_url="",
    )
    if success:
        return {"ok": True, "message": "Test message sent successfully"}
    else:
        from fastapi import HTTPException
        raise HTTPException(status_code=502, detail="Failed to send test message")


@router.post("/sync")
async def sync_alerts() -> dict:
    source_type = _config.get("source_type")
    if source_type == "standalone":
        if not _config.get("api_token"):
            raise HTTPException(status_code=400, detail="Missing required field: api_token (GenieKey)")
    elif source_type == "opsgenie":
        for field in ("cloud_id", "email", "api_token"):
            if not _config.get(field):
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
    else:
        raise HTTPException(
            status_code=400,
            detail="Source type must be 'opsgenie' (JSM) or 'standalone' to sync",
        )
    return await _run_opsgenie_sync(full_sync=True)
