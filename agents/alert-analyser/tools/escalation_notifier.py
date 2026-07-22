"""Escalation notifier — Alert Analyser copy."""
import httpx
import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_cooldown_cache: dict[str, float] = {}

SEVERITY_COLOUR = {
    "critical": "attention",   # red in Adaptive Cards
    "warning": "warning",      # orange
    "info": "good",            # green
}

SEVERITY_EMOJI = {
    "critical": "🔴",
    "warning": "🟡",
    "info": "🟢",
}

CATEGORY_LABEL = {
    "broker_heap": "Broker Heap",
    "under_replicated_partitions": "Under-Replicated Partitions",
    "consumer_lag": "Consumer Lag",
    "consumer_group_dead": "Consumer Group Dead",
    "topic_retention": "Topic Retention",
    "connector_failure": "Connector Failure",
    "cost_spike": "Cost Spike",
    "noise_alert": "Noise Alert",
    "broker_gc": "Broker GC Pause",
    "broker_fetch_latency": "Broker Fetch Latency",
    "genuine_p1": "P1 Critical Alert",
    "genuine_p2": "P2 High Alert",
    "unacknowledged_genuine": "Unacknowledged Genuine Alert",
}

def build_adaptive_card(
    agent_name: str,
    cluster_name: str,
    anomaly: dict,
    dashboard_url: str = "",
) -> dict:
    severity = anomaly.get("severity", "info")
    category = anomaly.get("category", "unknown")
    description = anomaly.get("description", "No description")
    recommended_action = anomaly.get("recommended_action", "")
    colour = SEVERITY_COLOUR.get(severity, "default")
    emoji = SEVERITY_EMOJI.get(severity, "⚪")
    cat_label = CATEGORY_LABEL.get(category, category.replace("_", " ").title())
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    body = [
        {
            "type": "TextBlock",
            "text": f"{emoji} Operative Intelligence — {agent_name}",
            "weight": "Bolder",
            "size": "Medium",
            "color": colour,
        },
        {
            "type": "FactSet",
            "facts": [
                {"title": "Severity", "value": severity.upper()},
                {"title": "Category", "value": cat_label},
                {"title": "Cluster", "value": cluster_name},
                {"title": "Time", "value": timestamp},
            ],
        },
        {
            "type": "TextBlock",
            "text": description,
            "wrap": True,
            "spacing": "Medium",
        },
    ]

    if recommended_action:
        body.append({
            "type": "TextBlock",
            "text": f"💡 {recommended_action}",
            "wrap": True,
            "color": "accent",
            "spacing": "Small",
        })

    actions = []
    if dashboard_url:
        actions.append({
            "type": "Action.OpenUrl",
            "title": "View Dashboard",
            "url": dashboard_url,
        })

    card = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.4",
                    "body": body,
                    "actions": actions if actions else [],
                },
            }
        ],
    }
    return card


async def send_to_teams(webhook_url: str, card: dict) -> bool:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(webhook_url, json=card)
            if resp.status_code in (200, 202):
                logger.info("Teams notification sent successfully")
                return True
            else:
                logger.error("Teams webhook returned %s: %s", resp.status_code, resp.text)
                return False
    except Exception as exc:
        logger.exception("Teams notification failed: %s", exc)
        return False


async def escalate(
    agent_name: str,
    cluster_name: str,
    anomaly: dict,
    config: dict,
    dashboard_url: str = "",
) -> bool:
    """
    Main entry point. Call this from any agent after anomaly detection.
    config dict must contain:
      - teams_webhook_url: str
      - teams_enabled: bool
      - teams_severity_filter: list[str] e.g. ["critical", "warning"]
      - teams_cooldown_mins: int
    Cooldown is enforced via _cooldown_cache (in-memory, resets on restart).
    For persistent cooldown, caller should check escalations table.
    """
    if not config.get("teams_enabled", False):
        return False

    webhook_url = config.get("teams_webhook_url", "")
    if not webhook_url:
        logger.warning("Teams escalation enabled but no webhook URL configured")
        return False

    severity = anomaly.get("severity", "info")
    severity_filter = config.get("teams_severity_filter", ["critical", "warning"])
    if severity not in severity_filter:
        logger.info("Skipping escalation — severity %s not in filter %s", severity, severity_filter)
        return False

    cooldown_mins = config.get("teams_cooldown_mins", 60)
    if cooldown_mins > 0:
        cooldown_key = f"{anomaly.get('category', 'unknown')}_{anomaly.get('severity', 'info')}"
        now = time.time()
        last_sent = _cooldown_cache.get(cooldown_key, 0)
        if now - last_sent < cooldown_mins * 60:
            logger.info(
                "Skipping escalation — cooldown active for %s (%.0f mins remaining)",
                cooldown_key,
                (cooldown_mins * 60 - (now - last_sent)) / 60,
            )
            return False
        _cooldown_cache[cooldown_key] = now

    card = build_adaptive_card(agent_name, cluster_name, anomaly, dashboard_url)
    return await send_to_teams(webhook_url, card)


async def send_anomaly_summary(
    agent_name: str,
    cluster_name: str,
    anomalies: list[dict],
    config: dict,
    dashboard_url: str = "",
) -> bool:
    """Send a single summary card for all anomalies instead of one per anomaly."""
    if not config.get("teams_enabled", False):
        return False
    webhook_url = config.get("teams_webhook_url", "")
    if not webhook_url:
        return False
    if not anomalies:
        return False

    severity_filter = config.get("teams_severity_filter", ["critical", "warning"])
    filtered = [a for a in anomalies if a.get("severity") in severity_filter]
    if not filtered:
        return False

    critical_count = sum(1 for a in filtered if a.get("severity") == "critical")
    warning_count = sum(1 for a in filtered if a.get("severity") == "warning")

    # Overall severity = worst in the list
    overall_severity = "critical" if critical_count > 0 else "warning"
    emoji = SEVERITY_EMOJI[overall_severity]
    colour = SEVERITY_COLOUR[overall_severity]

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    body = [
        {
            "type": "TextBlock",
            "text": f"{emoji} Operative Intelligence — {agent_name}",
            "weight": "Bolder",
            "size": "Medium",
            "color": colour,
        },
        {
            "type": "FactSet",
            "facts": [
                {"title": "Cluster", "value": cluster_name},
                {"title": "Critical", "value": str(critical_count)},
                {"title": "Warnings", "value": str(warning_count)},
                {"title": "Time", "value": timestamp},
            ],
        },
    ]

    # Add each anomaly as a TextBlock
    for a in filtered[:8]:  # max 8 anomalies in one card
        sev = a.get("severity", "info")
        cat = CATEGORY_LABEL.get(a.get("category", ""),
              a.get("category", "").replace("_", " ").title())
        desc = a.get("description", "")
        sev_emoji = SEVERITY_EMOJI.get(sev, "⚪")
        body.append({
            "type": "TextBlock",
            "text": f"{sev_emoji} **{cat}** — {desc}",
            "wrap": True,
            "spacing": "Small",
        })

    actions = []
    if dashboard_url:
        actions.append({
            "type": "Action.OpenUrl",
            "title": "View Dashboard",
            "url": dashboard_url,
        })

    card = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.4",
                    "body": body,
                    "actions": actions,
                },
            }
        ],
    }
    try:
        result = await send_to_teams(webhook_url, card)
        if result:
            # Log successful escalation
            try:
                from database import SessionLocal
                from datetime import datetime as dt, timezone as tz
                from sqlalchemy import text
                if SessionLocal is not None:
                    async with SessionLocal() as sess:
                        await sess.execute(text("""
                            INSERT INTO alert_escalation_log
                            (agent_slug, channel, severity, alert_count, message_summary, recipients, status, sent_at)
                            VALUES (:slug, 'teams', :severity, :count, :summary, :recipients, 'sent', :sent_at)
                        """), {
                            "slug": "alert-analyser",
                            "severity": overall_severity,
                            "count": len(filtered),
                            "summary": f"{len(filtered)} alert(s) escalated — {critical_count} critical, {warning_count} warning",
                            "recipients": webhook_url[:100],
                            "sent_at": dt.now(tz.utc),
                        })
                        await sess.commit()
            except Exception as log_exc:
                logger.warning("Failed to log escalation: %s", log_exc)
            return True
        else:
            return False
    except Exception as exc:
        logger.error("Teams summary send failed: %s", exc)
        # Log failed escalation
        try:
            from database import SessionLocal
            from datetime import datetime as dt, timezone as tz
            from sqlalchemy import text
            if SessionLocal is not None:
                async with SessionLocal() as sess:
                    await sess.execute(text("""
                        INSERT INTO alert_escalation_log
                        (agent_slug, channel, severity, alert_count, status, error_message, sent_at)
                        VALUES (:slug, 'teams', :severity, :count, 'failed', :error, :sent_at)
                    """), {
                        "slug": "alert-analyser",
                        "severity": overall_severity,
                        "count": len(filtered),
                        "error": str(exc)[:500],
                        "sent_at": dt.now(tz.utc),
                    })
                    await sess.commit()
        except Exception:
            pass
        return False


async def send_email_escalation(
    anomalies: list[dict],
    config: dict,
) -> bool:
    """Send email escalation via Office 365 SMTP."""
    if not config.get("email_enabled", False):
        return False
    from_addr = config.get("email_from", "").strip()
    password = config.get("email_password", "").strip()
    to_str = config.get("email_to", "").strip()
    smtp_server = config.get("email_smtp_server", "smtp.office365.com")
    smtp_port = int(config.get("email_smtp_port", 587))
    subject_prefix = config.get("email_subject_prefix", "[Operative Alert]")
    if not from_addr or not password or not to_str:
        return False
    to_addrs = [e.strip() for e in to_str.split(",") if e.strip()]
    if not to_addrs or not anomalies:
        return False
    severity_filter = config.get("teams_severity_filter", ["critical", "warning"])
    filtered = [a for a in anomalies if a.get("severity") in severity_filter]
    if not filtered:
        return False
    critical = [a for a in filtered if a.get("severity") == "critical"]
    warning = [a for a in filtered if a.get("severity") == "warning"]
    overall = "critical" if critical else "warning"
    emoji = SEVERITY_EMOJI[overall]
    subject = f"{subject_prefix} {emoji} {len(filtered)} Alert(s) Escalated — {len(critical)} Critical, {len(warning)} Warning"
    rows = "".join(
        f"<tr><td style='padding:6px 12px;border-bottom:1px solid #e2e8f0'>{SEVERITY_EMOJI.get(a.get('severity','info'))} {a.get('severity','').upper()}</td>"
        f"<td style='padding:6px 12px;border-bottom:1px solid #e2e8f0'>{CATEGORY_LABEL.get(a.get('category',''), a.get('category',''))}</td>"
        f"<td style='padding:6px 12px;border-bottom:1px solid #e2e8f0'>{a.get('description','')}</td></tr>"
        for a in filtered[:10]
    )
    html_body = f"""<html><body style='font-family:sans-serif;color:#1e293b'>
    <h2 style='color:#dc2626'>{emoji} Operative Intelligence — Alert Escalation</h2>
    <p><strong>{len(filtered)} alert(s)</strong> require attention — {len(critical)} critical, {len(warning)} warning</p>
    <table style='width:100%;border-collapse:collapse;margin-top:16px'>
      <thead><tr style='background:#f1f5f9'>
        <th style='padding:8px 12px;text-align:left'>Severity</th>
        <th style='padding:8px 12px;text-align:left'>Category</th>
        <th style='padding:8px 12px;text-align:left'>Description</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>
    <p style='margin-top:16px;color:#64748b;font-size:12px'>Sent by Operative Intelligence Alert Analyser · {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}</p>
    </body></html>"""
    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        import asyncio
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = ", ".join(to_addrs)
        msg.attach(MIMEText(html_body, "html"))
        def _send():
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.ehlo()
                server.starttls()
                server.login(from_addr, password)
                server.sendmail(from_addr, to_addrs, msg.as_string())
        await asyncio.get_event_loop().run_in_executor(None, _send)
        # Log to DB
        try:
            from database import SessionLocal
            from sqlalchemy import text
            if SessionLocal is not None:
                async with SessionLocal() as sess:
                    await sess.execute(text("""
                        INSERT INTO alert_escalation_log
                        (agent_slug, channel, severity, alert_count, message_summary, recipients, status, sent_at)
                        VALUES (:slug, 'email', :severity, :count, :summary, :recipients, 'sent', now())
                    """), {
                        "slug": "alert-analyser",
                        "severity": overall,
                        "count": len(filtered),
                        "summary": f"{len(filtered)} alert(s) — {len(critical)} critical, {len(warning)} warning",
                        "recipients": to_str[:100],
                    })
                    await sess.commit()
        except Exception as log_exc:
            logger.warning("Failed to log email escalation: %s", log_exc)
        logger.info("Email escalation sent to %s", to_addrs)
        return True
    except Exception as exc:
        logger.error("Email escalation failed: %s", exc)
        try:
            from database import SessionLocal
            from sqlalchemy import text
            if SessionLocal is not None:
                async with SessionLocal() as sess:
                    await sess.execute(text("""
                        INSERT INTO alert_escalation_log
                        (agent_slug, channel, severity, alert_count, status, error_message, sent_at)
                        VALUES (:slug, 'email', :severity, :count, 'failed', :error, now())
                    """), {
                        "slug": "alert-analyser",
                        "severity": overall,
                        "count": len(filtered),
                        "error": str(exc)[:500],
                    })
                    await sess.commit()
        except Exception:
            pass
        return False


async def send_incident_escalation(
    new_incidents: list[dict],
    open_count: int,
    new_alert_count: int,
    config: dict,
    dashboard_url: str = "",
) -> bool:
    """Send Teams card: top 3 new incidents + open summary."""
    if not config.get("teams_enabled", False):
        return False
    webhook_url = config.get("teams_webhook_url", "")
    if not webhook_url:
        return False
    if not new_incidents and new_alert_count == 0:
        return False
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    # Build incident rows
    incident_rows = []
    for inc in new_incidents[:3]:
        priority = inc.get("priority", "—")
        title = (inc.get("title") or "")[:80]
        recurrence = inc.get("recurrence_count", 1)
        emoji = "🔴" if priority in ("P1",) else "🟡" if priority == "P2" else "🟢"
        incident_rows.append({
            "type": "TextBlock",
            "text": f"{emoji} **{priority}** — {title}{'...' if len(inc.get('title','')) > 80 else ''}{' *(recurring x'+str(recurrence)+')* ' if recurrence > 1 else ''}",
            "wrap": True,
            "spacing": "Small",
        })
    if not incident_rows:
        incident_rows = [{"type": "TextBlock", "text": "No new incidents this cycle", "color": "good"}]
    body = [
        {"type": "TextBlock", "text": f"🚨 Operative Intelligence — Incident Update", "weight": "Bolder", "size": "Medium", "color": "attention"},
        {"type": "TextBlock", "text": f"**{now_str}**", "isSubtle": True, "spacing": "None"},
        {"type": "TextBlock", "text": f"📋 NEW INCIDENTS ({len(new_incidents)})", "weight": "Bolder", "spacing": "Medium"},
        *incident_rows,
        {"type": "FactSet", "spacing": "Medium", "facts": [
            {"title": "Total Open", "value": str(open_count)},
            {"title": "New This Cycle", "value": str(len(new_incidents))},
            {"title": "New Alerts", "value": str(new_alert_count)},
        ]},
        {"type": "TextBlock", "text": "📧 Check email for full open incident report", "isSubtle": True, "wrap": True, "spacing": "Small"},
    ]
    actions = []
    if dashboard_url:
        actions.append({"type": "Action.OpenUrl", "title": "View Dashboard", "url": dashboard_url})
        actions.append({"type": "Action.OpenUrl", "title": "View Incidents", "url": dashboard_url.replace("/dashboard", "/dashboard#incidents")})
    card = {
        "type": "message",
        "attachments": [{"contentType": "application/vnd.microsoft.card.adaptive", "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard", "version": "1.4",
            "body": body, "actions": actions,
        }}]
    }
    try:
        result = await send_to_teams(webhook_url, card)
        if result:
            try:
                from database import SessionLocal
                from sqlalchemy import text
                if SessionLocal is not None:
                    async with SessionLocal() as sess:
                        await sess.execute(text("""
                            INSERT INTO alert_escalation_log
                            (agent_slug, channel, severity, alert_count, message_summary, recipients, status, sent_at)
                            VALUES (:slug, 'teams', 'critical', :count, :summary, :recipients, 'sent', now())
                        """), {
                            "slug": "alert-analyser",
                            "count": len(new_incidents),
                            "summary": f"{len(new_incidents)} new incidents, {open_count} total open",
                            "recipients": webhook_url[:100],
                        })
                        await sess.commit()
            except Exception as log_exc:
                logger.warning("Failed to log incident escalation: %s", log_exc)
        return result
    except Exception as exc:
        logger.error("Incident Teams escalation failed: %s", exc)
        return False


async def send_incident_email_report(
    open_incidents: list[dict],
    new_incidents: list[dict],
    config: dict,
) -> bool:
    """Send HTML email: full open incident report."""
    if not config.get("email_enabled", False):
        return False
    from_addr = config.get("email_from", "").strip()
    password = config.get("email_password", "").strip()
    to_str = config.get("email_to", "").strip()
    smtp_server = config.get("email_smtp_server", "smtp.office365.com")
    smtp_port = int(config.get("email_smtp_port", 587))
    subject_prefix = config.get("email_subject_prefix", "[Operative Alert]")
    if not from_addr or not password or not to_str:
        return False
    to_addrs = [e.strip() for e in to_str.split(",") if e.strip()]
    if not to_addrs:
        return False
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    subject = f"{subject_prefix} Open Incident Report — {now_str}"
    # Build HTML table rows
    def priority_color(p):
        return {"P1": "#dc2626", "P2": "#f59e0b", "P3": "#3b82f6"}.get(p, "#64748b")
    rows = ""
    for inc in open_incidents[:50]:
        p = inc.get("priority", "—")
        title = (inc.get("title") or "")[:100]
        created = inc.get("created_at")
        elapsed = ""
        if created:
            try:
                from datetime import datetime as _dt, timezone as _tz
                delta = _dt.now(_tz.utc) - created
                h, m = divmod(int(delta.total_seconds()), 3600)
                elapsed = f"{h}h {m//60}m" if h else f"{m//60}m"
            except Exception:
                pass
        is_new = any(str(inc.get("id")) == str(n.get("id")) for n in new_incidents)
        new_badge = '<span style="background:#dcfce7;color:#15803d;padding:1px 6px;border-radius:8px;font-size:11px;margin-left:6px">NEW</span>' if is_new else ""
        rows += f"""<tr>
            <td style="padding:8px 12px;border-bottom:1px solid #e2e8f0">
                <span style="color:{priority_color(p)};font-weight:700">{p}</span>
            </td>
            <td style="padding:8px 12px;border-bottom:1px solid #e2e8f0">{title}{new_badge}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #e2e8f0;color:#64748b;font-size:12px">{elapsed or '—'}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #e2e8f0;color:#64748b;font-size:12px">{inc.get('recurrence_count',1)}x</td>
        </tr>"""
    more_note = f"<p style='color:#64748b;font-size:12px'>Showing top 50 of {len(open_incidents)} open incidents.</p>" if len(open_incidents) > 50 else ""
    html_body = f"""<html><body style='font-family:sans-serif;color:#1e293b;max-width:800px;margin:0 auto'>
    <h2 style='color:#dc2626'>🚨 Operative Intelligence — Open Incident Report</h2>
    <p><strong>{len(open_incidents)} open incidents</strong> · {len(new_incidents)} new this cycle · Generated {now_str}</p>
    <table style='width:100%;border-collapse:collapse;margin-top:16px'>
      <thead><tr style='background:#f1f5f9'>
        <th style='padding:8px 12px;text-align:left;font-size:12px'>Priority</th>
        <th style='padding:8px 12px;text-align:left;font-size:12px'>Title</th>
        <th style='padding:8px 12px;text-align:left;font-size:12px'>Open For</th>
        <th style='padding:8px 12px;text-align:left;font-size:12px'>Recurrence</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>
    {more_note}
    <p style='margin-top:24px;color:#64748b;font-size:12px;border-top:1px solid #e2e8f0;padding-top:12px'>
      Sent by Operative Intelligence Alert Analyser · {now_str}<br>
      This is an automated report — do not reply to this email.
    </p>
    </body></html>"""
    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        import asyncio
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = ", ".join(to_addrs)
        msg.attach(MIMEText(html_body, "html"))
        def _send():
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.ehlo(); server.starttls()
                server.login(from_addr, password)
                server.sendmail(from_addr, to_addrs, msg.as_string())
        await asyncio.get_event_loop().run_in_executor(None, _send)
        try:
            from database import SessionLocal
            from sqlalchemy import text
            if SessionLocal is not None:
                async with SessionLocal() as sess:
                    await sess.execute(text("""
                        INSERT INTO alert_escalation_log
                        (agent_slug, channel, severity, alert_count, message_summary, recipients, status, sent_at)
                        VALUES (:slug, 'email', 'info', :count, :summary, :recipients, 'sent', now())
                    """), {
                        "slug": "alert-analyser",
                        "count": len(open_incidents),
                        "summary": f"Open incident report: {len(open_incidents)} open, {len(new_incidents)} new",
                        "recipients": to_str[:100],
                    })
                    await sess.commit()
        except Exception as log_exc:
            logger.warning("Failed to log email report: %s", log_exc)
        logger.info("Incident email report sent to %s (%d incidents)", to_addrs, len(open_incidents))
        return True
    except Exception as exc:
        logger.error("Incident email report failed: %s", exc)
        return False
