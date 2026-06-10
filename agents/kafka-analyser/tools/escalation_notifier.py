"""Escalation notifier — Kafka Analyser copy."""
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
    return await send_to_teams(webhook_url, card)
