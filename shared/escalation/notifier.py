import httpx
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

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


def build_health_summary_card(
    agent_name: str,
    process_count: int,
    rows: list[dict],
) -> dict:
    """Builds a multi-cluster health summary table card -- a periodic
    routine status card (distinct from build_adaptive_card's single-
    anomaly cards), showing per-cluster data freshness and recent job
    success/failure counts at a glance. Uses a ColumnSet-based table
    layout (not the native Adaptive Card Table element) for reliable
    rendering across both desktop and mobile Teams clients.

    rows: one dict per cluster, each with keys:
      - name: str (cluster display name)
      - data_age_minutes: float | None (None if no data ever collected)
      - success_count: int (recent window)
      - failed_count: int (recent window)
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    def _age_text(age: float | None) -> tuple[str, str]:
        # Returns (display_text, color) -- color follows the same
        # SEVERITY_COLOUR palette used elsewhere in this file for
        # visual consistency.
        if age is None:
            return ("no data", "warning")
        if age < 5:
            return (f"{age:.0f}m ago", "good")
        if age < 10:
            return (f"{age:.0f}m ago", "warning")
        return (f"{age:.0f}m ago", "attention")

    def _cell(text: str, color: str = "default", weight: str = "default") -> dict:
        return {
            "type": "Column",
            "width": "stretch",
            "items": [{
                "type": "TextBlock",
                "text": text,
                "wrap": True,
                "size": "Small",
                "color": color,
                "weight": weight,
            }],
        }

    header_row = {
        "type": "ColumnSet",
        "columns": [
            _cell("Cluster", weight="Bolder"),
            _cell("Data Age", weight="Bolder"),
            _cell("Jobs (recent)", weight="Bolder"),
        ],
        "separator": True,
    }

    data_rows = []
    for r in rows:
        age_text, age_color = _age_text(r.get("data_age_minutes"))
        success_count = r.get("success_count", 0)
        failed_count = r.get("failed_count", 0)
        jobs_color = "attention" if failed_count > 0 else "good"
        jobs_text = f"{success_count}✅ {failed_count}❌" if failed_count > 0 else f"{success_count}✅"
        data_rows.append({
            "type": "ColumnSet",
            "columns": [
                _cell(r.get("name", "Unknown")),
                _cell(age_text, color=age_color),
                _cell(jobs_text, color=jobs_color),
            ],
            "separator": True,
        })

    body = [
        {
            "type": "TextBlock",
            "text": f"📊 Operative Intelligence — {agent_name} Health Summary",
            "weight": "Bolder",
            "size": "Medium",
        },
        {
            "type": "FactSet",
            "facts": [
                {"title": "Time", "value": timestamp},
                {"title": "Process Count", "value": str(process_count)},
            ],
        },
        header_row,
    ] + data_rows

    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.4",
                    "body": body,
                    "actions": [],
                },
            }
        ],
    }


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

    card = build_adaptive_card(agent_name, cluster_name, anomaly, dashboard_url)
    return await send_to_teams(webhook_url, card)
