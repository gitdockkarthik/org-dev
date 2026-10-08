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
    "cluster.urp_total": "Under-Replicated Partitions",
    "cluster.rf_below_min": "Topics Below Minimum RF",
    "cluster.leader_skew": "Leader Partition Skew",
    "cluster.msg_rate_in": "Message Rate In",
    "cluster.data_gb_max": "Data On Disk (Max Broker)",
    "cluster.data_spread_pct": "Data Spread Between Brokers",
    "cluster.zk_ensemble": "ZooKeeper Ensemble",
    "cluster.connect_failed_connectors": "Connect Failed Connectors",
    "cluster.connect_failed_tasks": "Connect Failed Tasks",
    "cluster.connect_workers_down": "Connect Workers Down",
    "broker.cpu_pct": "Broker CPU %",
    "broker.heap_pct": "Broker Heap %",
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


def _format_open_duration(open_minutes: float) -> str:
    total = max(0, int(round(open_minutes)))
    if total < 60:
        return f"{total} min"
    hours, minutes = divmod(total, 60)
    return f"{hours} h {minutes} min" if minutes else f"{hours} h"


def build_resolve_card(
    agent_name: str,
    cluster_name: str,
    rule_name: str,
    subject: str | None,
    severity: str | None,
    open_minutes: float | None,
) -> dict:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    facts = [
        {"title": "Status", "value": "RESOLVED"},
        {"title": "Rule", "value": rule_name},
        {"title": "Cluster", "value": cluster_name},
    ]
    if subject:
        facts.append({"title": "Subject", "value": subject})
    if severity:
        facts.append({"title": "Was", "value": severity.upper()})
    if open_minutes is not None:
        facts.append({"title": "Open for", "value": _format_open_duration(open_minutes)})
    facts.append({"title": "Time", "value": timestamp})

    body = [
        {
            "type": "TextBlock",
            "text": f"✅ Operative Intelligence — {agent_name}",
            "weight": "Bolder",
            "size": "Medium",
            "color": "good",
        },
        {
            "type": "FactSet",
            "facts": facts,
        },
        {
            "type": "TextBlock",
            "text": "The condition that triggered this alert has cleared.",
            "wrap": True,
            "spacing": "Medium",
        },
    ]

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
                    "actions": [],
                },
            }
        ],
    }
    return card


def build_health_summary_card(
    agent_name: str,
    process_count: int,
    health_status: str,
    rows: list[dict],
) -> dict:
    """Builds a multi-cluster health summary table card -- a periodic
    routine status card (distinct from build_adaptive_card's single-
    anomaly cards), showing per-cluster data freshness, recent job
    success/failure counts, broker online status (both our own
    collector's view and a real, live TCP check), and per-broker stale
    (CLOSE_WAIT) connection counts, all at a glance. Uses a
    ColumnSet-based table layout (not the native Adaptive Card Table
    element) for reliable rendering across both desktop and mobile
    Teams clients. Extended 2026-09-29 for proactive issue detection,
    beyond the original process-count-only version.

    health_status: overall agent health, e.g. "alive" or "stale" (from
    the same scheduler-dispatch-activity signal /livez uses). Shown as
    its own colored TextBlock below the FactSet rather than as a fact,
    since FactSet values don't support per-fact color.

    rows: one dict per cluster, each with keys:
      - name: str (cluster display name)
      - data_age_minutes: float | None (None if no data ever collected)
      - success_count: int (recent window)
      - failed_count: int (recent window)
      - brokers_online_agent: str | None (e.g. "2/3", our own
        collector-freshness-based signal; None if unavailable)
      - brokers_online_real: str | None (e.g. "3/3", a real, live TCP
        reachability check; None if unavailable)
      - broker_connections: list[dict] | None (each
        {"label": "B4", "close_wait": int}; None if unavailable)
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    def _age_text(age: float | None) -> tuple[str, str]:
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

    def _broker_online_text(agent: str | None, real: str | None) -> tuple[str, str]:
        if agent is None and real is None:
            return ("n/a", "default")
        agent_disp = agent if agent is not None else "?"
        real_disp = real if real is not None else "?"
        # Mismatch between the two is itself the signal worth flagging --
        # this is precisely the ambiguity a real incident (2026-09-28)
        # showed can otherwise go unnoticed for hours.
        mismatch = agent is not None and real is not None and agent != real
        color = "attention" if mismatch else "good"
        return (f"🖥️{agent_disp} 🌐{real_disp}", color)

    legend_row = {
        "type": "TextBlock",
        "text": "🖥️ = our collector · 🌐 = live TCP check",
        "size": "Small",
        "color": "default",
        "isSubtle": True,
        "spacing": "Small",
    }

    header_row = {
        "type": "ColumnSet",
        "columns": [
            _cell("Cluster", weight="Bolder"),
            _cell("Data Age", weight="Bolder"),
            _cell("Jobs (recent)", weight="Bolder"),
            _cell("Brokers Online", weight="Bolder"),
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
        online_text, online_color = _broker_online_text(r.get("brokers_online_agent"), r.get("brokers_online_real"))
        data_rows.append({
            "type": "ColumnSet",
            "columns": [
                _cell(r.get("name", "Unknown")),
                _cell(age_text, color=age_color),
                _cell(jobs_text, color=jobs_color),
                _cell(online_text, color=online_color),
            ],
            "separator": True,
        })

        conns = r.get("broker_connections")
        if conns:
            any_stale = any(c.get("close_wait", 0) > 0 for c in conns)
            conn_text = "🔌 Stale: " + " ".join(f"{c.get('label', '?')}:{c.get('close_wait', 0)}" for c in conns)
            data_rows.append({
                "type": "ColumnSet",
                "columns": [{
                    "type": "Column",
                    "width": "stretch",
                    "items": [{
                        "type": "TextBlock",
                        "text": conn_text,
                        "wrap": True,
                        "size": "Small",
                        "color": "attention" if any_stale else "default",
                    }],
                }],
            })

    health_color = "good" if health_status == "alive" else "attention"
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
        {
            "type": "TextBlock",
            "text": f"Health Status: {health_status.upper()}",
            "weight": "Bolder",
            "size": "Small",
            "color": health_color,
            "spacing": "Small",
        },
        legend_row,
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


def health_card_has_issue(health_status: str, rows: list[dict]) -> bool:
    """True when build_health_summary_card would colour anything red
    ("attention") for these inputs -- used to send the next health card
    sooner so recovery is visible without logging in. Yellow data age
    (5 to 10 minutes) does not count. Mirrors the card's own conditions;
    keep the two in step."""
    if health_status != "alive":
        return True
    for r in rows:
        if r.get("failed_count", 0) > 0:
            return True
        age = r.get("data_age_minutes")
        if age is not None and age >= 10:
            return True
        agent = r.get("brokers_online_agent")
        real = r.get("brokers_online_real")
        if agent is not None and real is not None and agent != real:
            return True
        conns = r.get("broker_connections")
        if conns and any(c.get("close_wait", 0) > 0 for c in conns):
            return True
    return False


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
