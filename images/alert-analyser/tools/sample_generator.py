"""Synthetic OpsGenie-like alert generator for demo and testing."""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone


_NOISY_ALIASES = [
    "cpu-high", "memory-pressure", "disk-space-warning",
    "heartbeat-failure", "ssl-cert-expiry", "synthetic-check-timeout",
    "swap-usage-high", "load-average-spike",
]
_GENUINE_ALIASES = [
    "database-connection-refused", "payment-service-down",
    "api-gateway-5xx-spike", "kafka-consumer-lag",
    "redis-cluster-failover", "deployment-rollback-triggered",
    "slo-breach-detected", "security-scan-critical",
]
_SOURCES = [
    "aws-cloudwatch", "datadog", "prometheus",
    "grafana", "pingdom", "opsgenie-integration",
]
_TEAMS = [
    ["platform"], ["backend"], ["frontend"],
    ["data"], ["infra"], ["sre"],
]
_PRIORITIES = ["P1", "P2", "P3", "P4", "P5"]


def generate_synthetic_alerts(n: int = 200) -> list[dict]:
    """Return *n* synthetic OpsGenie-format alert dicts with realistic noise patterns."""
    base_time = datetime.now(timezone.utc) - timedelta(days=7)
    alerts: list[dict] = []

    for _ in range(n):
        is_noisy = random.random() < 0.65

        if is_noisy:
            alias = random.choice(_NOISY_ALIASES)
            priority = random.choices(_PRIORITIES, weights=[1, 3, 20, 40, 36])[0]
            close_time = random.randint(10, 250)
            ack = False
        else:
            alias = random.choice(_GENUINE_ALIASES)
            priority = random.choices(_PRIORITIES, weights=[10, 25, 35, 25, 5])[0]
            close_time = random.randint(400, 7200)
            ack = random.random() < 0.7

        source = random.choice(_SOURCES)
        team = random.choice(_TEAMS)
        offset = random.uniform(0, 7 * 24 * 3600)
        created_at = (
            (base_time + timedelta(seconds=offset))
            .isoformat()
            .replace("+00:00", "Z")
        )
        status = "closed" if close_time < 300 else random.choice(["open", "resolved", "closed"])

        alerts.append({
            "alias": alias,
            "message": f"{alias.replace('-', ' ').title()} on {source}",
            "source": source,
            "priority": priority,
            "status": status,
            "acknowledged": ack,
            "createdAt": created_at,
            "report": {"closeTime": close_time},
            "teams": team,
        })

    return alerts
