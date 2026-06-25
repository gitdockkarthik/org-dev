#!/usr/bin/env bash
# Quick smoke test — POSTs a sample message with inline alert data and prints the response.
# Usage: ./test_invoke.sh [HOST]
# Default HOST: http://localhost:8001

set -euo pipefail

HOST="${1:-http://localhost:8001}"

# Sample alert data: 5 alerts with a mix of noise and genuine signals
ALERTS='[
  {
    "id": "A1B2C3",
    "message": "High CPU utilization on payment-service",
    "alias": "cpu-high-payment-service",
    "status": "closed",
    "acknowledged": false,
    "source": "payment-service",
    "priority": "P3",
    "teams": ["backend"],
    "tags": ["payment-service", "p3", "prod"],
    "createdAt": "2024-01-15T10:00:00Z",
    "updatedAt": "2024-01-15T10:02:00Z",
    "count": 5,
    "integration": {"name": "Datadog"},
    "report": {"closeTime": 120, "acknowledgedBy": ""}
  },
  {
    "id": "D4E5F6",
    "message": "High CPU utilization on payment-service",
    "alias": "cpu-high-payment-service",
    "status": "closed",
    "acknowledged": false,
    "source": "payment-service",
    "priority": "P3",
    "teams": ["backend"],
    "tags": ["payment-service", "p3", "prod"],
    "createdAt": "2024-01-15T10:15:00Z",
    "updatedAt": "2024-01-15T10:17:00Z",
    "count": 4,
    "integration": {"name": "Datadog"},
    "report": {"closeTime": 130, "acknowledgedBy": ""}
  },
  {
    "id": "G7H8I9",
    "message": "High CPU utilization on payment-service",
    "alias": "cpu-high-payment-service",
    "status": "closed",
    "acknowledged": false,
    "source": "payment-service",
    "priority": "P3",
    "teams": ["backend"],
    "tags": ["payment-service", "p3", "prod"],
    "createdAt": "2024-01-15T10:30:00Z",
    "updatedAt": "2024-01-15T10:32:00Z",
    "count": 4,
    "integration": {"name": "Datadog"},
    "report": {"closeTime": 125, "acknowledgedBy": ""}
  },
  {
    "id": "J1K2L3",
    "message": "Database primary down — connection refused",
    "alias": "db-primary-down",
    "status": "acknowledged",
    "acknowledged": true,
    "source": "db-primary",
    "priority": "P1",
    "teams": ["platform"],
    "tags": ["db-primary", "p1", "prod"],
    "createdAt": "2024-01-15T09:00:00Z",
    "updatedAt": "2024-01-15T09:45:00Z",
    "count": 1,
    "integration": {"name": "PagerDuty"},
    "report": {"closeTime": 2700, "acknowledgedBy": "john.doe"}
  },
  {
    "id": "M4N5O6",
    "message": "SSL certificate expiring for api-gateway",
    "alias": "ssl-expire-api-gateway",
    "status": "open",
    "acknowledged": false,
    "source": "api-gateway",
    "priority": "P2",
    "teams": ["infra"],
    "tags": ["api-gateway", "p2", "prod"],
    "createdAt": "2024-01-15T08:00:00Z",
    "updatedAt": "2024-01-15T08:00:00Z",
    "count": 1,
    "integration": {"name": "CloudWatch"},
    "report": {"closeTime": 9999, "acknowledgedBy": ""}
  }
]'

PAYLOAD=$(jq -n \
  --arg sid "test-session-$(date +%s)" \
  --argjson alerts "$ALERTS" \
  '{
    session_id: $sid,
    user_message: "Classify these alerts and give me a summary of noise vs genuine, then recommend what to suppress.",
    context: {alerts: $alerts},
    history: []
  }')

echo "=== POST ${HOST}/invoke ==="
echo ""

curl -s -X POST "${HOST}/invoke" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD" | jq .
