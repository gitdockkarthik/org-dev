#!/usr/bin/env bash
# Smoke test — generates synthetic CUR data inline and POSTs a cost question.
# Usage: ./test_invoke.sh [HOST]
# Default HOST: http://localhost:8002

set -euo pipefail

HOST="${1:-http://localhost:8002}"

# Minimal CUR CSV: 10 rows across 3 services, 2 regions, 3 days
CUR_CSV="line_item_product_code,line_item_unblended_cost,line_item_usage_start_date,line_item_usage_account_id,product_region
Amazon EC2,210.500000,2024-01-01,123456789012,us-east-1
Amazon EC2,198.750000,2024-01-02,123456789012,us-east-1
Amazon EC2,225.000000,2024-01-03,123456789012,us-west-2
Amazon RDS,95.200000,2024-01-01,123456789012,us-east-1
Amazon RDS,102.400000,2024-01-02,123456789012,us-east-1
Amazon S3,4.320000,2024-01-01,123456789012,us-east-1
Amazon S3,3.810000,2024-01-02,123456789012,eu-west-1
AWS Lambda,1.050000,2024-01-01,123456789012,us-east-1
AWS Lambda,0.920000,2024-01-02,123456789012,us-east-1
Amazon CloudFront,12.600000,2024-01-03,123456789012,us-east-1"

PAYLOAD=$(jq -n \
  --arg sid "cur-test-$(date +%s)" \
  --arg csv "$CUR_CSV" \
  '{
    session_id: $sid,
    user_message: "What is my total AWS spend? Which service costs the most? Give me a daily trend breakdown and suggest where I can save money.",
    context: {cur_csv: $csv},
    history: []
  }')

echo "=== POST ${HOST}/invoke ==="
echo ""

curl -s -X POST "${HOST}/invoke" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD" | jq .
