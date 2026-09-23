#!/usr/bin/env bash
# Push a few raw readings straight onto the Kinesis stream through the webhook.
# Usage: demo/webhook-example.sh <WebhookUrl> <token>
set -euo pipefail
URL=${1:?WebhookUrl output}; TOKEN=${2:?X-Northfen-Token secret}
now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
curl -sS -X POST "$URL/readings" -H "X-Northfen-Token: $TOKEN" -H 'Content-Type: application/json' -d "[
 {\"session_id\":\"webhook-raw\",\"run_id\":\"manual\",\"equipment_id\":\"ETCH-12\",\"tool_type\":\"plasma_etch\",\"sensor_id\":\"esc_temp\",\"sensor_type\":\"temperature\",\"unit\":\"C\",\"tick\":0,\"ts\":\"$now\",\"value\":60.1},
 {\"session_id\":\"webhook-raw\",\"run_id\":\"manual\",\"equipment_id\":\"ETCH-12\",\"tool_type\":\"plasma_etch\",\"sensor_id\":\"esc_temp\",\"sensor_type\":\"temperature\",\"unit\":\"C\",\"tick\":1,\"ts\":\"$now\",\"value\":null}
]"
echo
