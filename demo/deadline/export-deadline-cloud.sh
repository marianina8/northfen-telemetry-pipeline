#!/usr/bin/env bash
# Export render history from AWS Deadline Cloud in the format `northfen ingest`
# reads. Read-only: it only calls get/list APIs. Run it on your own machine;
# the file stays with you.
#
#   demo/deadline/export-deadline-cloud.sh FARM_ID QUEUE_ID JOB_ID [JOB_ID ...] > export.json
#   bin/northfen ingest -pool FARM-LGT                          export.json   # list hosts
#   bin/northfen ingest -pool FARM-LGT -host render-node07=node07_frame_time export.json
#
# Needs the AWS CLI (v2, with the `deadline` commands) and jq. Uses your
# default AWS profile/region unless AWS_PROFILE / AWS_REGION are set. Calls:
# GetJob, ListSessions, GetWorker, ListSessionActions (permissions:
# deadline:GetJob, deadline:ListSessions, deadline:GetWorker,
# deadline:ListSessionActions).
#
# NOTE: written against the Deadline Cloud API reference; not yet run against
# a live farm. The bundled demo/deadline/lgt-overnight-export.json is a
# synthetic sample in this format, not a real export.
set -euo pipefail
FARM=${1:?usage: $0 FARM_ID QUEUE_ID JOB_ID [JOB_ID ...]}
QUEUE=${2:?queue id}
shift 2
[ $# -ge 1 ] || { echo "at least one JOB_ID" >&2; exit 2; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 2; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
echo '[]' >"$tmp/jobs.json"
echo '[]' >"$tmp/sessions.json"
echo '{}' >"$tmp/actions.json"

for JOB in "$@"; do
  echo "job $JOB" >&2
  aws deadline get-job --farm-id "$FARM" --queue-id "$QUEUE" --job-id "$JOB" \
    | jq '{jobId, name}' >"$tmp/job.json"
  jq --slurpfile j "$tmp/job.json" '. + $j' "$tmp/jobs.json" >"$tmp/x" && mv "$tmp/x" "$tmp/jobs.json"

  aws deadline list-sessions --farm-id "$FARM" --queue-id "$QUEUE" --job-id "$JOB" \
    | jq --arg job "$JOB" '[.sessions[] | . + {jobId: $job}]' >"$tmp/s.json"
  jq -s 'add' "$tmp/sessions.json" "$tmp/s.json" >"$tmp/x" && mv "$tmp/x" "$tmp/sessions.json"

  for SID in $(jq -r '.[].sessionId' "$tmp/s.json"); do
    aws deadline list-session-actions --farm-id "$FARM" --queue-id "$QUEUE" --job-id "$JOB" --session-id "$SID" \
      | jq --arg sid "$SID" '{($sid): .sessionActions}' >"$tmp/a.json"
    jq -s 'add' "$tmp/actions.json" "$tmp/a.json" >"$tmp/x" && mv "$tmp/x" "$tmp/actions.json"
  done
done

echo '[]' >"$tmp/workers.json"
jq -r '[.[] | "\(.fleetId) \(.workerId)"] | unique[]' "$tmp/sessions.json" | while read -r FLEET WID; do
  aws deadline get-worker --farm-id "$FARM" --fleet-id "$FLEET" --worker-id "$WID" \
    | jq '{workerId, fleetId, status, hostProperties: {hostName: .hostProperties.hostName}}' >"$tmp/w.json"
  jq --slurpfile w "$tmp/w.json" '. + $w' "$tmp/workers.json" >"$tmp/x" && mv "$tmp/x" "$tmp/workers.json"
done

jq -n \
  --arg farm "$FARM" --arg queue "$QUEUE" --arg now "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --slurpfile jobs "$tmp/jobs.json" --slurpfile workers "$tmp/workers.json" \
  --slurpfile sessions "$tmp/sessions.json" --slurpfile actions "$tmp/actions.json" \
  '{format: "northfen.deadline-cloud-export/v1", exportedAt: $now, farmId: $farm, queueId: $queue,
    jobs: $jobs[0], workers: $workers[0], sessions: $sessions[0], sessionActions: $actions[0]}'
