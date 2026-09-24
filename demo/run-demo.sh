#!/usr/bin/env bash
# Offline end-to-end demo: every scenario through detect -> explain (mock) -> dispatch.
# Add BEDROCK=1 to use Amazon Bedrock (profile demos-admin) for the explain step.
set -euo pipefail
cd "$(dirname "$0")/.."
B=./bin/northfen
FLAGS=(-data .northfen -quiet)
[[ "${BEDROCK:-}" == 1 ]] && FLAGS+=(-bedrock)
$B "${FLAGS[@]}" reset >/dev/null
$B scenarios
for s in 01 02 03 04 05 06 07 08 09 10 11; do
  echo; echo "================================================================"
  $B "${FLAGS[@]}" simulate "$s"
done
echo; echo "================================================================"; echo "Anomaly feed:"
$B "${FLAGS[@]}" feed
