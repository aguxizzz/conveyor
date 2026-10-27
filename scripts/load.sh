#!/usr/bin/env bash
# Enqueues a mix of demo jobs so the Grafana dashboard has something to show.
# Usage: scripts/load.sh [jobs_per_second=30] [seconds=120]
# The two compose replicas together run ~50 of these jobs/s; go above that to watch the backlog grow.
set -euo pipefail

rate=${1:-30}
duration=${2:-120}
ports=(8080 8081)

enqueue() {
  local port=${ports[$((RANDOM % ${#ports[@]}))]} body
  if (( RANDOM % 4 == 0 )); then
    body='{"type":"demo.flaky","payload":{"failRate":0.5},"maxAttempts":3}'
  else
    body="{\"type\":\"demo.sleep\",\"payload\":{\"ms\":$((50 + RANDOM % 500))}}"
  fi
  curl -sf -o /dev/null -X POST "localhost:$port/api/jobs" -H 'Content-Type: application/json' -d "$body" \
    || echo "enqueue failed on :$port" >&2
}

echo "Enqueuing $rate jobs/s for ${duration}s. Dashboard: http://localhost:3000"
for ((s = 0; s < duration; s++)); do
  for ((i = 0; i < rate; i++)); do enqueue & done
  wait
  sleep 1
done
