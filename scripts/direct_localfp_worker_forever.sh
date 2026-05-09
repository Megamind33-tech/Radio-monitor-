#!/usr/bin/env bash
set -u

WORKER_INDEX="${1:-0}"
WORKER_COUNT="${2:-2}"
LIMIT_PER_LOOP="${3:-250}"
SLEEP_SECONDS="${4:-10}"

cd /opt/radio-monitor || exit 1
source /opt/radio-monitor/.venv/bin/activate

echo "=== DIRECT LOCALFP WORKER START ==="
date
echo "worker=$WORKER_INDEX/$WORKER_COUNT"
echo "limit_per_loop=$LIMIT_PER_LOOP"

while true; do
  echo
  echo "=== WORKER $WORKER_INDEX LOOP START $(date) ==="

  DIRECT_SKIP_DOMAINS=zedhousezambia.com \
  WORKER_INDEX="$WORKER_INDEX" \
  WORKER_COUNT="$WORKER_COUNT" \
  LIMIT="$LIMIT_PER_LOOP" \
  OFFSET=45 \
  SAMPLE_SECONDS=30 \
  TRUSTED_LOCALFP_SOURCE=zambian_trusted_35s \
  python -u scripts/direct_audio_sources_to_localfp.py

  echo "=== WORKER $WORKER_INDEX LOOP END $(date) ==="
  sleep "$SLEEP_SECONDS"
done
