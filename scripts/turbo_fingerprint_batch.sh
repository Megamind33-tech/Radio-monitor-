#!/usr/bin/env bash
set -euo pipefail

BASE="/opt/radio-monitor"
cd "$BASE"
source "$BASE/.venv/bin/activate"

mkdir -p data/turbo-batches logs

FULL="data/authorized_audio_sources.csv"

if [ ! -f "$FULL" ]; then
  echo "Missing $FULL"
  exit 1
fi

echo "Creating turbo batches from $FULL..."

# Keep header, split body into 2 worker files
HEADER="$(head -1 "$FULL")"
tail -n +2 "$FULL" > data/turbo-batches/all_body.csv

split -n l/2 -d --additional-suffix=.csv data/turbo-batches/all_body.csv data/turbo-batches/worker_

for f in data/turbo-batches/worker_*.csv; do
  tmp="${f}.tmp"
  echo "$HEADER" > "$tmp"
  cat "$f" >> "$tmp"
  mv "$tmp" "$f"
done

echo "Starting 2 parallel fingerprint workers..."

i=0
for f in data/turbo-batches/worker_*.csv; do
  i=$((i+1))
  (
    cp "$FULL" "$FULL.bak.turbo_worker_$i"
    cp "$f" "$FULL"
    python scripts/fingerprint_only_indexer_fast.py
  ) > "logs/turbo-worker-$i.log" 2>&1 &
done

wait

echo "Turbo workers finished."

# Restore main input if backup exists
if [ -f "$FULL.bak.turbo_worker_1" ]; then
  cp "$FULL.bak.turbo_worker_1" "$FULL"
fi

python scripts/auto_clean_fingerprint_metadata.py || true
python scripts/export_fingerprint_index_for_app.py || true

sqlite3 data/fingerprint-index/zambian_fingerprint_index.db "
SELECT status, COUNT(*)
FROM tracks
GROUP BY status;
"
