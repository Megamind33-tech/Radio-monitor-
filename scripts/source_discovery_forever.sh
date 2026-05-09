#!/usr/bin/env bash
set -u

cd /opt/radio-monitor || exit 1
source /opt/radio-monitor/.venv/bin/activate 2>/dev/null || true

echo "=== SOURCE DISCOVERY FOREVER START ==="
date

while true; do
  echo
  echo "=== BOOTSTRAP FRONTIER $(date) ==="
  python3 scripts/bootstrap_zambian_frontier.py || true

  echo
  echo "=== RANK FRONTIER $(date) ==="
  python3 scripts/rank_zambian_frontier.py || true

  echo
  echo "=== DISCOVER AUDIO URLS $(date) ==="
  MAX_NEW_PAGES=500 \
  AUDIO_LIMIT=3000 \
  CRAWL_SLEEP=0.5 \
  python3 scripts/zambian_music_url_discovery_persistent.py --max-pages 500 2>&1 || true

  echo
  echo "=== SOURCE DB SNAPSHOT $(date) ==="
  sqlite3 data/source-discovery/zambian_crawl_state.db "
  SELECT 
    COUNT(*) AS rows,
    COUNT(DISTINCT audio_url) AS unique_audio_urls
  FROM audio_sources
  WHERE audio_url IS NOT NULL AND audio_url != '';
  " || true

  echo
  echo "=== SLEEP 60 THEN CONTINUE ==="
  sleep 60
done
