#!/usr/bin/env bash
set -u

cd /opt/radio-monitor || exit 1
source /opt/radio-monitor/.venv/bin/activate 2>/dev/null || true

echo "=== SONG DISCOVERY FOREVER START $(date) ==="

while true; do
  echo
  echo "=== SONG DISCOVERY LOOP $(date) ==="

  ZAMBIAN_MUSIC_DOMAINS="zambianplay.com,cdn.zambianplay.com,ckmusicpromos.com,ilovezedmusic.com.zm,zedwap.co,zedhousezambia.com,zambianmusicpromos.co,mvesesani.com,africanmusicblog.com,boomplay.com,youtube.com,youtu.be" \
  MAX_PAGES_PER_DOMAIN=1000 \
  DISCOVERY_SLEEP=0.15 \
  python3 -u scripts/zambian_song_source_discovery.py

  echo
  echo "=== SOURCE COUNT SNAPSHOT $(date) ==="
  sqlite3 data/source-discovery/zambian_crawl_state.db "
  SELECT 'audio_sources', COUNT(*), COUNT(DISTINCT audio_url)
  FROM audio_sources
  WHERE audio_url IS NOT NULL AND audio_url != ''
  UNION ALL
  SELECT 'zambian_song_seeds', COUNT(*), COUNT(DISTINCT source_url)
  FROM zambian_song_seeds;
  "

  echo "=== SLEEP 180 ==="
  sleep 180
done
