#!/usr/bin/env bash
set -euo pipefail

BASE="/opt/radio-monitor"
LOCK_FILE="$BASE/data/pipeline-state/zambian-fingerprint-pipeline.lock"
FINGERPRINT_DB="$BASE/data/fingerprint-index/zambian_fingerprint_index.db"
SRC_AUDIO="$BASE/data/source-discovery/zambian_audio_sources_alpha.csv"
DST_AUDIO="$BASE/data/authorized_audio_sources.csv"

MAX_NEW_PAGES="${MAX_NEW_PAGES:-100}"
AUDIO_LIMIT="${AUDIO_LIMIT:-100}"
CRAWL_SLEEP="${CRAWL_SLEEP:-1.0}"

cd "$BASE"
source "$BASE/.venv/bin/activate"

mkdir -p "$BASE/logs" "$BASE/data/pipeline-state" "$BASE/data/app-exports" "$BASE/data/fingerprint-index" "$BASE/data/tmp-fp"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "$(date -Is) Pipeline already running. Exiting."
  exit 0
fi

echo "=================================================="
echo "$(date -Is) Zambian fingerprint pipeline started"
echo "MAX_NEW_PAGES=$MAX_NEW_PAGES AUDIO_LIMIT=$AUDIO_LIMIT CRAWL_SLEEP=$CRAWL_SLEEP"
echo "=================================================="

echo ""
echo "1. Ranking crawl frontier..."
if [ -f scripts/rank_zambian_frontier.py ]; then
  python scripts/rank_zambian_frontier.py || true
else
  echo "Ranker missing: scripts/rank_zambian_frontier.py"
fi

echo ""
echo "2. Scraping controlled small batch..."
if [ -f scripts/zambian_music_url_discovery_persistent.py ]; then
  timeout 45m nice -n 15 python scripts/zambian_music_url_discovery_persistent.py --max-new-pages "$MAX_NEW_PAGES" --sleep "$CRAWL_SLEEP" || true
else
  echo "Crawler missing: scripts/zambian_music_url_discovery_persistent.py"
fi

echo ""
echo "3. Preparing LIMITED new audio URL input..."
python - <<'PY_AUDIO'
import csv, os, sqlite3, hashlib

BASE = "/opt/radio-monitor"
SRC = f"{BASE}/data/source-discovery/zambian_audio_sources_alpha.csv"
DST = f"{BASE}/data/authorized_audio_sources.csv"
DB  = f"{BASE}/data/fingerprint-index/zambian_fingerprint_index.db"
LIMIT = int(os.environ.get("AUDIO_LIMIT", "20"))

os.makedirs(os.path.dirname(DST), exist_ok=True)

def clean_url(u):
    return (u or "").split("?")[0].strip()

def url_hash(u):
    return hashlib.sha256(u.encode("utf-8")).hexdigest()

indexed = set()

if os.path.exists(DB):
    con = sqlite3.connect(DB)
    try:
        for (h,) in con.execute("SELECT audio_url_hash FROM tracks WHERE audio_url_hash IS NOT NULL"):
            indexed.add(h)
    except Exception:
        for (u,) in con.execute("SELECT audio_url FROM tracks WHERE audio_url IS NOT NULL"):
            indexed.add(url_hash(clean_url(u)))
    con.close()

seen = set()
written = 0
skipped_existing = 0
skipped_duplicate = 0

with open(DST, "w", newline="", encoding="utf-8") as f_out:
    writer = csv.DictWriter(f_out, fieldnames=["source_page", "audio_url", "title", "artist", "album"])
    writer.writeheader()

    if not os.path.exists(SRC):
        print(f"Missing source CSV: {SRC}")
    else:
        with open(SRC, newline="", encoding="utf-8") as f_in:
            reader = csv.DictReader(f_in)

            for row in reader:
                audio_url = clean_url(row.get("audio_url"))
                if not audio_url:
                    continue

                if audio_url in seen:
                    skipped_duplicate += 1
                    continue
                seen.add(audio_url)

                h = url_hash(audio_url)
                if h in indexed:
                    skipped_existing += 1
                    continue

                writer.writerow({
                    "source_page": row.get("source_page", ""),
                    "audio_url": audio_url,
                    "title": row.get("title", ""),
                    "artist": row.get("artist", ""),
                    "album": "",
                })

                written += 1
                if written >= LIMIT:
                    break

print(f"New audio URLs prepared this run: {written}")
print(f"Already indexed skipped: {skipped_existing}")
print(f"Duplicate URLs skipped: {skipped_duplicate}")
print(f"Input CSV: {DST}")
PY_AUDIO

echo ""
echo "4. Fingerprinting only prepared new tracks..."
if [ -f scripts/fingerprint_only_indexer_fast.py ]; then
  timeout 45m nice -n 15 python scripts/fingerprint_only_indexer_fast.py || true
else
  echo "Indexer missing: scripts/fingerprint_only_indexer_fast.py"
fi

echo ""
echo "5. Auto-cleaning metadata..."
if [ -f scripts/auto_clean_fingerprint_metadata.py ]; then
  python scripts/auto_clean_fingerprint_metadata.py || true
else
  echo "Metadata cleaner missing: scripts/auto_clean_fingerprint_metadata.py"
fi

echo ""
echo "6. Exporting app-ready data..."
if [ -f scripts/export_fingerprint_index_for_app.py ]; then
  python scripts/export_fingerprint_index_for_app.py || true
else
  echo "Export script missing, skipping app export."
fi

echo ""
echo "7. Health check..."

echo "TRACK STATUS:"
sqlite3 "$FINGERPRINT_DB" "
SELECT status, COUNT(*)
FROM tracks
GROUP BY status;
" || true

echo ""
echo "UNKNOWN COUNT:"
sqlite3 "$FINGERPRINT_DB" "
SELECT COUNT(*)
FROM tracks
WHERE artist='UNKNOWN' OR artist='' OR artist IS NULL;
" || true

echo ""
echo "DUPLICATE AUDIO URLS:"
sqlite3 "$FINGERPRINT_DB" "
SELECT audio_url, COUNT(*)
FROM tracks
GROUP BY audio_url
HAVING COUNT(*) > 1;
" || true

echo ""
echo "TEMP AUDIO:"
find "$BASE/data/tmp-fp" -type f 2>/dev/null || true
du -sh "$BASE/data/tmp-fp" 2>/dev/null || true

echo ""

echo ""
echo "7b. Cleaning leftover temporary audio..."
rm -rf "$BASE/data/tmp-fp"/*
mkdir -p "$BASE/data/tmp-fp"

echo "8. Backing up fingerprint DB..."
if [ -f "$FINGERPRINT_DB" ]; then
  cp "$FINGERPRINT_DB" "$BASE/data/fingerprint-index/zambian_fingerprint_index.db.bak.pipeline_$(date +%Y%m%d_%H%M%S)"
  ls -1t "$BASE"/data/fingerprint-index/zambian_fingerprint_index.db.bak.pipeline_* 2>/dev/null | tail -n +11 | xargs -r rm -f
fi

echo ""
echo "$(date -Is) Pipeline finished"
