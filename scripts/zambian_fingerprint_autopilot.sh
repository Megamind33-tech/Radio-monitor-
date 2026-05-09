#!/usr/bin/env bash
set -uo pipefail

BASE="/opt/radio-monitor"
DB="$BASE/data/fingerprint-index/zambian_fingerprint_index.db"
SRC="$BASE/data/source-discovery/zambian_audio_sources_alpha.csv"
EXPORT="$BASE/data/app-exports/zambian_fingerprint_index.json"
LATEST="$BASE/data/app-exports/latest_zambian_fingerprint_index.json"
LOG="$BASE/logs/zambian-fingerprint-autopilot.log"
LOCK="$BASE/data/zambian-fingerprint-autopilot.lock"
PY="$BASE/.venv/bin/python"

MAX_NEW_PAGES="${MAX_NEW_PAGES:-1500}"
CRAWL_SLEEP="${CRAWL_SLEEP:-0.35}"
FRESH_TRIGGER="${FRESH_TRIGGER:-50}"
WORKERS="${WORKERS:-2}"
BATCH_LIMIT="${BATCH_LIMIT:-500}"
MONITOR_SERVICE="${MONITOR_SERVICE:-mostify-monitor.service}"
RESTART_MONITOR_ON_EXPORT="${RESTART_MONITOR_ON_EXPORT:-1}"

mkdir -p "$BASE/logs" "$BASE/data" "$BASE/data/app-exports"

exec >> "$LOG" 2>&1
exec 9>"$LOCK"

if ! flock -n 9; then
  echo "$(date -Is) another autopilot run is already active; exiting"
  exit 0
fi

cd "$BASE" || exit 1
source "$BASE/.venv/bin/activate" || exit 1

echo ""
echo "============================================================"
echo "$(date -Is) Zambian fingerprint autopilot started"
echo "============================================================"

disk_used="$(df -P "$BASE" | awk 'NR==2 {gsub("%","",$5); print $5}')"
echo "Disk used: ${disk_used}%"

if [ "${disk_used:-0}" -ge 90 ]; then
  echo "Disk too full; skipping run."
  exit 0
fi

before_hash="$(sha256sum "$EXPORT" 2>/dev/null | awk '{print $1}')"

echo ""
echo "1. Ranking crawl frontier..."
"$PY" scripts/rank_zambian_frontier.py || true

echo ""
echo "2. Discovering audio pages..."
timeout 3600s nice -n 10 "$PY" scripts/zambian_music_url_discovery_persistent.py \
  --max-new-pages "$MAX_NEW_PAGES" \
  --sleep "$CRAWL_SLEEP" || true

fresh="$(
SRC="$SRC" DB="$DB" "$PY" - <<'PY'
import csv, sqlite3, hashlib, os

SRC=os.environ["SRC"]
DB=os.environ["DB"]

def clean(u):
    return (u or "").split("?")[0].strip()

def h(u):
    return hashlib.sha256(u.encode("utf-8")).hexdigest()

con=sqlite3.connect(DB)
indexed={r[0] for r in con.execute("SELECT audio_url_hash FROM tracks WHERE audio_url_hash IS NOT NULL")}
con.close()

seen=set()
fresh=0
total=0

if os.path.exists(SRC):
    with open(SRC, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            u=clean(r.get("audio_url"))
            if not u or u in seen:
                continue
            seen.add(u)
            total += 1
            if h(u) not in indexed:
                fresh += 1

print(fresh)
PY
)"

fresh="${fresh:-0}"
echo ""
echo "Fresh unindexed audio URLs: $fresh"

if [ "$fresh" -ge "$FRESH_TRIGGER" ]; then
  echo ""
  echo "3. Fingerprinting fresh audio..."
  WORKERS="$WORKERS" BATCH_LIMIT="$BATCH_LIMIT" timeout 7200s scripts/turbo_fingerprint_new_only.sh || true
else
  echo "Fresh count below trigger; skipping fingerprint batch."
fi

echo ""
echo "4. Cleaning metadata..."
"$PY" scripts/auto_clean_fingerprint_metadata.py || true

echo ""
echo "5. Exporting app-ready fingerprint index..."
"$PY" scripts/export_fingerprint_index_for_app.py || true

if [ -f "$EXPORT" ]; then
  ln -sfn "$EXPORT" "$LATEST"
fi

after_hash="$(sha256sum "$EXPORT" 2>/dev/null | awk '{print $1}')"

echo ""
echo "6. Track status:"
sqlite3 "$DB" "
SELECT status, COUNT(*)
FROM tracks
GROUP BY status;
" || true

echo ""
echo "7. Unknown count:"
sqlite3 "$DB" "
SELECT COUNT(*)
FROM tracks
WHERE artist='UNKNOWN' OR artist='' OR artist IS NULL;
" || true

if [ -n "$after_hash" ] && [ "$after_hash" != "$before_hash" ]; then
  echo ""
  echo "Fingerprint export changed."

  if [ "$RESTART_MONITOR_ON_EXPORT" = "1" ]; then
    if systemctl list-unit-files | grep -q "^${MONITOR_SERVICE}"; then
      echo "Reloading/restarting monitor service: $MONITOR_SERVICE"
      systemctl try-reload-or-restart "$MONITOR_SERVICE" || true
    else
      echo "Monitor service not found: $MONITOR_SERVICE"
    fi
  fi
else
  echo "Fingerprint export unchanged."
fi

echo ""
echo "$(date -Is) Zambian fingerprint autopilot finished"
