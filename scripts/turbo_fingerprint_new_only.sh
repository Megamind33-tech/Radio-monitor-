#!/usr/bin/env bash
set -euo pipefail

BASE="/opt/radio-monitor"
cd "$BASE"
source "$BASE/.venv/bin/activate"

WORKERS="${WORKERS:-2}"
BATCH_LIMIT="${BATCH_LIMIT:-40}"

SRC="$BASE/data/source-discovery/zambian_audio_sources_alpha.csv"
DB="$BASE/data/fingerprint-index/zambian_fingerprint_index.db"
OUT="$BASE/data/turbo-batches"
LOG="$BASE/logs"

mkdir -p "$OUT" "$LOG"

echo "Preparing turbo NEW-only batch..."
echo "WORKERS=$WORKERS BATCH_LIMIT=$BATCH_LIMIT"
echo "Source: $SRC"

python - <<'PY'
import csv, os, sqlite3, hashlib
from pathlib import Path

BASE="/opt/radio-monitor"
SRC=f"{BASE}/data/source-discovery/zambian_audio_sources_alpha.csv"
DB=f"{BASE}/data/fingerprint-index/zambian_fingerprint_index.db"
OUT=Path(f"{BASE}/data/turbo-batches")

WORKERS=int(os.environ.get("WORKERS","2"))
BATCH_LIMIT=int(os.environ.get("BATCH_LIMIT","40"))

OUT.mkdir(parents=True, exist_ok=True)

def clean_url(u):
    return (u or "").split("?")[0].strip()

def h(u):
    return hashlib.sha256(u.encode("utf-8")).hexdigest()

indexed=set()

if os.path.exists(DB):
    con=sqlite3.connect(DB, timeout=60)
    try:
        for (x,) in con.execute("SELECT audio_url_hash FROM tracks WHERE audio_url_hash IS NOT NULL"):
            indexed.add(x)
    except Exception:
        for (u,) in con.execute("SELECT audio_url FROM tracks WHERE audio_url IS NOT NULL"):
            indexed.add(h(clean_url(u)))
    con.close()

seen=set()
fresh=[]

if not os.path.exists(SRC):
    raise SystemExit(f"Missing source CSV: {SRC}")

with open(SRC, newline="", encoding="utf-8") as f:
    reader=csv.DictReader(f)
    for row in reader:
        audio=clean_url(row.get("audio_url"))
        if not audio:
            continue

        uh=h(audio)
        if audio in seen:
            continue
        seen.add(audio)

        if uh in indexed:
            continue

        fresh.append({
            "source_page": row.get("source_page",""),
            "audio_url": audio,
            "title": row.get("title",""),
            "artist": row.get("artist",""),
            "album": row.get("album",""),
        })

        if len(fresh) >= BATCH_LIMIT:
            break

for old in OUT.glob("worker_new_*.csv"):
    old.unlink()

fields=["source_page","audio_url","title","artist","album"]

for i in range(WORKERS):
    path=OUT / f"worker_new_{i+1}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for idx, row in enumerate(fresh):
            if idx % WORKERS == i:
                w.writerow(row)

print(f"Fresh unindexed URLs prepared: {len(fresh)}")
for i in range(WORKERS):
    path=OUT / f"worker_new_{i+1}.csv"
    count=max(0, sum(1 for _ in open(path, encoding="utf-8")) - 1)
    print(f"{path}: {count} rows")
PY

echo ""
echo "Starting turbo workers..."

pids=()
for f in "$OUT"/worker_new_*.csv; do
  name="$(basename "$f" .csv)"
  echo "Starting $name using $f"
  INPUT="$f" nice -n 10 python scripts/fingerprint_only_indexer_fast.py > "$LOG/${name}.log" 2>&1 &
  pids+=("$!")
done

for pid in "${pids[@]}"; do
  wait "$pid" || true
done

echo ""
echo "Turbo workers finished."

echo ""
echo "Cleaning metadata and exporting..."
python scripts/auto_clean_fingerprint_metadata.py || true
python scripts/export_fingerprint_index_for_app.py || true

echo ""
echo "TRACK STATUS:"
sqlite3 "$DB" "
SELECT status, COUNT(*)
FROM tracks
GROUP BY status;
"

echo ""
echo "UNKNOWN COUNT:"
sqlite3 "$DB" "
SELECT COUNT(*)
FROM tracks
WHERE artist='UNKNOWN' OR artist='' OR artist IS NULL;
"
