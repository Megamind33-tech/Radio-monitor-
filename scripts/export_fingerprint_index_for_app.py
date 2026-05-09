#!/usr/bin/env python3
import json, os, sqlite3
from datetime import datetime, timezone

BASE="/opt/radio-monitor"
DB=f"{BASE}/data/fingerprint-index/zambian_fingerprint_index.db"
OUT_DIR=f"{BASE}/data/app-exports"
OUT=f"{OUT_DIR}/zambian_fingerprint_index.json"

os.makedirs(OUT_DIR, exist_ok=True)

con=sqlite3.connect(DB)
con.row_factory=sqlite3.Row

rows=con.execute("""
SELECT
  id,
  artist,
  title,
  album,
  duration,
  audio_url,
  source_page,
  status,
  indexed_at,
  metadata_status,
  metadata_confidence,
  metadata_source,
  metadata_notes
FROM tracks
WHERE status='indexed'
ORDER BY id DESC
""").fetchall()

data={
  "exported_at": datetime.now(timezone.utc).isoformat(),
  "count": len(rows),
  "tracks": [dict(r) for r in rows]
}

with open(OUT, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

con.close()

print(f"Exported {len(rows)} tracks to {OUT}")
