#!/usr/bin/env python3
import csv, os, shutil, sqlite3, hashlib
from pathlib import Path

BASE="/opt/radio-monitor"
DB=f"{BASE}/data/fingerprint-index/zambian_fingerprint_index.db"
CSV_PATHS=[
    f"{BASE}/data/source-discovery/zambian_audio_sources_alpha.csv",
    f"{BASE}/data/authorized_audio_sources.csv",
]

def fix_url(u):
    u=(u or "").strip()
    changed=True
    while changed:
        old=u
        u=u.replace("https://https://","https://")
        u=u.replace("http://https://","https://")
        u=u.replace("https://http://","http://")
        u=u.replace("http://http://","http://")
        changed=(old!=u)
    return u

def h(u):
    return hashlib.sha256((u or "").split("?")[0].strip().encode("utf-8")).hexdigest()

fixed_csv_fields=0

for path in CSV_PATHS:
    p=Path(path)
    if not p.exists():
        continue

    shutil.copy2(path, f"{path}.bak.normalize")
    with open(path, newline="", encoding="utf-8") as f:
        rows=list(csv.DictReader(f))

    if not rows:
        continue

    fields=list(rows[0].keys())

    for r in rows:
        for col in ("audio_url","source_page"):
            if col in r:
                old=r.get(col,"")
                new=fix_url(old)
                if new != old:
                    r[col]=new
                    fixed_csv_fields += 1

    with open(path, "w", newline="", encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

fixed_db=0
deleted_dupes=0

if os.path.exists(DB):
    con=sqlite3.connect(DB)
    con.row_factory=sqlite3.Row

    bad=con.execute("""
    SELECT id, audio_url
    FROM tracks
    WHERE audio_url LIKE '%https://https://%'
       OR audio_url LIKE '%http://https://%'
       OR audio_url LIKE '%https://http://%'
       OR audio_url LIKE '%http://http://%'
    """).fetchall()

    for r in bad:
        old=r["audio_url"] or ""
        new=fix_url(old)
        new_hash=h(new)

        existing=con.execute("""
        SELECT id FROM tracks
        WHERE audio_url_hash=?
          AND id != ?
        LIMIT 1
        """, (new_hash, r["id"])).fetchone()

        if existing:
            con.execute("DELETE FROM tracks WHERE id=?", (r["id"],))
            deleted_dupes += 1
        else:
            con.execute("""
            UPDATE tracks
            SET audio_url=?,
                audio_url_hash=?,
                error=NULL,
                status=CASE WHEN status='failed' THEN 'pending_retry' ELSE status END
            WHERE id=?
            """, (new, new_hash, r["id"]))
            fixed_db += 1

    con.commit()
    con.close()

print("CSV fields normalized:", fixed_csv_fields)
print("DB malformed URLs fixed:", fixed_db)
print("DB duplicate bad rows deleted:", deleted_dupes)
