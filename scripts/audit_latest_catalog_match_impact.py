#!/usr/bin/env python3
import sqlite3, re
from pathlib import Path
from datetime import datetime, timedelta

APP_DB = Path("prisma/dev_runtime.db")
STATE_DB = Path("data/fingerprint-index/direct_audio_to_localfp_state.db")

def connect(p):
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    return conn

def tables(conn):
    return [r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]

def cols(conn, table):
    return [r["name"] for r in conn.execute(f'PRAGMA table_info("{table}")')]

def pick(cols, names):
    low = {c.lower(): c for c in cols}
    for n in names:
        if n.lower() in low:
            return low[n.lower()]
    return None

def safe_count(conn, sql, args=()):
    try:
        return conn.execute(sql, args).fetchone()[0]
    except Exception as e:
        return f"ERR: {str(e)[:120]}"

app = connect(APP_DB)

print("=== LOCALFINGERPRINT SIZE ===")
try:
    for r in app.execute("""
        SELECT source,
               COUNT(*) AS segments,
               COUNT(DISTINCT LOWER(TRIM(COALESCE(artist,''))) || ' - ' || LOWER(TRIM(COALESCE(title,'')))) AS unique_songs
        FROM LocalFingerprint
        WHERE source='zambian_trusted_35s'
        GROUP BY source
    """):
        print(dict(r))
except Exception as e:
    print("LocalFingerprint count error:", e)

print("\n=== DIRECT IMPORT STATE ===")
if STATE_DB.exists():
    st = connect(STATE_DB)
    try:
        for r in st.execute("SELECT status, COUNT(*) AS count FROM direct_imports GROUP BY status"):
            print(dict(r))
        print("\nlatest_direct_imports:")
        for r in st.execute("""
            SELECT artist, title, status, imported_at, updated_at
            FROM direct_imports
            ORDER BY updated_at DESC
            LIMIT 15
        """):
            print(dict(r))
    except Exception as e:
        print("direct_import state error:", e)
    st.close()
else:
    print("No direct import state DB found")

print("\n=== POSSIBLE RADIO MATCH / LOG TABLES ===")
keywords = re.compile(r"(match|recogn|detect|fingerprint|play|spin|log|sample|unresolved|monitor|air)", re.I)

candidate_tables = []
for t in tables(app):
    if t.startswith("_"):
        continue
    c = cols(app, t)
    joined = " ".join([t] + c)
    if keywords.search(joined):
        candidate_tables.append((t, c))
        total = safe_count(app, f'SELECT COUNT(*) FROM "{t}"')
        print(f"\nTABLE: {t} | rows={total}")
        useful = [x for x in c if re.search(r"(artist|title|track|song|status|source|provider|confidence|score|created|updated|matched|local|fingerprint|station)", x, re.I)]
        print("columns:", ", ".join(useful[:40]) if useful else ", ".join(c[:30]))

print("\n=== RECENT MATCH-LIKE COUNTS ===")
for t, c in candidate_tables:
    status_col = pick(c, ["status", "matchStatus", "recognitionStatus"])
    source_col = pick(c, ["source", "provider", "matchSource", "recognitionSource"])
    created_col = pick(c, ["createdAt", "created_at", "updatedAt", "updated_at", "timestamp", "playedAt", "detectedAt"])

    print(f"\n{t}:")
    if status_col:
        try:
            print(" by_status:")
            for r in app.execute(f'SELECT "{status_col}" AS status, COUNT(*) AS count FROM "{t}" GROUP BY "{status_col}" ORDER BY count DESC LIMIT 20'):
                print(" ", dict(r))
        except Exception as e:
            print(" status_count_error:", str(e)[:100])

    if source_col:
        try:
            print(" by_source:")
            for r in app.execute(f'SELECT "{source_col}" AS source, COUNT(*) AS count FROM "{t}" GROUP BY "{source_col}" ORDER BY count DESC LIMIT 20'):
                print(" ", dict(r))
        except Exception as e:
            print(" source_count_error:", str(e)[:100])

    if created_col:
        try:
            since = (datetime.utcnow() - timedelta(hours=24)).isoformat(timespec="seconds")
            recent = safe_count(app, f'SELECT COUNT(*) FROM "{t}" WHERE "{created_col}" >= ?', (since,))
            print(" recent_24h_rows:", recent)
        except Exception as e:
            print(" recent_error:", str(e)[:100])

print("\n=== EXACT ARTIST/TITLE OVERLAP: LATEST DIRECT IMPORTS VS TABLES ===")
if STATE_DB.exists():
    st = connect(STATE_DB)
    latest = []
    try:
        for r in st.execute("""
            SELECT LOWER(TRIM(COALESCE(artist,''))) AS artist,
                   LOWER(TRIM(COALESCE(title,''))) AS title
            FROM direct_imports
            WHERE status='imported'
              AND artist IS NOT NULL AND title IS NOT NULL
              AND artist != '' AND title != ''
            ORDER BY updated_at DESC
            LIMIT 1000
        """):
            latest.append((r["artist"], r["title"]))
    except Exception:
        latest = []

    print("latest_import_pairs_checked:", len(latest))

    if latest:
        latest_set = set(latest)
        for t, c in candidate_tables:
            artist_col = pick(c, ["artist", "matchedArtist", "trackArtist", "songArtist", "displayArtist"])
            title_col = pick(c, ["title", "matchedTitle", "trackTitle", "songTitle"])
            if not artist_col or not title_col:
                continue

            hits = 0
            checked = 0
            try:
                for r in app.execute(f'''
                    SELECT "{artist_col}" AS artist, "{title_col}" AS title
                    FROM "{t}"
                    WHERE "{artist_col}" IS NOT NULL
                      AND "{title_col}" IS NOT NULL
                    ORDER BY rowid DESC
                    LIMIT 5000
                '''):
                    checked += 1
                    pair = ((r["artist"] or "").strip().lower(), (r["title"] or "").strip().lower())
                    if pair in latest_set:
                        hits += 1
                if checked:
                    print({"table": t, "checked_recent_rows": checked, "latest_catalog_exact_hits": hits})
            except Exception as e:
                print({"table": t, "error": str(e)[:120]})

    st.close()

app.close()
