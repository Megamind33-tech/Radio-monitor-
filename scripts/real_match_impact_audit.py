#!/usr/bin/env python3
import sqlite3, re, json
from pathlib import Path

APP_DB = Path("prisma/dev_runtime.db")
STATE_DB = Path("data/fingerprint-index/direct_audio_to_localfp_state.db")

def con(path):
    db = sqlite3.connect(str(path))
    db.row_factory = sqlite3.Row
    return db

def tables(db):
    return [r["name"] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]

def cols(db, table):
    return [r["name"] for r in db.execute(f'PRAGMA table_info("{table}")')]

def pick(columns, choices):
    low = {c.lower(): c for c in columns}
    for x in choices:
        if x.lower() in low:
            return low[x.lower()]
    return None

def count(db, sql, args=()):
    try:
        return db.execute(sql, args).fetchone()[0]
    except Exception as e:
        return "ERR: " + str(e)[:120]

app = con(APP_DB)
all_tables = tables(app)

print("=== REAL TABLES THAT LOOK MATCH / RADIO / UNKNOWN RELATED ===")
interesting = []
for t in all_tables:
    c = cols(app, t)
    joined = (t + " " + " ".join(c)).lower()
    if any(k in joined for k in [
        "detect", "match", "unknown", "unresolved", "sample", "fingerprint",
        "station", "spin", "play", "review", "recogn", "metadata"
    ]):
        interesting.append((t, c))
        print()
        print("TABLE:", t, "| rows:", count(app, f'SELECT COUNT(*) FROM "{t}"'))
        print("COLUMNS:", ", ".join(c[:80]))

print("\n=== LOCALFINGERPRINT COUNT ===")
if "LocalFingerprint" in all_tables:
    c = cols(app, "LocalFingerprint")
    source_col = pick(c, ["source"])
    artist_col = pick(c, ["artist", "displayArtist"])
    title_col = pick(c, ["title", "titleWithoutFeat"])
    if source_col:
        for r in app.execute(f'''
            SELECT "{source_col}" AS source, COUNT(*) AS segments
            FROM LocalFingerprint
            GROUP BY "{source_col}"
            ORDER BY segments DESC
        '''):
            print(dict(r))

    if source_col and artist_col and title_col:
        for r in app.execute(f'''
            SELECT "{source_col}" AS source,
                   COUNT(*) AS segments,
                   COUNT(DISTINCT LOWER(TRIM(COALESCE("{artist_col}",''))) || ' - ' || LOWER(TRIM(COALESCE("{title_col}",'')))) AS unique_songs
            FROM LocalFingerprint
            WHERE "{source_col}"='zambian_trusted_35s'
            GROUP BY "{source_col}"
        '''):
            print("zambian_unique:", dict(r))

    print("\nlatest LocalFingerprint rows:")
    order_col = "rowid"
    for r in app.execute(f'''
        SELECT *
        FROM LocalFingerprint
        WHERE {f'"{source_col}"' if source_col else '1'} = {'?' if source_col else '1'}
        ORDER BY rowid DESC
        LIMIT 10
    ''', ("zambian_trusted_35s",) if source_col else ()):
        d = dict(r)
        small = {k: d.get(k) for k in d.keys() if k.lower() in ["source","artist","displayartist","title","titlewithoutfeat","duration","fingerprintsha1","status","metadata_source","metadatasource"]}
        print(small)

print("\n=== DIRECT IMPORT STATE ===")
if STATE_DB.exists():
    st = con(STATE_DB)
    for r in st.execute("SELECT status, COUNT(*) AS count FROM direct_imports GROUP BY status ORDER BY count DESC"):
        print(dict(r))
    print("\nlatest direct imports:")
    for r in st.execute("SELECT * FROM direct_imports ORDER BY rowid DESC LIMIT 10"):
        d = dict(r)
        print({k: d.get(k) for k in d.keys() if k in ["artist","title","status","duration","fingerprint_sha1","updated_at","error"]})
    st.close()
else:
    print("No direct import state DB")

print("\n=== STATUS COUNTS IN REAL TABLES ===")
for t, c in interesting:
    status_col = pick(c, ["status", "recoveryStatus", "reviewStatus", "matchStatus", "state"])
    source_col = pick(c, ["source", "sourceProvider", "matchSource", "provider", "fingerprintSource"])
    station_col = pick(c, ["stationId", "station_id", "stationName", "station"])
    artist_col = pick(c, ["artist", "artistNorm", "parsedArtist", "matchedArtist", "displayArtist"])
    title_col = pick(c, ["title", "titleNorm", "parsedTitle", "matchedTitle", "trackTitle"])

    print("\nTABLE:", t)
    if status_col:
        print("status column:", status_col)
        try:
            for r in app.execute(f'SELECT "{status_col}" AS status, COUNT(*) AS count FROM "{t}" GROUP BY "{status_col}" ORDER BY count DESC LIMIT 20'):
                print(" ", dict(r))
        except Exception as e:
            print(" status error:", str(e)[:120])

    if source_col:
        print("source column:", source_col)
        try:
            for r in app.execute(f'SELECT "{source_col}" AS source, COUNT(*) AS count FROM "{t}" GROUP BY "{source_col}" ORDER BY count DESC LIMIT 20'):
                print(" ", dict(r))
        except Exception as e:
            print(" source error:", str(e)[:120])

    if artist_col and title_col:
        print("sample artist/title rows:")
        try:
            for r in app.execute(f'''
                SELECT "{artist_col}" AS artist, "{title_col}" AS title
                FROM "{t}"
                WHERE "{artist_col}" IS NOT NULL OR "{title_col}" IS NOT NULL
                ORDER BY rowid DESC LIMIT 5
            '''):
                print(" ", dict(r))
        except Exception as e:
            print(" sample error:", str(e)[:120])

print("\n=== LATEST DIRECT IMPORTS EXACTLY PRESENT IN REAL TABLES ===")
latest = []
if STATE_DB.exists():
    st = con(STATE_DB)
    try:
        for r in st.execute("""
            SELECT LOWER(TRIM(COALESCE(artist,''))) AS artist,
                   LOWER(TRIM(COALESCE(title,''))) AS title
            FROM direct_imports
            WHERE status='imported'
            ORDER BY rowid DESC
            LIMIT 1000
        """):
            if r["artist"] and r["title"]:
                latest.append((r["artist"], r["title"]))
    except Exception as e:
        print("could not load direct imports:", e)
    st.close()

latest_set = set(latest)
print("latest direct pairs checked:", len(latest_set))

for t, c in interesting:
    artist_col = pick(c, ["artist", "artistNorm", "parsedArtist", "matchedArtist", "displayArtist"])
    title_col = pick(c, ["title", "titleNorm", "parsedTitle", "matchedTitle", "trackTitle"])
    if not artist_col or not title_col:
        continue
    hits = 0
    checked = 0
    try:
        for r in app.execute(f'''
            SELECT "{artist_col}" AS artist, "{title_col}" AS title
            FROM "{t}"
            WHERE "{artist_col}" IS NOT NULL OR "{title_col}" IS NOT NULL
            ORDER BY rowid DESC
            LIMIT 10000
        '''):
            checked += 1
            pair = ((r["artist"] or "").strip().lower(), (r["title"] or "").strip().lower())
            if pair in latest_set:
                hits += 1
        if checked:
            print({"table": t, "checked": checked, "exact_latest_hits": hits})
    except Exception as e:
        print({"table": t, "error": str(e)[:120]})

print("\n=== NEXT DECISION ===")
print("If latest hits are mostly only in LocalFingerprint, then catalogue insertion works but rematch has not been run.")
print("If UnknownAudioReview/UnresolvedSample has many needs_audio_fingerprint rows, run a controlled rematch next.")
print("If exact hits appear in match/review tables, catalogue growth is already improving matches.")

app.close()
