#!/usr/bin/env python3
import sqlite3, sys

DB = "prisma/dev_runtime.db"
q = (sys.argv[1] if len(sys.argv) > 1 else "home").lower()

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA busy_timeout=30000;")

stations = conn.execute("""
SELECT id, name
FROM Station
WHERE lower(name) LIKE ?
   OR lower(name) LIKE ?
   OR lower(id) LIKE ?
   OR lower(id) LIKE ?
ORDER BY name
""", (f"%{q}%", "%hone%", f"%{q}%", "%hone%")).fetchall()

print("MATCHING_STATIONS:")
for s in stations:
    print(dict(s))

if len(stations) != 1:
    print("")
    print("STOPPED: I found zero or multiple station matches. Run again with the exact station id:")
    print("python3 scripts/fix_home_fm_radiobox_nowplaying.py STATION_ID_HERE")
    sys.exit(1)

station_id = stations[0]["id"]
print("")
print("USING_STATION:", station_id, stations[0]["name"])

current = conn.execute("""
SELECT *
FROM CurrentNowPlaying
WHERE stationId=?
""", (station_id,)).fetchone()

print("")
print("CURRENT_NOW_PLAYING_BEFORE:")
print(dict(current) if current else "NO_CURRENT_ROW")

orb = conn.execute("""
SELECT id, observedAt,
       COALESCE(artistFinal, parsedArtist, '') AS artist,
       COALESCE(titleFinal, parsedTitle, rawStreamText, '') AS title,
       sourceProvider, detectionMethod, reasonCode, status
FROM DetectionLog
WHERE stationId=?
  AND sourceProvider='onlineradiobox.com'
  AND trim(COALESCE(titleFinal, parsedTitle, rawStreamText, '')) != ''
ORDER BY observedAt DESC
LIMIT 1
""", (station_id,)).fetchone()

if not orb:
    print("")
    print("STOPPED: No recent onlineradiobox.com DetectionLog found for this station.")
    print("Run the RadioBox poller first:")
    print("node scripts/orb_track_poller.mjs")
    sys.exit(1)

print("")
print("LATEST_RADIOBOX_DETECTION:")
print(dict(orb))

cols = [r["name"] for r in conn.execute("PRAGMA table_info(CurrentNowPlaying)").fetchall()]

updates = []
params = []

def add(col, val):
    if col in cols:
        updates.append(f"{col}=?")
        params.append(val)

add("artist", orb["artist"])
add("title", orb["title"])
add("sourceProvider", "onlineradiobox.com")
add("detectionMethod", orb["detectionMethod"] or "onlineradiobox")
add("reasonCode", "forced_radiobox_priority_home_fm_loop_fix")
add("detectionLogId", orb["id"])
add("updatedAt", None)

# Handle updatedAt separately because CURRENT_TIMESTAMP should not be bound as text.
updates = [u for u in updates if u != "updatedAt=?"]
if "updatedAt" in cols:
    updates.append("updatedAt=CURRENT_TIMESTAMP")

if not current:
    print("")
    print("STOPPED: CurrentNowPlaying row does not exist. Not inserting blindly because schema may require extra fields.")
    sys.exit(1)

params.append(station_id)
sql = f"UPDATE CurrentNowPlaying SET {', '.join(updates)} WHERE stationId=?"
conn.execute(sql, params)
conn.commit()

after = conn.execute("SELECT * FROM CurrentNowPlaying WHERE stationId=?", (station_id,)).fetchone()

print("")
print("CURRENT_NOW_PLAYING_AFTER:")
print(dict(after))

print("")
print("DONE: Forced Home/Hone FM current track from latest RadioBox detection only.")
print("NOTE: This did not touch Station, DetectionLog history, spins, or play counts.")

conn.close()
