#!/usr/bin/env python3
import datetime as dt
import html
import json
import re
import sqlite3
import urllib.request
from zoneinfo import ZoneInfo

URL = "https://onlineradiobox.com/zm/chikuni/"
DB = "/opt/radio-monitor/prisma/dev_runtime.db"
STATION_SLUG = "chikuni"
STATION_NAME = "CHIKUNI RADIO"
SOURCE = "external_playlist_onlineradiobox"

def clean(x):
    x = html.unescape(str(x or ""))
    x = re.sub(r"<[^>]+>", " ", x)
    x = x.replace("\xa0", " ")
    return re.sub(r"\s+", " ", x).strip()

def fetch_page():
    req = urllib.request.Request(URL, headers={"User-Agent": "Mozilla/5.0 Chrome/122 Safari/537.36"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", errors="ignore")

def html_lines(page):
    page = re.sub(r"(?is)<script.*?</script>", " ", page)
    page = re.sub(r"(?is)<style.*?</style>", " ", page)
    page = re.sub(r"(?i)<br\s*/?>", "\n", page)
    page = re.sub(r"(?i)</(div|li|tr|td|p|span|section|h1|h2|h3)>", "\n", page)
    page = re.sub(r"<[^>]+>", " ", page)
    page = html.unescape(page).replace("\xa0", " ")
    return [clean(x) for x in page.splitlines() if clean(x)]

def split_track(raw):
    raw = clean(raw)
    for sep in [" - ", " – ", " — ", " | "]:
        if sep in raw:
            artist, title = raw.split(sep, 1)
            return clean(artist), clean(title)
    return None, raw

def is_junk(raw, artist, title):
    s = clean(raw).lower()
    t = clean(title).lower()
    if t in ["", "-", "–", "_", "unknown", "n/a", "na", "none"]:
        return True
    if "jingle" in s or "advert" in s or "slogan" in s:
        return True
    if s == "chikuni radio" or t == "chikuni radio":
        return True
    return False

def parse_rows():
    lines = html_lines(fetch_page())
    start = None

    for i, line in enumerate(lines):
        if line.lower().startswith("on the air"):
            start = i
            break

    if start is None:
        return []

    rows = []
    now = dt.datetime.now(ZoneInfo("Africa/Lusaka"))
    i = start + 1

    while i < len(lines):
        line = lines[i]

        if "playlist" in line.lower() or line.lower().startswith("top songs") or line.lower().startswith("reviews"):
            break

        if re.match(r"^(Live|\d{1,2}:\d{2})$", line, re.I):
            label = line

            if i + 1 < len(lines):
                raw = clean(lines[i + 1])
                artist, title = split_track(raw)

                if title and not is_junk(raw, artist, title):
                    if label.lower() == "live":
                        observed = now
                    else:
                        hh, mm = [int(x) for x in label.split(":")]
                        observed = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
                        if observed > now + dt.timedelta(minutes=10):
                            observed -= dt.timedelta(days=1)

                    rows.append({
                        "stationSlug": STATION_SLUG,
                        "stationName": STATION_NAME,
                        "observedAt": observed.isoformat(),
                        "artist": artist,
                        "title": title,
                        "rawText": raw,
                        "sourceProvider": SOURCE,
                        "sourceUrl": URL
                    })

            i += 2
            continue

        i += 1

    return rows

def save_rows(rows):
    conn = sqlite3.connect(DB, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS ExternalPlaylistBackfill (
      id TEXT PRIMARY KEY,
      stationSlug TEXT NOT NULL,
      stationName TEXT NOT NULL,
      observedAt TEXT NOT NULL,
      artist TEXT,
      title TEXT NOT NULL,
      rawText TEXT NOT NULL,
      sourceProvider TEXT NOT NULL,
      sourceUrl TEXT NOT NULL,
      insertedAt TEXT NOT NULL,
      applied INTEGER NOT NULL DEFAULT 0,
      appliedAt TEXT,
      confidence REAL NOT NULL DEFAULT 0.62,
      UNIQUE(stationSlug, observedAt, title, artist)
    )
    """)

    inserted = 0
    inserted_at = dt.datetime.utcnow().isoformat() + "Z"

    for r in rows:
        rid = f"{r['stationSlug']}:{r['observedAt']}:{r.get('artist') or ''}:{r['title']}"
        try:
            conn.execute("""
            INSERT INTO ExternalPlaylistBackfill
            (id, stationSlug, stationName, observedAt, artist, title, rawText, sourceProvider, sourceUrl, insertedAt, applied, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0.62)
            """, (
                rid, r["stationSlug"], r["stationName"], r["observedAt"],
                r.get("artist"), r["title"], r["rawText"], r["sourceProvider"],
                r["sourceUrl"], inserted_at
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()
    return inserted

rows = parse_rows()
inserted = save_rows(rows)
print(json.dumps({"found": len(rows), "inserted": inserted, "tracks": rows}, indent=2, ensure_ascii=False))
