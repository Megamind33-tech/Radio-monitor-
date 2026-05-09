#!/usr/bin/env python3
import argparse
import html
import json
import re
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from uuid import uuid4

DB_PATH = Path("/opt/radio-monitor/prisma/dev_runtime.db")
STATION_ID = "zm_st_3e74ef3af66aa6d5"
STATION_NAME = "Radio Mano 89.3FM"
SOURCE_URL = "https://onlineradiobox.com/zm/mano893/?cs=zm.qzambia&played=1"
SOURCE_PROVIDER = "onlineradiobox_mano893"
LOCAL_TZ = ZoneInfo("Africa/Lusaka")

BAD_TEXT = (
    "radio mano",
    "playlist",
    "top songs",
    "reviews",
    "contacts",
    "listen live",
    "onlineradiobox",
    "application",
    "install",
    "cookie",
    "privacy",
)

def fetch_html(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 Chrome/122 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urllib.request.urlopen(req, timeout=25) as res:
        return res.read().decode("utf-8", errors="replace")

def strip_tags(raw: str) -> str:
    raw = re.sub(r"(?is)<script.*?</script>", " ", raw)
    raw = re.sub(r"(?is)<style.*?</style>", " ", raw)
    raw = re.sub(r"(?is)<[^>]+>", " ", raw)
    raw = html.unescape(raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw

def clean_track_text(text: str) -> str:
    text = html.unescape(text or "")
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"^\s*live\s+", "", text, flags=re.I).strip()
    return text

def split_artist_title(text: str):
    text = clean_track_text(text)

    if not text:
        return None, None

    low = text.lower()
    if any(bad in low for bad in BAD_TEXT):
        return None, None

    # Common ORB formats:
    # Artist - Title
    # Artist — Title
    # <b>Artist</b> — Title
    for sep in [" — ", " - ", " – "]:
        if sep in text:
            artist, title = text.split(sep, 1)
            artist = artist.strip(" -—–")
            title = title.strip(" -—–")
            if title:
                return artist or None, title

    return None, text

def parse_orb_tracks(raw_html: str):
    # Narrow to the On the air section to avoid Top Songs pollution.
    lower = raw_html.lower()
    start = lower.find("on the air radio mano")
    if start < 0:
        start = lower.find("track_history_item")

    end_candidates = []
    for marker in ["top songs on radio mano", "radio mano 89.3fm reviews", "station_contacts"]:
        idx = lower.find(marker, start + 1)
        if idx > start:
            end_candidates.append(idx)

    end = min(end_candidates) if end_candidates else min(len(raw_html), start + 20000)
    block = raw_html[start:end] if start >= 0 else raw_html

    tracks = []

    # Parse table rows first.
    rows = re.findall(r"(?is)<tr[^>]*>(.*?)</tr>", block)
    for row in rows:
        text = strip_tags(row)
        if not text:
            continue

        m = re.search(r"\b(\d{1,2}:\d{2})\b\s*(.+)", text)
        if m:
            time_part = m.group(1)
            track_text = clean_track_text(m.group(2))
        else:
            time_part = None
            track_text = clean_track_text(text)

        artist, title = split_artist_title(track_text)
        if not title:
            continue

        tracks.append({
            "time": time_part,
            "artist": artist,
            "title": title,
            "raw": track_text,
        })

    # Fallback: parse anchors around track_history_item.
    if not tracks:
        for m in re.finditer(r'(?is)<td[^>]*class="[^"]*track_history_item[^"]*"[^>]*>(.*?)</td>', block):
            cell = m.group(1)
            text = clean_track_text(strip_tags(cell))
            artist, title = split_artist_title(text)
            if title:
                tracks.append({
                    "time": None,
                    "artist": artist,
                    "title": title,
                    "raw": text,
                })

    # Deduplicate while preserving order.
    seen = set()
    deduped = []
    for t in tracks:
        key = (t.get("time"), (t.get("artist") or "").lower(), (t.get("title") or "").lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(t)

    return deduped[:50]

def track_time_to_utc(time_part: str):
    if not time_part:
        return datetime.now(timezone.utc)

    now_local = datetime.now(LOCAL_TZ)
    hour, minute = map(int, time_part.split(":"))
    candidate = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)

    # If page time appears ahead of now by more than 2 hours, assume yesterday.
    if candidate - now_local > timedelta(hours=2):
        candidate -= timedelta(days=1)

    return candidate.astimezone(timezone.utc)

def iso(dt: datetime):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def upsert_current(conn, track):
    now = iso(datetime.now(timezone.utc))
    stream_text = track["raw"]
    artist = track.get("artist")
    title = track.get("title")

    conn.execute("""
        INSERT INTO CurrentNowPlaying
        (stationId, title, artist, sourceProvider, streamText, updatedAt)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(stationId) DO UPDATE SET
            title=excluded.title,
            artist=excluded.artist,
            sourceProvider=excluded.sourceProvider,
            streamText=excluded.streamText,
            updatedAt=excluded.updatedAt
    """, (STATION_ID, title, artist, SOURCE_PROVIDER, stream_text, now))

def insert_detection_if_new(conn, track):
    observed_at = iso(track_time_to_utc(track.get("time")))
    now = iso(datetime.now(timezone.utc))
    raw = track["raw"]
    artist = track.get("artist")
    title = track.get("title")

    recent = conn.execute("""
        SELECT id, rawStreamText, titleFinal, artistFinal, observedAt
        FROM DetectionLog
        WHERE stationId = ?
        ORDER BY observedAt DESC
        LIMIT 1
    """, (STATION_ID,)).fetchone()

    if recent:
        old_raw = (recent["rawStreamText"] or "").strip().lower()
        old_title = (recent["titleFinal"] or "").strip().lower()
        old_artist = (recent["artistFinal"] or "").strip().lower()
        if old_raw == raw.lower() or (old_title == (title or "").lower() and old_artist == (artist or "").lower()):
            return False

    conn.execute("""
        INSERT INTO DetectionLog
        (id, stationId, observedAt, detectionMethod, rawStreamText,
         parsedArtist, parsedTitle, confidence, titleFinal, artistFinal,
         sourceProvider, sampleSeconds, processingMs, status, reasonCode,
         matchDiagnosticsJson, manuallyTagged)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "orb_" + uuid4().hex,
        STATION_ID,
        observed_at,
        "external_playlist",
        raw,
        artist,
        title,
        0.96,
        title,
        artist,
        SOURCE_PROVIDER,
        0,
        0,
        "matched",
        "onlineradiobox_playlist",
        json.dumps({"sourceUrl": SOURCE_URL, "syncedAt": now, "time": track.get("time")}),
        0,
    ))

    return True

def backfill_unknowns(conn, tracks, window_minutes=20, limit=100):
    rows = conn.execute("""
        SELECT id, observedAt, status, titleFinal, artistFinal, rawStreamText
        FROM DetectionLog
        WHERE stationId = ?
          AND (
            status != 'matched'
            OR titleFinal IS NULL
            OR trim(titleFinal) = ''
            OR lower(coalesce(titleFinal,'')) LIKE '%unknown%'
            OR lower(coalesce(artistFinal,'')) LIKE '%unknown%'
          )
        ORDER BY observedAt DESC
        LIMIT ?
    """, (STATION_ID, limit)).fetchall()

    updates = []

    for row in rows:
        try:
            row_dt = datetime.fromisoformat(row["observedAt"].replace("Z", "+00:00"))
        except Exception:
            continue

        best = None
        best_diff = None

        for track in tracks:
            t_dt = track_time_to_utc(track.get("time"))
            diff = abs((row_dt - t_dt).total_seconds()) / 60
            if best_diff is None or diff < best_diff:
                best = track
                best_diff = diff

        if best and best_diff is not None and best_diff <= window_minutes:
            updates.append((row["id"], best, best_diff))

    for row_id, track, diff in updates:
        conn.execute("""
            UPDATE DetectionLog
            SET detectionMethod = ?,
                rawStreamText = ?,
                parsedArtist = ?,
                parsedTitle = ?,
                confidence = ?,
                titleFinal = ?,
                artistFinal = ?,
                sourceProvider = ?,
                status = ?,
                reasonCode = ?,
                matchDiagnosticsJson = ?
            WHERE id = ?
        """, (
            "external_playlist_backfill",
            track["raw"],
            track.get("artist"),
            track.get("title"),
            0.94,
            track.get("title"),
            track.get("artist"),
            SOURCE_PROVIDER,
            "matched",
            "onlineradiobox_backfill",
            json.dumps({"sourceUrl": SOURCE_URL, "time": track.get("time"), "minuteDiff": diff}),
            row_id,
        ))

    return updates

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--print", action="store_true")
    parser.add_argument("--sync-current", action="store_true")
    parser.add_argument("--backfill", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--window-minutes", type=int, default=20)
    args = parser.parse_args()

    raw = fetch_html(SOURCE_URL)
    tracks = parse_orb_tracks(raw)

    if args.print:
        print(json.dumps({
            "ok": True,
            "stationId": STATION_ID,
            "stationName": STATION_NAME,
            "sourceUrl": SOURCE_URL,
            "count": len(tracks),
            "tracks": tracks[:20],
        }, indent=2, ensure_ascii=False))

    if not tracks:
        print(json.dumps({"ok": False, "error": "no_tracks_found", "sourceUrl": SOURCE_URL}, indent=2))
        sys.exit(2)

    if args.sync_current or args.backfill:
        conn = connect_db()
        try:
            if args.sync_current:
                upsert_current(conn, tracks[0])
                inserted = insert_detection_if_new(conn, tracks[0])
                print(json.dumps({"sync_current": True, "current": tracks[0], "insertedDetection": inserted}, indent=2, ensure_ascii=False))

            if args.backfill:
                updates = backfill_unknowns(conn, tracks, args.window_minutes, args.limit)
                print(json.dumps({
                    "backfill": True,
                    "dryRun": args.dry_run,
                    "candidateUpdates": [
                        {"id": row_id, "track": track, "minuteDiff": round(diff, 2)}
                        for row_id, track, diff in updates[:30]
                    ],
                    "count": len(updates),
                }, indent=2, ensure_ascii=False))

            if args.dry_run:
                conn.rollback()
            else:
                conn.commit()
        finally:
            conn.close()

if __name__ == "__main__":
    main()
