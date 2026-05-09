#!/usr/bin/env python3
import argparse
import html
import json
import re
import sqlite3
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from uuid import uuid4

DB_PATH = Path("/opt/radio-monitor/prisma/dev_runtime.db")
STATION_ID = "zm_st_bdf23f2d85054cf7"
STATION_IDS = ["zm_st_bdf23f2d85054cf7", "zm_mt_497523", "zm_rg_GDYjn3eD"]
STATION_NAME = "Laka FM Group"
BASE_URL = "https://onlineradiobox.com"
PLAYLIST_URL = "https://onlineradiobox.com/zm/laka/?played=1"
SOURCE_PROVIDER = "onlineradiobox_laka_group"
LAKA_SOURCE_URL = "https://onlineradiobox.com/zm/laka/?played=1"
LOCAL_TZ = ZoneInfo("Africa/Lusaka")

BAD_TEXT = (
    "install the free",
    "online radio box",
    "application",
    "recommended",
    "radio mano",
    "playlist",
    "reviews",
    "contacts",
    "listen live",
    "top songs",
    "cookie",
    "privacy",
    "authorization",
    "server connection",
)

def fetch(url):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 Chrome/122 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as res:
        return res.read().decode("utf-8", errors="replace")

def strip_tags(raw):
    raw = re.sub(r"(?is)<script.*?</script>", " ", raw)
    raw = re.sub(r"(?is)<style.*?</style>", " ", raw)
    raw = re.sub(r"(?is)<[^>]+>", "\n", raw)
    raw = html.unescape(raw)
    lines = []
    for line in raw.splitlines():
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            lines.append(line)
    return lines

def clean_track(text):
    text = html.unescape(text or "")
    text = re.sub(r"\s+", " ", text).strip()
    return text

def split_artist_title(text):
    text = clean_track(text)
    if not text:
        return None, None

    low = text.lower()
    if any(bad in low for bad in BAD_TEXT):
        return None, None

    for sep in [" — ", " - ", " – "]:
        if sep in text:
            artist, title = text.split(sep, 1)
            artist = artist.strip(" -—–")
            title = title.strip(" -—–")
            if title:
                return artist or None, title

    return None, text

def infer_date_from_label(label):
    """
    OnlineRadioBox date labels often look like:
    Sat 11.10, Sun 12.10, Fri 17.10
    We infer year near current local date.
    """
    m = re.search(r"(\d{1,2})\.(\d{1,2})", label or "")
    if not m:
        return None

    day = int(m.group(1))
    month = int(m.group(2))
    now = datetime.now(LOCAL_TZ)

    candidates = []
    for year in [now.year - 1, now.year, now.year + 1]:
        try:
            dt = datetime(year, month, day, tzinfo=LOCAL_TZ)
            candidates.append(dt)
        except ValueError:
            pass

    if not candidates:
        return None

    # Choose the date closest to now, but avoid dates too far in future.
    candidates.sort(key=lambda d: abs((now - d).total_seconds()))
    chosen = candidates[0]
    if chosen - now > timedelta(days=2):
        chosen = chosen.replace(year=chosen.year - 1)
    return chosen.date()

def extract_day_links(raw):
    links = []

    for m in re.finditer(r'(?is)<a[^>]+href="([^"]*?/zm/mano893/playlist/[^"]*)"[^>]*>(.*?)</a>', raw):
        href = html.unescape(m.group(1))
        label = " ".join(strip_tags(m.group(2)))
        url = urllib.parse.urljoin(BASE_URL, href)
        links.append({"url": url, "label": label})

    # Include base page as current visible day.
    links.append({"url": PLAYLIST_URL, "label": "current"})

    # Deduplicate.
    out = []
    seen = set()
    for link in links:
        if link["url"] in seen:
            continue
        seen.add(link["url"])
        out.append(link)

    return out[:10]

def parse_tracks_from_page(raw, page_label):
    lines = strip_tags(raw)

    # Try to infer selected page date from label, otherwise page text.
    page_date = infer_date_from_label(page_label)

    if not page_date:
        for line in lines:
            d = infer_date_from_label(line)
            if d:
                page_date = d

    tracks = []

    for line in lines:
        m = re.match(r"^(\d{1,2}:\d{2})\s+(.+)$", line)
        if not m:
            continue

        time_part = m.group(1)
        raw_track = clean_track(m.group(2))
        artist, title = split_artist_title(raw_track)

        if not title:
            continue

        # If date is unknown, use today. This is safer for current page;
        # for older pages, labels usually provide date.
        if page_date is None:
            page_date = datetime.now(LOCAL_TZ).date()

        hour, minute = map(int, time_part.split(":"))
        local_dt = datetime(
            page_date.year,
            page_date.month,
            page_date.day,
            hour,
            minute,
            tzinfo=LOCAL_TZ,
        )

        tracks.append({
            "observedAt": local_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "localDate": str(page_date),
            "time": time_part,
            "artist": artist,
            "title": title,
            "raw": raw_track,
            "pageLabel": page_label,
        })

    # Deduplicate.
    seen = set()
    out = []
    for t in tracks:
        key = (t["observedAt"], (t["raw"] or "").lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(t)

    return out

def get_weekly_tracks():
    # Reuse the already-working main-page parser from mano_orb_sync.py.
    # The /playlist/ page is not exposing weekly rows to this server, but
    # ?played=1 reliably exposes current + recent played rows.
    import importlib.util

    working_parser_path = "/opt/radio-monitor/scripts/external-playlists/mano_orb_sync.py"
    spec = importlib.util.spec_from_file_location("mano_orb_sync", working_parser_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    raw = mod.fetch_html(LAKA_SOURCE_URL)
    recent_raw = mod.parse_orb_tracks(raw)

    # Keep all timed played-history rows, but only the first untimed live/current row.
    # Extra untimed rows can be Top Songs/chart/list pollution and must not affect logs.
    first_live = None
    timed_recent = []

    for item in recent_raw:
        if item.get("time"):
            timed_recent.append(item)
        elif first_live is None:
            first_live = item

    recent = ([first_live] if first_live else []) + timed_recent

    tracks = []
    page_date = datetime.now(LOCAL_TZ).date()

    for t in recent:
        time_part = t.get("time")

        if time_part:
            hour, minute = map(int, time_part.split(":"))
            local_dt = datetime(
                page_date.year,
                page_date.month,
                page_date.day,
                hour,
                minute,
                tzinfo=LOCAL_TZ,
            )

            now_local = datetime.now(LOCAL_TZ)
            if local_dt - now_local > timedelta(hours=2):
                local_dt -= timedelta(days=1)

            observed_at = int(local_dt.astimezone(timezone.utc).timestamp() * 1000)
            local_date = str(local_dt.date())
            page_label = "main_played"
        else:
            observed_at = int(datetime.now(timezone.utc).timestamp() * 1000)
            local_date = str(page_date)
            page_label = "main_played_live"

        tracks.append({
            "observedAt": observed_at,
            "localDate": local_date,
            "time": time_part,
            "artist": t.get("artist"),
            "title": t.get("title"),
            "raw": t.get("raw"),
            "pageLabel": page_label,
        })

    seen = set()
    out = []
    for t in tracks:
        key = ((t.get("time") or ""), (t.get("raw") or "").lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(t)

    pages = [{"url": LAKA_SOURCE_URL, "label": "laka_main_played_working_parser", "count": len(out)}]
    return pages, out

def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def already_exists(conn, track):
    # For live/current rows, observedAt changes every run. Avoid inserting
    # the same live title repeatedly within 30 minutes.
    if not track.get("time"):
        cutoff = int(track["observedAt"]) - (30 * 60 * 1000)
        row = conn.execute("""
            SELECT id FROM DetectionLog
            WHERE stationId = ?
              AND lower(coalesce(rawStreamText,'')) = lower(?)
              AND CAST(observedAt AS INTEGER) >= ?
            LIMIT 1
        """, (STATION_ID, track["raw"], cutoff)).fetchone()
        return row is not None

    row = conn.execute("""
        SELECT id FROM DetectionLog
        WHERE stationId = ?
          AND observedAt = ?
          AND lower(coalesce(rawStreamText,'')) = lower(?)
        LIMIT 1
    """, (STATION_ID, track["observedAt"], track["raw"])).fetchone()
    return row is not None

def insert_track(conn, track):
    # Do not insert untimed live/current rows into DetectionLog.
    # They change timestamp every run and should only update CurrentNowPlaying.
    if not track.get("time"):
        return False

    if already_exists(conn, track):
        return False

    conn.execute("""
        INSERT INTO DetectionLog
        (id, stationId, observedAt, detectionMethod, rawStreamText,
         parsedArtist, parsedTitle, confidence, titleFinal, artistFinal,
         sourceProvider, sampleSeconds, processingMs, status, reasonCode,
         matchDiagnosticsJson, manuallyTagged)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "orbw_" + uuid4().hex,
        STATION_ID,
        track["observedAt"],
        "external_playlist_weekly",
        track["raw"],
        track["artist"],
        track["title"],
        0.96,
        track["title"],
        track["artist"],
        SOURCE_PROVIDER,
        0,
        0,
        "matched",
        "onlineradiobox_weekly_playlist",
        json.dumps({
            "sourceUrl": PLAYLIST_URL,
            "pageLabel": track.get("pageLabel"),
            "localDate": track.get("localDate"),
            "time": track.get("time"),
        }),
        0,
    ))

    return True

def backfill_unknowns(conn, tracks, window_minutes=45, limit=500):
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
            OR lower(coalesce(rawStreamText,'')) LIKE '%unknown%'
          )
        ORDER BY observedAt DESC
        LIMIT ?
    """, (STATION_ID, limit)).fetchall()

    updates = []

    parsed_tracks = []
    for track in tracks:
        # Only timed played-history rows are allowed for backfill.
        # Untimed live/current rows can be stale/program/talk and must not rewrite logs.
        if not track.get("time"):
            continue
        try:
            dt = datetime.fromtimestamp(int(track["observedAt"]) / 1000, tz=timezone.utc)
            parsed_tracks.append((dt, track))
        except Exception:
            pass

    for row in rows:
        try:
            row_dt = datetime.fromtimestamp(int(row["observedAt"]) / 1000, tz=timezone.utc)
        except Exception:
            continue

        best = None
        best_diff = None

        for track_dt, track in parsed_tracks:
            diff = abs((row_dt - track_dt).total_seconds()) / 60
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
            "external_playlist_weekly_backfill",
            track["raw"],
            track["artist"],
            track["title"],
            0.94,
            track["title"],
            track["artist"],
            SOURCE_PROVIDER,
            "matched",
            "onlineradiobox_weekly_backfill",
            json.dumps({
                "sourceUrl": PLAYLIST_URL,
                "pageLabel": track.get("pageLabel"),
                "localDate": track.get("localDate"),
                "time": track.get("time"),
                "minuteDiff": round(diff, 2),
            }),
            row_id,
        ))

    return updates

def update_current(conn, tracks):
    if not tracks:
        return None

    # CurrentNowPlaying should prefer the live/current ORB row.
    # Timed rows are for DetectionLog, not necessarily the current song.
    live_rows = [t for t in tracks if not t.get("time")]
    if live_rows:
        latest = live_rows[0]
    else:
        latest = sorted(tracks, key=lambda x: int(x["observedAt"]), reverse=True)[0]

    now = int(datetime.now(timezone.utc).timestamp() * 1000)

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
    """, (
        STATION_ID,
        latest["title"],
        latest["artist"],
        SOURCE_PROVIDER,
        latest["raw"],
        now,
    ))

    return latest

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--print", action="store_true")
    parser.add_argument("--insert-weekly", action="store_true")
    parser.add_argument("--backfill", action="store_true")
    parser.add_argument("--sync-current", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--window-minutes", type=int, default=45)
    args = parser.parse_args()

    pages, tracks = get_weekly_tracks()

    if args.print:
        print(json.dumps({
            "ok": True,
            "stationId": STATION_ID,
            "stationName": STATION_NAME,
            "sourceUrl": PLAYLIST_URL,
            "pages": pages,
            "trackCount": len(tracks),
            "tracks": tracks[:80],
        }, indent=2, ensure_ascii=False))

    if not (args.insert_weekly or args.backfill or args.sync_current):
        return

    conn = connect_db()
    try:
        inserted = 0
        updates = []
        current = None

        if args.insert_weekly:
            for sid in STATION_IDS:
                globals()["STATION_ID"] = sid
                for track in tracks:
                    if insert_track(conn, track):
                        inserted += 1

        if args.backfill:
            for sid in STATION_IDS:
                globals()["STATION_ID"] = sid
                updates.extend(backfill_unknowns(conn, tracks, args.window_minutes, args.limit))

        if args.sync_current:
            current = []
            for sid in STATION_IDS:
                globals()["STATION_ID"] = sid
                current.append(update_current(conn, tracks))

        print(json.dumps({
            "ok": True,
            "dryRun": args.dry_run,
            "tracksFound": len(tracks),
            "insertedWeekly": inserted,
            "backfilled": len(updates),
            "current": current,
            "candidateUpdates": [
                {"id": row_id, "track": track, "minuteDiff": round(diff, 2)}
                for row_id, track, diff in updates[:30]
            ],
        }, indent=2, ensure_ascii=False))

        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
