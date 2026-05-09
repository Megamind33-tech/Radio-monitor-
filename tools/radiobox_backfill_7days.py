#!/usr/bin/env python3
import urllib.request
import urllib.parse
import re
import html
import json
import csv
import time
import datetime
from pathlib import Path

BASE = "https://onlineradiobox.com"
COUNTRY_URL = "https://onlineradiobox.com/zm/"
OUT_DIR = Path("data/radiobox_backfill")
OUT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
JSONL_OUT = OUT_DIR / f"radiobox_zambia_7day_backfill_{STAMP}.jsonl"
CSV_OUT = OUT_DIR / f"radiobox_zambia_7day_backfill_{STAMP}.csv"
REPORT_OUT = OUT_DIR / f"radiobox_zambia_7day_report_{STAMP}.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": COUNTRY_URL,
}

SKIP_SLUGS = {
    "", "genre", "music", "news", "talk", "sport", "favorites", "games",
    "all", "search", "contacts", "feedback", "widgets"
}

def fetch(url, timeout=30):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")

def clean_text(raw):
    page = re.sub(r"(?is)<script.*?</script>", " ", raw)
    page = re.sub(r"(?is)<style.*?</style>", " ", page)
    page = re.sub(r"(?i)<br\s*/?>", "\n", page)
    page = re.sub(r"(?i)</(div|li|tr|td|p|span|section|h1|h2|h3|a)>", "\n", page)
    page = re.sub(r"<[^>]+>", " ", page)
    text = html.unescape(page)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n+", "\n", text)
    return text.strip()

def discover_station_urls():
    raw = fetch(COUNTRY_URL)
    urls = set()

    for m in re.finditer(r'href=["\']([^"\']+)["\']', raw):
        href = html.unescape(m.group(1))
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = urllib.parse.urljoin(BASE, href)
        elif not href.startswith("http"):
            continue

        u = urllib.parse.urlparse(href)
        if u.netloc not in ("onlineradiobox.com", "www.onlineradiobox.com"):
            continue

        path = u.path.strip("/")
        parts = path.split("/")

        # Zambia station pages usually look like /zm/chikuni/
        if len(parts) == 2 and parts[0] == "zm":
            slug = parts[1].strip().lower()
            if slug not in SKIP_SLUGS and not slug.startswith("playlist"):
                urls.add(f"{BASE}/zm/{slug}/")

    return sorted(urls)

def station_name_from_page(raw, fallback_slug):
    text = clean_text(raw)
    m = re.search(r"#\s*(.+?)\s+playlist", text, re.I)
    if m:
        return m.group(1).strip()
    m = re.search(r"Listen to\s+(.+?)\s+on your smartphone", text, re.I)
    if m:
        return m.group(1).strip()
    return fallback_slug.replace("-", " ").title()

def parse_tracks(raw, station_name, slug, station_url, playlist_url, day_offset):
    text = clean_text(raw)

    if "playlist stores" not in text.lower() and "show by radio station time" not in text.lower():
        return []

    if "unfortunately, the radio station did not provide a playlist for this day" in text.lower():
        return []

    today = datetime.date.today()
    playlist_date = today - datetime.timedelta(days=day_offset)

    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        # Stop around page footer / ads
        if line.lower().startswith("install the free online radio box"):
            break
        if line.lower().startswith("recommended"):
            break

        # Playlist rows look like: 21:54 Artist - Title
        m = re.match(r"^([0-2]?\d:[0-5]\d)\s+(.{3,200})$", line)
        if not m:
            continue

        play_time = m.group(1).zfill(5)
        raw_track = m.group(2).strip()

        # Filter obvious garbage/page UI
        bad_bits = [
            "show by radio station time",
            "now in",
            "online radio box",
            "application",
            "cookies",
            "log in",
        ]
        if any(b in raw_track.lower() for b in bad_bits):
            continue

        artist = ""
        title = raw_track
        if " - " in raw_track:
            artist, title = raw_track.split(" - ", 1)
            artist = artist.strip()
            title = title.strip()

        rows.append({
            "station_name": station_name,
            "station_slug": slug,
            "station_url": station_url,
            "playlist_url": playlist_url,
            "playlist_date": str(playlist_date),
            "playlist_time": play_time,
            "artist": artist,
            "title": title,
            "raw_track": raw_track,
            "source": "onlineradiobox",
            "fetched_at_utc": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        })

    # Deduplicate same station/date/time/raw
    seen = set()
    unique = []
    for r in rows:
        key = (r["station_slug"], r["playlist_date"], r["playlist_time"], r["raw_track"])
        if key not in seen:
            seen.add(key)
            unique.append(r)

    return unique

def playlist_urls_for_station(station_url):
    slug = urllib.parse.urlparse(station_url).path.strip("/").split("/")[-1]
    cs = f"zm.{slug}"

    urls = []
    urls.append((0, f"{station_url}playlist/?cs={urllib.parse.quote(cs)}&played=1&nocache={int(time.time())}"))

    # RadioBox older days: /playlist/1 to /playlist/6
    for offset in range(1, 7):
        urls.append((offset, f"{station_url}playlist/{offset}?nocache={int(time.time())}"))

    return slug, urls

def main():
    print("Discovering Zambian RadioBox stations...")
    station_urls = discover_station_urls()

    print(f"Stations discovered: {len(station_urls)}")
    print("Backfilling past 7 days where playlist metadata exists...")

    all_rows = []
    report = {
        "country_url": COUNTRY_URL,
        "stations_discovered": len(station_urls),
        "stations_with_playlist_rows": 0,
        "stations_without_playlist_rows": 0,
        "total_rows": 0,
        "stations": [],
    }

    for idx, station_url in enumerate(station_urls, 1):
        slug, urls = playlist_urls_for_station(station_url)
        print(f"[{idx}/{len(station_urls)}] {slug}")

        station_rows = []
        station_name = slug.replace("-", " ").title()

        for day_offset, playlist_url in urls:
            try:
                raw = fetch(playlist_url)
                if day_offset == 0:
                    station_name = station_name_from_page(raw, slug)

                rows = parse_tracks(
                    raw=raw,
                    station_name=station_name,
                    slug=slug,
                    station_url=station_url,
                    playlist_url=playlist_url,
                    day_offset=day_offset,
                )
                station_rows.extend(rows)
                print(f"  day_offset={day_offset} rows={len(rows)}")

            except Exception as e:
                print(f"  day_offset={day_offset} ERROR: {e}")

            time.sleep(0.7)

        all_rows.extend(station_rows)

        report["stations"].append({
            "station_slug": slug,
            "station_name": station_name,
            "station_url": station_url,
            "rows": len(station_rows),
            "has_playlist_metadata": len(station_rows) > 0,
        })

        if station_rows:
            report["stations_with_playlist_rows"] += 1
        else:
            report["stations_without_playlist_rows"] += 1

    report["total_rows"] = len(all_rows)

    with JSONL_OUT.open("w", encoding="utf-8") as f:
        for row in all_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    with CSV_OUT.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "station_name", "station_slug", "station_url", "playlist_url",
            "playlist_date", "playlist_time", "artist", "title",
            "raw_track", "source", "fetched_at_utc"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    REPORT_OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("")
    print("DONE")
    print(f"Stations discovered: {report['stations_discovered']}")
    print(f"Stations with playlist metadata: {report['stations_with_playlist_rows']}")
    print(f"Stations without playlist metadata: {report['stations_without_playlist_rows']}")
    print(f"Total playlist rows collected: {report['total_rows']}")
    print(f"JSONL: {JSONL_OUT}")
    print(f"CSV: {CSV_OUT}")
    print(f"REPORT: {REPORT_OUT}")

if __name__ == "__main__":
    main()
