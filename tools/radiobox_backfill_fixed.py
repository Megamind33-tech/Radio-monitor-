#!/usr/bin/env python3
import urllib.request, urllib.parse, re, html, json, csv, time, datetime
from pathlib import Path

BASE = "https://onlineradiobox.com"
COUNTRY_URL = "https://onlineradiobox.com/zm/"
OUT_DIR = Path("data/radiobox_backfill")
OUT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
JSONL_OUT = OUT_DIR / f"radiobox_zambia_fixed_7day_{STAMP}.jsonl"
CSV_OUT = OUT_DIR / f"radiobox_zambia_fixed_7day_{STAMP}.csv"
REPORT_OUT = OUT_DIR / f"radiobox_zambia_fixed_report_{STAMP}.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": COUNTRY_URL,
}

def fetch(url, timeout=35):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")

def flat_text(raw):
    raw = re.sub(r"(?is)<script.*?</script>", " ", raw)
    raw = re.sub(r"(?is)<style.*?</style>", " ", raw)
    raw = re.sub(r"<[^>]+>", " ", raw)
    txt = html.unescape(raw)
    txt = txt.replace("\xa0", " ")
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt

def discover_station_urls():
    raw = fetch(COUNTRY_URL)
    urls = set()

    for href in re.findall(r'href=["\']([^"\']+)["\']', raw):
        href = html.unescape(href)
        full = urllib.parse.urljoin(BASE, href)
        u = urllib.parse.urlparse(full)

        if u.netloc not in ("onlineradiobox.com", "www.onlineradiobox.com"):
            continue

        parts = u.path.strip("/").split("/")
        if len(parts) == 2 and parts[0] == "zm":
            slug = parts[1].strip().lower()
            if slug and slug not in {"genre", "music", "news", "talk", "sport", "favorites", "games"}:
                urls.add(f"{BASE}/zm/{slug}/")

    return sorted(urls)

def station_name(raw, slug):
    m = re.search(r"<h1[^>]*>(.*?)</h1>", raw, re.I | re.S)
    if m:
        name = re.sub(r"<[^>]+>", " ", m.group(1))
        name = html.unescape(re.sub(r"\s+", " ", name)).strip()
        name = re.sub(r"\s+playlist$", "", name, flags=re.I)
        if name:
            return name
    return slug.replace("-", " ").title()

def split_track(track):
    track = re.sub(r"\s+", " ", track).strip(" -–|")
    artist = ""
    title = track

    if " - " in track:
        artist, title = track.split(" - ", 1)
    elif " – " in track:
        artist, title = track.split(" – ", 1)

    return artist.strip(), title.strip(), track

def parse_tracks(raw, station, slug, station_url, playlist_url, day_back):
    txt = flat_text(raw)
    low = txt.lower()

    if "did not provide a playlist for this day" in low:
        return []

    start = low.find("show by radio station time")
    if start >= 0:
        txt = txt[start:]

    # Remove station clock, e.g. "Show by radio station time (now in Chikuni 17:45)"
    txt = re.sub(r"Show by radio station time\s*\([^)]*\)", " ", txt, flags=re.I)

    for stop in [
        "Install the free Online Radio Box",
        "Recommended",
        "Online Radio Zambia Language",
        "Listen to",
    ]:
        p = txt.find(stop)
        if p > 0:
            txt = txt[:p]

    rows = []
    today = datetime.date.today()
    playlist_date = today - datetime.timedelta(days=day_back)

    # Capture: 21:54 Artist - Title up to next time
    pattern = r"(?<!\d)([0-2]?\d:[0-5]\d)\s+(.{3,260}?)(?=\s+[0-2]?\d:[0-5]\d\s+|$)"

    for m in re.finditer(pattern, txt):
        play_time = m.group(1).zfill(5)
        track = m.group(2).strip()

        bad = [
            "online radio box",
            "application",
            "recommended",
            "cookies",
            "authorization",
            "server connection lost",
            "unfortunately",
            "show by radio station time",
            "now in",
        ]
        if any(b in track.lower() for b in bad):
            continue

        if len(track) < 3:
            continue

        artist, title, raw_track = split_track(track)

        rows.append({
            "station_name": station,
            "station_slug": slug,
            "station_url": station_url,
            "playlist_url": playlist_url,
            "day_back": day_back,
            "playlist_date": str(playlist_date),
            "playlist_time": play_time,
            "artist": artist,
            "title": title,
            "raw_track": raw_track,
            "source": "onlineradiobox",
            "fetched_at_utc": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        })

    seen = set()
    unique = []
    for r in rows:
        key = (r["station_slug"], r["playlist_date"], r["playlist_time"], r["raw_track"])
        if key not in seen:
            seen.add(key)
            unique.append(r)

    return unique

def playlist_urls(station_url):
    slug = urllib.parse.urlparse(station_url).path.strip("/").split("/")[-1]
    cs = urllib.parse.quote(f"zm.{slug}")

    urls = []
    urls.append((0, f"{station_url}playlist/?cs={cs}&played=1&nocache={int(time.time())}"))

    for day_back in range(1, 7):
        urls.append((day_back, f"{station_url}playlist/{day_back}?cs={cs}&played=1&nocache={int(time.time())}"))

    return slug, urls

def main():
    station_urls = discover_station_urls()
    print("Stations discovered:", len(station_urls))

    all_rows = []
    report = []

    for idx, station_url in enumerate(station_urls, 1):
        slug, urls = playlist_urls(station_url)
        station_rows = []
        name = slug.replace("-", " ").title()

        print(f"[{idx}/{len(station_urls)}] {slug}")

        for day_back, url in urls:
            try:
                raw = fetch(url)
                if day_back == 0:
                    name = station_name(raw, slug)

                rows = parse_tracks(raw, name, slug, station_url, url, day_back)
                station_rows.extend(rows)
                print(f"  day_back={day_back} rows={len(rows)}")

            except Exception as e:
                print(f"  day_back={day_back} ERROR={e}")

            time.sleep(0.5)

        all_rows.extend(station_rows)

        report.append({
            "station_slug": slug,
            "station_name": name,
            "station_url": station_url,
            "rows": len(station_rows),
            "has_playlist_metadata": len(station_rows) > 0,
        })

    with JSONL_OUT.open("w", encoding="utf-8") as f:
        for row in all_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    with CSV_OUT.open("w", encoding="utf-8", newline="") as f:
        fields = [
            "station_name", "station_slug", "station_url", "playlist_url",
            "day_back", "playlist_date", "playlist_time",
            "artist", "title", "raw_track", "source", "fetched_at_utc"
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    summary = {
        "stations_discovered": len(station_urls),
        "stations_with_playlist_metadata": sum(1 for s in report if s["has_playlist_metadata"]),
        "stations_without_playlist_metadata": sum(1 for s in report if not s["has_playlist_metadata"]),
        "total_rows": len(all_rows),
        "jsonl": str(JSONL_OUT),
        "csv": str(CSV_OUT),
        "stations": report,
    }

    REPORT_OUT.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("")
    print("DONE")
    print("Stations discovered:", summary["stations_discovered"])
    print("Stations with playlist metadata:", summary["stations_with_playlist_metadata"])
    print("Total rows:", summary["total_rows"])
    print("JSONL:", JSONL_OUT)
    print("CSV:", CSV_OUT)
    print("REPORT:", REPORT_OUT)
    print("")
    print("Stations with metadata:")
    for s in report:
        if s["has_playlist_metadata"]:
            print("-", s["station_slug"], "|", s["rows"], "|", s["station_name"])

if __name__ == "__main__":
    main()
