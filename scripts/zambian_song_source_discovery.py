#!/usr/bin/env python3
import os, re, json, time, sqlite3, urllib.request, urllib.parse
from pathlib import Path
from html import unescape
from datetime import datetime

BASE = Path("/opt/radio-monitor")
DB = BASE / "data/source-discovery/zambian_crawl_state.db"
OUT = BASE / "data/source-discovery/zambian_song_discovery_events.ndjson"

MAX_PAGES_PER_DOMAIN = int(os.environ.get("MAX_PAGES_PER_DOMAIN", "800"))
SLEEP = float(os.environ.get("DISCOVERY_SLEEP", "0.2"))

DOMAINS = [x.strip().lower() for x in os.environ.get("ZAMBIAN_MUSIC_DOMAINS", """
zambianplay.com,
cdn.zambianplay.com,
ckmusicpromos.com,
ilovezedmusic.com.zm,
zedwap.co,
zedhousezambia.com,
zambianmusicpromos.co,
mvesesani.com,
africanmusicblog.com,
youtube.com,
youtu.be,
boomplay.com
""").replace("\n","").split(",") if x.strip()]

MEDIA_EXTS = (".mp3", ".m4a", ".wav", ".aac", ".ogg", ".flac", ".mp4", ".m4v", ".webm")
MEDIA_RE = re.compile(r'https?://[^\s\'"<>]+?\.(?:mp3|m4a|wav|aac|ogg|flac|mp4|m4v|webm)(?:\?[^\s\'"<>]*)?', re.I)
LINK_RE = re.compile(r'https?://[^\s\'"<>]+', re.I)

ARTIST_HINTS = [
    "yo maps", "macky 2", "chef 187", "slapdee", "jemax", "mordecaii", "dizmo",
    "mampi", "towela kaira", "xaven", "chanda na kay", "ndine emma", "driemo",
    "chile one", "t sean", "bow chase", "b flow", "wezi", "pompi", "mag44",
    "esther chungu", "abel chungu", "kings malembe", "rich bizzy", "ne zlong",
    "blood kid", "super kena", "y celeb", "b quan", "vinchenzo", "kanina kandalama",
    "mordecai", "dandy krazy", "shatel", "macky 2", "xyz", "kalandanya"
]

def now():
    return datetime.utcnow().isoformat(timespec="seconds")

def clean_url(u):
    u = unescape(u).strip().replace("\\/", "/")
    return u.strip(".,);]}'\"")

def fetch(url, timeout=25):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/html,application/xml,text/xml,*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "ignore")

def domain_of(u):
    try:
        return urllib.parse.urlparse(u).netloc.lower()
    except Exception:
        return ""

def title_from_url(u):
    try:
        stem = Path(urllib.parse.unquote(urllib.parse.urlparse(u).path)).stem
        return stem.replace("-", " ").replace("_", " ").strip()[:240] or "UNKNOWN"
    except Exception:
        return "UNKNOWN"

def ensure_db(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS audio_sources (
      audio_url TEXT UNIQUE,
      title TEXT,
      artist TEXT,
      source_page TEXT,
      source_domain TEXT,
      created_at TEXT
    )
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS zambian_song_seeds (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      artist TEXT,
      title TEXT,
      source_url TEXT UNIQUE,
      source_domain TEXT,
      seed_type TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()

def cols(conn, table):
    return [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')]

def insert_audio(conn, url, source_page="", title="", artist=""):
    url = clean_url(url)
    if not url.lower().startswith("http"):
        return False

    d = domain_of(url)
    title = title or title_from_url(url)

    c = cols(conn, "audio_sources")
    values = {
        "audio_url": url,
        "url": url,
        "title": title,
        "track_title": title,
        "name": title,
        "artist": artist or "",
        "artist_name": artist or "",
        "source_page": source_page,
        "page_url": source_page,
        "source_domain": d,
        "domain": d,
        "created_at": now(),
        "createdAt": now(),
        "updated_at": now(),
        "updatedAt": now(),
    }
    insert_cols = [x for x in c if x in values]
    if not insert_cols:
        return False

    before = conn.total_changes
    conn.execute(
        f'INSERT OR IGNORE INTO audio_sources ({",".join(chr(34)+x+chr(34) for x in insert_cols)}) VALUES ({",".join("?" for _ in insert_cols)})',
        [values[x] for x in insert_cols]
    )
    conn.commit()
    return conn.total_changes > before

def insert_seed(conn, artist, title, source_url, seed_type):
    artist = (artist or "").strip()[:240]
    title = (title or "").strip()[:240]
    source_url = clean_url(source_url)
    if not source_url:
        return False
    before = conn.total_changes
    conn.execute("""
    INSERT OR IGNORE INTO zambian_song_seeds
    (artist, title, source_url, source_domain, seed_type)
    VALUES (?, ?, ?, ?, ?)
    """, (artist, title, source_url, domain_of(source_url), seed_type))
    conn.commit()
    return conn.total_changes > before

def log_event(obj):
    OUT.parent.mkdir(parents=True, exist_ok=True)
    obj["ts"] = now()
    with OUT.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def harvest_wp_media(conn, domain):
    added = 0
    for page in range(1, MAX_PAGES_PER_DOMAIN + 1):
        url = f"https://{domain}/wp-json/wp/v2/media?per_page=100&page={page}"
        try:
            txt = fetch(url)
            data = json.loads(txt)
            if not isinstance(data, list) or not data:
                break

            page_added = 0
            for item in data:
                src = item.get("source_url") or item.get("guid", {}).get("rendered") or ""
                mime = str(item.get("mime_type") or "").lower()
                if not src:
                    continue
                if not (src.lower().split("?")[0].endswith(MEDIA_EXTS) or "audio" in mime or "video" in mime):
                    continue

                title = ""
                if isinstance(item.get("title"), dict):
                    title = item["title"].get("rendered") or ""

                if insert_audio(conn, src, url, title=title):
                    added += 1
                    page_added += 1
                    log_event({"type":"audio_url", "domain":domain, "url":src, "title":title, "from":url})

            print("WP_MEDIA", domain, "page", page, "added", page_added, "total", added, flush=True)
            time.sleep(SLEEP)
        except Exception as e:
            print("WP_MEDIA_STOP", domain, "page", page, str(e)[:140], flush=True)
            break
    return added

def harvest_sitemaps(conn, domain):
    added = 0
    seeds = [
        f"https://{domain}/sitemap.xml",
        f"https://{domain}/wp-sitemap.xml",
        f"https://{domain}/wp-sitemap-posts-attachment-1.xml",
        f"https://{domain}/wp-sitemap-posts-post-1.xml",
        f"https://{domain}/post-sitemap.xml",
        f"https://{domain}/page-sitemap.xml",
    ]
    seen = set()
    q = list(seeds)

    while q and len(seen) < 1000:
        u = q.pop(0)
        if u in seen:
            continue
        seen.add(u)

        try:
            txt = fetch(u)
            for m in MEDIA_RE.findall(txt):
                m = clean_url(m)
                if insert_audio(conn, m, u):
                    added += 1
                    log_event({"type":"audio_url", "domain":domain, "url":m, "from":u})

            links = [clean_url(x) for x in LINK_RE.findall(txt)]
            for link in links:
                if "sitemap" in link.lower() and domain_of(link).endswith(domain) and link not in seen:
                    q.append(link)

            # Also store song/artist seed pages for later targeted search.
            for link in links[:5000]:
                d = domain_of(link)
                if d.endswith(domain):
                    low = urllib.parse.unquote(link.lower())
                    if any(a.replace(" ", "-") in low or a.replace(" ", "%20") in low or a in low for a in ARTIST_HINTS):
                        insert_seed(conn, "", title_from_url(link), link, "artist_hint_page")

            print("SITEMAP", domain, "checked", u, "added_total", added, flush=True)
            time.sleep(SLEEP)
        except Exception as e:
            print("SITEMAP_FAIL", domain, u, str(e)[:120], flush=True)

    return added

def harvest_existing_pages(conn):
    added = 0
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    for table in tables:
        try:
            c = cols(conn, table)
            text_cols = [x for x in c if any(k in x.lower() for k in ["url","page","html","body","text","source"])]
            for col in text_cols[:10]:
                for r in conn.execute(f'SELECT "{col}" FROM "{table}" WHERE "{col}" IS NOT NULL LIMIT 40000'):
                    val = str(r[0] or "")
                    for m in MEDIA_RE.findall(val):
                        if insert_audio(conn, m, f"db:{table}.{col}"):
                            added += 1
            print("DB_SCAN", table, "added_total", added, flush=True)
        except Exception as e:
            print("DB_SCAN_FAIL", table, str(e)[:120], flush=True)
    return added

def harvest_boomplay_metadata_seeds(conn):
    # This stores Boomplay links/titles as discovery seeds only.
    # We do not fingerprint Boomplay protected/streaming audio here.
    added = 0
    pages = [
        "https://www.boomplay.com/songs",
        "https://www.boomplay.com/artists",
        "https://www.boomplay.com/playlists",
    ]
    for url in pages:
        try:
            txt = fetch(url)
            links = [clean_url(x) for x in LINK_RE.findall(txt)]
            for link in links:
                if "boomplay.com" not in link:
                    continue
                low = urllib.parse.unquote(link.lower())
                if "/songs/" in low or "/albums/" in low or "/artists/" in low or "/playlists/" in low:
                    title = title_from_url(link)
                    if insert_seed(conn, "", title, link, "boomplay_metadata"):
                        added += 1
                        log_event({"type":"boomplay_seed", "url":link, "title":title})
            print("BOOMPLAY_SEEDS", url, "added_total", added, flush=True)
        except Exception as e:
            print("BOOMPLAY_SEED_FAIL", url, str(e)[:140], flush=True)
    return added

def main():
    DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB)
    ensure_db(conn)

    print("=== ZAMBIAN SONG SOURCE DISCOVERY START ===", flush=True)
    print("domains:", ",".join(DOMAINS), flush=True)

    added_existing = harvest_existing_pages(conn)
    print("existing_db_audio_added:", added_existing, flush=True)

    boom = harvest_boomplay_metadata_seeds(conn)
    print("boomplay_metadata_seeds_added:", boom, flush=True)

    total_media = 0
    for d in DOMAINS:
        if d in ("boomplay.com", "youtube.com", "youtu.be"):
            continue
        print("\n=== DOMAIN", d, "===", flush=True)
        total_media += harvest_wp_media(conn, d)
        total_media += harvest_sitemaps(conn, d)

    print("\n=== FINAL COUNTS ===", flush=True)
    for row in conn.execute("""
    SELECT 'audio_sources' AS table_name, COUNT(*) AS rows, COUNT(DISTINCT audio_url) AS unique_items
    FROM audio_sources
    WHERE audio_url IS NOT NULL AND audio_url != ''
    UNION ALL
    SELECT 'zambian_song_seeds', COUNT(*), COUNT(DISTINCT source_url)
    FROM zambian_song_seeds
    """):
        print(row, flush=True)

    conn.close()

if __name__ == "__main__":
    main()
