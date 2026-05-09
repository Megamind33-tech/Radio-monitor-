#!/usr/bin/env python3
import sqlite3
from urllib.parse import urlparse

DB="/opt/radio-monitor/data/source-discovery/zambian_crawl_state.db"

AUDIO_EXTS = (
    ".mp3",".wav",".m4a",".aac",".ogg",".oga",".flac",".opus",".webm"
)

BLOCK_EXTS = (
    ".jpg",".jpeg",".png",".gif",".webp",".svg",".ico",
    ".css",".js",".json",
    ".pdf",".zip",".rar",".7z",
    ".mp4",".mov",".avi",".mkv",
)

BAD = (
    "news","sports","sport","football","politics","business","jobs","job",
    "career","careers","about","contact","privacy","terms","advertise",
    "arsenal","chelsea","man-city","manchester","barcelona","real-madrid",
    "final","semi-final","league","premier","laliga","uefa","fifa"
)

VERY_GOOD = (
    "mp3-download","download","downloads",
    "category/music","category/download","category/downloads"
)

GOOD = (
    "music","song","audio","artist","album","mixtape","gospel","lyrics",
    "track","zed","zambian","ft-","feat-","prod-by"
)

MUSIC_DOMAINS = (
    "zambianplay.com",
    "ilovezedmusic.com.zm",
    "zedwap.co",
    "zedhousezambia.com",
    "ckmusicpromos.com",
)

def score(url):
    low=(url or "").lower()
    parsed=urlparse(low)
    domain=parsed.netloc.lower()
    path=parsed.path.lower()
    slug=path.strip("/").split("/")[-1] if path else ""

    if path.endswith(AUDIO_EXTS):
        return 100

    if path.endswith(BLOCK_EXTS):
        return -200

    if any(b in low for b in BAD):
        return -100

    s=0

    if domain in MUSIC_DOMAINS:
        s += 10

    if any(v in low for v in VERY_GOOD):
        s += 60

    if any(g in low for g in GOOD):
        s += 25

    if slug:
        words=[x for x in slug.split("-") if x]
        if len(words) >= 3:
            s += 15
        if len(words) >= 5:
            s += 10
        if "ft" in words or "feat" in words:
            s += 20

    return s

con=sqlite3.connect(DB)
con.row_factory=sqlite3.Row

cols=[r[1] for r in con.execute("PRAGMA table_info(crawl_frontier)").fetchall()]
if "priority" not in cols:
    con.execute("ALTER TABLE crawl_frontier ADD COLUMN priority INTEGER DEFAULT 0")

rows=con.execute("""
SELECT url
FROM crawl_frontier
WHERE status='pending'
""").fetchall()

high=medium=low=skipped=0

for r in rows:
    url=r["url"]
    sc=score(url)

    if sc < 0:
        con.execute("""
        UPDATE crawl_frontier
        SET status='skipped',
            error='ranker_filtered_static_or_non_music',
            priority=?
        WHERE url=?
        """,(sc,url))
        skipped += 1
    else:
        con.execute("""
        UPDATE crawl_frontier
        SET priority=?
        WHERE url=?
        """,(sc,url))

        if sc >= 60:
            high += 1
        elif sc >= 30:
            medium += 1
        else:
            low += 1

con.commit()

print("High priority:", high)
print("Medium priority:", medium)
print("Low priority:", low)
print("Skipped:", skipped)

print("")
print("Top 40 pending URLs:")
for r in con.execute("""
SELECT priority, url
FROM crawl_frontier
WHERE status='pending'
ORDER BY priority DESC, discovered_at ASC
LIMIT 40
"""):
    print(r["priority"], "|", r["url"])

con.close()
