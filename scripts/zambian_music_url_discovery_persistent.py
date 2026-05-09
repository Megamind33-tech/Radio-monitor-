#!/usr/bin/env python3
import csv, re, time, hashlib, sqlite3
from collections import deque
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urldefrag
import requests
from bs4 import BeautifulSoup

BASE="/opt/radio-monitor"
SEEDS=f"{BASE}/data/source-discovery/zambian_seed_pages.txt"
CRAWL_DB=f"{BASE}/data/source-discovery/zambian_crawl_state.db"
OUT_AUDIO=f"{BASE}/data/source-discovery/zambian_audio_sources_alpha.csv"
OUT_PAGES=f"{BASE}/data/source-discovery/zambian_artist_source_pages_alpha.csv"

MUSIC_HINTS=(
    "music","mp3","download","song","audio","artist","album","mixtape",
    "video","zambian","zed","gospel","lyrics","category/music",
    "category/download","category/downloads","mp3-download","track"
)

AUDIO_EXTS=(".mp3",".wav",".m4a",".aac",".ogg",".oga",".flac",".opus",".webm")
SKIP_EXTS=(".jpg",".jpeg",".png",".gif",".webp",".svg",".css",".js",".ico",".pdf",".zip",".rar",".mp4",".mov",".avi",".webm")
SKIP_WORDS=(
    "facebook.com","instagram.com","twitter.com","x.com","tiktok.com",
    "youtube.com","youtu.be","whatsapp","telegram","mailto:","tel:",
    "/login","/cart","/checkout","/wp-json/","/feed/","replytocom=",
    "/news/","/sports/","/sport/","/football/","/politics/","/business/",
    "/jobs/","/job/","/careers/","/contact","/about","/privacy","/terms",
    "/tag/news","/tag/sports","/category/news","/category/sports",
    "/category/politics","/category/business"
)

def now():
    return datetime.now(timezone.utc).isoformat()

def clean_url(u):
    u=(u or "").strip()
    u,_=urldefrag(u)
    return u

def clean_audio_url(u):
    return clean_url(u).split("?")[0].strip()

def h(x):
    return hashlib.sha256(x.encode("utf-8")).hexdigest()

def is_audio(u):
    return urlparse(u.lower()).path.endswith(AUDIO_EXTS)

def skip_url(u):
    low=(u or "").lower()
    return any(x in low for x in SKIP_WORDS) or urlparse(low).path.endswith(SKIP_EXTS)

def is_music_candidate(u):
    """
    Strict music-score filter.
    Keeps direct audio, music/download/listing pages, and song-like post slugs.
    Rejects news, sports, politics, business, jobs, static pages, and weak random posts.
    """
    if not u:
        return False

    low = u.lower()
    if skip_url(low):
        return False

    if is_audio(low):
        return True

    parsed = urlparse(low)
    domain = parsed.netloc.lower()
    path = parsed.path.lower().strip("/")
    parts = [x for x in path.split("/") if x]
    slug = parts[-1] if parts else ""

    BAD = (
        "news","sports","sport","football","politics","business","jobs","job",
        "career","careers","about","contact","privacy","terms","advertise",
        "arsenal","chelsea","man-city","manchester","barcelona","real-madrid",
        "final","semi-final","league","premier","laliga","uefa","fifa"
    )

    GOOD = (
        "mp3","download","music","song","audio","artist","album","mixtape",
        "gospel","lyrics","video","track","zed","zambian"
    )

    LISTING = (
        "category/music","category/download","category/downloads",
        "music/","downloads/","mp3-download","artist/"
    )

    MUSIC_DOMAINS = (
        "zambianplay.com",
        "ilovezedmusic.com.zm",
        "zedwap.co",
        "zedhousezambia.com",
        "ckmusicpromos.com",
    )

    if any(b in low for b in BAD):
        return False

    if any(x in low for x in LISTING):
        return True

    if any(g in low for g in GOOD):
        return True

    # Keep song-like post slugs on known music domains.
    # Example: yo-maps-hello, chile-one-nayo-nayo, macky-2-ft-slapdee-title
    if domain in MUSIC_DOMAINS and slug:
        hyphen_count = slug.count("-")
        word_count = len([x for x in slug.split("-") if x])

        if hyphen_count >= 2 and word_count >= 3:
            return True

    return False


    low=u.lower()
    if skip_url(low):
        return False

    if is_audio(low):
        return True

    domain=urlparse(low).netloc.lower()
    path=urlparse(low).path.lower().strip("/")

    if any(h in low for h in MUSIC_HINTS):
        return True

    # These sources are mostly music/blog catalogs. Allow normal post slugs
    # as long as they are not blocked by SKIP_WORDS.
    music_focused_domains=(
        "zambianplay.com",
        "ilovezedmusic.com.zm",
        "zedwap.co",
        "zedhousezambia.com",
        "ckmusicpromos.com",
    )

    if domain in music_focused_domains:
        # Allow article/post slugs, but not homepage-only or static pages.
        if path and len(path.split("/")) <= 3:
            return True

    return False

def norm(t):
    return re.sub(r"\s+"," ",t or "").strip()

def page_title(soup):
    h1=soup.find("h1")
    if h1 and norm(h1.get_text(" ",strip=True)):
        return norm(h1.get_text(" ",strip=True))
    og=soup.find("meta",attrs={"property":"og:title"})
    if og and og.get("content"):
        return norm(og["content"])
    t=soup.find("title")
    return norm(t.get_text(" ",strip=True)) if t else ""

def clean_music_title(t):
    t=norm(t)
    t=re.sub(r"\b(download|mp3|new song|latest zambian music|zambian music|official audio|official video|music video)\b","",t,flags=re.I)
    t=re.sub(r"\((prod\.?|produced).*?\)","",t,flags=re.I)
    t=re.sub(r"\[(prod\.?|produced).*?\]","",t,flags=re.I)
    return norm(t).strip(" -–—|:•")

def parse_artist_song(raw):
    t=clean_music_title(raw)
    t=re.sub(r"\s*[-|]\s*(Zambianplay|I Love Zed Music|Zedwap|Zedhouse Zambia|CK Music Promos).*$","",t,flags=re.I)

    m=re.search(r"^(.*?)\s+by\s+(.+)$",t,flags=re.I)
    if m:
        song=clean_music_title(m.group(1))
        artist=clean_music_title(m.group(2))
        artist=re.split(r"\s+(ft\.?|feat\.?|featuring)\s+",artist,flags=re.I)[0].strip()
        if artist and song:
            return artist,song

    for sep in [" – "," — "," - "," | "," : "]:
        if sep in t:
            left,right=t.split(sep,1)
            artist=clean_music_title(left)
            song=clean_music_title(right)
            artist=re.split(r"\s+(ft\.?|feat\.?|featuring)\s+",artist,flags=re.I)[0].strip()
            if artist and song:
                return artist,song

    return "UNKNOWN",t

def read_seeds():
    out=[]
    with open(SEEDS,encoding="utf-8") as f:
        for line in f:
            u=clean_url(line)
            if u and not u.startswith("#"):
                out.append(u)
    return out

def init_db():
    con=sqlite3.connect(CRAWL_DB)

    con.execute("""
    CREATE TABLE IF NOT EXISTS visited_pages (
      url TEXT PRIMARY KEY,
      source_domain TEXT,
      artist TEXT,
      title TEXT,
      status TEXT,
      error TEXT,
      fetched_at TEXT
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS audio_sources (
      audio_url_hash TEXT PRIMARY KEY,
      audio_url TEXT,
      source_page TEXT,
      source_domain TEXT,
      artist TEXT,
      title TEXT,
      extractor TEXT,
      discovered_at TEXT
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS crawl_frontier (
      url TEXT PRIMARY KEY,
      source_domain TEXT,
      discovered_from TEXT,
      status TEXT DEFAULT 'pending',
      discovered_at TEXT,
      fetched_at TEXT,
      error TEXT
    )
    """)

    con.commit()
    return con

def already_visited(con,url):
    return con.execute("SELECT 1 FROM visited_pages WHERE url=?", (url,)).fetchone() is not None

def enqueue(con,url,source_domain,discovered_from=""):
    url=clean_url(url)
    if not url:
        return
    if skip_url(url):
        return
    if not is_music_candidate(url):
        return
    if already_visited(con,url):
        return

    con.execute("""
    INSERT OR IGNORE INTO crawl_frontier
    (url,source_domain,discovered_from,status,discovered_at)
    VALUES (?,?,?,?,?)
    """,(url,source_domain,discovered_from,"pending",now()))
    con.commit()

def mark_frontier(con,url,status,error=""):
    con.execute("""
    UPDATE crawl_frontier
    SET status=?, fetched_at=?, error=?
    WHERE url=?
    """,(status,now(),error[:500],url))
    con.commit()

def next_pending(con,domains,limit=5000):
    rows=con.execute("""
    SELECT url FROM crawl_frontier
    WHERE status='pending'
    ORDER BY discovered_at ASC
    LIMIT ?
    """,(limit,)).fetchall()

    q=deque()
    for (url,) in rows:
        domain=urlparse(url).netloc.lower()
        if domain in domains and not already_visited(con,url) and not skip_url(url):
            q.append(url)
    return q

def mark_visited(con,url,domain,artist,title,status,error=""):
    con.execute("""
    INSERT OR REPLACE INTO visited_pages
    (url,source_domain,artist,title,status,error,fetched_at)
    VALUES (?,?,?,?,?,?,?)
    """,(url,domain,artist,title,status,error[:500],now()))
    con.commit()

def save_audio(con,audio_url,source_page,domain,artist,title,extractor):
    au=clean_audio_url(audio_url)
    if not au:
        return
    con.execute("""
    INSERT OR IGNORE INTO audio_sources
    (audio_url_hash,audio_url,source_page,source_domain,artist,title,extractor,discovered_at)
    VALUES (?,?,?,?,?,?,?,?)
    """,(h(au),au,source_page,domain,artist,title,extractor,now()))
    con.commit()

def extract(page_url,html):
    soup=BeautifulSoup(html,"lxml")
    raw=page_title(soup)
    artist,title=parse_artist_song(raw)
    domain=urlparse(page_url).netloc.lower()

    audio_candidates=[]

    for tag in soup.find_all(["audio","source"]):
        src=tag.get("src")
        typ=tag.get("type","")
        if src and (tag.name=="audio" or "audio" in typ.lower() or is_audio(src)):
            audio_candidates.append((clean_audio_url(urljoin(page_url,src)),tag.name))

    for attr,val in [
        ("property","og:audio"),
        ("property","og:audio:url"),
        ("property","og:audio:secure_url"),
        ("name","twitter:player:stream"),
        ("property","twitter:player:stream"),
    ]:
        for m in soup.find_all("meta",attrs={attr:val}):
            c=m.get("content")
            if c:
                audio_candidates.append((clean_audio_url(urljoin(page_url,c)),f"meta:{val}"))

    links=[]
    for a in soup.find_all("a",href=True):
        href=clean_url(urljoin(page_url,a["href"]))
        if not href.startswith(("http://","https://")):
            continue

        if is_audio(href):
            audio_candidates.append((clean_audio_url(href),"direct-audio-link"))
        elif not skip_url(href):
            links.append(href)

    return artist,title,domain,audio_candidates,links

def export_csvs(con):
    fields=["artist_sort","artist","title","source_page","audio_url","source_domain","extractor","page_hash"]

    audio_rows=con.execute("""
    SELECT
      lower(COALESCE(artist,'UNKNOWN')) AS artist_sort,
      COALESCE(artist,'UNKNOWN') AS artist,
      COALESCE(title,'') AS title,
      source_page,
      audio_url,
      source_domain,
      extractor,
      audio_url_hash AS page_hash
    FROM audio_sources
    ORDER BY artist_sort, artist, title, audio_url
    """).fetchall()

    page_rows=con.execute("""
    SELECT
      lower(COALESCE(artist,'UNKNOWN')) AS artist_sort,
      COALESCE(artist,'UNKNOWN') AS artist,
      COALESCE(title,'') AS title,
      url AS source_page,
      '' AS audio_url,
      source_domain,
      'source-page' AS extractor,
      '' AS page_hash
    FROM visited_pages
    WHERE status='ok'
    ORDER BY artist_sort, artist, title, source_page
    """).fetchall()

    with open(OUT_AUDIO,"w",newline="",encoding="utf-8") as f:
        w=csv.writer(f)
        w.writerow(fields)
        w.writerows(audio_rows)

    with open(OUT_PAGES,"w",newline="",encoding="utf-8") as f:
        w=csv.writer(f)
        w.writerow(fields)
        w.writerows(page_rows)

    print(f"Exported audio rows: {len(audio_rows)}")
    print(f"Exported page rows: {len(page_rows)}")

def frontier_stats(con):
    return con.execute("""
    SELECT status, COUNT(*)
    FROM crawl_frontier
    GROUP BY status
    """).fetchall()

def main():
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--max-new-pages",type=int,default=300)
    p.add_argument("--sleep",type=float,default=1.5)
    p.add_argument("--refill-from-seeds",action="store_true")
    args=p.parse_args()

    con=init_db()
    seeds=read_seeds()
    domains={urlparse(u).netloc.lower() for u in seeds}

    for seed in seeds:
        enqueue(con,seed,urlparse(seed).netloc.lower(),"seed")

    q=next_pending(con,domains)

    # If queue is empty, refresh seed pages to discover links without losing visited memory.
    if not q and args.refill_from_seeds:
        print("No pending frontier. Re-scanning seeds to refill frontier.")
        for seed in seeds:
            con.execute("DELETE FROM crawl_frontier WHERE url=?", (seed,))
            con.commit()
            enqueue(con,seed,urlparse(seed).netloc.lower(),"seed_refill")
        q=next_pending(con,domains)

    s=requests.Session()
    s.headers.update({"User-Agent":"ZAMCOPS-ZambianMusicSourceDiscovery/1.0"})

    fetched_new=0
    skipped_seen=0
    queued_new_links=0

    while q and fetched_new < args.max_new_pages:
        url=clean_url(q.popleft())
        domain=urlparse(url).netloc.lower()

        if not url or domain not in domains or skip_url(url):
            mark_frontier(con,url,"skipped","bad_or_external")
            continue

        if already_visited(con,url):
            skipped_seen += 1
            mark_frontier(con,url,"skipped","already_visited")
            continue

        print(f"FETCH NEW {fetched_new+1}/{args.max_new_pages}: {url}")

        try:
            r=s.get(url,timeout=20,allow_redirects=True)
            final=clean_url(r.url)
            final_domain=urlparse(final).netloc.lower()

            if r.status_code>=400:
                mark_visited(con,final,final_domain,"UNKNOWN","",f"http_{r.status_code}")
                mark_frontier(con,url,"failed",f"http_{r.status_code}")
                fetched_new += 1
                continue

            ctype=r.headers.get("content-type","").lower()
            if "text/html" not in ctype:
                mark_visited(con,final,final_domain,"UNKNOWN","", "non_html")
                mark_frontier(con,url,"done","non_html")
                fetched_new += 1
                continue

            artist,title,domain,audios,links=extract(final,r.text)

            page_low=(final+" "+title).lower()
            looks_like_music_page=any(h in page_low for h in MUSIC_HINTS)

            # If a page has no audio and does not look like a music/listing page,
            # mark it and do not follow its child links.
            if not audios and not looks_like_music_page:
                mark_visited(con,final,domain,artist,title,"skipped_non_music","no_audio_no_music_hint")
                mark_frontier(con,url,"skipped","no_audio_no_music_hint")
                fetched_new += 1
                continue

            for au,extractor in audios:
                save_audio(con,au,final,domain,artist,title,extractor)

            for link in links:
                link_domain=urlparse(link).netloc.lower()
                if link_domain in domains:
                    before=con.total_changes
                    enqueue(con,link,link_domain,final)
                    if con.total_changes > before:
                        queued_new_links += 1
                        q.append(link)

            mark_visited(con,final,domain,artist,title,"ok")
            mark_frontier(con,url,"done")
            fetched_new += 1

            time.sleep(args.sleep)

        except Exception as e:
            mark_visited(con,url,domain,"UNKNOWN","", "error", str(e))
            mark_frontier(con,url,"failed",str(e))
            fetched_new += 1
            print(f"ERROR {url}: {e}")

    export_csvs(con)

    print("")
    print(f"New pages fetched this run: {fetched_new}")
    print(f"Already-seen pages skipped: {skipped_seen}")
    print(f"New links queued this run: {queued_new_links}")
    print("Frontier status:")
    for status,count in frontier_stats(con):
        print(f"{status}: {count}")
    print(f"Crawl DB: {CRAWL_DB}")

    con.close()

if __name__=="__main__":
    main()
