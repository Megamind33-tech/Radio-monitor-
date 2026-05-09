#!/usr/bin/env python3
import re
import sqlite3
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from datetime import datetime, timezone

BASE="/opt/radio-monitor"
SEEDS=f"{BASE}/data/source-discovery/zambian_seed_pages.txt"
CRAWL_DB=f"{BASE}/data/source-discovery/zambian_crawl_state.db"

SKIP_WORDS=[
    "facebook.com","instagram.com","twitter.com","x.com","tiktok.com",
    "youtube.com","youtu.be","whatsapp","telegram","mailto:","tel:",
    "/login","/cart","/checkout","/wp-json/","/feed/","replytocom="
]

GOOD_HINTS=[
    "music","download","mp3","song","audio","video",
    "zambian","zed","album","category","artist"
]

def now():
    return datetime.now(timezone.utc).isoformat()

def clean(u):
    return (u or "").split("#")[0].strip()

def skip(u):
    low=u.lower()
    return any(x in low for x in SKIP_WORDS)

def read_seeds():
    out=[]
    with open(SEEDS,encoding="utf-8") as f:
        for line in f:
            u=clean(line)
            if u and not u.startswith("#"):
                out.append(u.rstrip("/") + "/")
    return out

def init_db():
    con=sqlite3.connect(CRAWL_DB)
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

def visited(con,u):
    try:
        return con.execute("SELECT 1 FROM visited_pages WHERE url=?", (u,)).fetchone() is not None
    except sqlite3.OperationalError:
        return False

def enqueue(con,u,source="bootstrap"):
    u=clean(u)
    if not u or skip(u):
        return False

    d=urlparse(u).netloc.lower()
    if not d:
        return False

    if visited(con,u):
        return False

    before=con.total_changes
    con.execute("""
    INSERT OR IGNORE INTO crawl_frontier
    (url,source_domain,discovered_from,status,discovered_at)
    VALUES (?,?,?,?,?)
    """,(u,d,source,"pending",now()))
    con.commit()
    return con.total_changes > before

def fetch_links(session,u):
    try:
        r=session.get(u,timeout=20,allow_redirects=True)
        if r.status_code >= 400:
            return []
        text=r.text or ""
        links=[]

        # Works for XML sitemaps and HTML pages.
        soup=BeautifulSoup(text,"xml")
        for loc in soup.find_all("loc"):
            if loc.text:
                links.append(clean(loc.text))

        soup=BeautifulSoup(text,"lxml")
        for a in soup.find_all("a",href=True):
            links.append(clean(urljoin(u,a["href"])))

        return list(dict.fromkeys([x for x in links if x.startswith(("http://","https://"))]))
    except Exception as e:
        print("FETCH ERROR:", u, e)
        return []

def main():
    con=init_db()
    seeds=read_seeds()
    seed_domains={urlparse(s).netloc.lower() for s in seeds}

    s=requests.Session()
    s.headers.update({"User-Agent":"ZAMCOPS-ZambianMusicFrontierBootstrap/1.0"})

    added=0

    for seed in seeds:
        domain=urlparse(seed).netloc.lower()
        root=f"{urlparse(seed).scheme}://{domain}/"

        candidates=[
            root,
            urljoin(root,"sitemap.xml"),
            urljoin(root,"wp-sitemap.xml"),
            urljoin(root,"post-sitemap.xml"),
            urljoin(root,"page-sitemap.xml"),
            urljoin(root,"category-sitemap.xml"),
            urljoin(root,"wp-sitemap-posts-post-1.xml"),
            urljoin(root,"wp-sitemap-posts-page-1.xml"),
            urljoin(root,"wp-sitemap-taxonomies-category-1.xml"),
        ]

        # Add common paginated category/list pages.
        for base in ["category/music/","category/videos/","category/download/","category/downloads/","music/","downloads/","mp3-download/"]:
            for n in range(1,51):
                candidates.append(urljoin(root,base if n==1 else f"{base}page/{n}/"))

        print("BOOTSTRAP DOMAIN:", domain)

        for c in candidates:
            for link in fetch_links(s,c):
                ld=urlparse(link).netloc.lower()
                if ld not in seed_domains:
                    continue

                low=link.lower()
                if not any(h in low for h in GOOD_HINTS):
                    continue

                if enqueue(con,link,c):
                    added += 1

    print("")
    print("Pending links added:", added)

    print("FRONTIER STATUS:")
    for row in con.execute("SELECT status, COUNT(*) FROM crawl_frontier GROUP BY status"):
        print(row[0], row[1])

    con.close()

if __name__=="__main__":
    main()
