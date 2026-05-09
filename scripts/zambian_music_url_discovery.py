#!/usr/bin/env python3
import csv, re, time, hashlib
from collections import deque
from urllib.parse import urljoin, urlparse, urldefrag
import requests
from bs4 import BeautifulSoup

BASE="/opt/radio-monitor"
SEEDS=f"{BASE}/data/source-discovery/zambian_seed_pages.txt"
OUT_PAGES=f"{BASE}/data/source-discovery/zambian_artist_source_pages_alpha.csv"
OUT_AUDIO=f"{BASE}/data/source-discovery/zambian_audio_sources_alpha.csv"

AUDIO_EXTS=(".mp3",".wav",".m4a",".aac",".ogg",".oga",".flac",".opus",".webm")
SKIP_EXTS=(".jpg",".jpeg",".png",".gif",".webp",".svg",".css",".js",".ico",".pdf",".zip",".rar",".mp4",".mov",".avi")
SKIP_WORDS=("facebook.com","instagram.com","twitter.com","x.com","tiktok.com","youtube.com","youtu.be","whatsapp","telegram","mailto:","tel:","/login","/cart","/checkout","/wp-json/","/feed/")

def clean(u):
    u=(u or "").strip()
    u,_=urldefrag(u)
    return u

def is_audio(u):
    return urlparse(u.lower()).path.endswith(AUDIO_EXTS)

def skip(u):
    low=u.lower()
    return any(x in low for x in SKIP_WORDS) or urlparse(low).path.endswith(SKIP_EXTS)

def norm(t):
    return re.sub(r"\s+"," ",t or "").strip()

def title_from_page(soup):
    h=soup.find("h1")
    if h and norm(h.get_text(" ",strip=True)):
        return norm(h.get_text(" ",strip=True))
    og=soup.find("meta",attrs={"property":"og:title"})
    if og and og.get("content"):
        return norm(og["content"])
    t=soup.find("title")
    return norm(t.get_text(" ",strip=True)) if t else ""

def clean_music_title(t):
    t=norm(t)
    t=re.sub(r"\b(download|mp3|new song|latest zambian music|zambian music|official audio|official video)\b","",t,flags=re.I)
    t=re.sub(r"\((prod\.?|produced).*?\)","",t,flags=re.I)
    t=re.sub(r"\[(prod\.?|produced).*?\]","",t,flags=re.I)
    return norm(t).strip(" -–—|:•")

def parse_artist_song(raw):
    t=clean_music_title(raw)
    t=re.sub(r"\s*[-|]\s*(Zambianplay|I Love Zed Music|Zedwap|Zedhouse Zambia|CK Music Promos).*$","",t,flags=re.I)
    for sep in [" – "," — "," - "," | "," : "]:
        if sep in t:
            a,s=t.split(sep,1)
            a=clean_music_title(a)
            s=clean_music_title(s)
            a=re.split(r"\s+(ft\.?|feat\.?|featuring)\s+",a,flags=re.I)[0]
            if a and s:
                return a,s
    return "UNKNOWN",t

def hash_text(x):
    return hashlib.sha256(x.encode()).hexdigest()

def extract(page_url, html):
    soup=BeautifulSoup(html,"lxml")
    raw=title_from_page(soup)
    artist,song=parse_artist_song(raw)
    domain=urlparse(page_url).netloc.lower()
    phash=hash_text(page_url)

    page_row={
        "artist_sort":artist.lower(),
        "artist":artist,
        "title":song or raw,
        "source_page":page_url,
        "audio_url":"",
        "source_domain":domain,
        "extractor":"source-page",
        "page_hash":phash
    }

    audio_rows=[]
    candidates=[]

    for tag in soup.find_all(["audio","source"]):
        src=tag.get("src")
        typ=tag.get("type","")
        if src and (tag.name=="audio" or "audio" in typ.lower() or is_audio(src)):
            candidates.append((clean(urljoin(page_url,src)),tag.name))

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
                candidates.append((clean(urljoin(page_url,c)),f"meta:{val}"))

    links=[]
    for a in soup.find_all("a",href=True):
        href=clean(urljoin(page_url,a["href"]))
        if not href.startswith(("http://","https://")):
            continue
        if is_audio(href):
            candidates.append((href,"direct-audio-link"))
        elif not skip(href):
            links.append(href)

    seen=set()
    for au,ex in candidates:
        if au in seen:
            continue
        seen.add(au)
        audio_rows.append({
            "artist_sort":artist.lower(),
            "artist":artist,
            "title":song or raw,
            "source_page":page_url,
            "audio_url":au,
            "source_domain":domain,
            "extractor":ex,
            "page_hash":phash
        })

    return page_row,audio_rows,links

def read_seeds():
    out=[]
    with open(SEEDS,encoding="utf-8") as f:
        for line in f:
            u=clean(line)
            if u and not u.startswith("#"):
                out.append(u)
    return out

def write_csv(path,rows):
    fields=["artist_sort","artist","title","source_page","audio_url","source_domain","extractor","page_hash"]
    rows=sorted(rows,key=lambda r:(r["artist_sort"],r["artist"].lower(),r["title"].lower(),r["source_page"]))
    with open(path,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    return len(rows)

def main():
    import argparse
    p=argparse.ArgumentParser()
    p.add_argument("--max-pages",type=int,default=30)
    p.add_argument("--sleep",type=float,default=1.0)
    args=p.parse_args()

    seeds=read_seeds()
    domains={urlparse(u).netloc.lower() for u in seeds}
    q=deque(seeds)
    visited=set()
    pages=[]
    audios=[]
    seen_pages=set()
    seen_audio=set()

    s=requests.Session()
    s.headers.update({"User-Agent":"ZAMCOPS-ZambianMusicSourceDiscovery/1.0"})

    while q and len(visited)<args.max_pages:
        url=clean(q.popleft())
        dom=urlparse(url).netloc.lower()
        if url in visited or dom not in domains or skip(url):
            continue

        visited.add(url)
        print(f"FETCH {len(visited)}/{args.max_pages}: {url}")

        try:
            r=s.get(url,timeout=20,allow_redirects=True)
            if r.status_code>=400:
                print(f"SKIP status={r.status_code}")
                continue
            if "text/html" not in r.headers.get("content-type","").lower():
                continue

            final=clean(r.url)
            page,audio,links=extract(final,r.text)

            if page["source_page"] not in seen_pages:
                seen_pages.add(page["source_page"])
                pages.append(page)

            for row in audio:
                k=(row["source_page"],row["audio_url"])
                if k not in seen_audio:
                    seen_audio.add(k)
                    audios.append(row)

            for link in links:
                if urlparse(link).netloc.lower() in domains and link not in visited:
                    q.append(link)

            time.sleep(args.sleep)

        except Exception as e:
            print(f"ERROR {url}: {e}")

    pc=write_csv(OUT_PAGES,pages)
    ac=write_csv(OUT_AUDIO,audios)
    print("")
    print(f"DONE source pages: {OUT_PAGES} rows={pc}")
    print(f"DONE audio URLs:   {OUT_AUDIO} rows={ac}")
    print("Sorted alphabetically by artist.")

if __name__=="__main__":
    main()
