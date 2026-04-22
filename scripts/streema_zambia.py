"""
Streema.com Zambia listings: paginate country page, fetch each station profile,
extract stream URL from #source-stream data-src (same source the web player uses).

Real HTML only — no invented streams. ICY quality is decided in zambia_station_harvest.
"""
from __future__ import annotations

import re
import ssl
import urllib.request

UA = "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0 Safari/537.36"
STREEMA_ZM_LIST = "https://streema.com/radios/country/Zambia"

# Streema links many geographic “browse by city/region” pages from the Zambia index.
# Those pages have no #source-stream — we skip by slug so we never treat them as stations.
# Real stations use slugs like Hot_FM_877 or Choma_Maanu_Radio_Station, not bare place names.
STREEMA_GEO_LISTING_SLUGS: frozenset[str] = frozenset(
    {
        "Chama",
        "Chibombo",
        "Chiengi",
        "Chikuni",
        "Chingola",
        "Chipata",
        "Chirundu",
        "Choma",
        "Kabwe",
        "Kafue",
        "Kalomo",
        "Kapiri_Mposhi",
        "Kasama",
        "Katete",
        "Kawambwa",
        "Kitwe",
        "Livingstone",
        "Luanshya",
        "Lundazi",
        "Lusaka",
        "Mansa",
        "Mazabuka",
        "Mbala",
        "Mkushi",
        "Mongu",
        "Mpongwe",
        "Mpulungu",
        "Muchinga",
        "Mufulira",
        "Mungwi",
        "Ndola",
        "Sesheke",
        "Solwezi",
        "Isoka",
        "Itezhi_Tezhi",
        "Kalulushi",
        "Senga_Hill",
        "White_Mwandi",
        "Umezi",
        "Lunga",
        "Keembe",
        "Petauke",
        "Samfya",
        "Mwinilunga",
        "Nakonde",
    }
)


def fetch_html(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, context=ctx, timeout=60) as r:
        return r.read().decode("utf-8", errors="replace")


def should_skip_listing_path(path: str) -> bool:
    """Skip region/country indexes, city browse pages, and non-station links."""
    if not path.startswith("/radios/"):
        return True
    low = path.lower()
    if "/radios/country/" in path or "/radios/region/" in path:
        return True
    if "/state/" in path or "/city/" in path:
        return True
    if "/play/" in path or path.rstrip("/").endswith("/play"):
        return True
    if low.endswith("/search") or "/search?" in path:
        return True
    tail = path.rstrip("/").split("/")[-1]
    if not tail or len(tail) < 2:
        return True
    if tail in STREEMA_GEO_LISTING_SLUGS:
        return True
    return False


def discover_streema_station_paths() -> list[str]:
    """Unique paths from the Zambia country listing (city/region slugs excluded — see above)."""
    seen: set[str] = set()
    page = 1
    max_pages = 35
    while page <= max_pages:
        url = STREEMA_ZM_LIST if page == 1 else f"{STREEMA_ZM_LIST}?page={page}"
        try:
            html = fetch_html(url)
        except Exception:
            break
        batch: set[str] = set()
        for m in re.finditer(r'href="(/radios/[^"#?]+)"', html):
            p = m.group(1).split("?")[0].rstrip("/")
            if should_skip_listing_path(p):
                continue
            batch.add(p if p.startswith("/") else "/" + p)
        if not batch:
            break
        n0 = len(seen)
        seen |= batch
        if len(seen) == n0:
            break
        page += 1
    return sorted(seen)


def extract_stream_from_station_html(html: str) -> str | None:
    m = re.search(
        r'<div[^>]*id="source-stream"[^>]*data-src=[\'"]([^\'"]+)[\'"]',
        html,
        re.I,
    )
    if not m:
        return None
    u = m.group(1).strip().rstrip(";").strip()
    u = u.rstrip(";").strip()
    if u.startswith("http"):
        return u
    return None


def extract_station_title(html: str) -> str | None:
    m = re.search(r'<meta\s+property="og:title"\s+content="([^"]+)"', html, re.I)
    if m:
        t = m.group(1).strip()
        if " - " in t:
            t = t.split(" - ")[0].strip()
        return t
    m = re.search(r"<title>([^<]+)</title>", html, re.I)
    return (m.group(1).strip() if m else None) or None
