"""
OnlineRadioBox (onlineradiobox.com) Zambia: discover station pages and extract
direct stream URLs from `station_play` buttons (`stream="..."`).

No official API — HTML only. Used alongside ICY probing in zambia_station_harvest.
"""
from __future__ import annotations

import re
import ssl
import urllib.request

UA = "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0 Safari/537.36"
ORB_ORIGIN = "https://onlineradiobox.com"
ZM_COUNTRY_PATH = "/zm/"

# City / region listing pages under /zm/ — fetch each to collect more `station_play` entries.
# Slugs match hrefs on the Zambia country page (case-sensitive in URLs).
ORB_CITY_SLUGS: tuple[str, ...] = (
    "Chikuni",
    "Chingola",
    "Chipata",
    "Choma",
    "Isoka",
    "Itezhitezhi",
    "Kabwe",
    "Kalomo",
    "Kalulushi",
    "Kapiri_Mposhi",
    "Kasama",
    "Katete",
    "Keembe",
    "Kitwe",
    "Luanshya",
    "Lundazi",
    "Lunga",
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
    "Senga_Hill",
    "Sesheke",
    "Umezi",
    "White_Mwandi",
)


def fetch_orb_html(path: str) -> str:
    """path starts with / e.g. /zm/phoenix/"""
    url = ORB_ORIGIN + path
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, context=ctx, timeout=60) as r:
        return r.read().decode("utf-8", errors="replace")


def slug_to_city_name(slug: str) -> str:
    """Kapiri_Mposhi -> Kapiri Mposhi"""
    return slug.replace("_", " ").strip()


def is_offline_placeholder(html: str) -> bool:
    """Heuristic: skip pages that look like 'station not available' / offline."""
    low = html.lower()
    if "this station is offline" in low or "station is currently offline" in low:
        return True
    if "radio station not found" in low and "404" in low:
        return True
    return False


def extract_paths_from_zm_listing(html: str) -> set[str]:
    """href targets under /zm/ from a listing page (country or city)."""
    out: set[str] = set()
    for m in re.finditer(r'href="(/zm/[^"#?]+)"', html):
        p = m.group(1).rstrip("/")
        if not p or p == "/zm":
            continue
        if "/genre/" in p:
            continue
        # normalize to path with trailing slash for fetch
        out.add(p + "/")
    return out


def is_city_listing_path(path: str) -> bool:
    """Path is /zm/Lusaka/ style city index (not a station slug)."""
    slug = path.rstrip("/").rsplit("/", 1)[-1]
    for c in ORB_CITY_SLUGS:
        if slug.lower() == c.lower():
            return True
    return False


def extract_station_play_buttons(html: str) -> list[tuple[str, str, str]]:
    """
    Returns list of (stream_url, radio_name, radio_id).
    Parses `station_play` buttons only (same stream as the site player).
    """
    out: list[tuple[str, str, str]] = []
    for m in re.finditer(r'class="[^"]*station_play[^"]*"([^>]*)>', html, re.I):
        attrs = m.group(1)
        sm = re.search(r'stream="(https?://[^"]+)"', attrs, re.I)
        if not sm:
            continue
        url = sm.group(1).strip()
        nm_m = re.search(r'radioName="([^"]*)"', attrs)
        id_m = re.search(r'radioId="([^"]*)"', attrs)
        name = (nm_m.group(1) if nm_m else "").strip()
        rid = (id_m.group(1) if id_m else "").strip()
        if url.startswith("http"):
            out.append((url, name, rid))
    return out


def discover_orb_paths_to_fetch() -> list[str]:
    """
    Country page + each city listing + individual station pages linked from country
    (excluding genre links and city index paths — those are fetched explicitly).
    """
    html = fetch_orb_html(ZM_COUNTRY_PATH)
    paths = extract_paths_from_zm_listing(html)
    # Explicit city listings (covers pagination / completeness)
    for slug in ORB_CITY_SLUGS:
        paths.add(f"/zm/{slug}/")

    station_pages: list[str] = []
    for p in sorted(paths):
        if "/genre/" in p:
            continue
        if is_city_listing_path(p):
            continue
        station_pages.append(p)
    # Country + city indices + per-station pages
    index_paths = [ZM_COUNTRY_PATH] + [f"/zm/{s}/" for s in ORB_CITY_SLUGS]
    all_fetch = sorted(set(index_paths + station_pages))
    return all_fetch
