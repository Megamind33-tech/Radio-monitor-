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


def fetch_html(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, context=ctx, timeout=60) as r:
        return r.read().decode("utf-8", errors="replace")


def should_skip_listing_path(path: str) -> bool:
    """Skip region/country indexes and non-station links."""
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
    return False


def discover_streema_station_paths() -> list[str]:
    """Unique station profile paths from paginated Zambia country listing."""
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
