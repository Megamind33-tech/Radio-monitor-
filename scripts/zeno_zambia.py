"""
Discover Zambia stations listed on Zeno and extract direct stream URLs.

This module is intentionally conservative:
- It only keeps stations whose page metadata suggests Zambia.
- It only returns direct stream URLs that start with stream.zeno.fm or stream-*.zeno.fm.
"""
from __future__ import annotations

import re
import ssl
import urllib.parse
import urllib.request
from dataclasses import dataclass

UA = "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0 Safari/537.36"
ZENO_RADIO_PAGE = "https://zeno.fm/radio/"


@dataclass
class ZenoStation:
    slug: str
    name: str
    stream_url: str
    country: str
    language: str


def fetch_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, context=ctx, timeout=60) as r:
        return r.read().decode("utf-8", errors="replace")


def _extract_zeno_stream_from_html(html: str) -> str:
    # Match direct canonical stream forms. Ignore arbitrary links from ad payloads.
    m = re.search(r"https://stream(?:-[0-9]+)?\.zeno\.fm/[a-z0-9]+", html, re.I)
    return m.group(0) if m else ""


def discover_zeno_station_slugs(queries: list[str], max_slugs: int = 400) -> list[str]:
    """
    Zeno's old content API search endpoint is no longer public.
    Discover station slugs from public /search pages instead.
    """
    out: list[str] = []
    seen: set[str] = set()
    for q in queries:
        url = "https://zeno.fm/search/?q=" + urllib.parse.quote(q)
        try:
            html = fetch_text(url)
        except Exception:
            continue
        slugs = re.findall(r"/radio/([a-z0-9-]+)/", html, re.I)
        for slug in slugs:
            s = slug.strip().lower()
            if not s or s in seen:
                continue
            seen.add(s)
            out.append(s)
            if len(out) >= max_slugs:
                return out
    return out


def discover_zeno_zambia_stations(max_results: int = 220) -> list[ZenoStation]:
    """
    Return Zambia stations discovered from public Zeno search pages + page verification.
    """
    queries = [
        "zambia",
        "lusaka zambia",
        "kitwe zambia",
        "ndola zambia",
        "livingstone zambia",
        "chipata zambia",
        "kabwe zambia",
        "mongu zambia",
        "solwezi zambia",
        "mansa zambia",
        "kasama zambia",
        "luanshya zambia",
        "petauke zambia",
        "mazabuka zambia",
        "fm zambia",
        "radio zambia",
    ]
    by_slug: dict[str, ZenoStation] = {}
    slugs = discover_zeno_station_slugs(queries, max_slugs=max(max_results * 3, 500))
    for slug in slugs:
        if slug in by_slug:
            continue
        page_url = urllib.parse.urljoin(ZENO_RADIO_PAGE, slug.rstrip("/") + "/")
        try:
            html = fetch_text(page_url)
        except Exception:
            continue

        # Strict Zambia check from station page metadata/text.
        low = html.lower()
        if "zambia" not in low and " zmb " not in f" {low} " and "country\":\"zambia" not in low:
            continue

        stream_url = _extract_zeno_stream_from_html(html)
        if not stream_url.startswith("https://stream"):
            continue

        # Prefer page title for station name.
        title_match = re.search(r"<title>(.*?)</title>", html, re.I | re.S)
        title = title_match.group(1).strip() if title_match else slug
        if "|" in title:
            title = title.split("|", 1)[0].strip()
        if "—" in title:
            title = title.split("—", 1)[0].strip()
        name = title or slug

        by_slug[slug] = ZenoStation(
            slug=slug,
            name=name,
            stream_url=stream_url,
            country="Zambia",
            language="",
        )
        if len(by_slug) >= max_results:
            break
    return list(by_slug.values())
