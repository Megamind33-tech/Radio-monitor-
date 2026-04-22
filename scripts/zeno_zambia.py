"""
Discover Zambia stations listed on Zeno and extract direct stream URLs.

This module is intentionally conservative:
- It only keeps stations whose page metadata suggests Zambia.
- It only returns direct stream URLs that start with stream.zeno.fm or stream-*.zeno.fm.
"""
from __future__ import annotations

import json
import re
import ssl
import urllib.parse
import urllib.request
from dataclasses import dataclass

UA = "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0 Safari/537.36"
ZENO_SEARCH_URL = "https://content-api.zeno.fm/api/v1/search"
ZENO_RADIO_PAGE = "https://zeno.fm/radio/"


@dataclass
class ZenoStation:
    slug: str
    name: str
    stream_url: str
    country: str
    language: str


def fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, context=ctx, timeout=60) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))


def fetch_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, context=ctx, timeout=60) as r:
        return r.read().decode("utf-8", errors="replace")


def _extract_zeno_stream_from_html(html: str) -> str:
    # Match direct canonical stream forms. Ignore arbitrary links from ad payloads.
    m = re.search(r"https://stream(?:-[0-9]+)?\.zeno\.fm/[a-z0-9]+", html, re.I)
    return m.group(0) if m else ""


def _station_lookup_candidates(query: str, page_limit: int = 6, page_size: int = 50) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for page in range(1, page_limit + 1):
        params = urllib.parse.urlencode(
            {
                "query": query,
                "page": page,
                "perPage": page_size,
            }
        )
        url = f"{ZENO_SEARCH_URL}?{params}"
        try:
            data = fetch_json(url)
        except Exception:
            continue
        rows = data.get("results") or data.get("data") or []
        if not isinstance(rows, list):
            continue
        if not rows:
            break
        for row in rows:
            if not isinstance(row, dict):
                continue
            slug = (row.get("slug") or row.get("id") or "").strip()
            if not slug or slug in seen:
                continue
            seen.add(slug)
            out.append(row)
    return out


def _looks_zambia(row: dict) -> bool:
    hay = " ".join(
        str(row.get(k, "") or "")
        for k in ("name", "title", "description", "country", "countryName", "city")
    ).lower()
    if "zambia" in hay:
        return True
    # Allow country code hints in some payload variants.
    return " zm " in f" {hay} " or "zmb" in hay


def discover_zeno_zambia_stations(max_results: int = 220) -> list[ZenoStation]:
    """
    Return Zambia stations discovered via Zeno search API + page verification.
    """
    queries = [
        "zambia",
        "lusaka zambia",
        "kitwe zambia",
        "ndola zambia",
        "fm zambia",
        "radio zambia",
    ]
    by_slug: dict[str, ZenoStation] = {}
    for q in queries:
        rows = _station_lookup_candidates(q)
        for row in rows:
            if not _looks_zambia(row):
                continue
            slug = (row.get("slug") or row.get("id") or "").strip()
            if not slug:
                continue
            if slug in by_slug:
                continue
            page_url = urllib.parse.urljoin(ZENO_RADIO_PAGE, slug.rstrip("/") + "/")
            try:
                html = fetch_text(page_url)
            except Exception:
                continue
            stream_url = _extract_zeno_stream_from_html(html)
            if not stream_url.startswith("https://stream"):
                continue
            name = (row.get("name") or row.get("title") or slug).strip()
            country = str(row.get("country") or row.get("countryName") or "").strip()
            language = str(row.get("language") or row.get("languageName") or "").strip()
            by_slug[slug] = ZenoStation(
                slug=slug,
                name=name,
                stream_url=stream_url,
                country=country,
                language=language,
            )
            if len(by_slug) >= max_results:
                break
        if len(by_slug) >= max_results:
            break
    return list(by_slug.values())
