"""
Helpers for radio-zambia.com station discovery and stream extraction.
"""
from __future__ import annotations

import re
import ssl
import urllib.parse
import urllib.request

UA = "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0 Safari/537.36"
ROOT = "https://radio-zambia.com/"


def fetch_html(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, context=ctx, timeout=60) as r:
        return r.read().decode("utf-8", errors="replace")


def discover_station_paths() -> list[str]:
    html = fetch_html(ROOT)
    # Pattern used on country index pages, e.g. /308-hot-fm.html
    paths = sorted(set(re.findall(r"/\d+-[a-z0-9-]+\.html", html, re.I)))
    return paths


def extract_station_name(page_html: str) -> str:
    # Prefer title prefix, fallback to first h1
    m = re.search(r"<title>(.*?)</title>", page_html, re.I | re.S)
    if m:
        t = re.sub(r"\s+", " ", m.group(1)).strip()
        if "—" in t:
            t = t.split("—", 1)[0].strip()
        if t:
            return t
    h1 = re.search(r"<h1[^>]*>(.*?)</h1>", page_html, re.I | re.S)
    if h1:
        t = re.sub(r"<[^>]+>", " ", h1.group(1))
        t = re.sub(r"\s+", " ", t).strip()
        if t:
            return t
    return ""


def extract_stream_urls(page_html: str) -> list[str]:
    urls = sorted(set(re.findall(r"https?://[^\"'\s<>]+", page_html)))
    out: list[str] = []
    for u in urls:
        low = u.lower()
        if any(
            k in low
            for k in (
                "stream",
                "listen.mp3",
                ".m3u8",
                "shoutcast",
                "icecast",
                "radio.co",
                "rcast.net",
                "yesstreaming.net",
                "zeno.fm",
            )
        ):
            out.append(u.rstrip(".,);"))
    return out


def discover_radio_zambia_streams() -> list[tuple[str, str, str]]:
    """
    Return tuples: (path, station_name, stream_url)
    """
    out: list[tuple[str, str, str]] = []
    for path in discover_station_paths():
        page_url = urllib.parse.urljoin(ROOT, path)
        try:
            html = fetch_html(page_url)
        except Exception:
            continue
        name = extract_station_name(html) or path
        urls = extract_stream_urls(html)
        if not urls:
            continue
        # First candidate is usually the direct player stream.
        out.append((path, name, urls[0]))
    return out
