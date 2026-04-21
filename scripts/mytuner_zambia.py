"""
Shared MyTuner Radio (Zambia) helpers: list pages, decrypt playlist AES-CFB URLs.
Used by mytuner_zambia_audit.py and zambia_station_harvest.py.
"""
from __future__ import annotations

import json
import re
import ssl
import urllib.request
from typing import Any

try:
    from Crypto.Cipher import AES
except ImportError:  # pragma: no cover
    AES = None  # type: ignore

UA = "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0 Safari/537.36"
LIST_BASE = "https://mytuner-radio.com/radio/country/zambia-stations"


def fetch_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, context=ctx, timeout=60) as r:
        return r.read().decode("utf-8", errors="replace")


def parse_ld_station_urls(html: str) -> list[str]:
    m = re.search(
        r'<script type="application/ld\+json">\s*(\[.*?\])\s*</script>',
        html,
        re.S,
    )
    if not m:
        return []
    data = json.loads(m.group(1))
    urls: list[str] = []
    for block in data:
        if block.get("@type") == "ItemList":
            for it in block.get("itemListElement", []):
                u = it.get("url")
                if u:
                    urls.append(u)
    return urls


def normalize_url(u: str) -> str:
    u = u.strip().lower()
    u = u.split(";", 1)[0]
    return u.rstrip("/")


def remove_null_prefix(s: str) -> str:
    if len(s) > 6:
        return s[6:]
    return s


def genk(s: str) -> str:
    h = ""
    j = 0
    for _ in range(32):
        h += format(ord(s[j]), "x")
        j += 1
        if j >= len(s):
            j = 0
    return h


def decrypt_playlist_entry(iv_: str, cipher_b64: str, timestamp: str) -> str:
    if AES is None:
        raise RuntimeError("pycryptodome required")
    import base64

    iv_hex = remove_null_prefix(iv_)
    ct_b64 = remove_null_prefix(cipher_b64)
    iv = bytes.fromhex(iv_hex)
    ct_raw = base64.b64decode(ct_b64)
    key_hex = genk(str(timestamp))
    key = bytes.fromhex(key_hex)
    cipher = AES.new(key, AES.MODE_CFB, iv, segment_size=128)
    pt = cipher.decrypt(ct_raw)
    return pt.decode("utf-8", errors="replace").strip()


def parse_station_page(html: str) -> tuple[str | None, list[dict[str, Any]] | None, str | None]:
    ts_m = re.search(r'id="last-update"\s+data-timestamp="(\d+)"', html)
    timestamp = ts_m.group(1) if ts_m else None

    pl_m = re.search(
        r"var _playlist = formatPlaylist\((\[.*?\])\)\s*;",
        html,
        re.S,
    )
    playlist = None
    if pl_m:
        try:
            playlist = json.loads(pl_m.group(1))
        except json.JSONDecodeError:
            playlist = None

    name_m = re.search(r'"@type":\s*"RadioStation"[^}]*"name":\s*"([^"]*)"', html)
    name = name_m.group(1) if name_m else None

    return timestamp, playlist, name


def radio_id_from_mytuner_url(page_url: str) -> str:
    rid_m = re.search(r"-(\d+)/?$", page_url)
    return rid_m.group(1) if rid_m else ""


def decrypt_first_stream_url(html: str, page_url: str) -> tuple[str, str, str]:
    """
    Return (stream_url, station_display_name, radio_id).
    stream_url is empty if decrypt/parse failed.
    """
    ts, playlist, rg_name = parse_station_page(html)
    name = (rg_name or "").strip()
    rid = radio_id_from_mytuner_url(page_url)
    if not ts or not playlist:
        return "", name, rid
    entry = playlist[0]
    try:
        su = decrypt_playlist_entry(
            "/null/" + entry["iv"],
            "/null/" + entry["cipher"],
            ts,
        )
        url_m = re.search(r"https?://[^\s\x00]+", su)
        su = url_m.group(0).rstrip(".,);") if url_m else ""
        # strip control chars MyTuner sometimes leaves on the URL tail
        su = "".join(ch for ch in su if ch == "\t" or ord(ch) >= 32).strip()
        if not su.startswith("http"):
            su = ""
    except Exception:
        su = ""
    return su, name, rid


def discover_mytuner_zambia_station_page_urls() -> list[str]:
    """Unique station page URLs from country listing (pages 1–2)."""
    all_urls: list[str] = []
    for page in (1, 2):
        url = LIST_BASE if page == 1 else f"{LIST_BASE}?page=2"
        html = fetch_text(url)
        all_urls.extend(parse_ld_station_urls(html))
    seen: set[str] = set()
    out: list[str] = []
    for u in all_urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out
