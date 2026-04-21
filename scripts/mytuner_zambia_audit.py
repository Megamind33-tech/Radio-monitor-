#!/usr/bin/env python3
"""
Fetch all ~70 Zambia stations from myTuner (JSON-LD), resolve each station page,
decrypt stream URL using the same AES-CFB logic as the site (timestamp from
#last-update), probe ICY metadata, optionally skip URLs already in a harvest JSON.

Outputs: scripts/data/mytuner_zambia_audit.csv (real measurements, no mock data)

MyTuner web "now playing" API (metadata-api.mytuner.mobi) uses a signed
Authorization header from obfuscated JS — not replicated here. Workers should
use the decrypted direct stream URL + ICY (or ffprobe), same as other sources.
"""
from __future__ import annotations

import asyncio
import csv
import json
import os
import re
import ssl
import sys
import urllib.request
from dataclasses import dataclass, asdict
from typing import Any

try:
    from Crypto.Cipher import AES
except ImportError:
    print("pip install pycryptodome", file=sys.stderr)
    raise

import aiohttp

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


def load_existing_stream_urls(path: str) -> set[str]:
    if not os.path.isfile(path):
        return set()
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    urls = set()
    for row in data.get("stations", []):
        u = (row.get("streamUrl") or "").strip().lower()
        if u:
            urls.add(normalize_url(u))
    return urls


def normalize_url(u: str) -> str:
    u = u.strip().lower()
    u = u.split(";", 1)[0]  # strip ;stream.mp3 extras for compare
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
    iv_hex = remove_null_prefix(iv_)
    ct_b64 = remove_null_prefix(cipher_b64)
    import base64

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


async def icy_first_title(session: aiohttp.ClientSession, stream_url: str) -> tuple[str | None, str | None, str | None]:
    """Returns (metaint, stream_title_or_empty, error)."""
    headers = {"Icy-MetaData": "1", "User-Agent": "ZambiaMonitor/1.0 ICY"}
    try:
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=12, sock_read=20)
        async with session.get(stream_url, headers=headers, timeout=timeout) as resp:
            mi = resp.headers.get("icy-metaint")
            if not mi:
                return None, None, "no_icy_metaint"
            metaint = int(mi)
            left = metaint
            while left > 0:
                chunk = await resp.content.read(min(32768, left))
                if not chunk:
                    return mi, None, "eof_before_meta"
                left -= len(chunk)
            lb = await resp.content.readexactly(1)
            n = lb[0] * 16
            if n == 0:
                return mi, "", None
            meta = await resp.content.readexactly(n)
            text = meta.rstrip(b"\x00").decode("utf-8", errors="replace")
            title = None
            for field in text.split(";"):
                field = field.strip()
                if field.startswith("StreamTitle="):
                    v = field[len("StreamTitle=") :].strip().strip("'").strip('"')
                    title = v.strip()
                    break
            return mi, title, None
    except Exception as e:
        return None, None, repr(e)


@dataclass
class Row:
    mytuner_url: str
    station_name: str
    radio_id: str
    stream_url: str
    decrypt_ok: bool
    duplicate_of_existing: bool
    icy_metaint: str
    icy_stream_title_sample: str
    icy_error: str
    verdict: str


async def main_async(existing_json: str | None, out_csv: str) -> None:
    all_urls: list[str] = []
    for page in (1, 2):
        url = LIST_BASE if page == 1 else f"{LIST_BASE}?page=2"
        html = fetch_text(url)
        all_urls.extend(parse_ld_station_urls(html))

    seen: set[str] = set()
    unique_urls: list[str] = []
    for u in all_urls:
        if u not in seen:
            seen.add(u)
            unique_urls.append(u)

    existing = load_existing_stream_urls(existing_json) if existing_json else set()

    rows: list[Row] = []
    connector = aiohttp.TCPConnector(limit=15)
    async with aiohttp.ClientSession(connector=connector) as session:
        for page_url in unique_urls:
            rid_m = re.search(r"-(\d+)/?$", page_url)
            radio_id = rid_m.group(1) if rid_m else ""

            try:
                html = fetch_text(page_url)
            except Exception as e:
                rows.append(
                    Row(
                        mytuner_url=page_url,
                        station_name="",
                        radio_id=radio_id,
                        stream_url="",
                        decrypt_ok=False,
                        duplicate_of_existing=False,
                        icy_metaint="",
                        icy_stream_title_sample="",
                        icy_error=f"page_fetch:{e!r}",
                        verdict="page_error",
                    )
                )
                continue

            ts, playlist, rg_name = parse_station_page(html)
            name = rg_name or ""

            if not ts or not playlist:
                rows.append(
                    Row(
                        mytuner_url=page_url,
                        station_name=name,
                        radio_id=radio_id,
                        stream_url="",
                        decrypt_ok=False,
                        duplicate_of_existing=False,
                        icy_metaint="",
                        icy_stream_title_sample="",
                        icy_error="no_playlist_or_timestamp",
                        verdict="parse_error",
                    )
                )
                continue

            entry = playlist[0]
            try:
                su = decrypt_playlist_entry(
                    "/null/" + entry["iv"],
                    "/null/" + entry["cipher"],
                    ts,
                )
                url_m = re.search(r"https?://[^\s\x00]+", su)
                su = url_m.group(0).rstrip(".,);") if url_m else ""
                decrypt_ok = su.startswith("http")
            except Exception as e:
                su = ""
                decrypt_ok = False
                err_decrypt = repr(e)
            else:
                err_decrypt = ""

            dup = normalize_url(su) in existing if su else False

            mi_s, title, icy_err = ("", "", err_decrypt or "")
            if decrypt_ok and su:
                mi_s, title, icy_err = await icy_first_title(session, su)

            if not decrypt_ok:
                verdict = "decrypt_fail"
            elif dup:
                verdict = "duplicate_url"
            elif icy_err and icy_err != "no_icy_metaint":
                verdict = f"stream_{icy_err[:40]}"
            elif icy_err == "no_icy_metaint":
                verdict = "no_icy"
            elif title and len(title.strip()) > 1 and title.strip() not in {"-", " - "}:
                verdict = "icy_ok"
            else:
                verdict = "icy_empty"

            rows.append(
                Row(
                    mytuner_url=page_url,
                    station_name=name,
                    radio_id=radio_id,
                    stream_url=su,
                    decrypt_ok=decrypt_ok,
                    duplicate_of_existing=dup,
                    icy_metaint=mi_s or "",
                    icy_stream_title_sample=(title or "")[:300],
                    icy_error=(icy_err or "")[:200],
                    verdict=verdict,
                )
            )

    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(rows[0]).keys()) if rows else [])
        if rows:
            w.writeheader()
            for r in rows:
                w.writerow(asdict(r))

    ok = sum(1 for r in rows if r.verdict == "icy_ok")
    dup = sum(1 for r in rows if r.duplicate_of_existing)
    print(f"MyTuner Zambia: {len(unique_urls)} station pages")
    print(f"CSV: {out_csv}")
    print(f"ICY sample with title text: {ok}")
    print(f"Duplicate of existing harvest URL: {dup}")


def main():
    existing = sys.argv[1] if len(sys.argv) > 1 else "scripts/data/zambia_harvest.json"
    out = sys.argv[2] if len(sys.argv) > 2 else "scripts/data/mytuner_zambia_audit.csv"
    if not os.path.isfile(existing):
        existing = ""
    asyncio.run(main_async(existing or None, out))


if __name__ == "__main__":
    main()
