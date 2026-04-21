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
import sys
from dataclasses import dataclass, asdict

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

import aiohttp

from mytuner_zambia import (
    decrypt_playlist_entry,
    discover_mytuner_zambia_station_page_urls,
    fetch_text,
    normalize_url,
    parse_station_page,
    radio_id_from_mytuner_url,
)


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
    unique_urls = discover_mytuner_zambia_station_page_urls()
    existing = load_existing_stream_urls(existing_json) if existing_json else set()

    rows: list[Row] = []
    connector = aiohttp.TCPConnector(limit=15)
    async with aiohttp.ClientSession(connector=connector) as session:
        for page_url in unique_urls:
            radio_id = radio_id_from_mytuner_url(page_url)

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
                su = "".join(ch for ch in su if ch == "\t" or ord(ch) >= 32).strip()
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
