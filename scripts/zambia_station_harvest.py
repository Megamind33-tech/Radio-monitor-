#!/usr/bin/env python3
"""
Harvest Zambian radio stations from MyTuner, OnlineRadioBox, Radio Garden,
radio-browser, and TuneIn; resolve stream URLs, probe ICY metadata (up to 3 blocks),
classify, write JSON for Prisma import.

Target: up to --max-stations (default 150) unique entries.
"""
from __future__ import annotations

import os
import sys

# Allow `python3 scripts/zambia_station_harvest.py` to import sibling modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
import asyncio
import json
import re
import ssl
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import dataclasses
from dataclasses import dataclass, field

import aiohttp

from zambia_provinces import province_for_place, province_for_tunein_district
from mytuner_zambia import (
    decrypt_first_stream_url,
    discover_mytuner_zambia_station_page_urls,
    fetch_text as mytuner_fetch_text,
    normalize_url as mytuner_normalize_url,
)
from onlineradio_zambia import (
    ORB_CITY_SLUGS,
    discover_orb_paths_to_fetch,
    extract_station_play_buttons,
    fetch_orb_html,
    is_offline_placeholder,
    slug_to_city_name,
)
from streema_zambia import (
    discover_streema_station_paths,
    extract_station_title,
    extract_stream_from_station_html,
    fetch_html as streema_fetch_html,
)

UA_BROWSER = "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0 Safari/537.36"
UA_ICY = "ZambiaStationHarvest/1.0"

# Radio Garden: Zambia country page lists all place maps (map id -> district name)
RG_ZAMBIA_PAGE = "https://radio.garden/api/ara/content/page/XbLRE6NT"


@dataclass
class Candidate:
    stable_id: str
    name: str
    district: str
    province: str
    frequency_mhz: str | None
    stream_url: str
    source: str  # radio_garden | tunein | radio_browser | mytuner | onlineradiobox | streema
    source_detail: str  # channel id or tunein id
    """All discovery sources for this stream URL (merged when same URL from multiple sites)."""
    source_map: dict[str, str] = field(default_factory=dict)
    icy_qualification: str = "pending"
    icy_sample_title: str | None = None

    def __post_init__(self) -> None:
        if not self.source_map and self.source:
            self.source_map = {self.source: self.source_detail}


def fetch_json(url: str) -> dict:
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": UA_BROWSER})
    with urllib.request.urlopen(req, context=ctx, timeout=45) as r:
        return json.loads(r.read().decode())


def discover_rg_place_maps(zambia_country_json: dict | None = None) -> list[tuple[str, str]]:
    """Return [(district_name, map_id), ...] from Radio Garden Zambia country page JSON."""
    j = zambia_country_json if zambia_country_json is not None else fetch_json(RG_ZAMBIA_PAGE)
    out: list[tuple[str, str]] = []
    for block in j.get("data", {}).get("content", []):
        for item in block.get("items", []):
            p = item.get("page", {})
            if p.get("type") == "page" and p.get("map") and "/visit/" in (p.get("url") or ""):
                title = (p.get("title") or "").strip()
                mid = p.get("map")
                if title and mid:
                    out.append((title, mid))
    return out


def rg_channel_pages_from_country_json(j: dict) -> list[dict]:
    """
    All `type: channel` entries on the Zambia country page (e.g. Popular Stations).
    These are official Radio Garden listings for https://radio.garden/visit/zambia/XbLRE6NT — we
    previously only crawled per-city maps and missed this block.
    """
    out: list[dict] = []
    for block in j.get("data", {}).get("content", []):
        for item in block.get("items", []):
            p = item.get("page", {})
            if p.get("type") != "channel":
                continue
            ctry = (p.get("country") or {}).get("title") or ""
            if ctry and str(ctry).strip().lower() != "zambia":
                continue
            out.append(p)
    return out


def rg_district_from_channel_page(page: dict) -> str:
    """Prefer Radio Garden `place.title`, then subtitle, else country-wide."""
    place = page.get("place") or {}
    pt = (place.get("title") or "").strip()
    if pt:
        return pt
    st = (page.get("subtitle") or "").strip()
    if st:
        return st
    return "Zambia"


def rg_channels_for_map(map_id: str) -> list[dict]:
    url = f"https://radio.garden/api/ara/content/page/{map_id}/channels"
    try:
        j = fetch_json(url)
        blocks = j.get("data", {}).get("content") or []
        if not blocks:
            return []
        items = blocks[0].get("items") or []
        return [x["page"] for x in items if x.get("page", {}).get("type") == "channel"]
    except Exception:
        return []


ZAMBIA_NAME_HINTS = frozenset(
    """
    zambia lusaka ndola kitwe kabwe chipata choma livingstone luanshya mongu solwezi kasama
    mufulira chingola kapiri petauke monze mazabuka kalomo sesheke senanga mansa samfya
    copperbelt luapala luapula zambezi nakonde isoka mbala mwinilunga ikelenge ndola
    luanshya kitwe chililabombwe kalulushi
    """.split()
)


def is_likely_zambia_station_name(name: str) -> bool:
    low = (name or "").lower()
    if "zambia" in low or "zm " in low or low.endswith(" zm"):
        return True
    return any(h in low for h in ZAMBIA_NAME_HINTS if len(h) > 3)


def extract_frequency_mhz(name: str) -> str | None:
    """Parse FM frequency like 94.5 or 104.1 from station name."""
    m = re.search(r"\b(\d{2,3}\.\d)\s*(?:fm|mhz)?\b", name, re.I)
    if m:
        return m.group(1)
    m = re.search(r"\b(\d{2,3}\.\d)\b", name)
    return m.group(1) if m else None


def channel_slug_from_url(url: str) -> str | None:
    # /listen/some-name/CHANNELID
    m = re.search(r"/listen/[^/]+/([^/]+)/?$", url or "")
    return m.group(1) if m else None


def fetch_radio_browser_zambia_stations() -> list[tuple[str, str]]:
    """Return [(name, stream_url), ...] for Zambia (ZM). Merges multiple API mirrors."""
    endpoints = [
        "http://all.api.radio-browser.info/json/stations/bycountrycodeexact/ZM",
        "http://all.api.radio-browser.info/json/stations/bycountry/Zambia",
        "http://de1.api.radio-browser.info/json/stations/bycountrycodeexact/ZM",
    ]
    by_url: dict[str, tuple[str, str]] = {}
    for u in endpoints:
        try:
            ctx = ssl.create_default_context()
            with urllib.request.urlopen(u, context=ctx, timeout=45) as r:
                data = json.loads(r.read().decode())
            for row in data:
                name = (row.get("name") or "").strip()
                su = (row.get("url_resolved") or row.get("url") or "").strip()
                if name and su.startswith("http") and su not in by_url:
                    by_url[su] = (name, su)
        except Exception:
            continue
    return list(by_url.values())


def tunein_search_stations(query: str = "zambia") -> list[tuple[str, str]]:
    """Return [(name, station_id like s12345), ...] from TuneIn OPML search."""
    url = f"https://opml.radiotime.com/Search.ashx?query={urllib.parse.quote(query)}&types=station"
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(url, context=ctx, timeout=45) as r:
        xml_text = r.read().decode("utf-8", errors="replace")
    root = ET.fromstring(xml_text)
    out: list[tuple[str, str]] = []
    for el in root.iter():
        if el.tag.endswith("outline") and el.get("type") == "audio":
            u = el.get("URL") or ""
            m = re.search(r"id=([sp][0-9]+)", u)
            if not m:
                continue
            sid = m.group(1)
            name = (el.get("text") or "").strip() or sid
            out.append((name, sid))
    return out


async def resolve_rg(session: aiohttp.ClientSession, channel_id: str) -> str:
    url = f"https://radio.garden/api/ara/content/listen/{channel_id}/channel.mp3"
    async with session.get(url, allow_redirects=True, headers={"User-Agent": UA_BROWSER}) as resp:
        return str(resp.url)


async def resolve_tunein(session: aiohttp.ClientSession, station_id: str) -> str:
    url = f"https://opml.radiotime.com/Tune.ashx?id={station_id}"
    async with session.get(url, allow_redirects=True) as resp:
        text = (await resp.text()).strip()
        line = text.split("\n")[0].strip()
        if line.startswith("http"):
            return line
        return text


def is_likely_junk_title(s: str | None) -> bool:
    if not s:
        return True
    t = s.strip()
    if len(t) < 2:
        return True
    if t in {"-", " - ", "..."}:
        return True
    low = t.lower()
    # Automation sometimes sends offline / placeholder strings instead of tracks
    if any(
        x in low
        for x in (
            "offline",
            "not broadcasting",
            "stream offline",
            "no stream",
            "temporarily unavailable",
            "under maintenance",
        )
    ):
        return True
    # Long base64-ish blobs (Radio Maria / auth payloads)
    if len(t) > 400 and re.match(r"^[A-Za-z0-9+/=_-]+$", t[:200]):
        return True
    if "FOTMUL" in t and len(t) > 100:
        return True
    return False


def title_quality(titles: list[str | None]) -> tuple[str, str | None]:
    """Return (qualification, best_sample)."""
    cleaned = [x.strip() for x in titles if x is not None]
    non_junk = [x for x in cleaned if not is_likely_junk_title(x)]
    if not non_junk:
        if any(cleaned):
            return "weak", cleaned[0][:200]
        return "none", None
    # Prefer longest non-junk that looks like artist-title
    best = max(non_junk, key=lambda s: ((" - " in s) * 50 + len(s)))
    if " - " in best or len(best) > 12:
        return "good", best[:500]
    if len(best) >= 4:
        return "partial", best[:500]
    return "weak", best[:500]


async def read_icy_blocks(session: aiohttp.ClientSession, stream_url: str, max_blocks: int = 3) -> list[str | None]:
    headers = {"Icy-MetaData": "1", "User-Agent": UA_ICY}
    titles: list[str | None] = []
    try:
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=22)
        async with session.get(stream_url, headers=headers, timeout=timeout) as resp:
            if not resp.headers.get("icy-metaint"):
                return titles
            metaint = int(resp.headers["icy-metaint"])
            for _ in range(max_blocks):
                left = metaint
                while left > 0:
                    chunk = await resp.content.read(min(32768, left))
                    if not chunk:
                        return titles
                    left -= len(chunk)
                lb = await resp.content.readexactly(1)
                n = lb[0] * 16
                if n == 0:
                    titles.append("")
                    continue
                meta = await resp.content.readexactly(n)
                text = meta.rstrip(b"\x00").decode("utf-8", errors="replace")
                title = None
                for field in text.split(";"):
                    field = field.strip()
                    if field.startswith("StreamTitle="):
                        v = field[len("StreamTitle=") :].strip()
                        if (v.startswith("'") and v.endswith("'")) or (v.startswith('"') and v.endswith('"')):
                            v = v[1:-1]
                        title = v.strip()
                        break
                titles.append(title)
    except Exception:
        pass
    return titles


async def probe_one(session: aiohttp.ClientSession, c: Candidate) -> None:
    try:
        titles = await read_icy_blocks(session, c.stream_url, max_blocks=3)
        if not titles:
            c.icy_qualification = "none"
            c.icy_sample_title = None
            return
        q, sample = title_quality(titles)
        c.icy_qualification = q
        c.icy_sample_title = sample
    except Exception:
        c.icy_qualification = "error"
        c.icy_sample_title = None


def stable_id_rg(channel_id: str) -> str:
    return f"zm_rg_{channel_id}"


def stable_id_tunein(sid: str) -> str:
    return f"zm_tn_{sid}"


def stable_id_rb(name: str, url: str) -> str:
    h = sha1_str(f"{name}|{url}")
    return f"zm_rb_{h[:16]}"


def stable_id_mytuner(radio_id: str, page_url: str) -> str:
    if radio_id:
        return f"zm_mt_{radio_id}"
    return f"zm_mt_{sha1_str(page_url)[:16]}"


def stable_id_orb(radio_id: str, stream_url: str) -> str:
    rid = (radio_id or "").strip()
    if rid:
        safe = re.sub(r"[^a-zA-Z0-9_.-]+", "_", rid).strip("_")
        return f"zm_orb_{safe[:56]}"
    return f"zm_orb_{sha1_str(stream_url)[:16]}"


def stable_id_streema(path: str) -> str:
    return f"zm_st_{sha1_str(path)[:16]}"


def sha1_str(s: str) -> str:
    import hashlib

    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def infer_district_from_tunein_name(name: str) -> str:
    """Best-effort city/district from station name (TuneIn rarely encodes location elsewhere)."""
    low = name.lower()
    pairs = [
        ("lusaka", "Lusaka"),
        ("ndola", "Ndola"),
        ("kitwe", "Kitwe"),
        ("kabwe", "Kabwe"),
        ("livingstone", "Livingstone"),
        ("chipata", "Chipata"),
        ("choma", "Choma"),
        ("luanshya", "Luanshya"),
        ("kapiri mposhi", "Kapiri Mposhi"),
        ("kapiri", "Kapiri Mposhi"),
        ("mongu", "Mongu"),
        ("solwezi", "Solwezi"),
        ("kasama", "Kasama"),
        ("mufulira", "Mufulira"),
        ("chingola", "Chingola"),
        ("kalomo", "Kalomo"),
        ("petauke", "Petauke"),
        ("monze", "Monze"),
        ("mazabuka", "Mazabuka"),
        ("copperbelt", "Copperbelt"),
        ("zambia", "Zambia"),
    ]
    for needle, district in pairs:
        if needle in low:
            return district
    return "Zambia"


def build_mytuner_candidates() -> list[Candidate]:
    """Decrypt stream URLs from MyTuner Zambia listing (same AES logic as audit script)."""
    out: list[Candidate] = []
    for page_url in discover_mytuner_zambia_station_page_urls():
        try:
            html = mytuner_fetch_text(page_url)
        except Exception:
            continue
        su, name, rid = decrypt_first_stream_url(html, page_url)
        if not su.startswith("http"):
            continue
        name = (name or "").strip() or f"MyTuner {rid or 'station'}"
        dist = infer_district_from_tunein_name(name)
        prov = province_for_tunein_district(dist) if dist != "Zambia" else ""
        out.append(
            Candidate(
                stable_id=stable_id_mytuner(rid, page_url),
                name=name,
                district=dist,
                province=prov,
                frequency_mhz=extract_frequency_mhz(name),
                stream_url=su,
                source="mytuner",
                source_detail=rid or page_url,
            )
        )
    return out


def _orb_context_district(path: str) -> str:
    """If path is a city listing (/zm/Lusaka/), return that city name for metadata."""
    last = path.rstrip("/").rsplit("/", 1)[-1]
    for c in ORB_CITY_SLUGS:
        if last.lower() == c.lower():
            return slug_to_city_name(c)
    return ""


def build_orb_candidates() -> list[Candidate]:
    """Stream URLs from OnlineRadioBox Zambia HTML (`station_play` stream=...)."""
    out: list[Candidate] = []
    for path in discover_orb_paths_to_fetch():
        try:
            html = fetch_orb_html(path)
        except Exception:
            continue
        if is_offline_placeholder(html):
            continue
        ctx_dist = _orb_context_district(path)
        for su, rname, rid in extract_station_play_buttons(html):
            if not su.startswith("http"):
                continue
            name = (rname or "").strip() or f"ORB {rid or path}"
            dist = ctx_dist or infer_district_from_tunein_name(name)
            prov = province_for_tunein_district(dist) if dist != "Zambia" else ""
            out.append(
                Candidate(
                    stable_id=stable_id_orb(rid, su),
                    name=name,
                    district=dist,
                    province=prov,
                    frequency_mhz=extract_frequency_mhz(name),
                    stream_url=su,
                    source="onlineradiobox",
                    source_detail=rid or path,
                )
            )
    return out


def build_streema_candidates() -> list[Candidate]:
    """Stream URLs from Streema Zambia station profiles (#source-stream data-src)."""
    out: list[Candidate] = []
    for path in discover_streema_station_paths():
        try:
            html = streema_fetch_html("https://streema.com" + path)
        except Exception:
            continue
        su = extract_stream_from_station_html(html)
        if not su or not su.startswith("http"):
            continue
        title = extract_station_title(html)
        fallback = path.rstrip("/").split("/")[-1].replace("_", " ")
        name = ((title or "").strip() or fallback).strip()
        dist = infer_district_from_tunein_name(name)
        prov = province_for_tunein_district(dist) if dist != "Zambia" else ""
        out.append(
            Candidate(
                stable_id=stable_id_streema(path),
                name=name,
                district=dist,
                province=prov,
                frequency_mhz=extract_frequency_mhz(name),
                stream_url=su,
                source="streema",
                source_detail=path,
            )
        )
    return out


async def build_candidates(_max_total: int) -> list[Candidate]:
    """Collect Radio Garden (full Zambia API) + radio-browser ZM + filtered TuneIn."""
    rg_zm = fetch_json(RG_ZAMBIA_PAGE)
    place_maps = discover_rg_place_maps(rg_zm)
    by_url: dict[str, Candidate] = {}

    async with aiohttp.ClientSession() as session:

        async def add_radio_garden_channel(page: dict, district: str) -> None:
            url_path = page.get("url") or ""
            cid = channel_slug_from_url(url_path)
            if not cid:
                return
            name = (page.get("title") or cid).strip()
            prov = province_for_place(district)
            try:
                su = await resolve_rg(session, cid)
            except Exception:
                return
            if not su.startswith("http"):
                return
            norm = mytuner_normalize_url(su)
            if not norm:
                return
            sm_rg = {"radio_garden": cid}
            if norm in by_url:
                ex = by_url[norm]
                merged = dict(ex.source_map or {ex.source: ex.source_detail})
                if "radio_garden" not in merged:
                    merged["radio_garden"] = cid
                by_url[norm] = dataclasses.replace(
                    ex,
                    source_map=merged,
                    source=ex.source,
                    source_detail=ex.source_detail,
                )
                return
            by_url[norm] = Candidate(
                stable_id=stable_id_rg(cid),
                name=name,
                district=district,
                province=prov,
                frequency_mhz=extract_frequency_mhz(name),
                stream_url=su,
                source="radio_garden",
                source_detail=cid,
                source_map=sm_rg,
            )

        # --- Radio Garden: "Popular" / country-index channels (same API as https://radio.garden/visit/zambia/...) ---
        for page in rg_channel_pages_from_country_json(rg_zm):
            dist = rg_district_from_channel_page(page)
            await add_radio_garden_channel(page, dist)

        # --- Radio Garden: every place map linked from the Zambia page (city/region lists) ---
        for district, map_id in place_maps:
            for page in rg_channels_for_map(map_id):
                await add_radio_garden_channel(page, district)

        # --- radio-browser: country ZM only ---
        for name, su in fetch_radio_browser_zambia_stations():
            if not su.startswith("http"):
                continue
            norm = mytuner_normalize_url(su)
            if not norm:
                continue
            det_rb = name[:80]
            sm_rb = {"radio_browser": det_rb}
            if norm in by_url:
                ex = by_url[norm]
                merged = dict(ex.source_map or {ex.source: ex.source_detail})
                if "radio_browser" not in merged:
                    merged["radio_browser"] = det_rb
                by_url[norm] = dataclasses.replace(
                    ex,
                    source_map=merged,
                    source=ex.source,
                    source_detail=ex.source_detail,
                )
                continue
            dist = infer_district_from_tunein_name(name)
            prov = province_for_tunein_district(dist) if dist != "Zambia" else ""
            by_url[norm] = Candidate(
                stable_id=stable_id_rb(name, su),
                name=name,
                district=dist,
                province=prov,
                frequency_mhz=extract_frequency_mhz(name),
                stream_url=su,
                source="radio_browser",
                source_detail=det_rb,
                source_map=sm_rb,
            )

        # --- TuneIn: only stations that look Zambian by name ---
        queries = [
            "zambia",
            "lusaka zambia",
            "ndola zambia",
            "kitwe zambia",
            "copperbelt zambia",
            "radio zambia",
            "fm zambia",
            "chipata zambia",
            "mongu zambia",
            "solwezi zambia",
            "kabwe zambia",
            "livingstone zambia",
            "kasama zambia",
            "mansa zambia",
            "samfya zambia",
            "petauke zambia",
            "mazabuka zambia",
            "chalimbana zambia",
            "mufulira zambia",
            "chingola zambia",
            "kalomo zambia",
            "sesheke zambia",
            "monze zambia",
            "choma zambia",
        ]
        seen_tn: set[str] = set()
        for q in queries:
            try:
                pairs = tunein_search_stations(q)
            except Exception:
                continue
            for name, sid in pairs:
                if not is_likely_zambia_station_name(name):
                    continue
                if sid in seen_tn:
                    continue
                seen_tn.add(sid)
                try:
                    su = await resolve_tunein(session, sid)
                except Exception:
                    continue
                if not su.startswith("http"):
                    continue
                norm = mytuner_normalize_url(su)
                if not norm:
                    continue
                sm_tn = {"tunein": sid}
                if norm in by_url:
                    ex = by_url[norm]
                    merged = dict(ex.source_map or {ex.source: ex.source_detail})
                    if "tunein" not in merged:
                        merged["tunein"] = sid
                    by_url[norm] = dataclasses.replace(
                        ex,
                        source_map=merged,
                        source=ex.source,
                        source_detail=ex.source_detail,
                    )
                    continue
                dist = infer_district_from_tunein_name(name)
                prov = province_for_tunein_district(dist) if dist != "Zambia" else ""
                by_url[norm] = Candidate(
                    stable_id=stable_id_tunein(sid),
                    name=name,
                    district=dist,
                    province=prov,
                    frequency_mhz=extract_frequency_mhz(name),
                    stream_url=su,
                    source="tunein",
                    source_detail=sid,
                    source_map=sm_tn,
                )

    return list(by_url.values())


# When the same direct stream URL appears on multiple sites, keep one row but merge
# source hints (Radio Garden channel id + ORB id + MyTuner page, etc.) for self-healing / ORB poller.
SOURCE_PRIORITY_RANK: dict[str, int] = {
    "mytuner": 0,
    "onlineradiobox": 1,
    "streema": 2,
    "radio_garden": 3,
    "tunein": 4,
    "radio_browser": 5,
}


def _candidate_source_rank(c: Candidate) -> int:
    sm = c.source_map or {c.source: c.source_detail}
    return min((SOURCE_PRIORITY_RANK.get(k, 99) for k in sm), default=99)


def _canonical_source_fields(merged: dict[str, str]) -> tuple[str, str]:
    """Pick primary source key for stable_id (mytuner > orb > ...)."""
    if not merged:
        return "", ""
    best = min(merged.keys(), key=lambda k: SOURCE_PRIORITY_RANK.get(k, 99))
    return best, merged[best]


def stable_id_from_candidate(c: Candidate) -> str:
    """Primary DB id from merged sources (same priority as _canonical_source_fields)."""
    sm = c.source_map or {c.source: c.source_detail}
    src, det = _canonical_source_fields(sm)
    det = (det or "").strip()
    if src == "mytuner":
        if det.isdigit():
            return stable_id_mytuner(det, "")
        return stable_id_mytuner("", det)
    if src == "onlineradiobox":
        return stable_id_orb(det, c.stream_url)
    if src == "streema":
        return stable_id_streema(det)
    if src == "radio_garden":
        return stable_id_rg(det)
    if src == "tunein":
        return stable_id_tunein(det)
    if src == "radio_browser":
        return stable_id_rb(c.name, c.stream_url)
    h = sha1_str(f"{c.stream_url}|{json.dumps(sm, sort_keys=True)}")
    return f"zm_mg_{h[:16]}"


def merge_candidates_priority(*sequences: list[Candidate]) -> list[Candidate]:
    """De-dupe by normalized stream URL; merge source hints across sites; keep highest-priority row for id/name."""
    by_norm: dict[str, Candidate] = {}
    for seq in sequences:
        for c in seq:
            key = mytuner_normalize_url(c.stream_url)
            if not key:
                continue
            sm = dict(c.source_map or {c.source: c.source_detail})
            if key not in by_norm:
                src, det = _canonical_source_fields(sm)
                by_norm[key] = dataclasses.replace(c, source_map=sm, source=src, source_detail=det)
                continue
            ex = by_norm[key]
            merged_sources = dict(ex.source_map or {ex.source: ex.source_detail})
            merged_sources.update(sm)
            src, det = _canonical_source_fields(merged_sources)
            rc = _candidate_source_rank(c)
            rx = _candidate_source_rank(ex)
            if rc < rx:
                by_norm[key] = dataclasses.replace(
                    c,
                    source_map=merged_sources,
                    source=src,
                    source_detail=det,
                )
            else:
                by_norm[key] = dataclasses.replace(
                    ex,
                    source_map=merged_sources,
                    source=src,
                    source_detail=det,
                )
    return list(by_norm.values())


async def probe_batch(candidates: list[Candidate], concurrency: int = 25) -> None:
    sem = asyncio.Semaphore(concurrency)

    async def run(c: Candidate, session: aiohttp.ClientSession):
        async with sem:
            await probe_one(session, c)

    connector = aiohttp.TCPConnector(limit=concurrency + 5)
    async with aiohttp.ClientSession(connector=connector) as session:
        await asyncio.gather(*(run(c, session) for c in candidates))


def to_prisma_row(c: Candidate) -> dict | None:
    """Skip none/error — do not import dead streams."""
    if c.icy_qualification in {"none", "error"}:
        return None
    source_ids = dict(c.source_map or {c.source: c.source_detail})
    # good / partial / weak — all monitored (weak may improve over time)
    is_active = c.icy_qualification in {"good", "partial", "weak"}
    return {
        "id": stable_id_from_candidate(c),
        "name": c.name,
        "country": "Zambia",
        "district": c.district,
        "province": c.province or "",
        "frequencyMhz": c.frequency_mhz,
        "streamUrl": c.stream_url,
        "streamFormatHint": "icy",
        "sourceIdsJson": json.dumps(source_ids),
        "icyQualification": c.icy_qualification,
        "icySampleTitle": (c.icy_sample_title or "")[:500],
        "isActive": is_active,
        "metadataPriorityEnabled": True,
        "fingerprintFallbackEnabled": True,
        "metadataStaleSeconds": 300,
        "pollIntervalSeconds": 120,
        "sampleSeconds": 20,
        "archiveSongSamples": True,
    }


async def async_main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--max-probe",
        type=int,
        default=800,
        help="Max streams to ICY-probe (discovery can list more; raise for large merges).",
    )
    ap.add_argument("--out", type=str, default="scripts/data/zambia_harvest.json")
    args = ap.parse_args()

    print(
        "Discovering candidates (MyTuner + OnlineRadioBox + Streema + Radio Garden "
        "[https://radio.garden Zambia API: country channels + all place maps] + "
        "radio-browser ZM + filtered TuneIn)..."
    )
    mt = build_mytuner_candidates()
    orb = build_orb_candidates()
    st = build_streema_candidates()
    base = await build_candidates(0)
    cands = merge_candidates_priority(mt, orb, st, base)
    print(f"MyTuner decrypted URLs: {len(mt)}")
    print(f"OnlineRadioBox stream buttons: {len(orb)}")
    print(f"Streema station profiles: {len(st)}")
    print(f"After merge (deduped by URL): {len(cands)}")

    to_probe = cands[: args.max_probe]
    print(f"Probing ICY (3 blocks max) for {len(to_probe)} streams...")
    await probe_batch(to_probe, concurrency=25)

    good = [c for c in to_probe if c.icy_qualification in {"good", "partial"}]
    print(f"Qualified good/partial: {len(good)}")
    print(f"weak: {sum(1 for c in to_probe if c.icy_qualification == 'weak')}")
    print(f"none/error: {sum(1 for c in to_probe if c.icy_qualification in {'none','error'})}")

    rows: list[dict] = []
    for c in to_probe:
        row = to_prisma_row(c)
        if row is not None:
            rows.append(row)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(
            {
                "stations": rows,
                "stats": {
                    "probed": len(to_probe),
                    "imported": len(rows),
                    "active": sum(1 for r in rows if r["isActive"]),
                },
            },
            f,
            indent=2,
        )
    print(f"Wrote {args.out} (imported {len(rows)} stations with usable ICY — none/error excluded)")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
