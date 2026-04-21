#!/usr/bin/env python3
"""
Harvest Zambian radio stations from Radio Garden + TuneIn, resolve stream URLs,
probe ICY metadata (up to 3 blocks), classify, write JSON for Prisma import.

Target: up to --max-stations (default 150) unique entries.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import ssl
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass

import aiohttp

UA_BROWSER = "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0 Safari/537.36"
UA_ICY = "ZambiaStationHarvest/1.0"

# Radio Garden: Zambia country page lists all place maps (map id -> district name)
RG_ZAMBIA_PAGE = "https://radio.garden/api/ara/content/page/XbLRE6NT"


@dataclass
class Candidate:
    stable_id: str
    name: str
    district: str
    stream_url: str
    source: str  # radio_garden | tunein
    source_detail: str  # channel id or tunein id
    icy_qualification: str = "pending"
    icy_sample_title: str | None = None


def fetch_json(url: str) -> dict:
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(url, context=ctx, timeout=45) as r:
        return json.loads(r.read().decode())


def discover_rg_place_maps() -> list[tuple[str, str]]:
    """Return [(district_name, map_id), ...]"""
    j = fetch_json(RG_ZAMBIA_PAGE)
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


def rg_channels_for_map(map_id: str) -> list[dict]:
    url = f"https://radio.garden/api/ara/content/page/{map_id}/channels"
    j = fetch_json(url)
    items = j["data"]["content"][0]["items"]
    return [x["page"] for x in items if x.get("page", {}).get("type") == "channel"]


def channel_slug_from_url(url: str) -> str | None:
    # /listen/some-name/CHANNELID
    m = re.search(r"/listen/[^/]+/([^/]+)/?$", url or "")
    return m.group(1) if m else None


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


async def build_candidates(max_total: int) -> list[Candidate]:
    place_maps = discover_rg_place_maps()
    by_url: dict[str, Candidate] = {}

    async with aiohttp.ClientSession() as session:
        # --- Radio Garden (all districts) ---
        for district, map_id in place_maps:
            try:
                pages = rg_channels_for_map(map_id)
            except Exception:
                continue
            for page in pages:
                url_path = page.get("url") or ""
                cid = channel_slug_from_url(url_path)
                if not cid:
                    continue
                name = (page.get("title") or cid).strip()
                try:
                    su = await resolve_rg(session, cid)
                except Exception:
                    continue
                if su in by_url:
                    continue
                by_url[su] = Candidate(
                    stable_id=stable_id_rg(cid),
                    name=name,
                    district=district,
                    stream_url=su,
                    source="radio_garden",
                    source_detail=cid,
                )

        # --- TuneIn (multiple search queries to grow toward max_total unique URLs) ---
        queries = [
            "zambia",
            "lusaka zambia",
            "ndola zambia",
            "kitwe zambia",
            "copperbelt zambia",
            "radio zambia",
            "fm zambia",
            "luanshya",
            "chipata zambia",
            "mongu zambia",
            "solwezi zambia",
            "kabwe zambia",
            "livingstone zambia",
        ]
        seen_tn: set[str] = set()
        for q in queries:
            try:
                pairs = tunein_search_stations(q)
            except Exception:
                continue
            for name, sid in pairs:
                if sid in seen_tn:
                    continue
                seen_tn.add(sid)
                try:
                    su = await resolve_tunein(session, sid)
                except Exception:
                    continue
                if not su.startswith("http"):
                    continue
                if su in by_url:
                    continue
                by_url[su] = Candidate(
                    stable_id=stable_id_tunein(sid),
                    name=name,
                    district=infer_district_from_tunein_name(name),
                    stream_url=su,
                    source="tunein",
                    source_detail=sid,
                )
                if len(by_url) >= max_total:
                    break
            if len(by_url) >= max_total:
                break

    return list(by_url.values())


async def probe_batch(candidates: list[Candidate], concurrency: int = 25) -> None:
    sem = asyncio.Semaphore(concurrency)

    async def run(c: Candidate, session: aiohttp.ClientSession):
        async with sem:
            await probe_one(session, c)

    connector = aiohttp.TCPConnector(limit=concurrency + 5)
    async with aiohttp.ClientSession(connector=connector) as session:
        await asyncio.gather(*(run(c, session) for c in candidates))


def to_prisma_row(c: Candidate) -> dict:
    source_ids = {c.source: c.source_detail}
    # Activate only reasonably useful ICY; still store weak for manual review
    is_active = c.icy_qualification in {"good", "partial"}
    return {
        "id": c.stable_id,
        "name": c.name,
        "country": "Zambia",
        "district": c.district,
        "streamUrl": c.stream_url,
        "streamFormatHint": "icy",
        "sourceIdsJson": json.dumps(source_ids),
        "icyQualification": c.icy_qualification,
        "icySampleTitle": (c.icy_sample_title or "")[:500],
        "isActive": is_active,
        "metadataPriorityEnabled": True,
        "fingerprintFallbackEnabled": False,
        "metadataStaleSeconds": 300,
        "pollIntervalSeconds": 120,
        "sampleSeconds": 20,
    }


async def async_main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-stations", type=int, default=150)
    ap.add_argument("--out", type=str, default="scripts/data/zambia_harvest.json")
    args = ap.parse_args()

    print("Discovering candidates (Radio Garden + TuneIn)...")
    cands = await build_candidates(args.max_stations)
    print(f"Unique stream URLs: {len(cands)} (cap growth ~{args.max_stations*2})")

    # Hard-cap probe list
    to_probe = cands[: args.max_stations]
    print(f"Probing ICY (3 blocks max) for {len(to_probe)} streams...")
    await probe_batch(to_probe, concurrency=25)

    good = [c for c in to_probe if c.icy_qualification in {"good", "partial"}]
    print(f"Qualified good/partial: {len(good)}")
    print(f"weak: {sum(1 for c in to_probe if c.icy_qualification == 'weak')}")
    print(f"none/error: {sum(1 for c in to_probe if c.icy_qualification in {'none','error'})}")

    rows = [to_prisma_row(c) for c in to_probe]
    import os

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump({"stations": rows, "stats": {"total": len(rows), "active": sum(1 for r in rows if r["isActive"])}}, f, indent=2)
    print(f"Wrote {args.out}")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
