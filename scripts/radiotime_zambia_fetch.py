#!/usr/bin/env python3
"""
Fetch TuneIn / RadioTime OPML search results and resolve Tune.ashx links to
**direct** stream URLs (http/https) only — no HTML pages, no podcast browse links.

Output: harvest-compatible JSON { "stations": [...] } for:
  node scripts/import_zambia_stations.mjs scripts/data/radiotime_zambia.json

Rules:
  - Only `outline` rows with type="audio" and item="station" (live radio).
  - Skip shows, topics, stream_type=download, and type="link".
  - Resolve http://opml.radiotime.com/Tune.ashx?id=sNNNN → body is often a single stream URL.

Examples:
  python3 scripts/radiotime_zambia_fetch.py --out scripts/data/radiotime_zambia.json
  python3 scripts/radiotime_zambia_fetch.py --query "Hot FM Zambia" --out scripts/data/radiotime_hotfm.json
  python3 scripts/radiotime_zambia_fetch.py --query Zambia --extra-query "Radio Phoenix Zambia" --extra-query "Icengelo" --sleep 0.4
"""
from __future__ import annotations

import argparse
import json
import re
import ssl
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Iterable

UA = "Mozilla/5.0 (X11; Linux x86_64) RadioMonitor/1.0 (https://github.com/Megamind33-tech/Radio-monitor-)"
SEARCH_URL = "https://opml.radiotime.com/Search.ashx"

# Names we care about for gap-fill (substring match, case-insensitive)
DEFAULT_PRIORITY_TOKENS = (
    "hot fm",
    "phoenix",
    "icengelo",
    "oblate",
    "znbc",
)


@dataclass
class OutlineStation:
    title: str
    tune_url: str
    guide_id: str
    bitrate: str | None
    formats: str | None


def fetch_bytes(url: str, timeout: int = 45) -> bytes:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": UA, "Accept": "*/*"},
    )
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, context=ctx, timeout=timeout) as r:
        return r.read()


def fetch_text(url: str, timeout: int = 45) -> str:
    return fetch_bytes(url, timeout).decode("utf-8", errors="replace")


def _xml_localname(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def parse_opml_stations(xml_text: str) -> list[OutlineStation]:
    root = ET.fromstring(xml_text)
    out: list[OutlineStation] = []
    for el in root.iter():
        if _xml_localname(el.tag).lower() != "outline":
            continue
        typ = (el.get("type") or "").lower()
        item = (el.get("item") or "").lower()
        if typ != "audio":
            continue
        if item != "station":
            continue
        if (el.get("stream_type") or "").lower() == "download":
            continue
        url = (el.get("URL") or "").strip()
        if not url or "Tune.ashx" not in url:
            continue
        gid = (el.get("guide_id") or "").strip()
        if not gid:
            q = urllib.parse.urlparse(url).query
            parsed = urllib.parse.parse_qs(q)
            ids = parsed.get("id") or parsed.get("station_id")
            if ids:
                gid = ids[0].strip()
        if not gid:
            continue
        title = (el.get("text") or "").strip() or gid
        out.append(
            OutlineStation(
                title=title,
                tune_url=url if url.startswith("http") else "http:" + url,
                guide_id=gid,
                bitrate=el.get("bitrate"),
                formats=el.get("formats"),
            )
        )
    return out


def resolve_tune_to_stream_url(tune_url: str, timeout: int = 30) -> str | None:
    """Tune.ashx returns a tiny body: one line, usually https?://... stream."""
    try:
        body = fetch_text(tune_url, timeout=timeout).strip()
    except Exception:
        return None
    if not body:
        return None
    # First non-empty line
    line = body.splitlines()[0].strip()
    if line.startswith("http://") or line.startswith("https://"):
        return line.rstrip("\r\n")
    return None


def first_stream_url_from_m3u(text: str) -> str | None:
    for raw in text.splitlines():
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        if s.startswith("http://") or s.startswith("https://"):
            return s
    return None


def normalize_stream_url(url: str) -> str | None:
    u = url.strip()
    if not u.startswith("http"):
        return None
    low = u.lower()
    if any(x in low for x in ("/search", "/discover", "radiotime.com/browse", "tunein.com/browse")):
        return None
    return u


def stable_station_id(guide_id: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", guide_id).strip("_") or "unknown"
    return f"zm_rt_{safe}"


def dedupe_by_guide(stations: Iterable[OutlineStation]) -> list[OutlineStation]:
    seen: set[str] = set()
    out: list[OutlineStation] = []
    for s in stations:
        if s.guide_id in seen:
            continue
        seen.add(s.guide_id)
        out.append(s)
    return out


def matches_priority(name: str, tokens: tuple[str, ...]) -> bool:
    low = name.lower()
    return any(t in low for t in tokens)


def harvest_row(
    name: str,
    stream_url: str,
    guide_id: str,
    formats: str | None,
    icy_note: str = "pending",
) -> dict[str, Any]:
    hint = "icy" if (formats or "").lower() == "mp3" else None
    return {
        "id": stable_station_id(guide_id),
        "name": name.strip()[:500],
        "country": "Zambia",
        "district": "",
        "province": "",
        "frequencyMhz": None,
        "streamUrl": stream_url,
        "streamFormatHint": hint,
        "sourceIdsJson": json.dumps({"tunein": guide_id}, separators=(",", ":")),
        "icyQualification": icy_note,
        "icySampleTitle": None,
        "isActive": True,
        "metadataPriorityEnabled": True,
        "fingerprintFallbackEnabled": True,
        "metadataStaleSeconds": 300,
        "pollIntervalSeconds": 120,
        "audioFingerprintIntervalSeconds": 120,
        "sampleSeconds": 25,
        "archiveSongSamples": True,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="RadioTime OPML → direct stream URLs (Zambia harvest JSON)")
    ap.add_argument("--query", default="Zambia", help="Primary Search.ashx query string")
    ap.add_argument(
        "--extra-query",
        action="append",
        default=[],
        metavar="Q",
        help="Additional search queries (repeatable), merged and deduped by guide_id",
    )
    ap.add_argument("--out", default="scripts/data/radiotime_zambia.json", help="Output JSON path")
    ap.add_argument("--sleep", type=float, default=0.35, help="Seconds between Tune.ashx resolves (be polite)")
    ap.add_argument("--limit", type=int, default=0, help="Max stations to resolve (0 = no limit)")
    ap.add_argument(
        "--only-priority",
        action="store_true",
        help="Only include rows whose title matches priority tokens (hot fm, phoenix, …)",
    )
    ap.add_argument(
        "--priority-token",
        action="append",
        default=[],
        help="Extra substring tokens for --only-priority (repeatable)",
    )
    args = ap.parse_args()

    queries = [args.query, *args.extra_query]
    priority = tuple(x.lower() for x in (*DEFAULT_PRIORITY_TOKENS, *args.priority_token))

    all_outlines: list[OutlineStation] = []
    for q in queries:
        url = f"{SEARCH_URL}?{urllib.parse.urlencode({'query': q.strip()})}"
        print(f"Fetching OPML: {url}", flush=True)
        xml_text = fetch_text(url, timeout=60)
        found = parse_opml_stations(xml_text)
        print(f"  -> {len(found)} live station outline(s) (audio+station)", flush=True)
        all_outlines.extend(found)

    all_outlines = dedupe_by_guide(all_outlines)
    if args.only_priority:
        all_outlines = [s for s in all_outlines if matches_priority(s.title, priority)]
        print(f"After --only-priority: {len(all_outlines)} station(s)", flush=True)

    rows: list[dict[str, Any]] = []
    for st in all_outlines:
        if args.limit and len(rows) >= args.limit:
            break
        direct = resolve_tune_to_stream_url(st.tune_url)
        if args.sleep > 0:
            time.sleep(args.sleep)
        if not direct:
            print(f"  skip resolve failed: {st.title} ({st.guide_id})", flush=True)
            continue
        direct = normalize_stream_url(direct)
        if not direct:
            print(f"  skip non-stream URL: {st.title} -> {direct!r}", flush=True)
            continue
        # If Tune returned an M3U playlist URL, try to peel first entry (optional)
        if direct.lower().endswith(".m3u") or direct.lower().endswith(".m3u8"):
            try:
                pl = fetch_text(direct, timeout=20)
                inner = first_stream_url_from_m3u(pl)
                if inner:
                    direct = normalize_stream_url(inner) or direct
            except Exception:
                pass

        rows.append(harvest_row(st.title, direct, st.guide_id, st.formats))
        print(f"  ok: {st.title} -> {direct[:72]}...", flush=True)

    out_path = args.out
    payload = {"stations": rows, "meta": {"source": "radiotime_opml", "queries": queries}}
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(json.dumps({"wrote": out_path, "stations": len(rows)}, indent=2))


if __name__ == "__main__":
    main()
