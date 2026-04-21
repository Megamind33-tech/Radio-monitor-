#!/usr/bin/env python3
"""
Parallel ICY metadata probe for Zambian stations.
Resolves Radio Garden listen URLs and TuneIn Tune.ashx URLs, then reads one ICY block each.
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass

import aiohttp

UA_BROWSER = "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0 Safari/537.36"
UA_ICY = "ZambiaICYBatch/1.0"


@dataclass
class Result:
    label: str
    source: str
    stream_url: str
    metaint: str | None
    stream_title: str | None
    icy_name: str | None
    error: str | None


# Radio Garden channel IDs from https://radio.garden (Zambia country page + Lusaka listings)
RADIO_GARDEN: list[tuple[str, str]] = [
    ("5FM 89.9", "0eWe4Cjj"),
    ("Metro FM 94.5", "_YhsbQQM"),
    ("Hot FM 87.7", "9OaNJWsU"),
    ("Radio Phoenix FM 89.5", "W7WDghCR"),
    ("Rock FM 96.5", "a5BJ3FIb"),
    ("ZNBC Radio 1", "Bp7bG49i"),
    ("ZNBC Radio 2", "SqTiHh7g"),
    ("ZNBC Radio 4", "RNzY2Vvb"),
    ("Power FM 91.3", "sGqLKg4w"),
    ("One Love Radio 104.1", "eboycu6t"),
    ("Radio Maria Zambia", "hx8O1Ycu"),
    ("Culture Radio 105.7 Kitwe", "ix1orLmW"),
    ("Flava FM 87.7 Kitwe", "9ISdUYhy"),
    ("Byta FM 90.3 Choma", "YsDAFNNN"),
    ("Roan FM 106.5 Luanshya", "KBoM9J70"),
    ("Sun FM Zambia (Ndola RG)", "HeoNWcG1"),
    ("Spice FM 91.1 Kabwe", "-TpnK7Jt"),
    ("Parliament Radio", "h50ZcZF-"),
    ("Joy FM 92.1", "yDmUZkKt"),
    ("Kwithu FM 93.3", "n4DFcOzh"),
    ("Hone FM 94.1", "VGq1fz08"),
    ("Komboni Radio 94.9", "9H1I-_le"),
    ("Zi FM Stereo 100.3", "mS9Gs76G"),
    ("Iwave FM 90.1 Chingola", "v3qdYp8s"),
    ("Chikuni Radio 91.9 Monze", "h4BLgXAO"),
]

# TuneIn station IDs from opml Search.ashx?query=zambia (supplement / cross-check)
TUNEIN: list[tuple[str, str]] = [
    ("TuneIn: Hot FM Zambia", "s131737"),
    ("TuneIn: 5FM Radio Zambia", "s281129"),
    ("TuneIn: Rock FM Zambia", "s214604"),
    ("TuneIn: Sun FM Zambia", "s298331"),
    ("TuneIn: Radio Maria Zambia", "s183723"),
    ("TuneIn: Power FM Zambia", "s228654"),
    ("TuneIn: Flava FM", "s105323"),
    ("TuneIn: Lubwe Radio Mix", "s218869"),
    ("TuneIn: Kwacha Radio", "s337031"),
]


async def resolve_radio_garden(session: aiohttp.ClientSession, channel_id: str) -> str:
    url = f"https://radio.garden/api/ara/content/listen/{channel_id}/channel.mp3"
    async with session.get(
        url,
        allow_redirects=True,
        headers={"User-Agent": UA_BROWSER},
    ) as resp:
        # final url after redirects
        return str(resp.url)


async def resolve_tunein(session: aiohttp.ClientSession, station_id: str) -> str:
    url = f"https://opml.radiotime.com/Tune.ashx?id={station_id}"
    async with session.get(url, allow_redirects=True) as resp:
        text = (await resp.text()).strip()
        if text.startswith("http"):
            return text.split("\n")[0].strip()
        return text


def classify_title(raw: str | None) -> str:
    if raw is None:
        return "none"
    s = raw.strip()
    if not s or s in {"-", " - "}:
        return "empty_or_dash"
    return "has_text"


async def read_icy_first_title(session: aiohttp.ClientSession, stream_url: str) -> tuple[str | None, str | None, str | None, str | None]:
    """Returns (metaint, stream_title, icy_name, error)."""
    headers = {"Icy-MetaData": "1", "User-Agent": UA_ICY}
    try:
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=12, sock_read=18)
        async with session.get(stream_url, headers=headers, timeout=timeout) as resp:
            metaint = resp.headers.get("icy-metaint")
            icy_name = resp.headers.get("icy-name")
            if not metaint:
                return None, None, icy_name, "no icy-metaint"
            m = int(metaint)
            left = m
            while left > 0:
                chunk = await resp.content.read(min(16384, left))
                if not chunk:
                    return metaint, None, icy_name, "eof_before_meta"
                left -= len(chunk)
            lb = await resp.content.readexactly(1)
            n = lb[0] * 16
            if n == 0:
                return metaint, "", icy_name, None
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
            return metaint, title, icy_name, None
    except asyncio.TimeoutError:
        return None, None, None, "timeout"
    except Exception as e:
        return None, None, None, repr(e)


async def probe_rg(session: aiohttp.ClientSession, label: str, cid: str) -> Result:
    try:
        su = await resolve_radio_garden(session, cid)
    except Exception as e:
        return Result(label, "radio.garden", "", None, None, None, f"resolve:{e!r}")
    mt, st, iname, err = await read_icy_first_title(session, su)
    return Result(label, "radio.garden", su, str(mt) if mt else None, st, iname, err)


async def probe_tunein(session: aiohttp.ClientSession, label: str, sid: str) -> Result:
    try:
        su = await resolve_tunein(session, sid)
        if not su.startswith("http"):
            return Result(label, "tunein", su, None, None, None, "bad_tunein_body")
    except Exception as e:
        return Result(label, "tunein", "", None, None, None, f"resolve:{e!r}")
    mt, st, iname, err = await read_icy_first_title(session, su)
    return Result(label, "tunein", su, str(mt) if mt else None, st, iname, err)


async def main() -> None:
    connector = aiohttp.TCPConnector(limit=30)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks: list[asyncio.Task[Result]] = []
        for label, cid in RADIO_GARDEN:
            tasks.append(asyncio.create_task(probe_rg(session, label, cid)))
        for label, sid in TUNEIN:
            tasks.append(asyncio.create_task(probe_tunein(session, label, sid)))
        results = await asyncio.gather(*tasks)

    # Sort: has useful text first, then empty, then errors
    def sort_key(r: Result) -> tuple[int, str]:
        cls = classify_title(r.stream_title)
        tier = 0 if cls == "has_text" else 1 if cls in ("empty_or_dash", "none") else 2
        return (tier, r.label)

    results.sort(key=sort_key)

    print(f"Probed {len(results)} streams (Radio Garden + TuneIn).")
    print()
    working = [r for r in results if classify_title(r.stream_title) == "has_text"]
    weak = [r for r in results if classify_title(r.stream_title) in ("empty_or_dash", "none") and not r.error]
    broken = [r for r in results if r.error]

    print(f"=== ICY title looks useful ({len(working)}) ===")
    for r in working:
        print(f"  [{r.source}] {r.label}")
        print(f"      StreamTitle: {r.stream_title}")
        print(f"      URL: {r.stream_url[:100]}{'...' if len(r.stream_url) > 100 else ''}")
        print()

    print(f"=== Metaint but empty / dash / no title text ({len(weak)}) ===")
    for r in weak:
        st = repr(r.stream_title)
        print(f"  [{r.source}] {r.label} | title={st} | err={r.error}")

    print()
    print(f"=== Errors / no ICY ({len(broken)}) ===")
    for r in broken:
        print(f"  [{r.source}] {r.label} | {r.error}")

    # Summary counts
    print()
    print("--- Summary ---")
    print(f"  useful StreamTitle: {len(working)}")
    print(f"  weak/empty:         {len(weak)}")
    print(f"  errors:             {len(broken)}")


if __name__ == "__main__":
    asyncio.run(main())
