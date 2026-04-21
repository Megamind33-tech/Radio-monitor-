"""
audit_stations.py
-----------------
Tests a list of radio stream URLs to see which ones broadcast usable ICY metadata.

For each station it:
  1. Opens the stream with Icy-MetaData: 1
  2. Reads the first metadata block
  3. Waits 25 seconds and reads a second block
  4. Reports: does it have metadata? Does it actually change? What does it look like?

Output: stations_audit.csv  (tag each station as good / partial / none / dead)

Usage:
    python audit_stations.py stations.csv

stations.csv format (no header needed, but this works):
    station_id,name,stream_url
    hot_fm,Hot FM Zambia,http://...
    phoenix,Radio Phoenix,http://...
"""

import csv
import sys
import time
import socket
import urllib.request
import urllib.error
from dataclasses import dataclass, asdict
from typing import Optional

# ---------- tunables ----------
CONNECT_TIMEOUT = 10     # seconds to wait for initial connection
READ_TIMEOUT = 20        # seconds to wait for data
SECOND_SAMPLE_DELAY = 25 # wait this long before checking if metadata changed
USER_AGENT = "ZamPlayMonitor/0.1 (airplay audit)"
# ------------------------------


@dataclass
class AuditResult:
    station_id: str
    name: str
    stream_url: str
    reachable: bool
    has_icy_metaint: bool
    metaint: Optional[int]
    icy_name: Optional[str]
    first_title: Optional[str]
    second_title: Optional[str]
    title_changed: bool
    verdict: str  # good | partial | none | dead
    notes: str


def read_metadata_block(resp, metaint: int) -> Optional[str]:
    """Skip metaint audio bytes, read the metadata block, return StreamTitle."""
    # Skip audio payload
    remaining = metaint
    while remaining > 0:
        chunk = resp.read(min(4096, remaining))
        if not chunk:
            return None
        remaining -= len(chunk)

    # One byte says how long metadata is (multiplied by 16)
    length_byte = resp.read(1)
    if not length_byte:
        return None
    meta_len = length_byte[0] * 16
    if meta_len == 0:
        return ""  # no metadata in this block — stream is alive but quiet

    meta_bytes = resp.read(meta_len)
    if not meta_bytes:
        return None

    # Decode leniently; stations send all sorts of encodings
    meta_str = meta_bytes.rstrip(b"\x00").decode("utf-8", errors="replace")
    # meta_str looks like: StreamTitle='Artist - Song';StreamUrl='...';
    title = None
    for field in meta_str.split(";"):
        field = field.strip()
        if field.startswith("StreamTitle="):
            title = field[len("StreamTitle="):].strip("'").strip('"').strip()
            break
    return title


def audit_one(station_id: str, name: str, url: str) -> AuditResult:
    result = AuditResult(
        station_id=station_id,
        name=name,
        stream_url=url,
        reachable=False,
        has_icy_metaint=False,
        metaint=None,
        icy_name=None,
        first_title=None,
        second_title=None,
        title_changed=False,
        verdict="dead",
        notes="",
    )

    req = urllib.request.Request(url, headers={
        "Icy-MetaData": "1",
        "User-Agent": USER_AGENT,
    })

    try:
        resp = urllib.request.urlopen(req, timeout=CONNECT_TIMEOUT)
    except (urllib.error.URLError, socket.timeout, ConnectionError, OSError) as e:
        result.notes = f"connect failed: {e}"
        return result

    # socket read timeout for subsequent reads
    try:
        resp.fp.raw._sock.settimeout(READ_TIMEOUT)
    except Exception:
        pass

    result.reachable = True
    result.icy_name = resp.headers.get("icy-name")
    metaint_hdr = resp.headers.get("icy-metaint")

    if not metaint_hdr:
        result.verdict = "none"
        result.notes = "stream live but no icy-metaint header (station doesn't broadcast metadata)"
        resp.close()
        return result

    try:
        metaint = int(metaint_hdr)
    except ValueError:
        result.notes = f"bad icy-metaint: {metaint_hdr}"
        resp.close()
        return result

    result.has_icy_metaint = True
    result.metaint = metaint

    # First sample
    try:
        result.first_title = read_metadata_block(resp, metaint)
    except (socket.timeout, ConnectionError, OSError) as e:
        result.notes = f"read failed on first sample: {e}"
        resp.close()
        return result

    # Wait, then second sample (to see if title changes = station actively updates)
    time.sleep(SECOND_SAMPLE_DELAY)

    try:
        result.second_title = read_metadata_block(resp, metaint)
    except (socket.timeout, ConnectionError, OSError) as e:
        result.notes = f"read failed on second sample: {e}"
        resp.close()
        # Still salvageable — we got a first title
        if result.first_title:
            result.verdict = "partial"
            result.notes += " | got first title only"
        return result
    finally:
        try:
            resp.close()
        except Exception:
            pass

    # Decide verdict
    f = (result.first_title or "").strip()
    s = (result.second_title or "").strip()

    if not f and not s:
        result.verdict = "none"
        result.notes = "metaint present but StreamTitle empty in both samples"
    elif f and s and f == s:
        # Same title both times — could be a long song, or stuck metadata
        result.verdict = "partial"
        result.notes = "title didn't change in 25s — could be a long track or stuck metadata (re-run or watch longer)"
    elif f and s and f != s:
        result.title_changed = True
        result.verdict = "good"
        result.notes = "title changed between samples — station is actively updating"
    else:
        # one side empty
        result.verdict = "partial"
        result.notes = "metadata present but intermittent"

    return result


def main():
    if len(sys.argv) < 2:
        print("usage: python audit_stations.py stations.csv")
        sys.exit(1)

    infile = sys.argv[1]
    outfile = "stations_audit.csv"

    stations: list[tuple[str, str, str]] = []
    with open(infile, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or row[0].startswith("#"):
                continue
            if row[0].lower() == "station_id":
                continue  # header
            if len(row) < 3:
                print(f"skipping malformed row: {row}")
                continue
            stations.append((row[0].strip(), row[1].strip(), row[2].strip()))

    if not stations:
        print("no stations to audit — add rows with stream_url to stations.csv")
        sys.exit(1)

    print(f"auditing {len(stations)} stations...\n")

    results = []
    for sid, name, url in stations:
        print(f"[{sid}] {name}")
        r = audit_one(sid, name, url)
        verdict_tag = {
            "good": "✅ good",
            "partial": "⚠️  partial",
            "none": "❌ no metadata",
            "dead": "💀 dead",
        }.get(r.verdict, r.verdict)
        title_preview = (r.first_title or "")[:60]
        print(f"   -> {verdict_tag}  |  first: {title_preview!r}  |  {r.notes}\n")
        results.append(r)

    # Write CSV report
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(results[0]).keys()))
        writer.writeheader()
        for r in results:
            writer.writerow(asdict(r))

    # Summary
    counts = {"good": 0, "partial": 0, "none": 0, "dead": 0}
    for r in results:
        counts[r.verdict] = counts.get(r.verdict, 0) + 1

    print("=" * 50)
    print("SUMMARY")
    print("=" * 50)
    for k, v in counts.items():
        print(f"  {k:10s}: {v}")
    print(f"\nfull report written to: {outfile}")
    print("\nnext steps:")
    print("  - 'good' stations -> add to monitor.py station list")
    print("  - 'partial' stations -> re-run audit or watch longer")
    print("  - 'none'/'dead' stations -> fingerprinting fallback or drop")


if __name__ == "__main__":
    main()
