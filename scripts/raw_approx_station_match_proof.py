#!/usr/bin/env python3
import os, json, sqlite3, subprocess, tempfile, shutil, hashlib
from pathlib import Path
from urllib.parse import urlparse

BASE = Path("/opt/radio-monitor")
APP_DB = BASE / "prisma/dev_runtime.db"
SRC_DB = BASE / "data/source-discovery/zambian_crawl_state.db"
RAW_DB = BASE / "data/fingerprint-index/localfp_raw_match.db"

RAW_BUILD_LIMIT = int(os.environ.get("RAW_BUILD_LIMIT", "500"))
SAMPLE_SECONDS = int(os.environ.get("SAMPLE_SECONDS", "35"))
RAW_OFFSETS = [int(x) for x in os.environ.get("RAW_OFFSETS", "0,45,90,135").split(",") if x.strip()]
TOKENS = [x.strip().lower() for x in os.environ.get("FORCE_STATION_TOKENS", "hot,hone,horn,power,phoenix,qfm").split(",") if x.strip()]
STATION_LIMIT = int(os.environ.get("STATION_LIMIT", "12"))

def sh(args, timeout=100):
    return subprocess.check_output(args, stderr=subprocess.STDOUT, text=True, timeout=timeout)

def cols(conn, table):
    return [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')]

def pick(candidates, names):
    low = {c.lower(): c for c in candidates}
    for n in names:
        if n.lower() in low:
            return low[n.lower()]
    return None

def ensure_raw_db():
    RAW_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(RAW_DB)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_segments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      audio_url TEXT NOT NULL,
      artist TEXT,
      title TEXT,
      source_domain TEXT,
      offset_sec INTEGER NOT NULL,
      duration_sec REAL,
      raw_json TEXT NOT NULL,
      raw_len INTEGER NOT NULL,
      sha1 TEXT NOT NULL,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(audio_url, offset_sec)
    )
    """)
    conn.commit()
    return conn

def capture(url, offset, wav):
    cmd = [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel", "error",
        "-rw_timeout", "30000000",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_at_eof", "1",
        "-reconnect_delay_max", "5",
        "-user_agent", "Mozilla/5.0",
    ]
    if offset and offset > 0:
        cmd += ["-ss", str(offset)]
    cmd += [
        "-t", str(SAMPLE_SECONDS),
        "-i", url,
        "-vn",
        "-ac", "1",
        "-ar", "16000",
        "-y",
        str(wav),
    ]
    subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=SAMPLE_SECONDS + 70)
    if not wav.exists() or wav.stat().st_size < 10000:
        raise RuntimeError("sample too small")

def fpcalc_raw(wav):
    out = sh(["fpcalc", "-raw", "-json", str(wav)], timeout=60)
    data = json.loads(out)
    fp = data.get("fingerprint")
    if fp is None:
        raise RuntimeError("no raw fingerprint")
    if isinstance(fp, str):
        raw = [int(x) for x in fp.replace(";", ",").split(",") if x.strip()]
    else:
        raw = [int(x) for x in fp]
    if len(raw) < 20:
        raise RuntimeError("raw fingerprint too short")
    return raw, float(data.get("duration") or 0)

def bitdiff(a, b):
    return ((int(a) & 0xffffffff) ^ (int(b) & 0xffffffff)).bit_count()

def raw_distance(query, candidate):
    q = query
    c = candidate
    if len(q) < 20 or len(c) < 20:
        return 999.0

    # Compare shorter over longer using sliding window.
    if len(q) > len(c):
        short, long = c, q
    else:
        short, long = q, c

    window = len(short)
    best = 999.0
    step = 2 if len(long) - window > 20 else 1

    for start in range(0, len(long) - window + 1, step):
        total = 0
        part = long[start:start+window]
        for x, y in zip(short, part):
            total += bitdiff(x, y)
        avg = total / window
        if avg < best:
            best = avg
    return best

def load_audio_sources(limit):
    conn = sqlite3.connect(SRC_DB)
    conn.row_factory = sqlite3.Row
    c = cols(conn, "audio_sources")

    url_col = pick(c, ["audio_url", "url", "source_url", "media_url"])
    title_col = pick(c, ["title", "track_title", "name"])
    artist_col = pick(c, ["artist", "artist_name", "displayArtist"])

    if not url_col:
        raise RuntimeError("No URL column found in audio_sources")

    sql = f'''
    SELECT "{url_col}" AS audio_url,
           {f'"{title_col}"' if title_col else "NULL"} AS title,
           {f'"{artist_col}"' if artist_col else "NULL"} AS artist
    FROM audio_sources
    WHERE "{url_col}" IS NOT NULL
      AND "{url_col}" != ''
      AND lower("{url_col}") NOT LIKE '%zedhousezambia.com%'
    ORDER BY RANDOM()
    LIMIT {limit * 5}
    '''
    rows = [dict(r) for r in conn.execute(sql)]
    conn.close()
    return rows

def build_raw_index():
    raw = ensure_raw_db()
    existing = raw.execute("SELECT COUNT(*) FROM raw_segments").fetchone()[0]
    print("raw_index_existing:", existing, flush=True)

    added = 0
    attempted = 0
    sources = load_audio_sources(RAW_BUILD_LIMIT)

    for item in sources:
        if added >= RAW_BUILD_LIMIT:
            break

        url = item["audio_url"]
        domain = urlparse(url).netloc.lower()
        title = item.get("title") or ""
        artist = item.get("artist") or ""

        for offset in RAW_OFFSETS:
            if added >= RAW_BUILD_LIMIT:
                break

            exists = raw.execute("SELECT 1 FROM raw_segments WHERE audio_url=? AND offset_sec=?", (url, offset)).fetchone()
            if exists:
                continue

            attempted += 1
            tmp = Path(tempfile.mkdtemp(prefix="raw-localfp-"))
            wav = tmp / "sample.wav"

            try:
                capture(url, offset, wav)
                fp, dur = fpcalc_raw(wav)
                raw_json = json.dumps(fp, separators=(",", ":"))
                sha1 = hashlib.sha1(raw_json.encode()).hexdigest()
                raw.execute("""
                INSERT OR IGNORE INTO raw_segments
                (audio_url, artist, title, source_domain, offset_sec, duration_sec, raw_json, raw_len, sha1)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (url, artist, title, domain, offset, dur, raw_json, len(fp), sha1))
                raw.commit()
                added += 1
                print(f"RAW_ADDED {added}/{RAW_BUILD_LIMIT} offset={offset} {artist} - {title}", flush=True)
            except Exception as e:
                print(f"RAW_SKIP offset={offset} {artist} - {title} :: {str(e)[:120]}", flush=True)
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

    total = raw.execute("SELECT COUNT(*) FROM raw_segments").fetchone()[0]
    print("raw_index_total:", total, "added_now:", added, "attempted:", attempted, flush=True)
    raw.close()

def get_problem_stations():
    conn = sqlite3.connect(APP_DB)
    conn.row_factory = sqlite3.Row
    where = " OR ".join([f"lower(s.name) LIKE '%{t.replace(\"'\", \"''\")}%' " for t in TOKENS])
    rows = [dict(r) for r in conn.execute(f"""
    SELECT
      s.id AS stationId,
      s.name AS stationName,
      COALESCE(e.streamUrl, s.streamUrl) AS streamUrl
    FROM Station s
    LEFT JOIN StationStreamEndpoint e
      ON e.stationId = s.id
     AND COALESCE(e.isSuppressed, 0) = 0
    WHERE ({where})
      AND COALESCE(e.streamUrl, s.streamUrl) IS NOT NULL
      AND COALESCE(e.streamUrl, s.streamUrl) != ''
    GROUP BY s.id
    ORDER BY s.name
    LIMIT {STATION_LIMIT}
    """)]
    conn.close()
    return rows

def load_raw_segments():
    conn = sqlite3.connect(RAW_DB)
    conn.row_factory = sqlite3.Row
    rows = []
    for r in conn.execute("""
    SELECT id, artist, title, audio_url, offset_sec, raw_json, raw_len
    FROM raw_segments
    ORDER BY id DESC
    LIMIT 5000
    """):
        d = dict(r)
        d["raw"] = json.loads(d.pop("raw_json"))
        rows.append(d)
    conn.close()
    return rows

def test_stations():
    candidates = load_raw_segments()
    print("raw_candidates:", len(candidates), flush=True)

    if not candidates:
        print("NO_RAW_CANDIDATES_BUILD_INDEX_FIRST", flush=True)
        return

    stations = get_problem_stations()
    print("stations_found:", len(stations), flush=True)

    tested = 0
    fingerprinted = 0
    strong = 0
    possible = 0
    failed = 0

    for st in stations:
        tested += 1
        print("\n=== STATION", tested, "/", len(stations), st["stationName"], "===", flush=True)
        print("stream:", st["streamUrl"], flush=True)

        tmp = Path(tempfile.mkdtemp(prefix="raw-station-"))
        wav = tmp / "station.wav"

        try:
            capture(st["streamUrl"], 0, wav)
            qraw, qdur = fpcalc_raw(wav)
            fingerprinted += 1

            best = []
            for cand in candidates:
                dist = raw_distance(qraw, cand["raw"])
                best.append((dist, cand))
            best.sort(key=lambda x: x[0])

            print("TOP_MATCH_CANDIDATES:", flush=True)
            for dist, cand in best[:8]:
                level = "STRONG" if dist <= 8.5 else ("POSSIBLE" if dist <= 11.5 else "WEAK")
                print(f"{level} score={dist:.2f} offset={cand['offset_sec']} {cand.get('artist') or ''} - {cand.get('title') or ''} | {cand.get('audio_url')}", flush=True)

            if best and best[0][0] <= 8.5:
                strong += 1
                print("AUDIO_APPROX_STRONG_MATCH", flush=True)
            elif best and best[0][0] <= 11.5:
                possible += 1
                print("AUDIO_APPROX_POSSIBLE_MATCH", flush=True)
            else:
                print("AUDIO_APPROX_NO_SAFE_MATCH", flush=True)

        except Exception as e:
            failed += 1
            print("AUDIO_APPROX_FAILED", str(e)[:300], flush=True)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    print("\n=== SUMMARY ===", flush=True)
    print(json.dumps({
        "tested": tested,
        "fingerprinted": fingerprinted,
        "strong_matches": strong,
        "possible_matches": possible,
        "failed": failed,
        "temp_audio_kept": False,
        "decision": "strong/possible means exact LocalFingerprint lookup is too strict; no safe match means catalogue/source coverage is still weak for these stations"
    }, indent=2), flush=True)

if __name__ == "__main__":
    print("=== RAW APPROXIMATE MATCH PROOF START ===", flush=True)
    print("raw_build_limit:", RAW_BUILD_LIMIT, flush=True)
    print("offsets:", RAW_OFFSETS, flush=True)
    print("sample_seconds:", SAMPLE_SECONDS, flush=True)
    build_raw_index()
    test_stations()
