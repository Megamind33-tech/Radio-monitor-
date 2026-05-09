#!/usr/bin/env python3
import os, json, uuid, time, hashlib, sqlite3, tempfile, subprocess, shutil
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, unquote

BASE = Path("/opt/radio-monitor")
SOURCE_DB = BASE / "data/source-discovery/zambian_crawl_state.db"
APP_DB = BASE / "prisma/dev_runtime.db"
STATE_DB = BASE / "data/fingerprint-index/direct_audio_to_localfp_state.db"

LIMIT = int(os.environ.get("LIMIT", "100"))
OFFSET = int(os.environ.get("OFFSET", "45"))
SAMPLE_SECONDS = int(os.environ.get("SAMPLE_SECONDS", "35"))
SOURCE_NAME = os.environ.get("TRUSTED_LOCALFP_SOURCE", "zambian_trusted_35s")
SKIP_DOMAINS = [d.strip().lower() for d in os.environ.get("DIRECT_SKIP_DOMAINS", "zedhousezambia.com").split(",") if d.strip()]
MAX_FAILED_ATTEMPTS = int(os.environ.get("MAX_FAILED_ATTEMPTS", "2"))

def now():
    return datetime.utcnow().isoformat(timespec="seconds")

def sha1(s):
    return hashlib.sha1((s or "").encode("utf-8", "ignore")).hexdigest()

def qident(name):
    return '"' + name.replace('"', '""') + '"'

def connect(path):
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn

def cols(conn, table):
    return [dict(r) for r in conn.execute(f"PRAGMA table_info({qident(table)})")]

def col_names(conn, table):
    return [r["name"] for r in cols(conn, table)]

def pick(col_list, names):
    lower = {c.lower(): c for c in col_list}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None

def title_from_url(url):
    try:
        name = Path(unquote(urlparse(url).path)).stem
        name = name.replace("_", " ").replace("-", " ").strip()
        return name[:180] if name else "UNKNOWN"
    except Exception:
        return "UNKNOWN"

def ensure_state():
    STATE_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(STATE_DB)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS direct_imports (
      audio_url_hash TEXT PRIMARY KEY,
      audio_url TEXT,
      artist TEXT,
      title TEXT,
      status TEXT,
      attempts INTEGER DEFAULT 0,
      error TEXT,
      duration REAL,
      fingerprint_sha1 TEXT,
      created_at TEXT,
      updated_at TEXT
    )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_direct_status ON direct_imports(status)")
    conn.commit()
    return conn

def load_candidates(state):
    src = connect(SOURCE_DB)
    source_cols = col_names(src, "audio_sources")

    url_col = pick(source_cols, ["audio_url", "url", "source_url"])
    artist_col = pick(source_cols, ["artist", "artist_name"])
    title_col = pick(source_cols, ["title", "track_title", "name"])
    page_col = pick(source_cols, ["source_page", "page_url", "referer"])
    domain_col = pick(source_cols, ["source_domain", "domain"])

    if not url_col:
        raise RuntimeError("audio_sources has no audio_url/url column")

    select_cols = [url_col]
    for c in [artist_col, title_col, page_col, domain_col]:
        if c and c not in select_cols:
            select_cols.append(c)

    rows = []
    sql = f"""
    SELECT {",".join(qident(c) for c in select_cols)}
    FROM audio_sources
    WHERE {qident(url_col)} IS NOT NULL AND {qident(url_col)} != ''
    ORDER BY rowid DESC
    LIMIT ?
    """

    for r in src.execute(sql, (LIMIT * 300,)):
        url = str(r[url_col] or "").strip()
        if not url.startswith("http"):
            continue

        domain_now = urlparse(url).netloc.lower()
        if any(d in domain_now for d in SKIP_DOMAINS):
            continue

        url_hash = sha1(url)
        st = state.execute(
            "SELECT status, attempts FROM direct_imports WHERE audio_url_hash=?",
            (url_hash,)
        ).fetchone()

        if st and st["status"] == "imported":
            continue
        if st and st["status"] == "failed" and int(st["attempts"] or 0) >= MAX_FAILED_ATTEMPTS:
            continue

        artist = str(r[artist_col] or "").strip() if artist_col else ""
        title = str(r[title_col] or "").strip() if title_col else ""

        if not artist:
            artist = "UNKNOWN"
        if not title:
            title = title_from_url(url)

        rows.append({
            "audio_url": url,
            "audio_url_hash": url_hash,
            "artist": artist[:180],
            "title": title[:220],
            "source_page": str(r[page_col] or "").strip() if page_col else "",
            "source_domain": str(r[domain_col] or "").strip() if domain_col else urlparse(url).netloc.lower(),
        })

        if len(rows) >= LIMIT:
            break

    src.close()
    return rows

def make_wav(url, wav_path):
    last_err = None
    for off in [OFFSET, 0]:
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
            "-ss", str(off),
            "-i", url,
            "-t", str(SAMPLE_SECONDS),
            "-vn",
            "-ac", "1",
            "-ar", "16000",
            "-y",
            str(wav_path),
        ]
        try:
            subprocess.run(cmd, check=True, timeout=90)
            if wav_path.exists() and wav_path.stat().st_size > 10000:
                return off
            last_err = "tiny_or_empty_wav"
        except Exception as e:
            last_err = str(e)
    raise RuntimeError(last_err or "ffmpeg_failed")

def calc_fp(wav_path):
    out = subprocess.check_output(["fpcalc", "-json", str(wav_path)], stderr=subprocess.STDOUT, timeout=60)
    data = json.loads(out.decode("utf-8", "ignore"))
    fp = data.get("fingerprint") or ""
    duration = float(data.get("duration") or 0)
    if not fp or duration <= 0:
        raise RuntimeError("fpcalc_no_fingerprint")
    return duration, fp, sha1(fp)

def insert_localfp(app, item, duration, fp, fp_sha, used_offset):
    lf_cols_info = cols(app, "LocalFingerprint")
    lf_cols = [c["name"] for c in lf_cols_info]

    sha_col = pick(lf_cols, ["fingerprintSha1", "fingerprint_sha1"])
    if sha_col:
        exists = app.execute(
            f"SELECT 1 FROM {qident('LocalFingerprint')} WHERE {qident(sha_col)}=? LIMIT 1",
            (fp_sha,)
        ).fetchone()
        if exists:
            return "already_exists"

    values = {
        "id": "direct_" + fp_sha[:28],
        "source": SOURCE_NAME,
        "artist": item["artist"],
        "displayArtist": item["artist"],
        "title": item["title"],
        "titleWithoutFeat": item["title"],
        "duration": duration,
        "durationSec": duration,
        "durationSeconds": duration,
        "offsetSec": used_offset,
        "offset_sec": used_offset,
        "fingerprint": fp,
        "fingerprintSha1": fp_sha,
        "fingerprint_sha1": fp_sha,
        "audioUrl": item["audio_url"],
        "audio_url": item["audio_url"],
        "url": item["audio_url"],
        "sourcePage": item["source_page"],
        "source_page": item["source_page"],
        "sourceDomain": item["source_domain"],
        "source_domain": item["source_domain"],
        "metadataSource": "direct_audio_sources_to_localfp",
        "metadata_source": "direct_audio_sources_to_localfp",
        "createdAt": now(),
        "updatedAt": now(),
        "created_at": now(),
        "updated_at": now(),
    }

    insert_cols = []
    insert_vals = []

    for c in lf_cols_info:
        name = c["name"]
        typ = (c.get("type") or "").upper()
        required = bool(c.get("notnull"))
        has_default = c.get("dflt_value") is not None
        pk = bool(c.get("pk"))

        if name in values:
            insert_cols.append(name)
            insert_vals.append(values[name])
        elif required and not has_default and not pk:
            # safe fallback for unexpected NOT NULL columns
            if "INT" in typ or "REAL" in typ or "NUM" in typ:
                insert_cols.append(name)
                insert_vals.append(0)
            else:
                insert_cols.append(name)
                insert_vals.append("")

    if "fingerprint" not in insert_cols:
        raise RuntimeError("LocalFingerprint has no fingerprint column")

    sql = f"""
    INSERT OR IGNORE INTO {qident('LocalFingerprint')}
    ({",".join(qident(c) for c in insert_cols)})
    VALUES ({",".join("?" for _ in insert_cols)})
    """
    before = app.total_changes
    app.execute(sql, insert_vals)
    app.commit()

    return "inserted" if app.total_changes > before else "ignored"

def mark(state, item, status, err="", duration=None, fp_sha=""):
    old = state.execute(
        "SELECT attempts FROM direct_imports WHERE audio_url_hash=?",
        (item["audio_url_hash"],)
    ).fetchone()
    attempts = int(old["attempts"] or 0) + 1 if old else 1

    state.execute("""
    INSERT INTO direct_imports
    (audio_url_hash,audio_url,artist,title,status,attempts,error,duration,fingerprint_sha1,created_at,updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(audio_url_hash) DO UPDATE SET
      status=excluded.status,
      attempts=excluded.attempts,
      error=excluded.error,
      duration=excluded.duration,
      fingerprint_sha1=excluded.fingerprint_sha1,
      updated_at=excluded.updated_at
    """, (
        item["audio_url_hash"], item["audio_url"], item["artist"], item["title"],
        status, attempts, err[:600], duration, fp_sha, now(), now()
    ))
    state.commit()

def main():
    print("=== DIRECT AUDIO URL → LOCALFINGERPRINT POLLER ===")
    print("limit:", LIMIT)
    print("offset:", OFFSET)
    print("sample_seconds:", SAMPLE_SECONDS)
    print("source:", SOURCE_NAME)
    print("skip_domains:", ",".join(SKIP_DOMAINS) if SKIP_DOMAINS else "none")

    state = ensure_state()
    app = connect(APP_DB)
    candidates = load_candidates(state)

    print("candidates:", len(candidates))

    imported = skipped = failed = 0

    for i, item in enumerate(candidates, 1):
        print(f"\nDIRECT {i}/{len(candidates)} {item['artist']} - {item['title']}")
        print(item["audio_url"])

        tmpdir = tempfile.mkdtemp(prefix="direct-localfp-")
        wav = Path(tmpdir) / "sample.wav"

        try:
            used_offset = make_wav(item["audio_url"], wav)
            duration, fp, fp_sha = calc_fp(wav)
            result = insert_localfp(app, item, duration, fp, fp_sha, used_offset)

            if result == "inserted":
                imported += 1
            else:
                skipped += 1

            mark(state, item, "imported", result, duration, fp_sha)
            print(result.upper(), "duration=", duration, "offset=", used_offset, "sha1=", fp_sha)

        except Exception as e:
            failed += 1
            mark(state, item, "failed", str(e))
            print("FAILED", str(e)[:300])

        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    print("\n=== SUMMARY ===")
    print("imported:", imported)
    print("skipped:", skipped)
    print("failed:", failed)

    app.close()
    state.close()

if __name__ == "__main__":
    main()
