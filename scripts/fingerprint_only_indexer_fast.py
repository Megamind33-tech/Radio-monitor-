#!/usr/bin/env python3
import csv, os, sqlite3, hashlib, subprocess, tempfile
from pathlib import Path
from datetime import datetime, timezone

BASE="/opt/radio-monitor"
INPUT=os.environ.get("INPUT", f"{BASE}/data/authorized_audio_sources.csv")
DB=f"{BASE}/data/fingerprint-index/zambian_fingerprint_index.db"
TMP=f"{BASE}/data/tmp-fp"

MAX_SECONDS=180
FFMPEG_TIMEOUT=120
FP_TIMEOUT=60

os.makedirs(os.path.dirname(DB), exist_ok=True)
os.makedirs(TMP, exist_ok=True)

def now():
    return datetime.now(timezone.utc).isoformat()

def h(url):
    return hashlib.sha256(url.encode("utf-8")).hexdigest()

def connect():
    con=sqlite3.connect(DB, timeout=60)
    con.execute('PRAGMA journal_mode=WAL')
    con.execute('PRAGMA busy_timeout=60000')
    con.execute("""
    CREATE TABLE IF NOT EXISTS tracks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      audio_url_hash TEXT UNIQUE,
      source_page TEXT,
      audio_url TEXT,
      title TEXT,
      artist TEXT,
      album TEXT,
      duration REAL,
      fingerprint TEXT,
      engine TEXT,
      status TEXT,
      error TEXT,
      indexed_at TEXT
    )
    """)
    con.commit()
    return con

def already(con, audio_url):
    return con.execute(
        "SELECT id FROM tracks WHERE audio_url_hash=? AND status='indexed'",
        (h(audio_url),)
    ).fetchone() is not None

def save(con, row, duration, fingerprint, status, error=""):
    audio_url=row.get("audio_url","").strip()
    con.execute("""
    INSERT INTO tracks (
      audio_url_hash, source_page, audio_url, title, artist, album,
      duration, fingerprint, engine, status, error, indexed_at
    )
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(audio_url_hash) DO UPDATE SET
      source_page=excluded.source_page,
      audio_url=excluded.audio_url,
      title=excluded.title,
      artist=excluded.artist,
      album=excluded.album,
      duration=excluded.duration,
      fingerprint=excluded.fingerprint,
      engine=excluded.engine,
      status=excluded.status,
      error=excluded.error,
      indexed_at=excluded.indexed_at
    """, (
        h(audio_url),
        row.get("source_page",""),
        audio_url,
        row.get("title",""),
        row.get("artist",""),
        row.get("album",""),
        duration,
        fingerprint,
        "fpcalc",
        status,
        error[:1000],
        now()
    ))
    con.commit()

def make_wav(audio_url, wav_path):
    cmd=[
        "ffmpeg",
        "-nostdin",
        "-y",
        "-hide_banner",
        "-loglevel","error",
        "-rw_timeout","30000000",
        "-reconnect","1",
        "-reconnect_streamed","1",
        "-reconnect_delay_max","5",
        "-i", audio_url,
        "-t", str(MAX_SECONDS),
        "-vn",
        "-ac","1",
        "-ar","16000",
        wav_path
    ]
    subprocess.run(cmd, check=True, timeout=FFMPEG_TIMEOUT)

def fpcalc(wav_path):
    res=subprocess.run(
        ["fpcalc","-json",wav_path],
        check=True,
        capture_output=True,
        text=True,
        timeout=FP_TIMEOUT
    )

    import json
    data=json.loads(res.stdout)
    duration=float(data.get("duration",0) or 0)
    fingerprint=data.get("fingerprint","") or ""

    if not fingerprint:
        raise RuntimeError("empty fingerprint")

    return duration, fingerprint

def main():
    if not os.path.exists(INPUT):
        raise SystemExit(f"Missing input CSV: {INPUT}")

    con=connect()
    total=0
    indexed=0
    failed=0
    skipped=0

    with open(INPUT, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            audio=(row.get("audio_url") or "").strip()
            if not audio:
                continue

            total += 1

            if already(con, audio):
                print(f"SKIP already indexed: {row.get('artist','')} - {row.get('title','')}", flush=True)
                skipped += 1
                continue

            print("")
            print(f"[{total}] INDEXING: {row.get('artist','')} - {row.get('title','')}", flush=True)
            print(audio, flush=True)

            try:
                with tempfile.TemporaryDirectory(prefix="fp-", dir=TMP) as td:
                    wav=str(Path(td)/"sample.wav")
                    make_wav(audio, wav)
                    duration, fingerprint=fpcalc(wav)

                save(con,row,duration,fingerprint,"indexed","")
                indexed += 1
                print(f"OK indexed sampled duration: {duration}", flush=True)

            except Exception as e:
                failed += 1
                save(con,row,0,"","failed",str(e))
                print(f"FAILED: {e}", flush=True)

    con.close()

    print("")
    print(f"DONE. Input rows: {total}")
    print(f"Indexed: {indexed}")
    print(f"Skipped existing: {skipped}")
    print(f"Failed: {failed}")
    print(f"DB: {DB}")

if __name__=="__main__":
    main()
