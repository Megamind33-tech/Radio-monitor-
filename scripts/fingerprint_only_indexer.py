#!/usr/bin/env python3
import csv, hashlib, json, os, sqlite3, subprocess, tempfile
from datetime import datetime, timezone
from pathlib import Path

BASE="/opt/radio-monitor"
INPUT=f"{BASE}/data/authorized_audio_sources.csv"
DB=f"{BASE}/data/fingerprint-index/zambian_fingerprint_index.db"
TMP=f"{BASE}/data/tmp-fp"

os.makedirs(os.path.dirname(DB), exist_ok=True)
os.makedirs(TMP, exist_ok=True)

def clean_url(u):
    return (u or "").split("?")[0].strip()

def h(x):
    return hashlib.sha256(clean_url(x).encode("utf-8")).hexdigest()

def connect_db():
    con=sqlite3.connect(DB)
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

def save(con,row,duration,fingerprint,status,error=""):
    audio=clean_url(row.get("audio_url",""))

    con.execute("""
    INSERT INTO tracks
    (audio_url_hash,source_page,audio_url,title,artist,album,duration,fingerprint,engine,status,error,indexed_at)
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
      h(audio),
      row.get("source_page",""),
      audio,
      row.get("title",""),
      row.get("artist",""),
      row.get("album",""),
      duration,
      fingerprint,
      "chromaprint/fpcalc",
      status,
      error[:1000],
      datetime.now(timezone.utc).isoformat()
    ))
    con.commit()

def already_indexed(con,audio):
    audio=clean_url(audio)
    return con.execute(
        "SELECT id FROM tracks WHERE audio_url_hash=? AND status='indexed'",
        (h(audio),)
    ).fetchone() is not None

def make_wav(audio_url,wav_path):
    audio_url=clean_url(audio_url)

    subprocess.run([
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel","error",
        "-i",audio_url,
        "-vn",
        "-ac","1",
        "-ar","16000",
        wav_path
    ], check=True, timeout=240)

def fingerprint(wav_path):
    r=subprocess.run(
        ["fpcalc","-json",wav_path],
        check=True,
        capture_output=True,
        text=True,
        timeout=120
    )
    data=json.loads(r.stdout)
    duration=float(data.get("duration",0))
    fp=data.get("fingerprint","")
    if not fp:
        raise RuntimeError("empty fingerprint")
    return duration,fp

def main():
    if not os.path.exists(INPUT):
        raise SystemExit(f"Missing input CSV: {INPUT}")

    con=connect_db()

    with open(INPUT,newline="",encoding="utf-8") as f:
        for row in csv.DictReader(f):
            audio=clean_url(row.get("audio_url",""))

            if not audio:
                continue

            row["audio_url"]=audio

            if already_indexed(con,audio):
                print("SKIP already indexed:", row.get("artist",""), "-", row.get("title",""))
                continue

            print("")
            print("INDEXING:", row.get("artist",""), "-", row.get("title",""))
            print(audio)

            try:
                with tempfile.TemporaryDirectory(prefix="fp_",dir=TMP) as td:
                    wav=str(Path(td)/"temp.wav")
                    make_wav(audio,wav)
                    duration,fp=fingerprint(wav)
                    save(con,row,duration,fp,"indexed")
                    print("OK indexed duration:", duration)
            except Exception as e:
                save(con,row,0,"","failed",str(e))
                print("FAILED:", e)

    con.close()
    print("")
    print("DONE. Fingerprint DB:", DB)

if __name__=="__main__":
    main()
