#!/usr/bin/env python3
import hashlib, json, os, sqlite3, subprocess, tempfile
from pathlib import Path

BASE="/opt/radio-monitor"
ZMB_DB=f"{BASE}/data/fingerprint-index/zambian_fingerprint_index.db"
APP_DB=f"{BASE}/prisma/dev_runtime.db"
STATE_DB=f"{BASE}/data/fingerprint-index/trusted_localfp_sync_state.db"
TMP=f"{BASE}/data/tmp-localfp-test"

Path(TMP).mkdir(parents=True, exist_ok=True)

def sha1(s):
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

state=sqlite3.connect(STATE_DB)
zdb=sqlite3.connect(ZMB_DB)
zdb.row_factory=sqlite3.Row
app=sqlite3.connect(APP_DB)
app.row_factory=sqlite3.Row

row=state.execute("""
SELECT track_id, offset_sec, fingerprint_sha1
FROM imported_segments
WHERE status='imported'
ORDER BY updated_at DESC
LIMIT 1;
""").fetchone()

if not row:
    raise SystemExit("No imported trusted segment found.")

track_id, offset, expected_hash = row

track=zdb.execute("""
SELECT id, artist, title, audio_url
FROM tracks
WHERE id=?;
""",(track_id,)).fetchone()

if not track:
    raise SystemExit(f"Track not found: {track_id}")

print("Testing:")
print(track["id"], track["artist"], "-", track["title"])
print("Offset:", offset)
print("URL:", track["audio_url"])
print("Expected hash:", expected_hash)

with tempfile.TemporaryDirectory(prefix="exact-", dir=TMP) as td:
    wav=str(Path(td)/"sample.wav")

    subprocess.run([
        "ffmpeg",
        "-nostdin","-y","-hide_banner","-loglevel","error",
        "-rw_timeout","30000000",
        "-reconnect","1",
        "-reconnect_streamed","1",
        "-reconnect_delay_max","5",
        "-i", track["audio_url"],
        "-ss", str(offset),
        "-t", "35",
        "-vn",
        "-ac", "1",
        "-ar", "16000",
        wav
    ], check=True, timeout=120)

    out=subprocess.run(["fpcalc","-json",wav], check=True, capture_output=True, text=True, timeout=60)
    data=json.loads(out.stdout)
    fp=data["fingerprint"]
    got_hash=sha1(fp)

print("Computed hash:", got_hash)

found=app.execute("""
SELECT artist, title, durationSec, source, confidence, timesMatched
FROM LocalFingerprint
WHERE fingerprintSha1=?;
""",(got_hash,)).fetchone()

print("")
if found:
    print("EXACT LOCALFINGERPRINT MATCH OK")
    print(dict(found))
else:
    print("NO EXACT LOCALFINGERPRINT MATCH")
    raise SystemExit(1)

app.close()
zdb.close()
state.close()
