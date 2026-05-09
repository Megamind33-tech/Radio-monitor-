#!/usr/bin/env python3
import sqlite3

APP_DB="/opt/radio-monitor/prisma/dev_runtime.db"
STATE_DB="/opt/radio-monitor/data/fingerprint-index/trusted_localfp_sync_state.db"

app=sqlite3.connect(APP_DB)
state=sqlite3.connect(STATE_DB)

print("TRUSTED LOCALFP SOURCE COUNTS:")
for r in app.execute("""
SELECT source, COUNT(*) rows, COALESCE(SUM(timesMatched),0) matches
FROM LocalFingerprint
GROUP BY source
ORDER BY rows DESC;
"""):
    print(r)

print("")
print("CHECK IMPORTED HASHES EXIST IN LocalFingerprint:")
rows=state.execute("""
SELECT track_id, offset_sec, fingerprint_sha1
FROM imported_segments
WHERE status='imported'
ORDER BY updated_at DESC
LIMIT 20;
""").fetchall()

missing=0
for track_id, offset, fp_hash in rows:
    found=app.execute("""
    SELECT artist, title, durationSec, source, confidence, timesMatched
    FROM LocalFingerprint
    WHERE fingerprintSha1=?
    """,(fp_hash,)).fetchone()
    if found:
        print("OK", track_id, offset, fp_hash, "|", found)
    else:
        missing += 1
        print("MISSING", track_id, offset, fp_hash)

print("")
print("Missing imported hashes:", missing)

print("")
print("TRUSTED DURATION DISTRIBUTION:")
for r in app.execute("""
SELECT durationSec, COUNT(*)
FROM LocalFingerprint
WHERE source='zambian_trusted_35s'
GROUP BY durationSec
ORDER BY durationSec;
"""):
    print(r)

app.close()
state.close()
