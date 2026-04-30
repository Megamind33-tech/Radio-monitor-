#!/usr/bin/env python3
import json, hashlib, pathlib, sqlite3, subprocess, sys

DB = "prisma/dev_runtime.db"
LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 10

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA busy_timeout=30000;")

rows = conn.execute("""
SELECT us.id, us.stationId, us.detectionLogId, us.filePath
FROM UnresolvedSample us
JOIN UnknownAudioReview u ON u.detectionLogId = us.detectionLogId
LEFT JOIN SafeFingerprintRecoveryCandidate c ON c.unresolvedSampleId = us.id
WHERE u.status='needs_audio_fingerprint'
  AND us.recoveryStatus='pending'
  AND us.filePath IS NOT NULL
  AND trim(us.filePath) != ''
  AND c.id IS NULL
ORDER BY us.createdAt ASC
LIMIT ?
""", (LIMIT,)).fetchall()

print(json.dumps({"step":"start","selected":len(rows)}))

inserted = no_match = failed = missing = 0

for r in rows:
    fp_path = r["filePath"]

    if not pathlib.Path(fp_path).exists():
        missing += 1
        print(json.dumps({"sample":r["id"],"status":"missing_file"}))
        continue

    p = subprocess.run(["fpcalc","-json",fp_path], capture_output=True, text=True, timeout=25)

    if p.returncode != 0:
        failed += 1
        print(json.dumps({"sample":r["id"],"status":"fpcalc_failed","code":p.returncode}))
        continue

    try:
        data = json.loads(p.stdout or "{}")
        fingerprint = data["fingerprint"]
        duration = int(round(float(data["duration"])))
    except Exception as e:
        failed += 1
        print(json.dumps({"sample":r["id"],"status":"bad_fpcalc_json","error":str(e)}))
        continue

    sha1 = hashlib.sha1(fingerprint.encode()).hexdigest()
    hit = conn.execute("SELECT * FROM LocalFingerprint WHERE fingerprintSha1=? LIMIT 1", (sha1,)).fetchone()

    if not hit:
        no_match += 1
        print(json.dumps({"sample":r["id"],"status":"no_exact_match","duration":duration}))
        continue

    conn.execute("""
    INSERT OR REPLACE INTO SafeFingerprintRecoveryCandidate
    (id, unresolvedSampleId, detectionLogId, stationId, filePath,
     localFingerprintId, candidateArtist, candidateTitle,
     candidateDurationSec, sampleDurationSec, matchType, similarity,
     status, updatedAt)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'exact_sha1', 1.0, 'needs_review', CURRENT_TIMESTAMP)
    """, (
        "safe_fp_" + r["id"], r["id"], r["detectionLogId"], r["stationId"], fp_path,
        hit["id"], hit["artist"], hit["title"], hit["durationSec"], duration
    ))

    conn.commit()
    inserted += 1
    print(json.dumps({"sample":r["id"],"status":"candidate_inserted","artist":hit["artist"],"title":hit["title"]}))

print(json.dumps({"step":"done","inserted":inserted,"no_match":no_match,"failed":failed,"missing":missing}))
conn.close()
