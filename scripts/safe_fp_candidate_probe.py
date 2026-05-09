#!/usr/bin/env python3
import argparse
import base64
import hashlib
import json
import pathlib
import sqlite3
import subprocess
from typing import Optional

DB = "prisma/dev_runtime.db"
PREFIX_LEN = 48


def run_fpcalc(path: str):
    try:
        p = subprocess.run(["fpcalc", "-json", path], capture_output=True, text=True, timeout=30)
        if p.returncode != 0:
            return None, f"fpcalc_exit_{p.returncode}"
        data = json.loads(p.stdout or "{}")
        fp = data.get("fingerprint")
        dur = data.get("duration")
        if not fp or dur is None:
            return None, "missing_fingerprint_or_duration"
        return {"fingerprint": fp, "duration": int(round(float(dur)))}, None
    except Exception as e:
        return None, str(e)


def sim_ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    n = min(len(a), len(b))
    if n <= 0:
        return 0.0
    same = sum(1 for i in range(n) if a[i] == b[i])
    return same / max(len(a), len(b))


def decode_chromaprint(fp: str) -> Optional[list[int]]:
    if not fp:
        return None
    try:
        normalized = fp.replace("-", "+").replace("_", "/")
        normalized += "=" * ((4 - (len(normalized) % 4)) % 4)
        raw = base64.b64decode(normalized)
        if len(raw) < 8:
            return None
        body_len = ((len(raw) - 4) // 4) * 4
        body = raw[4 : 4 + body_len]
        if len(body) < 4:
            return None
        return [int.from_bytes(body[i : i + 4], "little") for i in range(0, len(body), 4)]
    except Exception:
        return None


def best_bit_error_rate(query: list[int], candidate: list[int]) -> float:
    if not query or not candidate:
        return 1.0
    if len(query) > len(candidate):
        return best_bit_error_rate(candidate, query)
    qn = len(query)
    cn = len(candidate)
    stride = max(1, qn // 32)
    best = 1.0
    for offset in range(0, cn - qn + 1, stride):
        errors = 0
        for i in range(qn):
            errors += ((query[i] ^ candidate[offset + i]) & 0xFFFFFFFF).bit_count()
        ber = errors / (qn * 32)
        if ber < best:
            best = ber
        if best == 0:
            break
    return best


def insert_candidate(conn, row, file_path, best, duration, match_type, similarity):
    conn.execute(
        """
        INSERT OR REPLACE INTO SafeFingerprintRecoveryCandidate (
          id, unresolvedSampleId, detectionLogId, stationId, filePath,
          localFingerprintId, candidateArtist, candidateTitle,
          candidateDurationSec, sampleDurationSec, matchType, similarity,
          status, updatedAt
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'needs_review', CURRENT_TIMESTAMP)
        """,
        (
            "safe_fp_" + row["id"],
            row["id"],
            row["detectionLogId"],
            row["stationId"],
            file_path,
            best["id"],
            best["artist"],
            best["title"],
            best["durationSec"],
            duration,
            match_type,
            similarity,
        ),
    )


def main():
    ap = argparse.ArgumentParser(description="Probe unresolved audio against LocalFingerprint safely.")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--duration-tolerance", type=int, default=30)
    ap.add_argument("--min-prefix-similarity", type=float, default=0.80)
    ap.add_argument("--max-ber", type=float, default=0.35)
    ap.add_argument("--max-duration-candidates", type=int, default=300)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000;")

    rows = conn.execute(
        """
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
        """,
        (args.limit,),
    ).fetchall()

    print(json.dumps({"step": "probe_start", "selected": len(rows), "dry_run": args.dry_run}))

    stats = {
        "inserted": 0,
        "would_insert": 0,
        "exact_sha1": 0,
        "prefix_similarity": 0,
        "chromaprint_ber": 0,
        "no_match": 0,
        "missing_file": 0,
        "fp_failed": 0,
    }

    for row in rows:
        file_path = row["filePath"]
        if not pathlib.Path(file_path).exists():
            stats["missing_file"] += 1
            print(json.dumps({"sample": row["id"], "status": "missing_file", "filePath": file_path}))
            continue

        fp, err = run_fpcalc(file_path)
        if not fp:
            stats["fp_failed"] += 1
            print(json.dumps({"sample": row["id"], "status": "fp_failed", "error": err}))
            continue

        fingerprint = fp["fingerprint"]
        duration = fp["duration"]
        sha1 = hashlib.sha1(fingerprint.encode()).hexdigest()
        prefix = fingerprint[:PREFIX_LEN]
        dur_min = max(1, duration - args.duration_tolerance)
        dur_max = duration + args.duration_tolerance

        best = conn.execute(
            "SELECT * FROM LocalFingerprint WHERE fingerprintSha1=? LIMIT 1", (sha1,)
        ).fetchone()
        match_type = "exact_sha1" if best else None
        similarity = 1.0 if best else 0.0
        candidate_count = 1 if best else 0

        if not best:
            candidates = conn.execute(
                """
                SELECT *
                FROM LocalFingerprint
                WHERE fingerprintPrefix=?
                  AND durationSec BETWEEN ? AND ?
                LIMIT 50
                """,
                (prefix, dur_min, dur_max),
            ).fetchall()
            candidate_count = len(candidates)
            for cand in candidates:
                s = sim_ratio(cand["fingerprint"], fingerprint)
                if s > similarity:
                    similarity = s
                    best = cand
                    match_type = "prefix_similarity"
            if best and similarity < args.min_prefix_similarity:
                best = None
                match_type = None

        if not best:
            query = decode_chromaprint(fingerprint)
            candidates = conn.execute(
                """
                SELECT *
                FROM LocalFingerprint
                WHERE durationSec BETWEEN ? AND ?
                ORDER BY lastMatchedAt DESC
                LIMIT ?
                """,
                (dur_min, dur_max, args.max_duration_candidates),
            ).fetchall()
            candidate_count = len(candidates)
            best_ber = 1.0
            if query:
                for cand in candidates:
                    cand_decoded = decode_chromaprint(cand["fingerprint"])
                    if not cand_decoded:
                        continue
                    ber = best_bit_error_rate(query, cand_decoded)
                    if ber < best_ber:
                        best_ber = ber
                        best = cand
                        match_type = "chromaprint_ber"
                    if best_ber == 0:
                        break
            similarity = 1.0 - best_ber
            if not best or best_ber > args.max_ber:
                best = None
                match_type = None

        if not best:
            stats["no_match"] += 1
            print(json.dumps({
                "sample": row["id"],
                "status": "no_local_candidate",
                "duration": duration,
                "durationWindow": [dur_min, dur_max],
                "candidatesChecked": candidate_count,
            }))
            continue

        stats[match_type] += 1
        payload = {
            "sample": row["id"],
            "status": "candidate_inserted" if not args.dry_run else "candidate_found_dry_run",
            "artist": best["artist"],
            "title": best["title"],
            "matchType": match_type,
            "similarity": round(similarity, 4),
            "duration": duration,
            "candidateDuration": best["durationSec"],
            "candidatesChecked": candidate_count,
        }
        if args.dry_run:
            stats["would_insert"] += 1
        else:
            insert_candidate(conn, row, file_path, best, duration, match_type, similarity)
            conn.commit()
            stats["inserted"] += 1
        print(json.dumps(payload))

    print(json.dumps({"step": "probe_done", **stats}))
    conn.close()


if __name__ == "__main__":
    main()
