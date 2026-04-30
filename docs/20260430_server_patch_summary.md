# Server Patch Summary - 2026-04-30

This branch captures server-side patch work done directly on the DigitalOcean deployment for the radio monitor.

## Main fixes done on server

- Added/used review tables for unknown metadata cleanup.
- Classified unknown rows into clearer statuses.
- Repaired 160 safe artist/title reversal rows.
- Verified those repairs in MetadataParseCandidate, UnknownAudioReview, and DetectionLog.
- Created SafeFingerprintRecoveryCandidate table for review-only fingerprint recovery candidates.
- Created safe exact fingerprint probe script: scripts/safe_fp_exact_probe.py

## Important warning

Do not commit runtime database contents, .env files, logs, unresolved audio samples, or song sample folders.

The SQLite database on the server contains runtime data and should not be pushed to GitHub.
Use the manual migration SQL as reference and convert needed schema into the real migration system.
