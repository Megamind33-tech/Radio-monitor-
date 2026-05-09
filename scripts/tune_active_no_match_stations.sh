#!/usr/bin/env bash
set -euo pipefail

DB="/opt/radio-monitor/prisma/dev_runtime.db"

sqlite3 "$DB" "
UPDATE Station
SET
  fingerprintFallbackEnabled=1,
  sampleSeconds=35,
  audioFingerprintIntervalSeconds=90,
  icyVerificationIntervalSeconds=120,
  fingerprintRetries=2,
  fingerprintRetryDelayMs=2500,
  catalogConfidenceFloor=0.68
WHERE isActive=1
  AND monitorState='ACTIVE_NO_MATCH';
"

echo "$(date -Is) Tuned ACTIVE_NO_MATCH stations"

sqlite3 "$DB" "
SELECT COUNT(*)
FROM Station
WHERE isActive=1
  AND monitorState='ACTIVE_NO_MATCH';
"
