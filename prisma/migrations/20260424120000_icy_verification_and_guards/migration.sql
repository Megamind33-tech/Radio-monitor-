-- Periodic ICY verification via fingerprint (seconds); default 120.
ALTER TABLE "Station" ADD COLUMN "icyVerificationIntervalSeconds" INTEGER NOT NULL DEFAULT 120;
-- Last time we fingerprinted solely to verify unchanged trusted ICY against audio.
ALTER TABLE "Station" ADD COLUMN "lastIcyVerificationFingerprintAt" DATETIME;

-- More frequent scheduled fingerprint when ICY is stable (was 300).
UPDATE "Station" SET "audioFingerprintIntervalSeconds" = 120 WHERE "audioFingerprintIntervalSeconds" >= 300;
