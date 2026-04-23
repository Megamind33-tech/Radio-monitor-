-- Structured diagnostics for detection / match pipeline tuning
ALTER TABLE "DetectionLog" ADD COLUMN "matchDiagnosticsJson" TEXT;

-- Station-level tuning (optional; defaults keep prior behavior)
ALTER TABLE "Station" ADD COLUMN "metadataTrustTightness" INTEGER NOT NULL DEFAULT 0;
ALTER TABLE "Station" ADD COLUMN "fingerprintRetries" INTEGER NOT NULL DEFAULT 2;
ALTER TABLE "Station" ADD COLUMN "fingerprintRetryDelayMs" INTEGER NOT NULL DEFAULT 3500;
ALTER TABLE "Station" ADD COLUMN "catalogConfidenceFloor" REAL;
