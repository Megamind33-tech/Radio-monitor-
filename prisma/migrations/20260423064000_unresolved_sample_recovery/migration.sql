-- Add recovery tracking fields to unresolved sample archive
ALTER TABLE "UnresolvedSample" ADD COLUMN "detectionLogId" TEXT;
ALTER TABLE "UnresolvedSample" ADD COLUMN "recoveryStatus" TEXT NOT NULL DEFAULT 'pending';
ALTER TABLE "UnresolvedSample" ADD COLUMN "recoveryAttempts" INTEGER NOT NULL DEFAULT 0;
ALTER TABLE "UnresolvedSample" ADD COLUMN "lastRecoveryAt" DATETIME;
ALTER TABLE "UnresolvedSample" ADD COLUMN "recoveredAt" DATETIME;
ALTER TABLE "UnresolvedSample" ADD COLUMN "lastRecoveryError" TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS "UnresolvedSample_detectionLogId_key" ON "UnresolvedSample"("detectionLogId");
CREATE INDEX IF NOT EXISTS "UnresolvedSample_stationId_recoveryStatus_createdAt_idx" ON "UnresolvedSample"("stationId", "recoveryStatus", "createdAt");
