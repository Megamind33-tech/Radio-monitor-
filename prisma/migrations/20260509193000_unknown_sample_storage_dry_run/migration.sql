-- Phase 3 foundation: storage visibility and purge dry-run metadata
ALTER TABLE "UnresolvedSample" ADD COLUMN "originalSha256" TEXT;
ALTER TABLE "UnresolvedSample" ADD COLUMN "audioBytes" INTEGER;
ALTER TABLE "UnresolvedSample" ADD COLUMN "purgeStatus" TEXT DEFAULT 'not_started';
ALTER TABLE "UnresolvedSample" ADD COLUMN "purgeReadyAt" DATETIME;
ALTER TABLE "UnresolvedSample" ADD COLUMN "purgeReason" TEXT;
ALTER TABLE "UnresolvedSample" ADD COLUMN "purgeDryRunAt" DATETIME;

CREATE INDEX "UnresolvedSample_purgeStatus_idx" ON "UnresolvedSample"("purgeStatus");
