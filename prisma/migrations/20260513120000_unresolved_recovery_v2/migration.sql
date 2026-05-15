-- Structured unresolved recovery: semantic reason codes, title cluster keys, backlog priority.
ALTER TABLE "UnresolvedSample" ADD COLUMN "recoveryReason" TEXT;
ALTER TABLE "UnresolvedSample" ADD COLUMN "titleNormKey" TEXT;
ALTER TABLE "UnresolvedSample" ADD COLUMN "recoveryPriority" INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS "UnresolvedSample_recoveryReason_idx" ON "UnresolvedSample"("recoveryReason");
CREATE INDEX IF NOT EXISTS "UnresolvedSample_titleNormKey_idx" ON "UnresolvedSample"("titleNormKey");
CREATE INDEX IF NOT EXISTS "UnresolvedSample_stationId_titleNormKey_createdAt_idx" ON "UnresolvedSample"("stationId", "titleNormKey", "createdAt");
