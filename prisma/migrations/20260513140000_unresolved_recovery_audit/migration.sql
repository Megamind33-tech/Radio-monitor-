-- Audit trail for automatic unresolved recoveries (title + audio paths) with optional revert.
CREATE TABLE "UnresolvedRecoveryAudit" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "unresolvedSampleId" TEXT NOT NULL,
    "stationId" TEXT NOT NULL,
    "kind" TEXT NOT NULL,
    "createdDetectionLogId" TEXT NOT NULL,
    "previousRecoveryStatus" TEXT NOT NULL,
    "previousRecoveryReason" TEXT,
    "previousRecoveredAt" DATETIME,
    "previousVerifiedTrackId" TEXT,
    "previousLastRecoveryError" TEXT,
    "titleFinal" TEXT,
    "artistFinal" TEXT,
    "titleNormKey" TEXT,
    "matchSnapshotJson" TEXT,
    "revertedAt" DATETIME,
    "revertNote" TEXT,
    "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX "UnresolvedRecoveryAudit_unresolvedSampleId_idx" ON "UnresolvedRecoveryAudit"("unresolvedSampleId");
CREATE INDEX "UnresolvedRecoveryAudit_createdDetectionLogId_idx" ON "UnresolvedRecoveryAudit"("createdDetectionLogId");
CREATE INDEX "UnresolvedRecoveryAudit_revertedAt_idx" ON "UnresolvedRecoveryAudit"("revertedAt");
CREATE INDEX "UnresolvedRecoveryAudit_stationId_createdAt_idx" ON "UnresolvedRecoveryAudit"("stationId", "createdAt");
