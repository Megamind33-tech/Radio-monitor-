CREATE TABLE "RematchJob" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "targetType" TEXT NOT NULL,
  "targetId" TEXT NOT NULL,
  "stationId" TEXT,
  "triggerReason" TEXT NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'pending',
  "priority" INTEGER NOT NULL DEFAULT 50,
  "attempts" INTEGER NOT NULL DEFAULT 0,
  "maxAttempts" INTEGER NOT NULL DEFAULT 3,
  "lockedAt" DATETIME,
  "completedAt" DATETIME,
  "failedAt" DATETIME,
  "error" TEXT,
  "oldArtist" TEXT,
  "oldTitle" TEXT,
  "oldStatus" TEXT,
  "newArtist" TEXT,
  "newTitle" TEXT,
  "newVerifiedTrackId" TEXT,
  "newConfidence" REAL,
  "evidenceJson" TEXT,
  "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" DATETIME NOT NULL
);
CREATE INDEX "RematchJob_status_idx" ON "RematchJob"("status");
CREATE INDEX "RematchJob_stationId_idx" ON "RematchJob"("stationId");
CREATE INDEX "RematchJob_targetType_targetId_idx" ON "RematchJob"("targetType","targetId");
CREATE INDEX "RematchJob_triggerReason_idx" ON "RematchJob"("triggerReason");
CREATE INDEX "RematchJob_priority_idx" ON "RematchJob"("priority");
