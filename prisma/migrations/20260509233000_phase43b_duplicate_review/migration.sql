CREATE TABLE "CatalogDuplicateReview" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "candidateSourceId" TEXT,
  "existingVerifiedTrackId" TEXT,
  "candidateArtist" TEXT,
  "candidateTitle" TEXT,
  "existingArtist" TEXT,
  "existingTitle" TEXT,
  "candidateDurationSeconds" REAL,
  "existingDurationSeconds" REAL,
  "confidence" REAL NOT NULL,
  "reason" TEXT NOT NULL,
  "evidenceJson" TEXT,
  "status" TEXT NOT NULL DEFAULT 'pending',
  "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "reviewedAt" DATETIME,
  "reviewedBy" TEXT,
  "decision" TEXT
);
CREATE INDEX "CatalogDuplicateReview_status_idx" ON "CatalogDuplicateReview"("status");
CREATE INDEX "CatalogDuplicateReview_existingVerifiedTrackId_idx" ON "CatalogDuplicateReview"("existingVerifiedTrackId");
CREATE INDEX "CatalogDuplicateReview_reason_idx" ON "CatalogDuplicateReview"("reason");
CREATE INDEX "CatalogDuplicateReview_confidence_idx" ON "CatalogDuplicateReview"("confidence");
