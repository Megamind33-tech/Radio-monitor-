-- Phase 2: manual unknown review + fingerprint linkage
CREATE TABLE "VerifiedTrack" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "artist" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "album" TEXT,
  "label" TEXT,
  "isrc" TEXT,
  "iswc" TEXT,
  "composerWriter" TEXT,
  "publisher" TEXT,
  "country" TEXT,
  "sourceSociety" TEXT,
  "notes" TEXT,
  "verificationStatus" TEXT NOT NULL DEFAULT 'human_verified',
  "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" DATETIME NOT NULL
);

ALTER TABLE "DetectionLog" ADD COLUMN "verifiedTrackId" TEXT;
ALTER TABLE "UnresolvedSample" ADD COLUMN "verifiedTrackId" TEXT;
ALTER TABLE "UnresolvedSample" ADD COLUMN "fingerprintStatus" TEXT NOT NULL DEFAULT 'not_started';
ALTER TABLE "UnresolvedSample" ADD COLUMN "fingerprintedAt" DATETIME;
ALTER TABLE "UnresolvedSample" ADD COLUMN "reviewNotes" TEXT;
ALTER TABLE "UnresolvedSample" ADD COLUMN "reviewedAt" DATETIME;

CREATE INDEX "DetectionLog_verifiedTrackId_idx" ON "DetectionLog"("verifiedTrackId");
CREATE INDEX "UnresolvedSample_verifiedTrackId_idx" ON "UnresolvedSample"("verifiedTrackId");
