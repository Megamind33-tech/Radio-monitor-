CREATE TABLE "CatalogSource" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "verifiedTrackId" TEXT,
  "sourceUrl" TEXT NOT NULL,
  "canonicalUrl" TEXT,
  "finalUrl" TEXT,
  "sourceType" TEXT,
  "sourceName" TEXT,
  "countryHint" TEXT,
  "countryConfidence" REAL,
  "sha256" TEXT,
  "audioBytes" INTEGER,
  "durationSeconds" REAL,
  "metadataJson" TEXT,
  "duplicateReason" TEXT,
  "confidence" REAL,
  "fingerprintStatus" TEXT,
  "discoveredAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "lastCheckedAt" DATETIME,
  "notes" TEXT,
  CONSTRAINT "CatalogSource_verifiedTrackId_fkey" FOREIGN KEY ("verifiedTrackId") REFERENCES "VerifiedTrack" ("id") ON DELETE SET NULL ON UPDATE CASCADE
);
CREATE INDEX "CatalogSource_verifiedTrackId_idx" ON "CatalogSource"("verifiedTrackId");
CREATE INDEX "CatalogSource_sourceUrl_idx" ON "CatalogSource"("sourceUrl");
CREATE INDEX "CatalogSource_finalUrl_idx" ON "CatalogSource"("finalUrl");
CREATE INDEX "CatalogSource_sha256_idx" ON "CatalogSource"("sha256");
CREATE INDEX "CatalogSource_sourceType_idx" ON "CatalogSource"("sourceType");
CREATE INDEX "CatalogSource_duplicateReason_idx" ON "CatalogSource"("duplicateReason");
