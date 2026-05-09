CREATE TABLE "CatalogCrawlSource" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "canonicalUrl" TEXT NOT NULL,
  "normalizedUrl" TEXT NOT NULL,
  "finalUrl" TEXT,
  "contentType" TEXT,
  "contentHash" TEXT,
  "mediaSha256" TEXT,
  "lastCheckedAt" DATETIME,
  "lastStatus" TEXT,
  "failureReason" TEXT,
  "failureCount" INTEGER NOT NULL DEFAULT 0,
  "nextRetryAt" DATETIME,
  "sourceType" TEXT,
  "discoveredFrom" TEXT,
  "classification" TEXT NOT NULL,
  "fingerprintStatus" TEXT NOT NULL DEFAULT 'not_started',
  "metadataStatus" TEXT NOT NULL DEFAULT 'unknown',
  "qualityScore" INTEGER NOT NULL DEFAULT 0,
  "titleRaw" TEXT,
  "artistRaw" TEXT,
  "albumRaw" TEXT,
  "isrc" TEXT,
  "country" TEXT,
  "sourceSociety" TEXT,
  "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" DATETIME NOT NULL
);

CREATE UNIQUE INDEX "CatalogCrawlSource_normalizedUrl_key" ON "CatalogCrawlSource"("normalizedUrl");
CREATE INDEX "CatalogCrawlSource_classification_lastStatus_idx" ON "CatalogCrawlSource"("classification", "lastStatus");
CREATE INDEX "CatalogCrawlSource_fingerprintStatus_updatedAt_idx" ON "CatalogCrawlSource"("fingerprintStatus", "updatedAt");
