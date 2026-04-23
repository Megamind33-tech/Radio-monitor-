-- Separate stream health signals from song recognition; source tiering
ALTER TABLE "Station" ADD COLUMN "preferredStreamUrl" TEXT;
ALTER TABLE "Station" ADD COLUMN "streamSourceType" TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE "Station" ADD COLUMN "streamSourceQualityScore" INTEGER NOT NULL DEFAULT 50;
ALTER TABLE "Station" ADD COLUMN "streamSourceLastEvaluatedAt" DATETIME;
ALTER TABLE "Station" ADD COLUMN "decodeHealthEma" REAL NOT NULL DEFAULT 1;
ALTER TABLE "Station" ADD COLUMN "fingerprintHitEma" REAL NOT NULL DEFAULT 0;
ALTER TABLE "Station" ADD COLUMN "metadataPresentEma" REAL NOT NULL DEFAULT 0;
ALTER TABLE "Station" ADD COLUMN "streamOnlineLast" INTEGER NOT NULL DEFAULT 0;
ALTER TABLE "Station" ADD COLUMN "audioDetectedLast" INTEGER NOT NULL DEFAULT 0;
ALTER TABLE "Station" ADD COLUMN "metadataAvailableLast" INTEGER NOT NULL DEFAULT 0;
ALTER TABLE "Station" ADD COLUMN "songIdentifiedLast" INTEGER NOT NULL DEFAULT 0;

ALTER TABLE "StationStreamEndpoint" ADD COLUMN "sourceTier" TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE "StationStreamEndpoint" ADD COLUMN "qualityScore" INTEGER NOT NULL DEFAULT 50;
ALTER TABLE "StationStreamEndpoint" ADD COLUMN "fingerprintHits" INTEGER NOT NULL DEFAULT 0;
ALTER TABLE "StationStreamEndpoint" ADD COLUMN "fingerprintAttempts" INTEGER NOT NULL DEFAULT 0;
ALTER TABLE "StationStreamEndpoint" ADD COLUMN "decodeHits" INTEGER NOT NULL DEFAULT 0;
ALTER TABLE "StationStreamEndpoint" ADD COLUMN "decodeAttempts" INTEGER NOT NULL DEFAULT 0;
ALTER TABLE "StationStreamEndpoint" ADD COLUMN "metadataFreshHits" INTEGER NOT NULL DEFAULT 0;
ALTER TABLE "StationStreamEndpoint" ADD COLUMN "metadataPolls" INTEGER NOT NULL DEFAULT 0;
