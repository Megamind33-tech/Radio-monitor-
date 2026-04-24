-- Rich metadata for self-learned fingerprint library + export quality
ALTER TABLE "LocalFingerprint" ADD COLUMN "displayArtist" TEXT;
ALTER TABLE "LocalFingerprint" ADD COLUMN "titleWithoutFeat" TEXT;
ALTER TABLE "LocalFingerprint" ADD COLUMN "featuredArtistsJson" TEXT;
ALTER TABLE "LocalFingerprint" ADD COLUMN "labelName" TEXT;
ALTER TABLE "LocalFingerprint" ADD COLUMN "countryCode" TEXT;
ALTER TABLE "LocalFingerprint" ADD COLUMN "durationMs" INTEGER;
ALTER TABLE "LocalFingerprint" ADD COLUMN "playCountTotal" INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS "LocalFingerprint_playCountTotal_idx" ON "LocalFingerprint"("playCountTotal");
