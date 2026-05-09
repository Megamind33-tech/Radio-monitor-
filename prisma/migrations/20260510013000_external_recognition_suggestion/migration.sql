CREATE TABLE "RecognitionSuggestion" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "unknownSampleId" TEXT NOT NULL,
  "provider" TEXT NOT NULL,
  "suggestedArtist" TEXT,
  "suggestedTitle" TEXT,
  "suggestedAlbum" TEXT,
  "externalUrl" TEXT,
  "rawResponseJson" TEXT,
  "confidence" REAL,
  "status" TEXT NOT NULL DEFAULT 'pending',
  "createdAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "reviewedAt" DATETIME
);
CREATE UNIQUE INDEX "RecognitionSuggestion_unknownSampleId_provider_key" ON "RecognitionSuggestion"("unknownSampleId","provider");
CREATE INDEX "RecognitionSuggestion_provider_status_idx" ON "RecognitionSuggestion"("provider","status");
