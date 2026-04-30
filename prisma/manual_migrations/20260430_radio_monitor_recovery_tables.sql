-- Manual schema export from DigitalOcean server patches.
-- Review and convert to proper Prisma migration if needed.

-- ===== BadMetadataPattern =====
CREATE TABLE BadMetadataPattern (
  id TEXT PRIMARY KEY,
  pattern TEXT NOT NULL UNIQUE,
  reason TEXT,
  createdAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ===== CorrectedTrackIdentity =====
CREATE TABLE CorrectedTrackIdentity (
  id TEXT PRIMARY KEY,
  artistCanonical TEXT NOT NULL,
  titleCanonical TEXT NOT NULL,
  artistNorm TEXT NOT NULL,
  titleNorm TEXT NOT NULL,
  fingerprint TEXT,
  rawAliases TEXT,
  confidence REAL DEFAULT 1.0,
  createdAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updatedAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(artistNorm, titleNorm)
);
CREATE INDEX idx_corrected_track_norm
ON CorrectedTrackIdentity(artistNorm, titleNorm);

-- ===== UnknownAudioReview =====
CREATE TABLE UnknownAudioReview (
  id TEXT PRIMARY KEY,
  stationId TEXT,
  detectionLogId TEXT UNIQUE,
  rawStreamText TEXT,
  parsedArtist TEXT,
  parsedTitle TEXT,
  reasonCode TEXT,
  sourceProvider TEXT,
  fingerprint TEXT,
  samplePath TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  reviewedArtist TEXT,
  reviewedTitle TEXT,
  reviewedBy TEXT,
  reviewedAt TEXT,
  createdAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updatedAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_unknown_audio_review_status
ON UnknownAudioReview(status);
CREATE INDEX idx_unknown_audio_review_station
ON UnknownAudioReview(stationId);

-- ===== MetadataParseCandidate =====
CREATE TABLE MetadataParseCandidate (id TEXT PRIMARY KEY, unknownReviewId TEXT UNIQUE, rawText TEXT, rawArtistCandidate TEXT, rawTitleCandidate TEXT, parsedArtist TEXT, parsedTitle TEXT, mismatchType TEXT, status TEXT NOT NULL DEFAULT 'needs_review', createdAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updatedAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE INDEX idx_metadata_parse_candidate_status ON MetadataParseCandidate(status);

-- ===== MetadataConflictReview =====
CREATE TABLE MetadataConflictReview (id TEXT PRIMARY KEY, conflictType TEXT NOT NULL, titleNorm TEXT, artistNorm TEXT, rawText TEXT, stationId TEXT, detectionLogId TEXT, unknownReviewId TEXT, sourceProvider TEXT, reasonCode TEXT, status TEXT NOT NULL DEFAULT 'needs_conflict_review', createdAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updatedAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE INDEX idx_metadata_conflict_status ON MetadataConflictReview(status);
CREATE INDEX idx_metadata_conflict_title ON MetadataConflictReview(titleNorm);

-- ===== SafeFingerprintRecoveryCandidate =====
CREATE TABLE SafeFingerprintRecoveryCandidate (id TEXT PRIMARY KEY, unresolvedSampleId TEXT UNIQUE, detectionLogId TEXT, stationId TEXT, filePath TEXT, localFingerprintId TEXT, candidateArtist TEXT, candidateTitle TEXT, candidateDurationSec INTEGER, sampleDurationSec INTEGER, matchType TEXT, similarity REAL, status TEXT NOT NULL DEFAULT 'needs_review', createdAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updatedAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE INDEX idx_safe_fp_candidate_status ON SafeFingerprintRecoveryCandidate(status);
CREATE INDEX idx_safe_fp_candidate_station ON SafeFingerprintRecoveryCandidate(stationId);

-- ===== LocalAudioMetadataCandidate =====
CREATE TABLE LocalAudioMetadataCandidate (
  id TEXT PRIMARY KEY,
  clusterId TEXT NOT NULL,
  artist TEXT,
  title TEXT,
  source TEXT,
  evidenceCount INTEGER DEFAULT 1,
  confidence REAL DEFAULT 0,
  createdAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ===== LocalFingerprint =====
CREATE TABLE IF NOT EXISTS "LocalFingerprint" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "fingerprint" TEXT NOT NULL,
    "fingerprintSha1" TEXT NOT NULL,
    "fingerprintPrefix" TEXT NOT NULL,
    "durationSec" INTEGER NOT NULL,
    "title" TEXT,
    "artist" TEXT,
    "releaseTitle" TEXT,
    "releaseDate" TEXT,
    "genre" TEXT,
    "acoustidTrackId" TEXT,
    "recordingMbid" TEXT,
    "isrcsJson" TEXT,
    "source" TEXT NOT NULL,
    "confidence" REAL NOT NULL DEFAULT 0,
    "timesMatched" INTEGER NOT NULL DEFAULT 0,
    "firstLearnedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "lastMatchedAt" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" DATETIME NOT NULL
, "displayArtist" TEXT, "titleWithoutFeat" TEXT, "featuredArtistsJson" TEXT, "labelName" TEXT, "countryCode" TEXT, "durationMs" INTEGER, "playCountTotal" INTEGER NOT NULL DEFAULT 0);
CREATE UNIQUE INDEX "LocalFingerprint_fingerprintSha1_key" ON "LocalFingerprint"("fingerprintSha1");
CREATE INDEX "LocalFingerprint_fingerprintPrefix_durationSec_idx" ON "LocalFingerprint"("fingerprintPrefix", "durationSec");
CREATE INDEX "LocalFingerprint_acoustidTrackId_idx" ON "LocalFingerprint"("acoustidTrackId");
CREATE INDEX "LocalFingerprint_artist_title_idx" ON "LocalFingerprint"("artist", "title");
CREATE INDEX "LocalFingerprint_playCountTotal_idx" ON "LocalFingerprint"("playCountTotal");

-- ===== LocalAudioReference =====
CREATE TABLE LocalAudioReference (
  id TEXT PRIMARY KEY,
  sourceType TEXT NOT NULL DEFAULT 'recorded_audio',
  filePath TEXT,
  stationId TEXT,
  detectionLogId TEXT,
  sha256 TEXT UNIQUE,
  durationSeconds REAL,
  fingerprint TEXT,
  fingerprintDuration INTEGER,
  artistRaw TEXT,
  titleRaw TEXT,
  artistCanonical TEXT,
  titleCanonical TEXT,
  metadataStatus TEXT NOT NULL DEFAULT 'unknown',
  confidence REAL DEFAULT 0,
  createdAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updatedAt TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_local_audio_reference_artist_title
ON LocalAudioReference(artistCanonical, titleCanonical);
