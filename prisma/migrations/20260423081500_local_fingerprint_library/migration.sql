-- AcoustID track UUID (distinct from MusicBrainz MBID) for detection logs.
ALTER TABLE "DetectionLog" ADD COLUMN "acoustidId" TEXT;

-- Self-learned Chromaprint fingerprint library.
CREATE TABLE "LocalFingerprint" (
    "id"                TEXT NOT NULL PRIMARY KEY,
    "fingerprint"       TEXT NOT NULL,
    "fingerprintSha1"   TEXT NOT NULL,
    "fingerprintPrefix" TEXT NOT NULL,
    "durationSec"       INTEGER NOT NULL,
    "title"             TEXT,
    "artist"            TEXT,
    "releaseTitle"      TEXT,
    "releaseDate"       TEXT,
    "genre"             TEXT,
    "acoustidTrackId"   TEXT,
    "recordingMbid"     TEXT,
    "isrcsJson"         TEXT,
    "source"            TEXT NOT NULL,
    "confidence"        REAL NOT NULL DEFAULT 0,
    "timesMatched"      INTEGER NOT NULL DEFAULT 0,
    "firstLearnedAt"    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "lastMatchedAt"     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt"         DATETIME NOT NULL
);

CREATE UNIQUE INDEX "LocalFingerprint_fingerprintSha1_key"
    ON "LocalFingerprint"("fingerprintSha1");

CREATE INDEX "LocalFingerprint_fingerprintPrefix_durationSec_idx"
    ON "LocalFingerprint"("fingerprintPrefix", "durationSec");

CREATE INDEX "LocalFingerprint_acoustidTrackId_idx"
    ON "LocalFingerprint"("acoustidTrackId");

CREATE INDEX "LocalFingerprint_artist_title_idx"
    ON "LocalFingerprint"("artist", "title");
