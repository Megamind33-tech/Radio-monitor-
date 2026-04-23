-- RedefineTables
PRAGMA foreign_keys=OFF;
CREATE TABLE "new_StationSongSpin" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "stationId" TEXT NOT NULL,
    "artistNorm" TEXT NOT NULL DEFAULT '',
    "titleNorm" TEXT NOT NULL,
    "albumNorm" TEXT NOT NULL DEFAULT '',
    "mixRuleApplied" TEXT,
    "mixSplitConfidence" REAL,
    "originalCombinedRaw" TEXT,
    "artistLast" TEXT NOT NULL DEFAULT '',
    "titleLast" TEXT NOT NULL DEFAULT '',
    "albumLast" TEXT NOT NULL DEFAULT '',
    "playCount" INTEGER NOT NULL DEFAULT 0,
    "firstPlayedAt" DATETIME NOT NULL,
    "lastPlayedAt" DATETIME NOT NULL,
    "lastDetectionLogId" TEXT,
    FOREIGN KEY ("stationId") REFERENCES "Station" ("id") ON DELETE CASCADE ON UPDATE CASCADE
);
INSERT INTO "new_StationSongSpin" ("albumLast", "albumNorm", "artistLast", "artistNorm", "firstPlayedAt", "id", "lastDetectionLogId", "lastPlayedAt", "playCount", "stationId", "titleLast", "titleNorm") SELECT "albumLast", "albumNorm", "artistLast", "artistNorm", "firstPlayedAt", "id", "lastDetectionLogId", "lastPlayedAt", "playCount", "stationId", "titleLast", "titleNorm" FROM "StationSongSpin";
DROP TABLE "StationSongSpin";
ALTER TABLE "new_StationSongSpin" RENAME TO "StationSongSpin";
CREATE UNIQUE INDEX "StationSongSpin_stationId_artistNorm_titleNorm_albumNorm_key" ON "StationSongSpin"("stationId" ASC, "artistNorm" ASC, "titleNorm" ASC, "albumNorm" ASC);
PRAGMA foreign_key_check;
PRAGMA foreign_keys=ON;

