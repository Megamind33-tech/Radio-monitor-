-- AlterTable: add manual-tagging audit fields to DetectionLog
ALTER TABLE "DetectionLog" ADD COLUMN "manuallyTagged" BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE "DetectionLog" ADD COLUMN "manualTaggedAt" DATETIME;

-- AlterTable: track which StationSongSpin rows came from manual editor
ALTER TABLE "StationSongSpin" ADD COLUMN "manuallyTagged" BOOLEAN NOT NULL DEFAULT false;
