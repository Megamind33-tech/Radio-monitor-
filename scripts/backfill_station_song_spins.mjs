/**
 * One-time: aggregate existing DetectionLog (matched) into StationSongSpin.
 * Run after migration: node scripts/backfill_station_song_spins.mjs
 */
import "dotenv/config";
import { PrismaClient } from "@prisma/client";
function norm(s) {
  return String(s ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

const prisma = new PrismaClient();

const logs = await prisma.detectionLog.findMany({
  where: { status: "matched", titleFinal: { not: null } },
  orderBy: { observedAt: "asc" },
  select: {
    id: true,
    stationId: true,
    artistFinal: true,
    titleFinal: true,
    releaseFinal: true,
    observedAt: true,
  },
});

const key = (sid, a, t, al) =>
  `${sid}|${norm(a)}|${norm(t)}|${norm(al)}`;

const groups = new Map();
for (const row of logs) {
  const t = (row.titleFinal ?? "").trim();
  if (!t) continue;
  const k = key(row.stationId, row.artistFinal, row.titleFinal, row.releaseFinal);
  if (!groups.has(k)) {
    groups.set(k, []);
  }
  groups.get(k).push(row);
}

let upserted = 0;
for (const [, rows] of groups) {
  rows.sort((a, b) => a.observedAt.getTime() - b.observedAt.getTime());
  const first = rows[0];
  const last = rows[rows.length - 1];
  const playCount = rows.length;

  await prisma.stationSongSpin.upsert({
    where: {
      stationId_artistNorm_titleNorm_albumNorm: {
        stationId: first.stationId,
        artistNorm: norm(first.artistFinal),
        titleNorm: norm(first.titleFinal),
        albumNorm: norm(first.releaseFinal),
      },
    },
    create: {
      stationId: first.stationId,
      artistNorm: norm(first.artistFinal),
      titleNorm: norm(first.titleFinal),
      albumNorm: norm(first.releaseFinal),
      artistLast: (first.artistFinal ?? "").trim(),
      titleLast: (first.titleFinal ?? "").trim(),
      albumLast: (first.releaseFinal ?? "").trim(),
      playCount,
      firstPlayedAt: first.observedAt,
      lastPlayedAt: last.observedAt,
      lastDetectionLogId: last.id,
    },
    update: {
      playCount,
      firstPlayedAt: first.observedAt,
      lastPlayedAt: last.observedAt,
      lastDetectionLogId: last.id,
      artistLast: (last.artistFinal ?? "").trim(),
      titleLast: (last.titleFinal ?? "").trim(),
      albumLast: (last.releaseFinal ?? "").trim(),
    },
  });
  upserted++;
}

console.log(`Backfill: ${upserted} StationSongSpin row(s) from ${logs.length} detection log(s).`);
await prisma.$disconnect();
