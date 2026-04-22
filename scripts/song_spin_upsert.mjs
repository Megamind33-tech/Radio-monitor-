/**
 * Shared with server/lib/song-spin.ts - keep normalization in sync.
 * Used by orb_track_poller.mjs (ESM).
 */
export function normalizeSongPart(s) {
  return String(s ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

export async function upsertSongSpinOnNewPlay(prisma, params) {
  const titleNorm = normalizeSongPart(params.title);
  if (!titleNorm) return;

  const artistNorm = normalizeSongPart(params.artist);
  const albumNorm = normalizeSongPart(params.album);
  const now = new Date();
  const artistLast = String(params.artist ?? "").trim();
  const titleLast = String(params.title ?? "").trim();
  const albumLast = String(params.album ?? "").trim();

  await prisma.stationSongSpin.upsert({
    where: {
      stationId_artistNorm_titleNorm_albumNorm: {
        stationId: params.stationId,
        artistNorm,
        titleNorm,
        albumNorm,
      },
    },
    create: {
      stationId: params.stationId,
      artistNorm,
      titleNorm,
      albumNorm,
      artistLast,
      titleLast,
      albumLast,
      playCount: 1,
      firstPlayedAt: now,
      lastPlayedAt: now,
      lastDetectionLogId: params.detectionLogId,
    },
    update: {
      playCount: { increment: 1 },
      lastPlayedAt: now,
      lastDetectionLogId: params.detectionLogId,
      artistLast,
      titleLast,
      albumLast,
    },
  });
}
