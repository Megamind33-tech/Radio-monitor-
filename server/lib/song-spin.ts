import type { PrismaClient } from "@prisma/client";

/** Normalize for stable per-station song identity (plays aggregate on this key). */
export function normalizeSongPart(s: string | null | undefined): string {
  return (s ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

/**
 * Call when a new DetectionLog row is created for a matched play (one row = one play).
 * Increments StationSongSpin.playCount for this station + song key.
 */
export async function upsertSongSpinOnNewPlay(
  prisma: PrismaClient,
  params: {
    stationId: string;
    artist: string | null | undefined;
    title: string | null | undefined;
    album: string | null | undefined;
    detectionLogId: string;
  }
): Promise<void> {
  const titleNorm = normalizeSongPart(params.title);
  if (!titleNorm) return;

  const artistNorm = normalizeSongPart(params.artist);
  const albumNorm = normalizeSongPart(params.album);
  const now = new Date();

  const artistLast = (params.artist ?? "").trim();
  const titleLast = (params.title ?? "").trim();
  const albumLast = (params.album ?? "").trim();

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
