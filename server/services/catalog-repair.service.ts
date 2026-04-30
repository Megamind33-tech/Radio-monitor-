import { prisma } from "../lib/prisma.js";
import { logger } from "../lib/logger.js";
import { CatalogLookupService } from "./catalog-lookup.service.js";
import { upsertSongSpinOnNewPlay } from "../lib/song-spin.js";
import { monitorEvents } from "../lib/monitor-events.js";
import type { NormalizedMetadata } from "../types.js";

function isRepairableMetadataText(combined: string, title: string, artist: string): boolean {
  if (isPlaceholderText(title) || isPlaceholderText(artist)) return false;
  if (/^[_\s-]{6,}$/.test(title) && /^preteen zenith$/i.test(artist)) return false;

  const line = [artist, title].filter(Boolean).join(" - ").trim() || combined.trim();
  if (!line || line.length < 3) return false;
  if (isPlaceholderText(line)) return false;
  if (/^[\s'"`.,:;|/\\()[\]{}<>~+=_*#-]+$/.test(line)) return false;
  const compact = line.replace(/\s+/g, "");
  if (compact.length >= 6) {
    const lettersOrDigits = (compact.match(/[A-Za-z0-9]/g) ?? []).length;
    const nonLatin = (compact.match(/[^\x00-\x7F]/g) ?? []).length;
    const bracketNoise = (compact.match(/[⫷⫸⫹⫺ꢂꢃꢄꢊ]/g) ?? []).length;
    if (lettersOrDigits / compact.length < 0.25) return false;
    if (bracketNoise >= 2 || nonLatin / compact.length > 0.45) return false;
  }
  if (/(['"`]\s*){5,}/.test(line)) return false;
  return true;
}

function isPlaceholderText(text: string): boolean {
  const t = String(text || "").trim();
  if (!t) return false;
  if (/^[_\s-]{6,}$/.test(t)) return true;
  if (/^[-=_*.!·•\s]{6,}$/.test(t)) return true;
  if (/^(online|live|replay|feel the power)$/i.test(t)) return true;
  return false;
}

/**
 * Re-run free catalog lookup on recent unresolved DetectionLog rows that still
 * carry raw ICY but no identified title — fixes rows that failed at poll time
 * or were skipped when catalog was temporarily unavailable.
 */
export class CatalogRepairService {
  static async runBatch(opts?: { limit?: number }): Promise<{
    scanned: number;
    repaired: number;
    skipped: number;
    errors: number;
  }> {
    const limit = Math.min(100, Math.max(1, opts?.limit ?? 30));
    let repaired = 0;
    let skipped = 0;
    let errors = 0;

    try {
      const rows = await prisma.detectionLog.findMany({
        where: {
          status: "unresolved",
          AND: [
            {
              OR: [{ titleFinal: null }, { titleFinal: "" }],
            },
            {
              OR: [{ rawStreamText: { not: null } }, { parsedTitle: { not: null } }],
            },
          ],
        },
        orderBy: { observedAt: "desc" },
        take: limit,
        select: {
          id: true,
          stationId: true,
          observedAt: true,
          rawStreamText: true,
          parsedTitle: true,
          parsedArtist: true,
          titleFinal: true,
        },
      });

      for (const row of rows) {
        const combined = (row.rawStreamText ?? "").trim();
        const title = (row.parsedTitle ?? "").trim();
        const artist = (row.parsedArtist ?? "").trim();
        if (!combined && !title) {
          skipped++;
          continue;
        }
        if (!isRepairableMetadataText(combined, title, artist)) {
          skipped++;
          continue;
        }

        const metadata: NormalizedMetadata = {
          rawTitle: title || undefined,
          rawArtist: artist || undefined,
          combinedRaw: combined || undefined,
          sourceType: "catalog_lookup",
        };

        try {
          const hit = await CatalogLookupService.lookupFromMetadata(metadata);
          if (!hit?.title) {
            skipped++;
            continue;
          }

          const hadNoFinal = !row.titleFinal || !String(row.titleFinal).trim();

          await prisma.detectionLog.update({
            where: { id: row.id },
            data: {
              titleFinal: hit.title,
              artistFinal: hit.artist ?? null,
              releaseFinal: hit.releaseTitle ?? null,
              releaseDate: hit.releaseDate ?? null,
              genreFinal: hit.genre ?? null,
              recordingMbid: hit.recordingId ?? null,
              acoustidId: hit.acoustidTrackId ?? null,
              isrcList: hit.isrcs?.length ? JSON.stringify(hit.isrcs) : null,
              confidence: hit.confidence,
              acoustidScore: hit.score,
              sourceProvider: hit.sourceProvider ?? "catalog_lookup",
              status: "matched",
              reasonCode: "catalog_repair_backfill",
              trackDurationMs: hit.durationMs ?? null,
            },
          });

          let spinPlayCount = 0;
          if (hadNoFinal) {
            const spin = await upsertSongSpinOnNewPlay(prisma, {
              stationId: row.stationId,
              artist: hit.artist ?? null,
              title: hit.title,
              album: hit.releaseTitle ?? null,
              detectionLogId: row.id,
              playedAt: row.observedAt,
              originalCombinedRaw: combined || null,
            });
            spinPlayCount = spin.playCount;
          }

          const latest = await prisma.detectionLog.findFirst({
            where: { stationId: row.stationId },
            orderBy: { observedAt: "desc" },
            select: { id: true },
          });
          if (latest?.id === row.id) {
            await prisma.currentNowPlaying.upsert({
              where: { stationId: row.stationId },
              update: {
                title: hit.title,
                artist: hit.artist ?? null,
                album: hit.releaseTitle ?? null,
                genre: hit.genre ?? null,
                sourceProvider: hit.sourceProvider ?? "catalog_lookup",
                streamText: row.rawStreamText,
                updatedAt: new Date(),
              },
              create: {
                stationId: row.stationId,
                title: hit.title,
                artist: hit.artist ?? null,
                album: hit.releaseTitle ?? null,
                genre: hit.genre ?? null,
                sourceProvider: hit.sourceProvider ?? "catalog_lookup",
                streamText: row.rawStreamText,
              },
            });
          }

          monitorEvents.emitStationPoll({
            stationId: row.stationId,
            ts: new Date().toISOString(),
            detectionStatus: "matched",
            detectionLogId: row.id,
            displayTitle: hit.title,
            displayArtist: hit.artist ?? null,
            streamText: row.rawStreamText,
            newDetectionLog: false,
          });
          monitorEvents.emitSongDetected({
            stationId: row.stationId,
            detectionLogId: row.id,
            observedAt: row.observedAt.toISOString(),
            title: hit.title,
            artist: hit.artist ?? null,
            playCount: spinPlayCount || 1,
          });

          repaired++;
        } catch (e) {
          errors++;
          logger.warn({ e, detectionLogId: row.id }, "Catalog repair row failed");
        }
      }

      if (repaired > 0) {
        logger.info({ scanned: rows.length, repaired, skipped, errors }, "Catalog repair batch");
      }
      return { scanned: rows.length, repaired, skipped, errors };
    } catch (error) {
      logger.error({ error }, "Catalog repair batch failed");
      return { scanned: 0, repaired: 0, skipped: 0, errors: 1 };
    }
  }
}
