import * as fs from "fs";
import * as path from "path";
import { prisma } from "../lib/prisma.js";
import { logger } from "../lib/logger.js";
import { mergeAcoustidAndCatalog } from "../lib/audio-id-merge.js";
import { FingerprintService } from "./fingerprint.service.js";
import { AcoustidService } from "./acoustid.service.js";
import { LocalFingerprintService } from "./local-fingerprint.service.js";
import { MusicbrainzService } from "./musicbrainz.service.js";
import { upsertSongSpinOnNewPlay } from "../lib/song-spin.js";
import { MatchResult } from "../types.js";
import { fingerprintPipelineGate } from "../lib/fingerprint-pipeline-gate.js";

function parseEnvInt(key: string, fallback: number): number {
  const raw = process.env[key];
  if (!raw) return fallback;
  const n = parseInt(raw, 10);
  return Number.isFinite(n) ? n : fallback;
}

function parseEnvFloat(key: string, fallback: number): number {
  const raw = process.env[key];
  if (!raw) return fallback;
  const n = parseFloat(raw);
  return Number.isFinite(n) ? n : fallback;
}

export class UnresolvedRecoveryService {
  private static running = false;
  private static lastRunAt: Date | null = null;

  static status() {
    return {
      running: this.running,
      lastRunAt: this.lastRunAt,
      acoustidEnabled: !!process.env.ACOUSTID_API_KEY,
      fpcalcExpected: true,
    };
  }

  static async runBatch(opts?: { stationId?: string; limit?: number }): Promise<{
    processed: number;
    recovered: number;
    noMatch: number;
    skipped: number;
    errored: number;
  }> {
    if (this.running) {
      return { processed: 0, recovered: 0, noMatch: 0, skipped: 0, errored: 0 };
    }
    this.running = true;
    this.lastRunAt = new Date();
    const started = Date.now();

    const limit = Math.min(200, Math.max(1, opts?.limit ?? parseEnvInt("UNRESOLVED_RECOVERY_BATCH_SIZE", 50)));
    const maxAttempts = Math.min(80, Math.max(1, parseEnvInt("UNRESOLVED_RECOVERY_MAX_ATTEMPTS", 24)));

    let processed = 0;
    let recovered = 0;
    let noMatch = 0;
    let skipped = 0;
    let errored = 0;

    try {
      const cooldownDays = Math.min(90, Math.max(1, parseEnvInt("UNRESOLVED_RECOVERY_NO_MATCH_COOLDOWN_DAYS", 7)));
      const cooldownBefore = new Date(Date.now() - cooldownDays * 86400000);
      const retried = await prisma.unresolvedSample.updateMany({
        where: {
          ...(opts?.stationId ? { stationId: opts.stationId } : {}),
          recoveryStatus: "no_match",
          lastRecoveryAt: { lt: cooldownBefore },
        },
        data: {
          recoveryStatus: "pending",
          recoveryAttempts: 0,
          lastRecoveryError: null,
        },
      });
      if (retried.count > 0) {
        logger.info({ retried: retried.count, cooldownDays }, "Re-queued stale no_match unresolved samples for AcoustID retry");
      }

      const rows = await prisma.unresolvedSample.findMany({
        where: {
          ...(opts?.stationId ? { stationId: opts.stationId } : {}),
          recoveryStatus: { in: ["pending", "error", "no_match"] },
          recoveryAttempts: { lt: maxAttempts },
        },
        orderBy: { createdAt: "asc" },
        take: limit,
      });

      for (const row of rows) {
        processed += 1;
        const baseUpdate = {
          recoveryAttempts: { increment: 1 as const },
          lastRecoveryAt: new Date(),
        };

        try {
          if (!row.filePath || !fs.existsSync(row.filePath)) {
            skipped += 1;
            await prisma.unresolvedSample.update({
              where: { id: row.id },
              data: {
                ...baseUpdate,
                recoveryStatus: "skipped",
                lastRecoveryError: "sample_missing",
              },
            });
            continue;
          }

          // Gate: respect the global 2/sec pipeline limit shared with real-time station polling.
          const releaseGate = await fingerprintPipelineGate.acquire();
          let fingerprint: Awaited<ReturnType<typeof FingerprintService.generateFingerprint>>;
          try {
            fingerprint = await FingerprintService.generateFingerprint(row.filePath);
          } finally {
            releaseGate();
          }
          if (!fingerprint) {
            noMatch += 1;
            await prisma.unresolvedSample.update({
              where: { id: row.id },
              data: {
                ...baseUpdate,
                recoveryStatus: "no_match",
                lastRecoveryError: "fingerprint_unavailable",
              },
            });
            continue;
          }

          // Try the self-learned library first — avoids hitting AcoustID/MB for repeat songs.
          let audioMatch: MatchResult | null = await LocalFingerprintService.lookup(fingerprint);
          let recoveredViaAcoustid = false;
          if (!audioMatch) {
            const acoustid = await AcoustidService.lookup(fingerprint);
            if (acoustid) {
              audioMatch = await MusicbrainzService.enrich(acoustid);
              recoveredViaAcoustid = !!audioMatch;
            }
          }
          if (!audioMatch) {
            noMatch += 1;
            await prisma.unresolvedSample.update({
              where: { id: row.id },
              data: {
                ...baseUpdate,
                recoveryStatus: "no_match",
                lastRecoveryError: "acoustid_no_match",
              },
            });
            continue;
          }

          const merged = mergeAcoustidAndCatalog(audioMatch, null, parseEnvFloat("ACOUSTID_PREFER_MIN_SCORE", 0.55));
          const match = merged.match || audioMatch;
          if (!match?.title) {
            noMatch += 1;
            await prisma.unresolvedSample.update({
              where: { id: row.id },
              data: {
                ...baseUpdate,
                recoveryStatus: "no_match",
                lastRecoveryError: "match_missing_title",
              },
            });
            continue;
          }

          const linkedDetection = row.detectionLogId
            ? await prisma.detectionLog.findUnique({ where: { id: row.detectionLogId } })
            : await prisma.detectionLog.findFirst({
                where: {
                  stationId: row.stationId,
                  status: "unresolved",
                  observedAt: { lte: row.createdAt },
                },
                orderBy: { observedAt: "desc" },
              });

          const observedAt = linkedDetection?.observedAt ?? row.createdAt;
          const titleFinal = (match.title || "").trim();
          const artistFinal = (match.artist || "").trim() || null;

          const recoveredLog = await prisma.detectionLog.create({
            data: {
              stationId: row.stationId,
              observedAt,
              detectionMethod: "fingerprint_acoustid",
              rawStreamText: linkedDetection?.rawStreamText ?? null,
              parsedArtist: linkedDetection?.parsedArtist ?? artistFinal,
              parsedTitle: linkedDetection?.parsedTitle ?? titleFinal,
              confidence: match.confidence,
              acoustidScore: match.score,
              acoustidId: match.acoustidTrackId ?? null,
              recordingMbid: match.recordingId,
              titleFinal,
              artistFinal,
              releaseFinal: match.releaseTitle,
              releaseDate: match.releaseDate,
              genreFinal: match.genre,
              sourceProvider: match.sourceProvider || "acoustid",
              isrcList: match.isrcs?.length ? JSON.stringify(match.isrcs) : null,
              trackDurationMs: match.durationMs ?? null,
              sampleSeconds: null,
              processingMs: null,
              status: "matched",
              reasonCode: "recovered_from_unresolved_sample",
            },
            select: { id: true },
          });

          await upsertSongSpinOnNewPlay(prisma, {
            stationId: row.stationId,
            artist: artistFinal,
            title: titleFinal,
            album: match.releaseTitle,
            detectionLogId: recoveredLog.id,
            playedAt: observedAt,
            mixRuleApplied: null,
            mixSplitConfidence: null,
            originalCombinedRaw: linkedDetection?.rawStreamText ?? null,
          });

          if (recoveredViaAcoustid) {
            await LocalFingerprintService.learn({
              fp: fingerprint,
              match,
              source: "acoustid",
            });
          }

          await prisma.currentNowPlaying.upsert({
            where: { stationId: row.stationId },
            update: {
              title: titleFinal,
              artist: artistFinal,
              album: match.releaseTitle ?? null,
              genre: match.genre ?? null,
              sourceProvider: "recovery_reprocess",
              streamText: linkedDetection?.rawStreamText ?? null,
              updatedAt: new Date(),
            },
            create: {
              stationId: row.stationId,
              title: titleFinal,
              artist: artistFinal,
              album: match.releaseTitle ?? null,
              genre: match.genre ?? null,
              sourceProvider: "recovery_reprocess",
              streamText: linkedDetection?.rawStreamText ?? null,
            },
          });

          await prisma.station.update({
            where: { id: row.stationId },
            data: {
              lastSongDetectedAt: observedAt,
              monitorState: "ACTIVE_MUSIC",
              monitorStateReason: "recovered_from_unresolved_sample",
              contentClassification: "music",
            },
          });

          await prisma.unresolvedSample.update({
            where: { id: row.id },
            data: {
              ...baseUpdate,
              detectionLogId: row.detectionLogId ?? linkedDetection?.id ?? null,
              recoveryStatus: "recovered",
              recoveredAt: new Date(),
              lastRecoveryError: null,
            },
          });
          recovered += 1;
        } catch (error) {
          errored += 1;
          await prisma.unresolvedSample.update({
            where: { id: row.id },
            data: {
              ...baseUpdate,
              recoveryStatus: "error",
              lastRecoveryError: String(error).slice(0, 1000),
            },
          });
          logger.warn({ error, unresolvedSampleId: row.id }, "Unresolved recovery item failed");
        }
      }
    } finally {
      this.running = false;
      logger.info(
        {
          processed,
          recovered,
          noMatch,
          skipped,
          errored,
          durationMs: Date.now() - started,
        },
        "Unresolved recovery batch completed"
      );
    }

    return { processed, recovered, noMatch, skipped, errored };
  }

  static async cleanupRecoveredFiles(maxKeepRecovered = 0): Promise<{ deletedRows: number; deletedFiles: number }> {
    const keep = Math.max(0, maxKeepRecovered);
    const recoveredRows = await prisma.unresolvedSample.findMany({
      where: { recoveryStatus: "recovered" },
      orderBy: { recoveredAt: "desc" },
      skip: keep,
      select: { id: true, filePath: true },
    });
    let deletedFiles = 0;
    for (const row of recoveredRows) {
      try {
        if (row.filePath && fs.existsSync(row.filePath)) {
          fs.unlinkSync(row.filePath);
          deletedFiles += 1;
        }
      } catch {
        // Best effort file cleanup.
      }
      await prisma.unresolvedSample.delete({ where: { id: row.id } });
    }
    return { deletedRows: recoveredRows.length, deletedFiles };
  }

  static unresolvedRoot(): string {
    return process.env.UNRESOLVED_SAMPLE_DIR || path.join(process.cwd(), "data/unresolved_samples");
  }
}
