import * as fs from "fs";
import * as path from "path";
import { prisma } from "../lib/prisma.js";
import { logger } from "../lib/logger.js";
import { mergeAcoustidAndCatalog } from "../lib/audio-id-merge.js";
import { FingerprintService } from "./fingerprint.service.js";
import { AcoustidService } from "./acoustid.service.js";
import { AuddService } from "./audd.service.js";
import { AcrcloudService } from "./acrcloud.service.js";
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

function envBoolTrue(key: string, fallback = true): boolean {
  const raw = process.env[key];
  if (raw === undefined || raw === null || String(raw).trim() === "") return fallback;
  const t = String(raw).trim().toLowerCase();
  return !(t === "0" || t === "false" || t === "no" || t === "off");
}

function detectionMethodForProvider(provider: MatchResult["sourceProvider"] | undefined): string {
  if (provider === "audd") return "fingerprint_audd";
  if (provider === "acrcloud") return "fingerprint_acrcloud";
  if (provider === "local_fingerprint") return "fingerprint_local";
  return "fingerprint_acoustid";
}

export class UnresolvedRecoveryService {
  private static running = false;
  private static lastRunAt: Date | null = null;
  private static forceRetryPasses = 0;

  static status() {
    const paidFallbacksEnabled = envBoolTrue("PAID_AUDIO_FALLBACKS_ENABLED", true);
    return {
      running: this.running,
      lastRunAt: this.lastRunAt,
      acoustidEnabled: !!process.env.ACOUSTID_API_KEY,
      auddEnabled: paidFallbacksEnabled && AuddService.isEnabled(),
      acrcloudEnabled: paidFallbacksEnabled && AcrcloudService.isEnabled(),
      paidFallbacksEnabled,
      fpcalcExpected: true,
      forceRetryPasses: this.forceRetryPasses,
    };
  }

  static async runUntilDrained(opts?: {
    stationId?: string;
    limit?: number;
    maxPasses?: number;
    continueWithoutAcoustid?: boolean;
  }): Promise<{
    passes: number;
    processed: number;
    recovered: number;
    noMatch: number;
    skipped: number;
    errored: number;
    remainingPending: number;
  }> {
    const maxPasses = Math.min(200, Math.max(1, opts?.maxPasses ?? parseEnvInt("UNRESOLVED_FORCE_MAX_PASSES", 25)));
    let passes = 0;
    let processed = 0;
    let recovered = 0;
    let noMatch = 0;
    let skipped = 0;
    let errored = 0;

    for (; passes < maxPasses; passes++) {
      const out = await this.runBatch({
        stationId: opts?.stationId,
        limit: opts?.limit,
        continueWithoutAcoustid: opts?.continueWithoutAcoustid,
      });
      processed += out.processed;
      recovered += out.recovered;
      noMatch += out.noMatch;
      skipped += out.skipped;
      errored += out.errored;
      const remainingPending = await prisma.unresolvedSample.count({
        where: {
          ...(opts?.stationId ? { stationId: opts.stationId } : {}),
          recoveryStatus: { in: ["pending", "error", "no_match"] },
        },
      });
      if (remainingPending === 0 || out.processed === 0) {
        this.forceRetryPasses = passes + 1;
        return {
          passes: passes + 1,
          processed,
          recovered,
          noMatch,
          skipped,
          errored,
          remainingPending,
        };
      }
    }

    const remainingPending = await prisma.unresolvedSample.count({
      where: {
        ...(opts?.stationId ? { stationId: opts.stationId } : {}),
        recoveryStatus: { in: ["pending", "error", "no_match"] },
      },
    });
    this.forceRetryPasses = passes;
    return { passes, processed, recovered, noMatch, skipped, errored, remainingPending };
  }

  static async runBatch(opts?: {
    stationId?: string;
    limit?: number;
    continueWithoutAcoustid?: boolean;
  }): Promise<{
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
    const acoustidEnabled = !!process.env.ACOUSTID_API_KEY;
    const paidFallbacksEnabled = envBoolTrue("PAID_AUDIO_FALLBACKS_ENABLED", true);
    const auddEnabled = paidFallbacksEnabled && AuddService.isEnabled();
    const acrcloudEnabled = paidFallbacksEnabled && AcrcloudService.isEnabled();
    if (!acoustidEnabled && !auddEnabled && !acrcloudEnabled && !opts?.continueWithoutAcoustid) {
      logger.info("Skipping unresolved recovery batch: no audio resolver configured");
      return { processed: 0, recovered: 0, noMatch: 0, skipped: 0, errored: 0 };
    }

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
        logger.info({ retried: retried.count, cooldownDays }, "Re-queued stale no_match unresolved samples for audio resolver retry");
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

          // Try the self-learned library first — avoids hitting paid/free APIs for repeat songs.
          let audioMatch: MatchResult | null = await LocalFingerprintService.lookup(fingerprint);
          let recoveredViaAcoustid = false;
          let recoveredViaAudd = false;
          let recoveredViaAcrcloud = false;

          if (!audioMatch && acoustidEnabled) {
            const acoustid = await AcoustidService.lookup(fingerprint);
            if (acoustid) {
              audioMatch = await MusicbrainzService.enrich(acoustid);
              recoveredViaAcoustid = !!audioMatch;
            }
          }

          // Important: saved unresolved samples must also reach AudD/ACRCloud.
          // Before this, recovery was AcoustID-only, leaving many valid music clips stuck as unknown.
          if (!audioMatch && auddEnabled) {
            const audd = await AuddService.lookupSample(row.filePath);
            if (audd) {
              audioMatch = audd;
              recoveredViaAudd = true;
            }
          }

          if (!audioMatch && acrcloudEnabled) {
            const acrcloud = await AcrcloudService.identifyAudioFile(row.filePath);
            if (acrcloud) {
              audioMatch = acrcloud.recordingId ? await MusicbrainzService.enrich(acrcloud) : acrcloud;
              recoveredViaAcrcloud = !!audioMatch;
            }
          }

          if (!audioMatch) {
            noMatch += 1;
            const resolvers = [
              "local",
              ...(acoustidEnabled ? ["acoustid"] : []),
              ...(auddEnabled ? ["audd"] : []),
              ...(acrcloudEnabled ? ["acrcloud"] : []),
            ];
            await prisma.unresolvedSample.update({
              where: { id: row.id },
              data: {
                ...baseUpdate,
                recoveryStatus: "no_match",
                lastRecoveryError: `${resolvers.join("+")}_no_match`,
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
              detectionMethod: detectionMethodForProvider(match.sourceProvider),
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
          await LocalFingerprintService.bumpPlayAggregates({
            recordingMbid: match.recordingId ?? null,
            artist: artistFinal,
            title: titleFinal,
          });

          if (recoveredViaAcoustid || recoveredViaAudd || recoveredViaAcrcloud) {
            await LocalFingerprintService.learn({
              fp: fingerprint,
              match,
              source: recoveredViaAcoustid ? "acoustid" : "manual",
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
              detectionLogId: row.detectionLogId ?? null,
              recoveryStatus: "recovered",
              recoveredAt: new Date(),
              lastRecoveryError: null,
            },
          });
          logger.info(
            {
              unresolvedSampleId: row.id,
              stationId: row.stationId,
              provider: match.sourceProvider,
              title: titleFinal,
              artist: artistFinal,
            },
            "Recovered unresolved sample via audio resolver"
          );
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
