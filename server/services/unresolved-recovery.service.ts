import * as fs from "fs";
import * as path from "path";
import type { DetectionLog, UnresolvedSample } from "@prisma/client";
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
import type { MatchResult } from "../types.js";
import { fingerprintPipelineGate } from "../lib/fingerprint-pipeline-gate.js";
import {
  RecoveryReason,
  analyzeTitleEvidence,
  buildTitleNormKey,
  matchResultFromLocalFingerprint,
  matchResultFromVerifiedTrack,
  recoveryPriorityForEvidence,
  screenProgrammeOrDirtyWeb,
} from "../lib/unresolved-evidence.js";
import { createUnresolvedRecoveryAudit, type RecoveryAuditKind } from "../lib/unresolved-recovery-audit.js";

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

async function resolveLinkedDetection(row: UnresolvedSample): Promise<DetectionLog | null> {
  if (row.detectionLogId) {
    return prisma.detectionLog.findUnique({ where: { id: row.detectionLogId } });
  }
  return prisma.detectionLog.findFirst({
    where: {
      stationId: row.stationId,
      status: "unresolved",
      observedAt: { lte: row.createdAt },
    },
    orderBy: { observedAt: "desc" },
  });
}

async function persistRecoveryMatch(params: {
  row: UnresolvedSample;
  baseUpdate: { recoveryAttempts: { increment: number }; lastRecoveryAt: Date };
  fingerprint: Awaited<ReturnType<typeof FingerprintService.generateFingerprint>>;
  linkedDetection: DetectionLog | null;
  match: MatchResult;
  detectionMethod: string;
  logReasonCode: string;
  sampleRecoveryReason: string;
  verifiedTrackId?: string | null;
  recoveredViaAcoustid: boolean;
  recoveredViaAudd: boolean;
  recoveredViaAcrcloud: boolean;
  titleNormKey?: string | null;
  recoveryPriority?: number;
  auditKind?: RecoveryAuditKind | null;
}): Promise<void> {
  const {
    row,
    baseUpdate,
    fingerprint,
    linkedDetection,
    match,
    detectionMethod,
    logReasonCode,
    sampleRecoveryReason,
    verifiedTrackId,
    recoveredViaAcoustid,
    recoveredViaAudd,
    recoveredViaAcrcloud,
    titleNormKey,
    recoveryPriority,
    auditKind,
  } = params;

  const observedAt = linkedDetection?.observedAt ?? row.createdAt;
  const titleFinal = (match.title || "").trim();
  const artistFinal = (match.artist || "").trim() || null;

  const recoveredLog = await prisma.detectionLog.create({
    data: {
      stationId: row.stationId,
      observedAt,
      detectionMethod,
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
      reasonCode: logReasonCode,
      verifiedTrackId: verifiedTrackId ?? null,
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
  } else if (detectionMethod === "catalog_lookup") {
    await LocalFingerprintService.learn({
      fp: fingerprint,
      match,
      source: "manual",
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
      recoveryReason: sampleRecoveryReason,
      verifiedTrackId: verifiedTrackId ?? null,
      ...(titleNormKey ? { titleNormKey } : {}),
      ...(typeof recoveryPriority === "number" ? { recoveryPriority } : {}),
    },
  });

  if (auditKind) {
    await createUnresolvedRecoveryAudit({
      row,
      createdDetectionLogId: recoveredLog.id,
      kind: auditKind,
      match,
      titleNormKey: titleNormKey ?? row.titleNormKey ?? null,
    });
  }
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

  /**
   * Cheap pass: attach titleNormKey / recoveryReason for pending rows without fingerprinting.
   */
  static async classifyPendingBatch(opts?: { stationId?: string; limit?: number }): Promise<{ updated: number }> {
    const limit = Math.min(500, Math.max(1, opts?.limit ?? parseEnvInt("UNRESOLVED_CLASSIFY_BATCH_SIZE", 120)));
    const rows = await prisma.unresolvedSample.findMany({
      where: {
        ...(opts?.stationId ? { stationId: opts.stationId } : {}),
        recoveryStatus: { in: ["pending", "no_match"] },
        detectionLogId: { not: null },
        OR: [{ recoveryReason: null }, { recoveryReason: RecoveryReason.PENDING_CLASSIFICATION }],
      },
      orderBy: [{ createdAt: "desc" }],
      take: limit,
    });

    let updated = 0;
    for (const row of rows) {
      const det = row.detectionLogId
        ? await prisma.detectionLog.findUnique({ where: { id: row.detectionLogId } })
        : null;
      if (!det) continue;
      const ev = await analyzeTitleEvidence({
        parsedArtist: det.parsedArtist,
        parsedTitle: det.parsedTitle,
        rawStreamText: det.rawStreamText,
      });
      const titleNormKey =
        "titleNormKey" in ev && ev.titleNormKey ? ev.titleNormKey : row.titleNormKey ?? null;
      const recoveryPriority = recoveryPriorityForEvidence(ev);
      let recoveryReason: string | null = null;
      let recoveryStatus: "pending" | "skipped" | "no_match" | undefined = undefined;

      if (ev.kind === "dirty_web") {
        recoveryReason = RecoveryReason.DIRTY_WEB_TITLE;
        recoveryStatus = "skipped";
      } else if (ev.kind === "programme") {
        recoveryReason = RecoveryReason.PROGRAMME_OR_NON_MUSIC;
        recoveryStatus = "skipped";
      } else if (ev.kind === "weak_local_only") {
        recoveryReason = RecoveryReason.WEAK_METADATA_REVIEW;
      } else if (ev.kind === "no_exact_support") {
        recoveryReason = RecoveryReason.NO_EXACT_TITLE_SUPPORT;
      } else if (ev.kind === "fingerprint_only") {
        recoveryReason = RecoveryReason.FINGERPRINT_ONLY_NO_TITLE;
      } else if (ev.kind === "verified_exact" || ev.kind === "trusted_local_exact") {
        recoveryReason = RecoveryReason.PENDING_CLASSIFICATION;
      } else {
        continue;
      }

      await prisma.unresolvedSample.update({
        where: { id: row.id },
        data: {
          titleNormKey,
          recoveryReason,
          recoveryPriority,
          ...(recoveryStatus ? { recoveryStatus } : {}),
        },
      });
      updated += 1;
    }
    return { updated };
  }

  /**
   * Safe auto-recovery: exact VerifiedTrack or trusted LocalFingerprint title pair only.
   * Does not call AcoustID/AudD/ACRCloud (title evidence was already validated in DB).
   */
  static async runSafeTitleAutoRecoveryBatch(opts?: {
    stationId?: string;
    limit?: number;
    dryRun?: boolean;
  }): Promise<{ examined: number; recovered: number; skipped: number; dryRunCandidates: number; dryRun: boolean }> {
    const limit = Math.min(500, Math.max(1, opts?.limit ?? 200));
    const dryRun = opts?.dryRun !== false;
    const rows = await prisma.unresolvedSample.findMany({
      where: {
        ...(opts?.stationId ? { stationId: opts.stationId } : {}),
        recoveryStatus: { in: ["pending", "no_match", "error"] },
        OR: [
          { recoveryReason: RecoveryReason.PENDING_CLASSIFICATION },
          { recoveryReason: null },
        ],
      },
      orderBy: [{ recoveryPriority: "desc" }, { createdAt: "asc" }],
      take: limit,
    });

    let examined = 0;
    let recovered = 0;
    let skipped = 0;
    let dryRunCandidates = 0;

    for (const row of rows) {
      examined += 1;
      if (!row.filePath || !fs.existsSync(row.filePath)) {
        skipped += 1;
        continue;
      }
      const linkedDetection = await resolveLinkedDetection(row);
      const ev = await analyzeTitleEvidence({
        parsedArtist: linkedDetection?.parsedArtist,
        parsedTitle: linkedDetection?.parsedTitle,
        rawStreamText: linkedDetection?.rawStreamText,
      });
      if (ev.kind !== "verified_exact" && ev.kind !== "trusted_local_exact") {
        skipped += 1;
        continue;
      }

      const releaseGate = await fingerprintPipelineGate.acquire();
      let fingerprint: Awaited<ReturnType<typeof FingerprintService.generateFingerprint>>;
      try {
        fingerprint = await FingerprintService.generateFingerprint(row.filePath);
      } finally {
        releaseGate();
      }
      if (!fingerprint) {
        skipped += 1;
        continue;
      }

      const match =
        ev.kind === "verified_exact"
          ? matchResultFromVerifiedTrack(ev.track)
          : matchResultFromLocalFingerprint(ev.row);
      const logReasonCode =
        ev.kind === "verified_exact"
          ? RecoveryReason.TITLE_AUTO_VERIFIED
          : RecoveryReason.TITLE_AUTO_TRUSTED_LOCAL;
      const sampleReason =
        ev.kind === "verified_exact"
          ? RecoveryReason.TITLE_AUTO_VERIFIED
          : RecoveryReason.TITLE_AUTO_TRUSTED_LOCAL;
      const verifiedTrackId = ev.kind === "verified_exact" ? ev.track.id : null;

      const baseUpdate = {
        recoveryAttempts: { increment: 1 as const },
        lastRecoveryAt: new Date(),
      };

      if (dryRun) {
        dryRunCandidates += 1;
        continue;
      }

      await persistRecoveryMatch({
        row,
        baseUpdate,
        fingerprint,
        linkedDetection,
        match,
        detectionMethod: "catalog_lookup",
        logReasonCode,
        sampleRecoveryReason: sampleReason,
        verifiedTrackId,
        recoveredViaAcoustid: false,
        recoveredViaAudd: false,
        recoveredViaAcrcloud: false,
        titleNormKey: ev.titleNormKey,
        recoveryPriority: recoveryPriorityForEvidence(ev),
        auditKind: ev.kind === "verified_exact" ? "title_verified" : "title_trusted_localfp",
      });
      recovered += 1;
      logger.info(
        { unresolvedSampleId: row.id, stationId: row.stationId, logReasonCode },
        "Safe title auto-recovery applied"
      );
    }

    return { examined, recovered, skipped, dryRunCandidates, dryRun };
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
    const titleLaneEnabled = envBoolTrue("UNRESOLVED_TITLE_LANE_ENABLED", true);

    const anyAudioResolver = acoustidEnabled || auddEnabled || acrcloudEnabled;
    if (!anyAudioResolver && !opts?.continueWithoutAcoustid && !titleLaneEnabled) {
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
          recoveryReason: RecoveryReason.PENDING_CLASSIFICATION,
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
        orderBy: [{ recoveryPriority: "desc" }, { createdAt: "asc" }],
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
                recoveryReason: "sample_missing",
              },
            });
            continue;
          }

          const linkedDetection = await resolveLinkedDetection(row);

          if (titleLaneEnabled && linkedDetection) {
            const screen = screenProgrammeOrDirtyWeb({
              parsedArtist: linkedDetection.parsedArtist,
              parsedTitle: linkedDetection.parsedTitle,
              rawStreamText: linkedDetection.rawStreamText,
            });
            if (screen === "dirty_web") {
              skipped += 1;
              await prisma.unresolvedSample.update({
                where: { id: row.id },
                data: {
                  ...baseUpdate,
                  recoveryStatus: "skipped",
                  lastRecoveryError: RecoveryReason.DIRTY_WEB_TITLE,
                  recoveryReason: RecoveryReason.DIRTY_WEB_TITLE,
                  titleNormKey: row.titleNormKey,
                },
              });
              continue;
            }
            if (screen === "programme") {
              skipped += 1;
              await prisma.unresolvedSample.update({
                where: { id: row.id },
                data: {
                  ...baseUpdate,
                  recoveryStatus: "skipped",
                  lastRecoveryError: RecoveryReason.PROGRAMME_OR_NON_MUSIC,
                  recoveryReason: RecoveryReason.PROGRAMME_OR_NON_MUSIC,
                  titleNormKey: row.titleNormKey,
                },
              });
              continue;
            }
          }

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
                lastRecoveryError: RecoveryReason.FINGERPRINT_UNAVAILABLE,
                recoveryReason: RecoveryReason.FINGERPRINT_UNAVAILABLE,
              },
            });
            continue;
          }

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
            if (titleLaneEnabled && linkedDetection) {
              const ev = await analyzeTitleEvidence({
                parsedArtist: linkedDetection.parsedArtist,
                parsedTitle: linkedDetection.parsedTitle,
                rawStreamText: linkedDetection.rawStreamText,
              });

              if (ev.kind === "verified_exact") {
                const match = matchResultFromVerifiedTrack(ev.track);
                await persistRecoveryMatch({
                  row,
                  baseUpdate,
                  fingerprint,
                  linkedDetection,
                  match,
                  detectionMethod: "catalog_lookup",
                  logReasonCode: RecoveryReason.TITLE_AUTO_VERIFIED,
                  sampleRecoveryReason: RecoveryReason.TITLE_AUTO_VERIFIED,
                  verifiedTrackId: ev.track.id,
                  recoveredViaAcoustid: false,
                  recoveredViaAudd: false,
                  recoveredViaAcrcloud: false,
                  titleNormKey: ev.titleNormKey,
                  recoveryPriority: recoveryPriorityForEvidence(ev),
                  auditKind: "title_verified",
                });
                recovered += 1;
                logger.info(
                  { unresolvedSampleId: row.id, stationId: row.stationId },
                  "Recovered unresolved sample via verified catalog title pair"
                );
                continue;
              }

              if (ev.kind === "trusted_local_exact") {
                const match = matchResultFromLocalFingerprint(ev.row);
                await persistRecoveryMatch({
                  row,
                  baseUpdate,
                  fingerprint,
                  linkedDetection,
                  match,
                  detectionMethod: "catalog_lookup",
                  logReasonCode: RecoveryReason.TITLE_AUTO_TRUSTED_LOCAL,
                  sampleRecoveryReason: RecoveryReason.TITLE_AUTO_TRUSTED_LOCAL,
                  verifiedTrackId: null,
                  recoveredViaAcoustid: false,
                  recoveredViaAudd: false,
                  recoveredViaAcrcloud: false,
                  titleNormKey: ev.titleNormKey,
                  recoveryPriority: recoveryPriorityForEvidence(ev),
                  auditKind: "title_trusted_localfp",
                });
                recovered += 1;
                logger.info(
                  { unresolvedSampleId: row.id, stationId: row.stationId },
                  "Recovered unresolved sample via trusted local fingerprint title pair"
                );
                continue;
              }

              if (ev.kind === "weak_local_only") {
                noMatch += 1;
                await prisma.unresolvedSample.update({
                  where: { id: row.id },
                  data: {
                    ...baseUpdate,
                    recoveryStatus: "no_match",
                    lastRecoveryError: "fingerprint_exhausted_weak_metadata_only",
                    recoveryReason: RecoveryReason.WEAK_METADATA_REVIEW,
                    titleNormKey: ev.titleNormKey,
                    recoveryPriority: recoveryPriorityForEvidence(ev),
                  },
                });
                continue;
              }

              if (ev.kind === "no_exact_support") {
                noMatch += 1;
                await prisma.unresolvedSample.update({
                  where: { id: row.id },
                  data: {
                    ...baseUpdate,
                    recoveryStatus: "no_match",
                    lastRecoveryError: "fingerprint_exhausted_no_exact_title_support",
                    recoveryReason: RecoveryReason.NO_EXACT_TITLE_SUPPORT,
                    titleNormKey: ev.titleNormKey,
                    recoveryPriority: recoveryPriorityForEvidence(ev),
                  },
                });
                continue;
              }

              if (ev.kind === "fingerprint_only") {
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
                    recoveryReason: RecoveryReason.FINGERPRINT_ONLY_NO_TITLE,
                    titleNormKey: ev.titleNormKey,
                    recoveryPriority: recoveryPriorityForEvidence(ev),
                  },
                });
                continue;
              }
            }

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
                recoveryReason: RecoveryReason.FINGERPRINT_EXHAUSTED,
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
                recoveryReason: RecoveryReason.FINGERPRINT_EXHAUSTED,
              },
            });
            continue;
          }

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
              recoveryReason: RecoveryReason.RECOVERED_FROM_AUDIO,
            },
          });
          await createUnresolvedRecoveryAudit({
            row,
            createdDetectionLogId: recoveredLog.id,
            kind: "audio_resolver",
            match,
            titleNormKey: buildTitleNormKey(artistFinal, titleFinal),
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
