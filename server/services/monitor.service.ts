import * as fs from "fs";
import * as path from "path";
import type { Prisma, Station } from "@prisma/client";
import { prisma } from "../lib/prisma.js";
import { logger } from "../lib/logger.js";
import { mergeAcoustidAndCatalog } from "../lib/audio-id-merge.js";
import { assessMetadataQuality } from "../lib/metadata-quality.js";
import { ResolverService } from "./resolver.service.js";
import { MetadataService } from "./metadata.service.js";
import { SamplerService } from "./sampler.service.js";
import { FingerprintService } from "./fingerprint.service.js";
import { AcoustidService } from "./acoustid.service.js";
import { LocalFingerprintService } from "./local-fingerprint.service.js";
import { MusicbrainzService } from "./musicbrainz.service.js";
import { CatalogLookupService } from "./catalog-lookup.service.js";
import {
  NormalizedMetadata,
  MatchResult,
  DetectionMethod,
  StationContentClassification,
  StationMonitorState,
  StreamHealthSnapshot,
} from "../types.js";
import { upsertSongSpinOnNewPlay } from "../lib/song-spin.js";
import { StreamRefreshService } from "./stream-refresh.service.js";
import { StreamHealthService } from "./stream-health.service.js";
import { classifyContent, deriveMonitorState } from "../lib/station-health.js";
import { monitorEvents } from "../lib/monitor-events.js";

function parseEnvMs(key: string, fallback: number): number {
  const v = process.env[key];
  if (!v) return fallback;
  const n = parseInt(v, 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

function parseEnvFloat(key: string, fallback: number): number {
  const v = process.env[key];
  if (!v) return fallback;
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : fallback;
}

function parseEnvBool(key: string, fallback = false): boolean {
  const v = process.env[key];
  if (!v) return fallback;
  const t = String(v).trim().toLowerCase();
  return t === "1" || t === "true" || t === "yes" || t === "on";
}

function parseEnvInt(key: string, fallback: number): number {
  const v = process.env[key];
  if (!v) return fallback;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : fallback;
}

function parseReasonSuffixFromStreamUrl(url: string): string {
  const lower = String(url || "").toLowerCase();
  if (!lower) return "stream";
  if (lower.includes("fastcast4u.com/proxy/")) return "fastcast_proxy";
  if (lower.includes("zeno.fm")) return "zeno_stream";
  if (lower.includes("icecast")) return "icecast_stream";
  if (lower.includes("shoutcast")) return "shoutcast_stream";
  if (lower.includes("/proxy/")) return "proxy_stream";
  return "stream";
}

function computeDetectionLagMs(
  station: {
    detectionLagMsAvg: number | null;
    detectionLagSamples: number;
    metadataStaleSeconds: number;
  },
  metadata: NormalizedMetadata | null
): { avg: number; samples: number } | null {
  if (!metadata?.combinedRaw) return null;
  const observed = Math.max(0, station.metadataStaleSeconds * 1000);
  const prevAvg = station.detectionLagMsAvg ?? 0;
  const prevN = Math.max(0, station.detectionLagSamples || 0);
  const nextN = Math.min(prevN + 1, 5000);
  const avg = Math.round((prevAvg * prevN + observed) / Math.max(nextN, 1));
  return { avg, samples: nextN };
}

function stationUpdateWithPreservedDates(
  station: {
    lastHealthyAt: Date | null;
    lastGoodAudioAt: Date | null;
    lastMetadataAt: Date | null;
    lastSongDetectedAt: Date | null;
    detectionLagMsAvg: number | null;
    detectionLagSamples: number;
  },
  input: {
    health: StreamHealthSnapshot;
    metadata: NormalizedMetadata | null;
    detectionStatus: "matched" | "unresolved";
    lag: { avg: number; samples: number } | null;
  }
): Prisma.StationUpdateInput {
  const { health, metadata, detectionStatus, lag } = input;
  return {
    lastHealthyAt:
      health.reachable && health.audioFlowing ? new Date() : station.lastHealthyAt,
    lastGoodAudioAt: health.decoderOk ? new Date() : station.lastGoodAudioAt,
    lastMetadataAt: metadata?.combinedRaw ? new Date() : station.lastMetadataAt,
    lastSongDetectedAt: detectionStatus === "matched" ? new Date() : station.lastSongDetectedAt,
    detectionLagMsAvg: lag?.avg ?? station.detectionLagMsAvg,
    detectionLagSamples: lag?.samples ?? station.detectionLagSamples,
  };
}

/** ICY text too weak to drive MusicBrainz/iTunes (avoids garbage catalog hits). */
function isJunkIcyMetadata(metadata: NormalizedMetadata | null): boolean {
  if (!metadata) return true;
  const raw = (metadata.combinedRaw ?? "").trim();
  if (raw.length < 2) return true;
  if (raw === "-" || raw === " - " || raw === "...") return true;
  return false;
}

function isProgramLikeTitle(text: string | null | undefined): boolean {
  const t = String(text ?? "").trim().toLowerCase();
  if (!t) return false;
  return /\b(on air|program|show|morning show|afternoon show|evening show|news|sports|talk)\b/.test(t);
}

function parseSourceIdsMap(json: string | null | undefined): Record<string, string> {
  if (!json) return {};
  try {
    const x = JSON.parse(json);
    if (!x || typeof x !== "object" || Array.isArray(x)) return {};
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(x as Record<string, unknown>)) {
      if (typeof v === "string" && v.trim()) out[k] = v.trim();
    }
    return out;
  } catch {
    return {};
  }
}

export class MonitorService {
  /**
   * Main logic for a single station poll.
   * Audio fingerprint: on ICY change, on interval (audioFingerprintIntervalSeconds), or when metadata is missing/untrusted/stale.
   */
  static async pollStation(stationId: string): Promise<void> {
    const start = Date.now();
    const station = await prisma.station.findUnique({ where: { id: stationId } });
    if (!station || !station.isActive) return;

    logger.info({ station: station.name }, "Polling station");
    const markPollError = async (message: string) => {
      await prisma.station.update({
        where: { id: stationId },
        data: {
          lastPollAt: new Date(),
          lastPollStatus: "error",
          lastPollError: message.slice(0, 2000),
          consecutivePollFailures: { increment: 1 },
          consecutiveHealthyPolls: 0,
        },
      });
    };

    try {
      const resolvedUrl = await ResolverService.resolveStreamUrl(station.streamUrl);
      const health = await StreamHealthService.validateStream(resolvedUrl);

      let metadata: NormalizedMetadata | null = null;
      let legacyFingerprint = false;
      let reasonCode: string | null = null;

      if (station.metadataPriorityEnabled) {
        metadata = await MetadataService.readStreamMetadata(resolvedUrl);
        if (!metadata) {
          metadata = await MetadataService.readProviderNowPlayingMetadata(resolvedUrl);
        }
        const latestNowPlaying = await prisma.currentNowPlaying.findUnique({ where: { stationId } });

        if (!metadata) {
          const tuneStub = await MetadataService.readTuneInStubMetadata(station.sourceIdsJson, station.name);
          if (tuneStub) {
            metadata = tuneStub;
            reasonCode = "tunein_stub_metadata";
          }
        }

        if (!metadata) {
          legacyFingerprint = true;
          reasonCode = "metadata_missing";
        } else {
          if (isJunkIcyMetadata(metadata)) {
            legacyFingerprint = true;
            reasonCode = "metadata_junk";
          }
          const check = MetadataService.isMetadataTrustworthy(metadata, latestNowPlaying?.streamText || undefined);
          if (!check.trusted) {
            legacyFingerprint = true;
            reasonCode = check.reason || "metadata_untrusted";
          } else if (latestNowPlaying && latestNowPlaying.streamText === metadata.combinedRaw) {
            const staleAt = new Date(
              latestNowPlaying.updatedAt.getTime() + station.metadataStaleSeconds * 1000
            );
            const repeatEarlyAt = new Date(
              latestNowPlaying.updatedAt.getTime() +
                Math.max(30_000, Math.floor(station.metadataStaleSeconds * 500))
            );
            if (Date.now() > staleAt.getTime()) {
              legacyFingerprint = true;
              reasonCode = "metadata_stale";
            } else if (Date.now() > repeatEarlyAt.getTime()) {
              legacyFingerprint = true;
              reasonCode = "metadata_repeated_same_text";
            }
          }
        }
      } else {
        legacyFingerprint = true;
        reasonCode = "metadata_disabled";
      }
      const latestNp = await prisma.currentNowPlaying.findUnique({ where: { stationId } });
      const icyText = (metadata?.combinedRaw ?? "").trim();
      const prevIcy = (latestNp?.streamText ?? "").trim();
      const icyChanged =
        !!metadata &&
        icyText.length > 0 &&
        prevIcy.length > 0 &&
        icyText !== prevIcy;

      const intervalSec = Math.max(30, station.audioFingerprintIntervalSeconds || 300);
      const lastFp = station.lastAudioFingerprintAt;
      const intervalElapsed =
        !lastFp || Date.now() - lastFp.getTime() >= intervalSec * 1000;

      const acoustidKey = process.env.ACOUSTID_API_KEY;
      const forceAudioFallback = parseEnvBool("FORCE_AUDIO_FALLBACK_WHEN_UNRESOLVED", true);
      /** When true, capture+fingerprint on every poll (not only ICY gap/stale/change). Heavy on CPU/network; use for stations with bad or missing ICY. */
      const fingerprintEveryPoll = parseEnvBool("FINGERPRINT_EVERY_POLL", false);

      const tightness = Math.max(0, Math.min(2, station.metadataTrustTightness ?? 0));
      const metaQuality =
        metadata && !isJunkIcyMetadata(metadata)
          ? assessMetadataQuality(
              metadata.combinedRaw,
              metadata.rawTitle,
              metadata.rawArtist,
              station.name,
              tightness
            )
          : {
              okForCatalog: false,
              forceFingerprint: !!metadata && isJunkIcyMetadata(metadata),
              catalogConfidenceScale: 0,
              reasons: metadata ? ["junk_icy"] : ["no_metadata"],
            };

      const trustCheck =
        metadata && !isJunkIcyMetadata(metadata)
          ? MetadataService.isMetadataTrustworthy(metadata, latestNp?.streamText || undefined)
          : { trusted: false as const, reason: "no_metadata" };

      let metaTrust = trustCheck.trusted ? 1 : 0;
      if (metaQuality.forceFingerprint) metaTrust = 0;
      if (reasonCode === "metadata_stale" || reasonCode === "metadata_repeated_same_text") metaTrust = 0;

      const catalogTrustFactor = metaTrust * metaQuality.catalogConfidenceScale;

      /** When true, also fingerprint when ICY exists but quality heuristics flag it (see metadata-quality). */
      const forceFingerprintAggressive =
        metaQuality.forceFingerprint || parseEnvBool("FINGERPRINT_AGGRESSIVE_ON_SUSPICIOUS_METADATA", false);

      const doAudioId =
        !!station.fingerprintFallbackEnabled &&
        (forceAudioFallback || !!acoustidKey) &&
        (legacyFingerprint ||
          icyChanged ||
          intervalElapsed ||
          fingerprintEveryPoll ||
          forceFingerprintAggressive);

      let audioMatch: MatchResult | null = null;
      let sampledForFingerprint = false;
      let sampledAudioPath: string | null = null;

      let capturedFingerprint: Awaited<ReturnType<typeof FingerprintService.generateFingerprint>> = null;
      let audioMatchSource: "local" | "acoustid" | null = null;
      const fingerprintAttempts: Array<{
        attempt: number;
        delaySec: number;
        sampleSec: number;
        outcome: "match_local" | "match_acoustid" | "no_match" | "no_sample" | "no_fingerprint";
      }> = [];

      if (doAudioId && resolvedUrl.startsWith("http")) {
        const baseSec = Math.min(
          120,
          Math.max(
            station.sampleSeconds,
            parseInt(process.env.FINGERPRINT_SAMPLE_SECONDS || "25", 10) || 25
          )
        );
        const bonusSec = parseEnvInt("FINGERPRINT_EXTRA_SAMPLE_SECONDS", 0);
        const maxRetries = Math.min(
          4,
          Math.max(1, station.fingerprintRetries ?? parseEnvInt("FINGERPRINT_MAX_RETRIES", 2))
        );
        const delayMs = Math.max(
          0,
          station.fingerprintRetryDelayMs ?? parseEnvInt("FINGERPRINT_RETRY_DELAY_MS", 3500)
        );

        for (let attempt = 0; attempt < maxRetries && !audioMatch; attempt++) {
          const fpSec = Math.min(120, baseSec + (attempt > 0 ? Math.min(25, bonusSec) : 0));
          const delaySec = attempt === 0 ? 0 : delayMs / 1000;
          logger.info(
            {
              station: station.name,
              attempt: attempt + 1,
              maxRetries,
              reason: reasonCode,
              icyChanged,
              intervalElapsed,
              fpSec,
              delaySec,
            },
            "Audio fingerprint sample (ICY change, interval, suspicious metadata, or retry)"
          );
          const samplePath = await SamplerService.captureSample(resolvedUrl, fpSec, delaySec);
          sampledForFingerprint = true;
          if (!samplePath) {
            fingerprintAttempts.push({
              attempt: attempt + 1,
              delaySec,
              sampleSec: fpSec,
              outcome: "no_sample",
            });
            continue;
          }
          sampledAudioPath = samplePath;
          const fp = await FingerprintService.generateFingerprint(samplePath);
          SamplerService.cleanup(samplePath);
          sampledAudioPath = null;
          if (!fp) {
            fingerprintAttempts.push({
              attempt: attempt + 1,
              delaySec,
              sampleSec: fpSec,
              outcome: "no_fingerprint",
            });
            continue;
          }
          capturedFingerprint = fp;
          const localMatch = await LocalFingerprintService.lookup(fp);
          if (localMatch) {
            audioMatch = localMatch;
            audioMatchSource = "local";
            fingerprintAttempts.push({
              attempt: attempt + 1,
              delaySec,
              sampleSec: fpSec,
              outcome: "match_local",
            });
            break;
          }
          if (acoustidKey) {
            const acoustidMatch = await AcoustidService.lookup(fp);
            if (acoustidMatch) {
              audioMatch = await MusicbrainzService.enrich(acoustidMatch);
              audioMatchSource = "acoustid";
              fingerprintAttempts.push({
                attempt: attempt + 1,
                delaySec,
                sampleSec: fpSec,
                outcome: "match_acoustid",
              });
              break;
            }
          }
          fingerprintAttempts.push({
            attempt: attempt + 1,
            delaySec,
            sampleSec: fpSec,
            outcome: "no_match",
          });
        }
        if (!acoustidKey && doAudioId) {
          logger.debug({ station: station.name }, "Fingerprint path ran without ACOUSTID_API_KEY after local miss");
        }
      }

      let catalogMatch: MatchResult | null = null;
      let catalogRejectedLowConfidence = false;
      if (metadata && !isJunkIcyMetadata(metadata) && metaQuality.okForCatalog) {
        const rawCat = await CatalogLookupService.lookupFromMetadata(metadata);
        const floor =
          typeof station.catalogConfidenceFloor === "number" && station.catalogConfidenceFloor > 0
            ? station.catalogConfidenceFloor
            : null;
        if (rawCat && floor != null && (rawCat.confidence ?? 0) < floor) {
          catalogMatch = null;
          catalogRejectedLowConfidence = true;
        } else {
          catalogMatch = rawCat;
        }
      }

      const minAcoust = parseEnvFloat("ACOUSTID_PREFER_MIN_SCORE", 0.55);
      const merged = mergeAcoustidAndCatalog(audioMatch, catalogMatch, minAcoust, catalogTrustFactor);

      let match = merged.match;
      let method: DetectionMethod = merged.method;
      const mergeReason = merged.reasonCode;

      const noMetadata = !metadata?.combinedRaw;
      const noCatalog = !catalogMatch;
      const noAudioMatch = !audioMatch;
      const noAcoustidKey = !acoustidKey;
      if (
        !match &&
        station.metadataPriorityEnabled &&
        station.fingerprintFallbackEnabled &&
        legacyFingerprint &&
        reasonCode === "metadata_missing" &&
        noMetadata &&
        noCatalog &&
        noAudioMatch &&
        noAcoustidKey &&
        health.reachable &&
        health.audioFlowing &&
        health.decoderOk
      ) {
        method = "unresolved";
        reasonCode = `no_song_metadata_available:${parseReasonSuffixFromStreamUrl(resolvedUrl)}`;
      }

      if (!match && metadata && !legacyFingerprint && station.metadataPriorityEnabled) {
        const check = MetadataService.isMetadataTrustworthy(metadata, latestNp?.streamText || undefined);
        if (metadata && check.trusted) {
          method = "stream_metadata";
        }
      }

      // Second-pass fallback: when metadata is present but unresolved/program-like,
      // attempt catalog lookup from combined stream text to reduce "Unknown".
      if (!match && metadata && station.fingerprintFallbackEnabled && !metaQuality.forceFingerprint) {
        const viaCombined = await CatalogLookupService.lookupFromMetadata({
          ...metadata,
          rawArtist: metadata.rawArtist || "",
          rawTitle: metadata.rawTitle || metadata.combinedRaw || "",
        });
        if (viaCombined) {
          match = viaCombined;
          method = "catalog_lookup";
        }
      }

      let unresolvedSamplePath: string | null = null;
      if (!match && sampledForFingerprint && resolvedUrl.startsWith("http")) {
        const archSec = Math.min(
          120,
          Math.max(
            station.sampleSeconds,
            parseInt(process.env.FINGERPRINT_SAMPLE_SECONDS || "25", 10) || 25
          )
        );
        const archTmp = await SamplerService.captureSample(resolvedUrl, archSec);
        if (archTmp) {
          unresolvedSamplePath = await this.archiveUnresolvedSample(stationId, archTmp);
          SamplerService.cleanup(archTmp);
        }
      }

      if (sampledForFingerprint) {
        await prisma.station.update({
          where: { id: stationId },
          data: { lastAudioFingerprintAt: new Date() },
        });
      }

      // Self-learning: persist the fingerprint whenever we have a confirmed match
      // so future plays of the same track are resolved locally (no AcoustID / MB call).
      if (capturedFingerprint && match && audioMatchSource !== "local") {
        const learnSource: "acoustid" | "stream_metadata" =
          audioMatchSource === "acoustid" ? "acoustid" : "stream_metadata";
        await LocalFingerprintService.learn({
          fp: capturedFingerprint,
          match,
          metadata,
          source: learnSource,
        });
      }

      const finalReason =
        mergeReason ||
        reasonCode ||
        (icyChanged ? "icy_changed_audio" : null) ||
        (intervalElapsed && !legacyFingerprint ? "audio_interval" : null);

      const matchDiagnostics = {
        pollReason: reasonCode,
        icyChanged,
        intervalElapsed,
        legacyFingerprint,
        fingerprintEveryPoll,
        doAudioId,
        metaTrust,
        metaQualityReasons: metaQuality.reasons,
        catalogTrustFactor,
        catalogConfidenceFloor: station.catalogConfidenceFloor ?? null,
        catalogRejectedLowConfidence,
        fingerprintAttempts,
        streamHealthOk: health.reachable && health.audioFlowing && health.decoderOk,
        healthReason: health.reason,
      };

      const processingMs = Date.now() - start;
      let detectionReason = finalReason;
      const detection = await this.saveDetection(
        station,
        resolvedUrl,
        method,
        metadata,
        match,
        processingMs,
        finalReason,
        JSON.stringify(matchDiagnostics)
      );
      if (
        unresolvedSamplePath &&
        detection.status === "unresolved" &&
        detection.detectionLogId
      ) {
        await prisma.unresolvedSample.updateMany({
          where: {
            stationId,
            filePath: unresolvedSamplePath,
            detectionLogId: null,
          },
          data: {
            detectionLogId: detection.detectionLogId,
          },
        });
      }
      const contentClassification: StationContentClassification =
        detection.status === "matched"
          ? "music"
          : classifyContent(metadata?.combinedRaw ?? null);

      const nextFailureThreshold = Math.max(1, station.failureThreshold || 3);
      const nextFailureCount = health.reachable && health.audioFlowing && health.decoderOk
        ? 0
        : Math.max(0, (station.consecutivePollFailures || 0) + 1);
      const nextHealthyCount = health.reachable && health.audioFlowing && health.decoderOk
        ? Math.max(0, (station.consecutiveHealthyPolls || 0) + 1)
        : 0;
      const monitor = deriveMonitorState({
        health,
        contentClassification,
        hasReliableMatch: detection.status === "matched",
        consecutiveFailures: nextFailureCount,
        failureThreshold: nextFailureThreshold,
      });
      if (!detectionReason) {
        detectionReason = monitor.reason;
      }
      const lag = computeDetectionLagMs(station, metadata);
      const preservedDates = stationUpdateWithPreservedDates(station, {
        health,
        metadata,
        detectionStatus: detection.status,
        lag,
      });

      await prisma.station.update({
        where: { id: stationId },
        data: {
          lastPollAt: new Date(),
          lastPollStatus:
            monitor.state === "INACTIVE"
              ? "error"
              : monitor.state === "DEGRADED"
                ? "degraded"
                : "ok",
          lastPollError: monitor.state === "INACTIVE" || monitor.state === "DEGRADED" ? monitor.reason : null,
          monitorState: monitor.state,
          monitorStateReason: monitor.reason,
          contentClassification,
          consecutivePollFailures: nextFailureCount,
          consecutiveHealthyPolls: nextHealthyCount,
          lastValidationAt: new Date(),
          lastValidationReason: health.reason,
          lastResolvedStreamUrl: health.resolvedUrl || resolvedUrl,
          lastStreamContentType: health.contentTypeHeader || null,
          lastStreamCodec: health.codec || null,
          lastStreamBitrate: health.bitrate ?? null,
          ...preservedDates,
          deepValidationIntervalSeconds: station.deepValidationIntervalSeconds || 600,
          failureThreshold: nextFailureThreshold,
          recoveryThreshold: Math.max(1, station.recoveryThreshold || 2),
        },
      });
      await this.upsertStreamEndpoint(station, health, monitor.state);
      await this.suppressFailingZenoEndpoints(station.id);
      await this.recordHealthEvent(stationId, monitor.state, monitor.reason, contentClassification, health, nextFailureCount);
    } catch (error) {
      logger.error({ error, station: station.name }, "Error polling station");
      const errMsg = String(error);
      await markPollError(errMsg);
      const refreshed = await StreamRefreshService.refreshFromSourceHints(
        station.sourceIdsJson,
        station.streamUrl
      );
      if (refreshed) {
        logger.info({ station: station.name }, "Auto-refreshed stream URL from source hints; retrying poll");
        await prisma.station.update({
          where: { id: stationId },
          data: {
            streamUrl: refreshed,
            streamRefreshedAt: new Date(),
            lastPollError: `recovered: url refreshed (${errMsg.slice(0, 400)})`,
          },
        });
        try {
          await MonitorService.pollStation(stationId);
        } catch (e2) {
          logger.error({ e2, station: station.name }, "Retry after stream refresh failed");
          await markPollError(String(e2));
          await prisma.jobRun.create({
            data: {
              stationId,
              status: "failure",
              error: `after refresh: ${String(e2)}`,
              durationMs: Date.now() - start,
            },
          });
        }
        return;
      }
      await prisma.jobRun.create({
        data: {
          stationId,
          status: "failure",
          error: errMsg,
          durationMs: Date.now() - start,
        },
      });
    }
  }

  private static async recordHealthEvent(
    stationId: string,
    state: StationMonitorState,
    reason: string,
    contentClassification: StationContentClassification,
    health: StreamHealthSnapshot,
    consecutiveFailures: number
  ) {
    try {
      await prisma.stationHealthEvent.create({
        data: {
          stationId,
          monitorState: state,
          reason,
          reachable: health.reachable,
          audioFlowing: health.audioFlowing,
          decoderOk: health.decoderOk,
          contentClassification,
          resolvedUrl: health.resolvedUrl || null,
          contentTypeHeader: health.contentTypeHeader || null,
          codec: health.codec || null,
          bitrate: health.bitrate ?? null,
          latencyMs: health.latencyMs ?? null,
          consecutiveFailures,
        },
      });
    } catch (error) {
      logger.warn({ error, stationId }, "Failed to write station health event");
    }
  }

  private static async upsertStreamEndpoint(
    station: Pick<Station, "id" | "streamUrl" | "sourceIdsJson">,
    health: StreamHealthSnapshot,
    monitorState: StationMonitorState
  ) {
    try {
      const sourceMap = parseSourceIdsMap(station.sourceIdsJson);
      const sourceEntries = Object.entries(sourceMap);
      const source = sourceEntries[0]?.[0] || "manual";
      const sourceDetail = sourceEntries[0]?.[1] || null;
      const status =
        monitorState === "INACTIVE"
          ? "inactive"
          : monitorState === "DEGRADED"
            ? "degraded"
            : monitorState === "UNKNOWN"
              ? "unknown"
              : "healthy";

      const row = await prisma.stationStreamEndpoint.findFirst({
        where: { stationId: station.id, streamUrl: station.streamUrl },
        select: { id: true, consecutiveFailures: true },
      });
      const nextFailures = health.reachable && health.audioFlowing && health.decoderOk
        ? 0
        : Math.max(0, (row?.consecutiveFailures || 0) + 1);

      if (row) {
        await prisma.stationStreamEndpoint.update({
          where: { id: row.id },
          data: {
            source,
            sourceDetail,
            resolvedUrl: health.resolvedUrl || null,
            isCurrent: true,
            isSuppressed: monitorState === "INACTIVE",
            lastValidatedAt: new Date(),
            lastValidationStatus: status,
            lastFailureReason: health.reason || null,
            lastHealthyAt:
              health.reachable && health.audioFlowing && health.decoderOk ? new Date() : undefined,
            consecutiveFailures: nextFailures,
            codec: health.codec || null,
            bitrate: health.bitrate ?? null,
          },
        });
      } else {
        await prisma.stationStreamEndpoint.create({
          data: {
            stationId: station.id,
            source,
            sourceDetail,
            streamUrl: station.streamUrl,
            resolvedUrl: health.resolvedUrl || null,
            isCurrent: true,
            isSuppressed: monitorState === "INACTIVE",
            lastValidatedAt: new Date(),
            lastValidationStatus: status,
            lastFailureReason: health.reason || null,
            lastHealthyAt:
              health.reachable && health.audioFlowing && health.decoderOk ? new Date() : null,
            consecutiveFailures: nextFailures,
            codec: health.codec || null,
            bitrate: health.bitrate ?? null,
          },
        });
      }

      // Only one current endpoint row per station.
      await prisma.stationStreamEndpoint.updateMany({
        where: {
          stationId: station.id,
          streamUrl: { not: station.streamUrl },
          isCurrent: true,
        },
        data: { isCurrent: false },
      });

    } catch (error) {
      logger.warn({ error, stationId: station.id }, "Failed to upsert stream endpoint");
    }
  }

  private static async suppressFailingZenoEndpoints(stationId: string): Promise<void> {
    try {
      await prisma.stationStreamEndpoint.updateMany({
        where: {
          stationId,
          source: "zeno",
          consecutiveFailures: { gte: 3 },
        },
        data: { isSuppressed: true },
      });
    } catch (error) {
      logger.warn({ error, stationId }, "Failed to suppress failing zeno endpoints");
    }
  }

  private static async saveDetection(
    station: Station,
    resolvedUrl: string,
    method: DetectionMethod,
    metadata: NormalizedMetadata | null,
    match: MatchResult | null,
    processingMs: number,
    reasonCode: string | null,
    matchDiagnosticsJson: string | null = null
  ): Promise<{ status: "matched" | "unresolved"; reasonCode: string | null; detectionLogId?: string }> {
    const stationId = station.id;
    const npRow = await prisma.currentNowPlaying.findUnique({ where: { stationId } });
    const trustedMeta =
      metadata &&
      station.metadataPriorityEnabled &&
      MetadataService.isMetadataTrustworthy(metadata, npRow?.streamText || undefined).trusted;
    const metadataProgramLike =
      isProgramLikeTitle(metadata?.rawTitle) ||
      (!metadata?.rawTitle && isProgramLikeTitle(metadata?.combinedRaw));

    const isMatched =
      !!match ||
      (method === "stream_metadata" &&
        !!metadata &&
        trustedMeta &&
        !isJunkIcyMetadata(metadata) &&
        !metadataProgramLike);
    const status = isMatched ? "matched" : "unresolved";
    const detectionReason =
      reasonCode ||
      (status === "unresolved" ? (metadataProgramLike ? "talk_or_program_content" : "no_reliable_match") : null);

    const rawTitle = (metadata?.rawTitle ?? "").trim();
    const rawArtist = (metadata?.rawArtist ?? "").trim();
    const titleFinal =
      match?.title || (!metadataProgramLike && rawTitle ? rawTitle : undefined);
    const artistFinal = match?.artist || (rawArtist ? rawArtist : undefined);

    let trackDurationMs: number | undefined = match?.durationMs;
    if (!trackDurationMs && match?.recordingId) {
      trackDurationMs = await MusicbrainzService.getRecordingLengthMs(match.recordingId);
    }

    const latestLog = await prisma.detectionLog.findFirst({
      where: { stationId },
      orderBy: { observedAt: "desc" },
    });

    const sameSpin =
      latestLog &&
      latestLog.titleFinal === titleFinal &&
      latestLog.artistFinal === artistFinal &&
      titleFinal != null &&
      String(titleFinal).trim() !== "";

    if (sameSpin) {
      const pollMs = Math.max(station.pollIntervalSeconds, 1) * 1000;
      const maxGuardMs = parseEnvMs("TRACK_GUARD_MAX_MS", 15 * 60 * 1000);
      const fallbackMs = parseEnvMs("TRACK_GUARD_FALLBACK_MS", 4 * 60 * 1000);

      const catalogGuard =
        trackDurationMs && trackDurationMs > 0
          ? trackDurationMs + Math.min(pollMs * 2, 120_000)
          : Math.min(Math.max(fallbackMs, pollMs * 3), maxGuardMs);

      const effectiveGuardMs = Math.min(catalogGuard, maxGuardMs);
      const anchor = latestLog.observedAt.getTime();
      if (Date.now() < anchor + effectiveGuardMs) {
        await prisma.currentNowPlaying.upsert({
          where: { stationId },
          update: { updatedAt: new Date() },
          create: { stationId, title: titleFinal, artist: artistFinal },
        });
        return { status, reasonCode: detectionReason ?? null, detectionLogId: latestLog.id };
      }
    }

    const log = await prisma.detectionLog.create({
      data: {
        stationId,
        detectionMethod: method,
        rawStreamText: metadata?.combinedRaw,
        parsedArtist: metadata?.rawArtist,
        parsedTitle: metadata?.rawTitle,
        confidence: match?.confidence,
        acoustidScore: match?.score,
        acoustidId: match?.acoustidTrackId ?? null,
        recordingMbid: match?.recordingId,
        titleFinal,
        artistFinal,
        releaseFinal: match?.releaseTitle,
        releaseDate: match?.releaseDate,
        genreFinal: match?.genre,
        sourceProvider:
          match?.sourceProvider ||
          (method === "stream_metadata"
            ? metadataProgramLike
              ? "stream_metadata_program"
              : "stream_metadata"
            : method),
        isrcList: match?.isrcs ? JSON.stringify(match.isrcs) : null,
        trackDurationMs: trackDurationMs ?? null,
        sampleSeconds: station.sampleSeconds,
        processingMs,
        status,
        reasonCode: detectionReason,
        matchDiagnosticsJson,
      },
      select: { id: true, observedAt: true },
    });

    let spinPlayCount = 0;
    if (status === "matched") {
      const spin = await upsertSongSpinOnNewPlay(prisma, {
        stationId,
        artist: artistFinal,
        title: titleFinal,
        album: match?.releaseTitle,
        detectionLogId: log.id,
        mixRuleApplied: (metadata as { splitRuleApplied?: string | undefined } | null)?.splitRuleApplied || null,
        mixSplitConfidence: (metadata as { splitConfidence?: number | undefined } | null)?.splitConfidence,
        originalCombinedRaw: (metadata?.combinedRaw ?? "").trim() || null,
      });
      spinPlayCount = spin.playCount;
    }

    if (
      status === "matched" &&
      station.archiveSongSamples &&
      spinPlayCount === 1 &&
      resolvedUrl.startsWith("http")
    ) {
      const sec = Math.min(Math.max(parseInt(process.env.ARCHIVE_SAMPLE_SECONDS || "30", 10) || 30, 10), 120);
      const archiveRoot = process.env.SONG_SAMPLE_ARCHIVE_DIR || path.join(process.cwd(), "data/song_samples");
      const dir = path.join(archiveRoot, stationId);
      try {
        fs.mkdirSync(dir, { recursive: true });
        const tmpPath = await SamplerService.captureSample(resolvedUrl, sec);
        if (tmpPath) {
          const destPath = path.join(dir, `${log.id}.wav`);
          fs.copyFileSync(tmpPath, destPath);
          SamplerService.cleanup(tmpPath);
          let chromaprint: string | null = null;
          const fp = await FingerprintService.generateFingerprint(destPath);
          if (fp?.fingerprint) chromaprint = fp.fingerprint;
          await prisma.songSampleArchive.create({
            data: {
              detectionLogId: log.id,
              stationId,
              filePath: destPath,
              durationSec: sec,
              chromaprint,
            },
          });
        }
      } catch (e) {
        logger.warn({ e, stationId }, "Song sample archive failed (non-fatal)");
      }
    }

    await prisma.currentNowPlaying.upsert({
      where: { stationId },
      update: {
        title: titleFinal,
        artist: artistFinal,
        album: match?.releaseTitle,
        genre: match?.genre,
        sourceProvider:
          match?.sourceProvider ||
          (method === "stream_metadata"
            ? metadataProgramLike
              ? "stream_metadata_program"
              : "stream_metadata"
            : method),
        streamText: metadata?.combinedRaw,
        updatedAt: new Date(),
      },
      create: {
        stationId,
        title: titleFinal,
        artist: artistFinal,
        album: match?.releaseTitle,
        genre: match?.genre,
        sourceProvider:
          match?.sourceProvider ||
          (method === "stream_metadata"
            ? metadataProgramLike
              ? "stream_metadata_program"
              : "stream_metadata"
            : method),
        streamText: metadata?.combinedRaw,
      },
    });

    await prisma.jobRun.create({
      data: {
        stationId,
        status: "success",
        durationMs: processingMs,
      },
    });
    if (status === "matched") {
      monitorEvents.emitSongDetected({
        stationId,
        detectionLogId: log.id,
        observedAt: log.observedAt.toISOString(),
        title: titleFinal ?? null,
        artist: artistFinal ?? null,
        playCount: spinPlayCount,
      });
    }
    return { status, reasonCode: detectionReason ?? null, detectionLogId: log.id };
  }

  private static async archiveUnresolvedSample(stationId: string, samplePath: string): Promise<string | null> {
    if (!parseEnvBool("ARCHIVE_UNRESOLVED_SAMPLES", true)) return null;
    try {
      const root =
        process.env.UNRESOLVED_SAMPLE_DIR || path.join(process.cwd(), "data/unresolved_samples");
      const dir = path.join(root, stationId);
      fs.mkdirSync(dir, { recursive: true });

      const fileName = `${Date.now()}_${path.basename(samplePath)}`;
      const destPath = path.join(dir, fileName);
      fs.copyFileSync(samplePath, destPath);
      await prisma.unresolvedSample.create({
        data: {
          stationId,
          filePath: destPath,
        },
      });

      const keepRaw = parseEnvInt("UNRESOLVED_SAMPLE_MAX_PER_STATION", 25);
      // Production retention mode:
      // set UNRESOLVED_SAMPLE_MAX_PER_STATION=0 to disable pruning entirely.
      if (keepRaw <= 0) return destPath;
      const keep = Math.min(200, Math.max(1, keepRaw));
      const stale = await prisma.unresolvedSample.findMany({
        where: { stationId },
        orderBy: { createdAt: "desc" },
        skip: keep,
        select: { id: true, filePath: true },
      });
      for (const row of stale) {
        try {
          if (row.filePath && fs.existsSync(row.filePath)) fs.unlinkSync(row.filePath);
        } catch {
          // Best effort disk cleanup; row delete still proceeds.
        }
        await prisma.unresolvedSample.delete({ where: { id: row.id } });
      }
      return destPath;
    } catch (error) {
      logger.warn({ error, stationId }, "Failed to archive unresolved sample");
      return null;
    }
  }
}
