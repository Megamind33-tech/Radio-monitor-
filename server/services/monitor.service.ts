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
import { AuddService } from "./audd.service.js";
import { AcrcloudService } from "./acrcloud.service.js";
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
import { classifyStreamUrl } from "../lib/stream-source.js";
import { classifyMusicContent } from "../lib/music-content-filter.js";

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
  const transportOk = health.reachable && health.audioFlowing;
  return {
    lastHealthyAt: transportOk ? new Date() : station.lastHealthyAt,
    /** Bytes flowing (decode/ffprobe issues do not clear this). */
    lastGoodAudioAt: transportOk ? new Date() : station.lastGoodAudioAt,
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
  if (/^[_\s-]{6,}$/.test(raw)) return true;
  if (/^[-=_*.!·•\s]{6,}$/.test(raw)) return true;
  if (/^(online|live|replay|feel the power)$/i.test(raw)) return true;
  if (/^[\s'"`.,:;|/\\()[\]{}<>~+=_*#-]+$/.test(raw)) return true;
  const compact = raw.replace(/\s+/g, "");
  if (compact.length >= 6) {
    const lettersOrDigits = (compact.match(/[A-Za-z0-9]/g) ?? []).length;
    const nonLatin = (compact.match(/[^\x00-\x7F]/g) ?? []).length;
    const bracketNoise = (compact.match(/[⫷⫸⫹⫺ꢂꢃꢄꢊ]/g) ?? []).length;
    if (lettersOrDigits / compact.length < 0.25) return true;
    if (bracketNoise >= 2 || nonLatin / compact.length > 0.45) return true;
  }
  if (/(['"`]\s*){5,}/.test(raw)) return true;
  return false;
}

function isProgramLikeTitle(text: string | null | undefined): boolean {
  return !classifyMusicContent(text).isMusic;
}

function effectiveStreamUrl(station: Pick<Station, "streamUrl" | "preferredStreamUrl">): string {
  const p = (station.preferredStreamUrl ?? "").trim();
  return p || station.streamUrl;
}

function emaUpdate(prev: number, hit: boolean, alpha = 0.12): number {
  const x = hit ? 1 : 0;
  return prev * (1 - alpha) + x * alpha;
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
   * Audio fingerprint: on ICY change, on interval (audioFingerprintIntervalSeconds), periodic ICY verification
   * when AcoustID is configured (icyVerificationIntervalSeconds), or when metadata is missing/untrusted/stale.
   */
  static async pollStation(stationId: string): Promise<void> {
    const start = Date.now();
    const station = await prisma.station.findUnique({ where: { id: stationId } });
    if (!station || !station.isActive) return;

    logger.info({ station: station.name }, "Polling station");
        let latestNowPlaying = null;
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
      const mountUrl = effectiveStreamUrl(station);
      const resolvedUrl = await ResolverService.resolveStreamUrl(mountUrl);
      const health = await StreamHealthService.validateStream(resolvedUrl);

      let metadata: NormalizedMetadata | null = null;
      let legacyFingerprint = false;
      let reasonCode: string | null = null;

      if (station.metadataPriorityEnabled) {
        metadata = await MetadataService.readStreamMetadata(resolvedUrl);
        if (!metadata) {
          metadata = await MetadataService.readProviderNowPlayingMetadata(resolvedUrl);
        }
        latestNowPlaying = await prisma.currentNowPlaying.findUnique({ where: { stationId } });

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

      const intervalSec = Math.max(30, station.audioFingerprintIntervalSeconds || 120);
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

      /** When true, also fingerprint when ICY exists but quality heuristics flag it. Default off for ICY stability (set true for bad streams). */
      const forceFingerprintAggressive =
        metaQuality.forceFingerprint ||
        parseEnvBool("FINGERPRINT_AGGRESSIVE_ON_SUSPICIOUS_METADATA", false);

      const combinedLine = (metadata?.combinedRaw ?? "").trim();
      const titleLine = (metadata?.rawTitle ?? combinedLine).trim();
      const contentClass = classifyMusicContent(combinedLine || titleLine);
      const programLikeIcy =
        isProgramLikeTitle(metadata?.rawTitle) ||
        (!metadata?.rawTitle && isProgramLikeTitle(metadata?.combinedRaw)) ||
        !contentClass.isMusic;
      const suspiciousIcyForPaidLane =
        programLikeIcy ||
        legacyFingerprint ||
        !metaQuality.okForCatalog ||
        metaQuality.forceFingerprint;
      const paidFallbacksEnabled = parseEnvBool("PAID_AUDIO_FALLBACKS_ENABLED", true);
      /** After AcoustID miss, try AudD/ACR even when ICY looked "clean" (default true when keys exist). */
      const paidOnAudioMiss =
        paidFallbacksEnabled &&
        parseEnvBool("PAID_ON_AUDIO_MISS", true) &&
        (AuddService.isEnabled() || AcrcloudService.isEnabled());
      const paidLaneEligible =
        paidFallbacksEnabled &&
        (suspiciousIcyForPaidLane || paidOnAudioMiss) &&
        (AuddService.isEnabled() || AcrcloudService.isEnabled());

      /** Trusted ICY but unchanged: still re-fingerprint on this cadence to catch wrong/stuck ICY. */
      const icyVerificationIntervalSec = Math.max(
        60,
        typeof station.icyVerificationIntervalSeconds === "number" && station.icyVerificationIntervalSeconds > 0
          ? station.icyVerificationIntervalSeconds
          : 120
      );
      const lastIcyVerify = station.lastIcyVerificationFingerprintAt;
      const icyVerificationDue =
        !lastIcyVerify || Date.now() - lastIcyVerify.getTime() >= icyVerificationIntervalSec * 1000;
      const icyCrossCheckAudio =
        !!acoustidKey &&
        !!metadata &&
        !isJunkIcyMetadata(metadata) &&
        metaTrust > 0 &&
        !legacyFingerprint &&
        reasonCode !== "metadata_disabled" &&
        icyVerificationDue;

      const doAudioId =
        !!station.fingerprintFallbackEnabled &&
        (forceAudioFallback || !!acoustidKey) &&
        (legacyFingerprint ||
          icyChanged ||
          intervalElapsed ||
          fingerprintEveryPoll ||
          forceFingerprintAggressive ||
          icyCrossCheckAudio);

      let audioMatch: MatchResult | null = null;
      let sampledForFingerprint = false;
      /** Last captured temp file in the fingerprint loop (cleaned up after archive or at end). */
      let fingerprintSamplePathPendingCleanup: string | null = null;

      let capturedFingerprint: Awaited<ReturnType<typeof FingerprintService.generateFingerprint>> = null;
      let audioMatchSource: "local" | "acoustid" | "audd" | "acrcloud" | null = null;
      const fingerprintAttempts: Array<{
        attempt: number;
        delaySec: number;
        sampleSec: number;
        outcome:
          | "match_local"
          | "match_acoustid"
          | "match_audd"
          | "match_acrcloud"
          | "no_match"
          | "no_sample"
          | "no_fingerprint";
      }> = [];

      if (doAudioId && resolvedUrl.startsWith("http")) {
        // AcoustID uses up to the first 120 s of audio for fingerprint generation.
        // Default to 120 s so every capture gives the algorithm its full working window.
        // Set FINGERPRINT_SAMPLE_SECONDS=30 (or lower station.sampleSeconds) to reduce
        // capture time on constrained hosts — accuracy may decrease for short clips.
        const baseSec = Math.min(
          120,
          Math.max(
            station.sampleSeconds,
            parseInt(process.env.FINGERPRINT_SAMPLE_SECONDS || "120", 10) || 120
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
          if (fingerprintSamplePathPendingCleanup) {
            SamplerService.cleanup(fingerprintSamplePathPendingCleanup);
            fingerprintSamplePathPendingCleanup = null;
          }
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
              icyCrossCheckAudio,
              fpSec,
              delaySec,
            },
            "Audio fingerprint sample (ICY change, interval, verification, suspicious metadata, or retry)"
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
          fingerprintSamplePathPendingCleanup = samplePath;
          const fp = await FingerprintService.generateFingerprint(samplePath);
          if (!fp) {
            fingerprintAttempts.push({
              attempt: attempt + 1,
              delaySec,
              sampleSec: fpSec,
              outcome: "no_fingerprint",
            });
            fingerprintSamplePathPendingCleanup = null;
            continue;
          }
          capturedFingerprint = fp;
          const localMatch = await LocalFingerprintService.lookup(fp);
          const localMinConf = parseEnvFloat("LOCAL_FP_MIN_CONFIDENCE_FOR_SKIP_ACOUSTID", 0.88);
          const localStrong =
            localMatch &&
            (localMatch.confidence ?? 0) >= localMinConf &&
            (localMatch.score ?? 0) >= localMinConf;
          if (localStrong) {
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
          if (!audioMatch && paidLaneEligible && samplePath) {
            const auddMatch = await AuddService.lookupSample(samplePath);
            if (auddMatch) {
              audioMatch = auddMatch;
              audioMatchSource = "audd";
              fingerprintAttempts.push({
                attempt: attempt + 1,
                delaySec,
                sampleSec: fpSec,
                outcome: "match_audd",
              });
              break;
            }
            const acrMatch = await AcrcloudService.identifyAudioFile(samplePath);
            if (acrMatch) {
              let enriched = acrMatch;
              if (acrMatch.recordingId) {
                enriched = await MusicbrainzService.enrich(acrMatch);
              }
              audioMatch = enriched;
              audioMatchSource = "acrcloud";
              fingerprintAttempts.push({
                attempt: attempt + 1,
                delaySec,
                sampleSec: fpSec,
                outcome: "match_acrcloud",
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
        if (fingerprintSamplePathPendingCleanup && audioMatch) {
          SamplerService.cleanup(fingerprintSamplePathPendingCleanup);
          fingerprintSamplePathPendingCleanup = null;
        }
        if (!acoustidKey && doAudioId) {
          logger.debug({ station: station.name }, "Fingerprint path ran without ACOUSTID_API_KEY after local miss");
        }
      }

      let catalogMatch: MatchResult | null = null;
      let catalogRejectedLowConfidence = false;
      /** Always try catalog when ICY is non-empty — okForCatalog only scales merge trust, it must not block lookup (was starving real songs). */
      if (metadata && !isJunkIcyMetadata(metadata)) {
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

      // ICY accuracy verification: when ICY has been stuck (metadata_stale) and the audio
      // fingerprint resolves a DIFFERENT song, log the contradiction so operators can investigate
      // the stream.  The fingerprint result is already preferred by mergeAcoustidAndCatalog;
      // this log line makes the discrepancy visible in the detection trail.
      if (
        audioMatch?.title &&
        metadata?.combinedRaw &&
        (reasonCode === "metadata_stale" || reasonCode === "metadata_repeated_same_text")
      ) {
        const icyLower = metadata.combinedRaw.toLowerCase();
        const fpTitle = (audioMatch.title ?? "").toLowerCase();
        const icyContainsFp = icyLower.includes(fpTitle) || fpTitle.includes(icyLower.slice(0, 20));
        if (!icyContainsFp && fpTitle.length > 3) {
          logger.warn(
            {
              stationId,
              icyText: metadata.combinedRaw,
              fingerprintTitle: audioMatch.title,
              fingerprintArtist: audioMatch.artist,
              fingerprintSource: audioMatch.sourceProvider,
              staleness: reasonCode,
            },
            "ICY title contradicts audio fingerprint — stream may have stuck ICY; fingerprint result used"
          );
        }
      }

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

      // Second-pass: combined-line catalog whenever still unmatched (even if quality heuristics flagged forceFingerprint).
      if (!match && metadata && station.fingerprintFallbackEnabled && !isJunkIcyMetadata(metadata)) {
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
            parseInt(process.env.FINGERPRINT_SAMPLE_SECONDS || "120", 10) || 120
          )
        );
        const reusePath = fingerprintSamplePathPendingCleanup;
        const archTmp =
          reusePath ||
          (await SamplerService.captureSample(resolvedUrl, archSec));
        if (archTmp) {
          unresolvedSamplePath = await this.archiveUnresolvedSample(stationId, archTmp);
          SamplerService.cleanup(archTmp);
        }
        fingerprintSamplePathPendingCleanup = null;
      } else if (fingerprintSamplePathPendingCleanup) {
        SamplerService.cleanup(fingerprintSamplePathPendingCleanup);
        fingerprintSamplePathPendingCleanup = null;
      }

      if (sampledForFingerprint) {
        await prisma.station.update({
          where: { id: stationId },
          data: {
            lastAudioFingerprintAt: new Date(),
            ...(icyCrossCheckAudio ? { lastIcyVerificationFingerprintAt: new Date() } : {}),
          },
        });
      }

      // Self-learning: persist the fingerprint whenever we have a confirmed match
      // so future plays of the same track are resolved locally (no AcoustID / MB call).
      if (capturedFingerprint && match && audioMatchSource !== "local") {
        const learnSource: "acoustid" | "stream_metadata" | "manual" =
          audioMatchSource === "acoustid"
            ? "acoustid"
            : audioMatchSource === "audd"
              ? "manual"
              : "stream_metadata";
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
        icyCrossCheckAudio,
        icyVerificationDue,
        suspiciousIcyForPaidLane,
        paidOnAudioMiss,
        paidLaneEligible,
        legacyFingerprint,
        fingerprintEveryPoll,
        forceFingerprintAggressive,
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
        JSON.stringify(matchDiagnostics),
        {
          capturedFingerprint:
            capturedFingerprint && audioMatchSource !== "local" ? capturedFingerprint : null,
          icyChanged,
        }
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
      const programLike =
        isProgramLikeTitle(metadata?.rawTitle) ||
        (!metadata?.rawTitle && isProgramLikeTitle(metadata?.combinedRaw));
      const contentClassification: StationContentClassification =
        detection.status === "matched"
          ? "music"
          : programLike
            ? "talk"
            : classifyContent(metadata?.combinedRaw ?? null);

      const nextFailureThreshold = Math.max(1, station.failureThreshold || 3);
      const transportOk = health.reachable && health.audioFlowing;
      const nextFailureCount = transportOk
        ? 0
        : Math.max(0, (station.consecutivePollFailures || 0) + 1);
      const nextHealthyCount = transportOk
        ? Math.max(0, (station.consecutiveHealthyPolls || 0) + 1)
        : 0;

      const srcClass = classifyStreamUrl(mountUrl, station.name);
      const decodeHit = health.decoderOk;
      const fpAttempted = !!doAudioId && resolvedUrl.startsWith("http");
      const fpHit = fpAttempted && !!audioMatch;
      const metaAvail = !!(metadata?.combinedRaw && !isJunkIcyMetadata(metadata));
      const metaFresh = metaAvail && (icyChanged || (!prevIcy && icyText.length > 0));

      const decodeHealthEma = emaUpdate(
        typeof station.decodeHealthEma === "number" ? station.decodeHealthEma : 1,
        decodeHit
      );
      const fingerprintHitEma = fpAttempted
        ? emaUpdate(
            typeof station.fingerprintHitEma === "number" ? station.fingerprintHitEma : 0,
            fpHit
          )
        : typeof station.fingerprintHitEma === "number"
          ? station.fingerprintHitEma
          : 0;
      const metadataPresentEma = emaUpdate(
        typeof station.metadataPresentEma === "number" ? station.metadataPresentEma : 0,
        metaAvail
      );
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

      let preferredOut = (station.preferredStreamUrl ?? "").trim() || null;
      if (
        parseEnvBool("AUTO_PROMOTE_DIRECT_STREAM_URL", true) &&
        !preferredOut &&
        (srcClass.tier === "aggregator" || srcClass.tier === "relay")
      ) {
        const better = await prisma.stationStreamEndpoint.findFirst({
          where: {
            stationId,
            isSuppressed: false,
            streamUrl: { not: station.streamUrl },
            qualityScore: { gte: srcClass.qualityScore + 12 },
          },
          orderBy: [{ qualityScore: "desc" }, { fingerprintHits: "desc" }],
        });
        if (better?.streamUrl) {
          preferredOut = better.streamUrl;
          logger.info(
            { stationId, from: mountUrl, to: preferredOut, tier: better.sourceTier },
            "Auto-promoted preferredStreamUrl to higher-tier endpoint"
          );
        }
      }

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
          streamSourceType: srcClass.tier,
          streamSourceQualityScore: srcClass.qualityScore,
          streamSourceLastEvaluatedAt: new Date(),
          decodeHealthEma,
          fingerprintHitEma,
          metadataPresentEma,
          streamOnlineLast: transportOk ? 1 : 0,
          audioDetectedLast: health.audioFlowing ? 1 : 0,
          metadataAvailableLast: metaAvail ? 1 : 0,
          songIdentifiedLast: detection.status === "matched" ? 1 : 0,
          ...(preferredOut ? { preferredStreamUrl: preferredOut } : {}),
          ...preservedDates,
          deepValidationIntervalSeconds: station.deepValidationIntervalSeconds || 600,
          failureThreshold: nextFailureThreshold,
          recoveryThreshold: Math.max(1, station.recoveryThreshold || 2),
        },
      });
      await this.upsertStreamEndpoint(
        { id: station.id, streamUrl: mountUrl, sourceIdsJson: station.sourceIdsJson },
        health,
        monitor.state,
        srcClass,
        {
          fpAttempted,
          fpHit,
          decodeHit,
          metaFresh,
        }
      );
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
    monitorState: StationMonitorState,
    srcClass: ReturnType<typeof classifyStreamUrl>,
    metrics: { fpAttempted: boolean; fpHit: boolean; decodeHit: boolean; metaFresh: boolean }
  ) {
    try {
      const sourceMap = parseSourceIdsMap(station.sourceIdsJson);
      const sourceEntries = Object.entries(sourceMap);
      const source = sourceEntries[0]?.[0] || "manual";
      const sourceDetail = sourceEntries[0]?.[1] || null;
      const transportOk = health.reachable && health.audioFlowing;
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
        select: {
          id: true,
          consecutiveFailures: true,
          fingerprintHits: true,
          fingerprintAttempts: true,
          decodeHits: true,
          decodeAttempts: true,
          metadataFreshHits: true,
          metadataPolls: true,
        },
      });
      const nextFailures = transportOk ? 0 : Math.max(0, (row?.consecutiveFailures || 0) + 1);

      const fpHits = (row?.fingerprintHits ?? 0) + (metrics.fpHit ? 1 : 0);
      const fpAtt = (row?.fingerprintAttempts ?? 0) + (metrics.fpAttempted ? 1 : 0);
      const decHits = (row?.decodeHits ?? 0) + (metrics.decodeHit ? 1 : 0);
      const decAtt = (row?.decodeAttempts ?? 0) + 1;
      const metaHits = (row?.metadataFreshHits ?? 0) + (metrics.metaFresh ? 1 : 0);
      const metaPolls = (row?.metadataPolls ?? 0) + 1;

      const endpointPayload = {
        source,
        sourceDetail,
        resolvedUrl: health.resolvedUrl || null,
        isCurrent: true,
        isSuppressed: monitorState === "INACTIVE",
        lastValidatedAt: new Date(),
        lastValidationStatus: status,
        lastFailureReason: health.reason || null,
        lastHealthyAt: transportOk ? new Date() : undefined,
        consecutiveFailures: nextFailures,
        codec: health.codec || null,
        bitrate: health.bitrate ?? null,
        sourceTier: srcClass.tier,
        qualityScore: srcClass.qualityScore,
        fingerprintHits: fpHits,
        fingerprintAttempts: fpAtt,
        decodeHits: decHits,
        decodeAttempts: decAtt,
        metadataFreshHits: metaHits,
        metadataPolls: metaPolls,
      };

      if (row) {
        await prisma.stationStreamEndpoint.update({
          where: { id: row.id },
          data: endpointPayload,
        });
      } else {
        await prisma.stationStreamEndpoint.create({
          data: {
            stationId: station.id,
            streamUrl: station.streamUrl,
            ...endpointPayload,
            lastHealthyAt: transportOk ? new Date() : null,
          },
        });
      }

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
    matchDiagnosticsJson: string | null = null,
    opts?: { capturedFingerprint: import("../types.js").FingerprintResult | null; icyChanged?: boolean }
  ): Promise<{
    status: "matched" | "unresolved";
    reasonCode: string | null;
    detectionLogId?: string;
    learnedLibraryThisPoll: boolean;
  }> {
    const stationId = station.id;
    const npRow = await prisma.currentNowPlaying.findUnique({ where: { stationId } });
    const trustedMeta =
      metadata &&
      station.metadataPriorityEnabled &&
      MetadataService.isMetadataTrustworthy(metadata, npRow?.streamText || undefined).trusted;
    const metadataProgramLike =
      isProgramLikeTitle(metadata?.rawTitle) ||
      (!metadata?.rawTitle && isProgramLikeTitle(metadata?.combinedRaw));

    /**
     * Default CHANGED to false: trusted ICY by itself is NOT enough to count as matched.
     * A real song match requires either:
     *   (a) a catalog/fingerprint result (`match` populated), OR
     *   (b) ICY metadata that clearly contains BOTH a parsed artist AND title
     *       (the upstream parser only fills `rawArtist`/`rawTitle` when an artist–title
     *       separator was found — branding strings like "Hot FM 87.7" never qualify),
     *       AND ALLOW_STREAM_METADATA_MATCH_WITHOUT_ID is explicitly enabled.
     *
     * Old behavior: any "trusted" combined ICY text (e.g. "ZNBC Radio 1") was logged as
     * matched, inflating the apparent match rate while polluting StationSongSpin with
     * non-songs. That was the "unknown treated as a real match" bug — fixed here.
     */
    const allowStreamMetadataOnlyMatch = parseEnvBool("ALLOW_STREAM_METADATA_MATCH_WITHOUT_ID", false);
    const rawTitleEarly = (metadata?.rawTitle ?? "").trim();
    const rawArtistEarly = (metadata?.rawArtist ?? "").trim();
    const hasArtistTitlePair =
      rawTitleEarly.length >= 2 &&
      rawArtistEarly.length >= 2 &&
      rawTitleEarly.toLowerCase() !== rawArtistEarly.toLowerCase();
    const stationNameLower = (station.name ?? "").toLowerCase().replace(/\s+/g, " ").trim();
    const looksLikeStationBranding =
      !!stationNameLower &&
      (rawTitleEarly.toLowerCase().includes(stationNameLower) ||
        (metadata?.combinedRaw ?? "").toLowerCase().includes(stationNameLower)) &&
      !hasArtistTitlePair;
    const isMatched =
      !!match ||
      (allowStreamMetadataOnlyMatch &&
        method === "stream_metadata" &&
        !!metadata &&
        trustedMeta &&
        !isJunkIcyMetadata(metadata) &&
        !metadataProgramLike &&
        hasArtistTitlePair &&
        !looksLikeStationBranding);
    const status = isMatched ? "matched" : "unresolved";
    const detectionReason =
      reasonCode ||
      (status === "unresolved"
        ? metadataProgramLike
          ? "talk_or_program_content"
          : looksLikeStationBranding
            ? "station_branding_only_not_a_song"
            : !hasArtistTitlePair && method === "stream_metadata"
              ? "icy_no_artist_title_pair"
              : "no_reliable_match"
        : null);

    const rawTitle = rawTitleEarly;
    const rawArtist = rawArtistEarly;
    /**
     * Only carry ICY title/artist into the persisted log when the row is actually a match
     * (real catalog/fingerprint hit, or a clear artist–title ICY pair when the operator opted in).
     * For unresolved rows we keep the raw text in `rawStreamText` only so reports never claim
     * a station ID like "Hot FM 87.7" was a played song.
     */
    const titleFinal = isMatched
      ? match?.title || (!metadataProgramLike && rawTitle ? rawTitle : undefined)
      : match?.title || undefined;
    const artistFinal = isMatched
      ? match?.artist || (rawArtist ? rawArtist : undefined)
      : match?.artist || undefined;

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
      /** ICY-only "match" can hide wrong stuck text; allow new logs sooner than full MB duration. */
      const streamMetaOnlyCap = parseEnvMs("TRACK_GUARD_STREAM_METADATA_MAX_MS", 6 * 60 * 1000);

      const catalogGuard =
        trackDurationMs && trackDurationMs > 0
          ? trackDurationMs + Math.min(pollMs * 2, 120_000)
          : Math.min(Math.max(fallbackMs, pollMs * 3), maxGuardMs);

      const cappedBySource =
        method === "stream_metadata" && !match
          ? Math.min(catalogGuard, streamMetaOnlyCap)
          : catalogGuard;
      const effectiveGuardMs = Math.min(cappedBySource, maxGuardMs);

      // Without an explicit ICY change signal the same re-detection is just the
      // periodic fingerprint loop re-matching the still-playing song.  Use the
      // full maxGuardMs so that identical consecutive detections driven by the
      // fingerprint interval (no ICY change, stuck ICY, or no-ICY station) do
      // not produce a new DetectionLog every few minutes and appear as a loop.
      const hasIcyChange = opts?.icyChanged === true;
      const loopSafeGuardMs = hasIcyChange ? effectiveGuardMs : maxGuardMs;

      const anchor = latestLog.observedAt.getTime();
      if (Date.now() < anchor + loopSafeGuardMs) {
        const icyTitle = rawTitle || (metadata?.combinedRaw ?? "").trim() || null;
        const icyArtist = rawArtist || null;
        const npTitle = (titleFinal && String(titleFinal).trim()) || icyTitle || null;
        const npArtist = (artistFinal && String(artistFinal).trim()) || icyArtist || null;
        await prisma.currentNowPlaying.upsert({
          where: { stationId },
          update: {
            title: npTitle,
            artist: npArtist,
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
            title: npTitle ?? undefined,
            artist: npArtist ?? undefined,
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
        MonitorService.emitStationPollRealtime(stationId, {
          detectionStatus: status,
          detectionLogId: latestLog.id,
          newDetectionLog: false,
          titleFinal,
          artistFinal,
          metadata,
          match,
        });
        return {
          status,
          reasonCode: detectionReason ?? null,
          detectionLogId: latestLog.id,
          learnedLibraryThisPoll: false,
        };
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
      await LocalFingerprintService.bumpPlayAggregates({
        recordingMbid: match?.recordingId ?? null,
        artist: artistFinal ?? null,
        title: titleFinal ?? null,
      });
    }

    let learnedLibraryThisPoll = false;
    const matchForLibrary = ((): MatchResult | null => {
      if (!match) return null;
      const m = { ...match };
      if (titleFinal) m.title = titleFinal;
      if (artistFinal) m.artist = artistFinal;
      if (!m.durationMs && trackDurationMs) m.durationMs = trackDurationMs;
      return m;
    })();

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

          // Persist to the self-learned fingerprint library so future plays of this
          // song are resolved locally — no AcoustID or catalog call needed.
          if (fp && matchForLibrary) {
            await LocalFingerprintService.learn({
              fp,
              match: matchForLibrary,
              metadata: metadata ?? null,
              source: match?.sourceProvider?.includes("acoustid") ? "acoustid" : "stream_metadata",
            });
            learnedLibraryThisPoll = true;
          }
        }
      } catch (e) {
        logger.warn({ e, stationId }, "Song sample archive failed (non-fatal)");
      }
    }

    if (
      status === "matched" &&
      !learnedLibraryThisPoll &&
      opts?.capturedFingerprint &&
      matchForLibrary
    ) {
      const learnSource: "acoustid" | "stream_metadata" | "manual" =
        match?.sourceProvider === "audd" || match?.sourceProvider === "acrcloud"
          ? "manual"
          : match?.sourceProvider?.includes("acoustid")
            ? "acoustid"
            : "stream_metadata";
      await LocalFingerprintService.learn({
        fp: opts.capturedFingerprint,
        match: matchForLibrary,
        metadata: metadata ?? null,
        source: learnSource,
      });
      learnedLibraryThisPoll = true;
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
    MonitorService.emitStationPollRealtime(stationId, {
      detectionStatus: status,
      detectionLogId: log.id,
      newDetectionLog: true,
      titleFinal,
      artistFinal,
      metadata,
      match,
    });
    return {
      status,
      reasonCode: detectionReason ?? null,
      detectionLogId: log.id,
      learnedLibraryThisPoll,
    };
  }

  private static emitStationPollRealtime(
    stationId: string,
    opts: {
      detectionStatus: "matched" | "unresolved";
      detectionLogId: string | null;
      newDetectionLog: boolean;
      titleFinal?: string | null | undefined;
      artistFinal?: string | null | undefined;
      metadata: NormalizedMetadata | null;
      match: MatchResult | null;
    }
  ): void {
    const rawT = (opts.metadata?.rawTitle ?? "").trim();
    const rawA = (opts.metadata?.rawArtist ?? "").trim();
    const displayTitle =
      (opts.titleFinal && String(opts.titleFinal).trim()) ||
      rawT ||
      (opts.metadata?.combinedRaw ?? "").trim() ||
      null;
    const displayArtist =
      (opts.artistFinal && String(opts.artistFinal).trim()) || rawA || null;
    monitorEvents.emitStationPoll({
      stationId,
      ts: new Date().toISOString(),
      detectionStatus: opts.detectionStatus,
      detectionLogId: opts.detectionLogId,
      displayTitle,
      displayArtist,
      streamText: opts.metadata?.combinedRaw ?? null,
      newDetectionLog: opts.newDetectionLog,
    });
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
