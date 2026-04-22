import * as fs from "fs";
import * as path from "path";
import type { Station } from "@prisma/client";
import { prisma } from "../lib/prisma.js";
import { logger } from "../lib/logger.js";
import { mergeAcoustidAndCatalog } from "../lib/audio-id-merge.js";
import { ResolverService } from "./resolver.service.js";
import { MetadataService } from "./metadata.service.js";
import { SamplerService } from "./sampler.service.js";
import { FingerprintService } from "./fingerprint.service.js";
import { AcoustidService } from "./acoustid.service.js";
import { MusicbrainzService } from "./musicbrainz.service.js";
import { CatalogLookupService } from "./catalog-lookup.service.js";
import { NormalizedMetadata, MatchResult, DetectionMethod } from "../types.js";
import { upsertSongSpinOnNewPlay } from "../lib/song-spin.js";
import { StreamRefreshService } from "./stream-refresh.service.js";

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
  return /\b(on air|live|program|show|morning show|afternoon show|evening show|news|sports|talk)\b/.test(t);
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
        },
      });
    };

    try {
      const resolvedUrl = await ResolverService.resolveStreamUrl(station.streamUrl);

      let metadata: NormalizedMetadata | null = null;
      let legacyFingerprint = false;
      let reasonCode: string | null = null;

      if (station.metadataPriorityEnabled) {
        metadata = await MetadataService.readStreamMetadata(resolvedUrl);
        const latestNowPlaying = await prisma.currentNowPlaying.findUnique({ where: { stationId } });

        if (!metadata) {
          legacyFingerprint = true;
          reasonCode = "metadata_missing";
        } else {
          const check = MetadataService.isMetadataTrustworthy(metadata, latestNowPlaying?.streamText || undefined);
          if (!check.trusted) {
            legacyFingerprint = true;
            reasonCode = check.reason || "metadata_untrusted";
          } else if (latestNowPlaying && latestNowPlaying.streamText === metadata.combinedRaw) {
            const staleAt = new Date(
              latestNowPlaying.updatedAt.getTime() + station.metadataStaleSeconds * 1000
            );
            if (Date.now() > staleAt.getTime()) {
              legacyFingerprint = true;
              reasonCode = "metadata_stale";
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
      const doAudioId =
        !!station.fingerprintFallbackEnabled &&
        (forceAudioFallback || !!acoustidKey) &&
        (legacyFingerprint || icyChanged || intervalElapsed);

      let audioMatch: MatchResult | null = null;
      let sampledForFingerprint = false;
      let sampledAudioPath: string | null = null;

      if (doAudioId && resolvedUrl.startsWith("http")) {
        const fpSec = Math.min(
          120,
          Math.max(
            station.sampleSeconds,
            parseInt(process.env.FINGERPRINT_SAMPLE_SECONDS || "25", 10) || 25
          )
        );
        logger.info(
          { station: station.name, reason: reasonCode, icyChanged, intervalElapsed, fpSec },
          "Audio fingerprint sample (ICY change, interval, or metadata fallback)"
        );
        const samplePath = await SamplerService.captureSample(resolvedUrl, fpSec);
        sampledForFingerprint = true;
        if (samplePath) {
          sampledAudioPath = samplePath;
          const fingerprint = await FingerprintService.generateFingerprint(samplePath);
          if (fingerprint) {
            const acoustidMatch = await AcoustidService.lookup(fingerprint);
            if (acoustidMatch) {
              audioMatch = await MusicbrainzService.enrich(acoustidMatch);
            }
          }
        }
      }

      let catalogMatch: MatchResult | null = null;
      if (metadata && !isJunkIcyMetadata(metadata)) {
        catalogMatch = await CatalogLookupService.lookupFromMetadata(metadata);
      }

      const minAcoust = parseEnvFloat("ACOUSTID_PREFER_MIN_SCORE", 0.55);
      const merged = mergeAcoustidAndCatalog(audioMatch, catalogMatch, minAcoust);

      let match = merged.match;
      let method: DetectionMethod = merged.method;
      const mergeReason = merged.reasonCode;

      if (!match && metadata && !legacyFingerprint && station.metadataPriorityEnabled) {
        const check = MetadataService.isMetadataTrustworthy(metadata, latestNp?.streamText || undefined);
        if (metadata && check.trusted) {
          method = "stream_metadata";
        }
      }

      // Second-pass fallback: when metadata is present but unresolved/program-like,
      // attempt catalog lookup from combined stream text to reduce "Unknown".
      if (!match && metadata && station.fingerprintFallbackEnabled) {
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

      if (sampledAudioPath && !match) {
        await this.archiveUnresolvedSample(stationId, sampledAudioPath);
      }
      if (sampledAudioPath) {
        SamplerService.cleanup(sampledAudioPath);
      }

      if (sampledForFingerprint) {
        await prisma.station.update({
          where: { id: stationId },
          data: { lastAudioFingerprintAt: new Date() },
        });
      }

      const finalReason =
        mergeReason ||
        reasonCode ||
        (icyChanged ? "icy_changed_audio" : null) ||
        (intervalElapsed && !legacyFingerprint ? "audio_interval" : null);

      const processingMs = Date.now() - start;
      await this.saveDetection(station, resolvedUrl, method, metadata, match, processingMs, finalReason);
      await prisma.station.update({
        where: { id: stationId },
        data: {
          lastPollAt: new Date(),
          lastPollStatus: "ok",
          lastPollError: null,
        },
      });
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

  private static async saveDetection(
    station: Station,
    resolvedUrl: string,
    method: DetectionMethod,
    metadata: NormalizedMetadata | null,
    match: MatchResult | null,
    processingMs: number,
    reasonCode: string | null
  ) {
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
        return;
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
        reasonCode,
      },
      select: { id: true },
    });

    let spinPlayCount = 0;
    if (status === "matched") {
      const spin = await upsertSongSpinOnNewPlay(prisma, {
        stationId,
        artist: artistFinal,
        title: titleFinal,
        album: match?.releaseTitle,
        detectionLogId: log.id,
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
  }

  private static async archiveUnresolvedSample(stationId: string, samplePath: string): Promise<void> {
    if (!parseEnvBool("ARCHIVE_UNRESOLVED_SAMPLES", true)) return;
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
    } catch (error) {
      logger.warn({ error, stationId }, "Failed to archive unresolved sample");
    }
  }
}
