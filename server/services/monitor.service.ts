import * as fs from "fs";
import * as path from "path";
import type { Station } from "@prisma/client";
import { prisma } from "../lib/prisma.js";
import { logger } from "../lib/logger.js";
import { ResolverService } from "./resolver.service.js";
import { MetadataService } from "./metadata.service.js";
import { SamplerService } from "./sampler.service.js";
import { FingerprintService } from "./fingerprint.service.js";
import { AcoustidService } from "./acoustid.service.js";
import { MusicbrainzService } from "./musicbrainz.service.js";
import { CatalogLookupService } from "./catalog-lookup.service.js";
import { NormalizedMetadata, MatchResult, DetectionMethod } from "../types.js";

function parseEnvMs(key: string, fallback: number): number {
  const v = process.env[key];
  if (!v) return fallback;
  const n = parseInt(v, 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

export class MonitorService {
  /**
   * Main logic for a single station poll.
   */
  static async pollStation(stationId: string): Promise<void> {
    const start = Date.now();
    const station = await prisma.station.findUnique({ where: { id: stationId } });
    if (!station || !station.isActive) return;

    logger.info({ station: station.name }, "Polling station");

    try {
      const resolvedUrl = await ResolverService.resolveStreamUrl(station.streamUrl);

      let metadata: NormalizedMetadata | null = null;
      let shouldFingerprint = false;
      let reasonCode: string | null = null;

      // 1. Try Metadata first
      if (station.metadataPriorityEnabled) {
        metadata = await MetadataService.readStreamMetadata(resolvedUrl);
        const latestNowPlaying = await prisma.currentNowPlaying.findUnique({ where: { stationId } });

        if (!metadata) {
          shouldFingerprint = true;
          reasonCode = "metadata_missing";
        } else {
          const check = MetadataService.isMetadataTrustworthy(metadata, latestNowPlaying?.streamText || undefined);
          if (!check.trusted) {
            shouldFingerprint = true;
            reasonCode = check.reason || "metadata_untrusted";
          } else if (latestNowPlaying && latestNowPlaying.streamText === metadata.combinedRaw) {
            const staleAt = new Date(
              latestNowPlaying.updatedAt.getTime() + station.metadataStaleSeconds * 1000
            );
            if (Date.now() > staleAt.getTime()) {
              shouldFingerprint = true;
              reasonCode = "metadata_stale";
            }
          }
        }
      } else {
        shouldFingerprint = true;
        reasonCode = "metadata_disabled";
      }

      let match: MatchResult | null = null;
      let method: DetectionMethod = "stream_metadata";

      // 2. Fingerprint fallback
      if (shouldFingerprint && station.fingerprintFallbackEnabled) {
        logger.info({ station: station.name, reason: reasonCode }, "Fallback to fingerprinting");
        method = "fingerprint_acoustid";

        const samplePath = await SamplerService.captureSample(resolvedUrl, station.sampleSeconds);
        if (samplePath) {
          const fingerprint = await FingerprintService.generateFingerprint(samplePath);
          if (fingerprint) {
            const acoustidMatch = await AcoustidService.lookup(fingerprint);
            if (acoustidMatch) {
              match = await MusicbrainzService.enrich(acoustidMatch);
            }
          }
          SamplerService.cleanup(samplePath);
        }
      }

      // 2b. Public catalog fallback based on stream metadata text when fingerprinting misses
      if (!match && metadata) {
        const catalogMatch = await CatalogLookupService.lookupFromMetadata(metadata);
        if (catalogMatch) {
          match = catalogMatch;
          method = "catalog_lookup";
        }
      }

      const processingMs = Date.now() - start;
      await this.saveDetection(station, resolvedUrl, method, metadata, match, processingMs, reasonCode);
    } catch (error) {
      logger.error({ error, station: station.name }, "Error polling station");
      await prisma.jobRun.create({
        data: {
          stationId,
          status: "failure",
          error: String(error),
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
    const isMatched = !!match || (method === "stream_metadata" && !!metadata && !reasonCode);
    const status = isMatched ? "matched" : "unresolved";

    const titleFinal = match?.title || metadata?.rawTitle;
    const artistFinal = match?.artist || metadata?.rawArtist;

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
        sourceProvider: match?.sourceProvider || method,
        isrcList: match?.isrcs ? JSON.stringify(match.isrcs) : null,
        trackDurationMs: trackDurationMs ?? null,
        sampleSeconds: station.sampleSeconds,
        processingMs,
        status,
        reasonCode,
      },
      select: { id: true },
    });

    if (status === "matched" && station.archiveSongSamples && resolvedUrl.startsWith("http")) {
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
        sourceProvider: match?.sourceProvider || method,
        streamText: metadata?.combinedRaw,
        updatedAt: new Date(),
      },
      create: {
        stationId,
        title: titleFinal,
        artist: artistFinal,
        album: match?.releaseTitle,
        genre: match?.genre,
        sourceProvider: match?.sourceProvider || method,
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
}
