import { prisma } from '../lib/prisma.js';
import { logger } from '../lib/logger.js';
import { ResolverService } from './resolver.service.js';
import { MetadataService } from './metadata.service.js';
import { SamplerService } from './sampler.service.js';
import { FingerprintService } from './fingerprint.service.js';
import { AcoustidService } from './acoustid.service.js';
import { MusicbrainzService } from './musicbrainz.service.js';
import { NormalizedMetadata, MatchResult, DetectionMethod } from '../types.js';

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
          reasonCode = 'metadata_missing';
        } else {
          const check = MetadataService.isMetadataTrustworthy(metadata, latestNowPlaying?.streamText || undefined);
          if (!check.trusted) {
            shouldFingerprint = true;
            reasonCode = check.reason || 'metadata_untrusted';
          } else if (latestNowPlaying && latestNowPlaying.streamText === metadata.combinedRaw) {
            // Meta is same as before. If it's been too long, it might be stuck.
            const staleAt = new Date(latestNowPlaying.updatedAt.getTime() + station.metadataStaleSeconds * 1000);
            if (Date.now() > staleAt.getTime()) {
              shouldFingerprint = true;
              reasonCode = 'metadata_stale';
            }
          }
        }
      } else {
        shouldFingerprint = true;
        reasonCode = 'metadata_disabled';
      }

      let match: MatchResult | null = null;
      let method: DetectionMethod = 'stream_metadata';

      // 2. Fingerprint fallback
      if (shouldFingerprint && station.fingerprintFallbackEnabled) {
        logger.info({ station: station.name, reason: reasonCode }, "Fallback to fingerprinting");
        method = 'fingerprint_acoustid';
        
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

      // 3. Normalize and log
      const processingMs = Date.now() - start;
      await this.saveDetection(stationId, method, metadata, match, processingMs, reasonCode);
      
    } catch (error) {
      logger.error({ error, station: station.name }, "Error polling station");
      await prisma.jobRun.create({
        data: {
          stationId,
          status: 'failure',
          error: String(error),
          durationMs: Date.now() - start
        }
      });
    }
  }

  private static async saveDetection(
    stationId: string, 
    method: DetectionMethod, 
    metadata: NormalizedMetadata | null, 
    match: MatchResult | null,
    processingMs: number,
    reasonCode: string | null
  ) {
    const isMatched = !!match || (method === 'stream_metadata' && !!metadata && !reasonCode);
    const status = isMatched ? 'matched' : 'unresolved';

    const titleFinal = match?.title || metadata?.rawTitle;
    const artistFinal = match?.artist || metadata?.rawArtist;

    // Check for deduplication: don't log if identical to latest match in last 2 mins
    const latestLog = await prisma.detectionLog.findFirst({
      where: { stationId },
      orderBy: { observedAt: 'desc' }
    });

    if (latestLog && latestLog.titleFinal === titleFinal && latestLog.artistFinal === artistFinal) {
      // Still update current now playing to keep it "fresh"
      await prisma.currentNowPlaying.upsert({
        where: { stationId },
        update: { updatedAt: new Date() },
        create: { stationId, title: titleFinal, artist: artistFinal }
      });
      return;
    }

    await prisma.detectionLog.create({
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
        isrcList: match?.isrcs ? JSON.stringify(match.isrcs) : null,
        processingMs,
        status,
        reasonCode
      }
    });

    await prisma.currentNowPlaying.upsert({
      where: { stationId },
      update: {
        title: titleFinal,
        artist: artistFinal,
        streamText: metadata?.combinedRaw,
        updatedAt: new Date()
      },
      create: {
        stationId,
        title: titleFinal,
        artist: artistFinal,
        streamText: metadata?.combinedRaw
      }
    });

    await prisma.jobRun.create({
      data: {
        stationId,
        status: 'success',
        durationMs: processingMs
      }
    });
  }
}
