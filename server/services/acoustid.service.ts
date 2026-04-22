import axios from 'axios';
import { logger } from '../lib/logger.js';
import { FingerprintResult, MatchResult } from '../types.js';

export class AcoustidService {
  private static lastRequestAt: number = 0;
  private static readonly RATE_LIMIT_MS = 500; // 2 req/sec to be safe (Limit is 3)

  private static async throttle() {
    const now = Date.now();
    const wait = this.lastRequestAt + this.RATE_LIMIT_MS - now;
    if (wait > 0) {
      await new Promise(resolve => setTimeout(resolve, wait));
    }
    this.lastRequestAt = Date.now();
  }

  static async lookup(fp: FingerprintResult): Promise<MatchResult | null> {
    // Free for non-commercial use: register an app at https://acoustid.org/new-application
    // (Chromaprint/fpcalc is always local; only this HTTP lookup needs a client key.)
    const apiKey = process.env.ACOUSTID_API_KEY;
    if (!apiKey) {
      logger.warn("ACOUSTID_API_KEY not set, skipping AcoustID lookup (use MusicBrainz/iTunes/Deezer text fallbacks)");
      return null;
    }

    await this.throttle();

    try {
      logger.info({ duration: fp.duration }, "Querying AcoustID API");
      
      const response = await axios.post('https://api.acoustid.org/v2/lookup', 
        new URLSearchParams({
          client: apiKey,
          duration: fp.duration.toString(),
          fingerprint: fp.fingerprint,
          meta: 'recordings releasegroups releases tracks compress'
        }), {
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          timeout: 10000
        }
      );

      const data = response.data;
      if (data.status !== 'ok' || !data.results || data.results.length === 0) {
        logger.debug({ status: data.status }, "AcoustID returned no results");
        return null;
      }

      // Find top match
      const result = data.results[0];
      const minScore = parseFloat(process.env.ACOUSTID_MIN_SCORE || '0.5');
      
      if (result.score < minScore) {
        logger.debug({ score: result.score, minScore }, "AcoustID match score too low");
        return null;
      }

      const recording = result.recordings?.[0];
      if (!recording) return null;

      // Best effort extraction
      const durSec = typeof recording.duration === "number" ? recording.duration : undefined;
      const match: MatchResult = {
        score: result.score,
        recordingId: recording.id,
        title: recording.title,
        artist: recording.artists?.[0]?.name,
        durationMs: durSec && durSec > 0 ? Math.round(durSec * 1000) : undefined,
        sourceProvider: "acoustid",
        confidence: result.score,
        reasonCode: 'fingerprint_acoustid'
      };

      return match;
    } catch (error) {
      logger.error({ error }, "AcoustID API request failed");
      return null;
    }
  }
}
