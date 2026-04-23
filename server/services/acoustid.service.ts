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
    const configuredClient = process.env.ACOUSTID_API_KEY;
    const apiKey = configuredClient || process.env.ACOUSTID_OPEN_CLIENT;
    if (!apiKey) {
      logger.warn("No AcoustID client configured (set ACOUSTID_API_KEY or ACOUSTID_OPEN_CLIENT)");
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

      // Find top match — sort by score descending in case results are unordered
      data.results.sort((a: { score: number }, b: { score: number }) => b.score - a.score);
      const result = data.results[0];
      // 0.65 minimum avoids false positives from partial fingerprint matches on short
      // or noisy audio segments.  The old 0.5 default caused wrong songs to be logged.
      const minScore = parseFloat(process.env.ACOUSTID_MIN_SCORE || '0.65');

      if (result.score < minScore) {
        logger.debug({ score: result.score, minScore }, "AcoustID match score too low");
        return null;
      }

      const recording = result.recordings?.[0];
      if (!recording) return null;

      const durSec = typeof recording.duration === "number" ? recording.duration : undefined;
      const match: MatchResult = {
        score: result.score,
        recordingId: recording.id,
        acoustidTrackId: typeof result.id === "string" ? result.id : undefined,
        title: recording.title,
        artist: recording.artists?.[0]?.name,
        durationMs: durSec && durSec > 0 ? Math.round(durSec * 1000) : undefined,
        sourceProvider: configuredClient ? "acoustid" : "acoustid_open",
        confidence: result.score,
        reasonCode: configuredClient ? "fingerprint_acoustid" : "fingerprint_acoustid_open"
      };

      return match;
    } catch (error) {
      logger.error({ error }, "AcoustID API request failed");
      return null;
    }
  }
}
