import axios from 'axios';
import { logger } from '../lib/logger.js';
import { MatchResult } from '../types.js';

export class MusicbrainzService {
  private static lastRequestAt: number = 0;
  private static readonly RATE_LIMIT_MS = 1100; // 1 req/sec strict limit

  private static async throttle() {
    const now = Date.now();
    const wait = this.lastRequestAt + this.RATE_LIMIT_MS - now;
    if (wait > 0) {
      await new Promise(resolve => setTimeout(resolve, wait));
    }
    this.lastRequestAt = Date.now();
  }

  /**
   * Enriches matching result with more detailed data from MusicBrainz.
   */
  static async enrich(match: MatchResult): Promise<MatchResult> {
    if (!match.recordingId) return match;

    await this.throttle();

    const userAgent = process.env.MUSICBRAINZ_USER_AGENT || 'RadioPulseMonitor/1.0.0 ( contact@example.com )';

    try {
      logger.info({ mbid: match.recordingId }, "Enriching with MusicBrainz");
      
      // https://musicbrainz.org/ws/2/recording/<MBID>?inc=artists+releases&fmt=json
      const response = await axios.get(`https://musicbrainz.org/ws/2/recording/${match.recordingId}`, {
        params: {
          inc: 'artists+releases+isrcs',
          fmt: 'json'
        },
        headers: {
          'User-Agent': userAgent
        },
        timeout: 10000
      });

      const data = response.data;
      
      const enriched: MatchResult = {
        ...match,
        title: data.title || match.title,
        artist: data['artist-credit']?.[0]?.name || match.artist,
        releaseTitle: data.releases?.[0]?.title,
        releaseDate: data.releases?.[0]?.date,
        isrcs: data.isrcs
      };

      return enriched;
    } catch (error) {
      logger.error({ error, mbid: match.recordingId }, "MusicBrainz enrichment failed");
      return match;
    }
  }
}
