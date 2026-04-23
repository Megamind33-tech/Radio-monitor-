import axios from 'axios';
import { logger } from '../lib/logger.js';
import { MatchResult } from '../types.js';

export class MusicbrainzService {
  private static lastRequestAt: number = 0;
  private static readonly RATE_LIMIT_MS = 1100; // 1 req/sec strict limit

  /** Lightweight fetch of recording length (ms) for duplicate-suppression when enrich was skipped. */
  static async getRecordingLengthMs(recordingId: string): Promise<number | undefined> {
    await this.throttle();
    const userAgent =
      process.env.MUSICBRAINZ_USER_AGENT || "MOSTIFY/1.0.0 ( chansamax198@gmail.com )";
    try {
      const response = await axios.get(`https://musicbrainz.org/ws/2/recording/${recordingId}`, {
        params: { fmt: "json" },
        headers: { "User-Agent": userAgent },
        timeout: 10000,
      });
      const len = response.data?.length;
      return typeof len === "number" && len > 0 ? len : undefined;
    } catch (error) {
      logger.warn({ error, recordingId }, "MusicBrainz recording length fetch failed");
      return undefined;
    }
  }

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

    const userAgent =
      process.env.MUSICBRAINZ_USER_AGENT || "MOSTIFY/1.0.0 ( chansamax198@gmail.com )";

    try {
      logger.info({ mbid: match.recordingId }, "Enriching with MusicBrainz");
      
      // https://musicbrainz.org/ws/2/recording/<MBID>?inc=artists+releases&fmt=json
      const response = await axios.get(`https://musicbrainz.org/ws/2/recording/${match.recordingId}`, {
        params: {
          inc: 'artists+releases+isrcs+genres',
          fmt: 'json'
        },
        headers: {
          'User-Agent': userAgent
        },
        timeout: 10000
      });

      const data = response.data;
      
      const len = data.length;
      const durationMs =
        typeof len === "number" && len > 0 ? len : match.durationMs;

      const keepAudioProvider =
        match.sourceProvider === "audd" ||
        match.sourceProvider === "acoustid" ||
        match.sourceProvider === "acoustid_open";

      const enriched: MatchResult = {
        ...match,
        title: data.title || match.title,
        artist: data['artist-credit']?.[0]?.name || match.artist,
        releaseTitle: data.releases?.[0]?.title,
        releaseDate: data.releases?.[0]?.date,
        isrcs: data.isrcs,
        genre: data.genres?.[0]?.name,
        durationMs,
        sourceProvider: keepAudioProvider ? match.sourceProvider : "musicbrainz",
      };

      return enriched;
    } catch (error) {
      logger.error({ error, mbid: match.recordingId }, "MusicBrainz enrichment failed");
      return match;
    }
  }
}
