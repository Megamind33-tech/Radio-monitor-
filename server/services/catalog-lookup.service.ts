import axios from "axios";
import { logger } from "../lib/logger.js";
import { MatchResult, NormalizedMetadata } from "../types.js";

export class CatalogLookupService {
  static async lookupFromMetadata(metadata: NormalizedMetadata | null): Promise<MatchResult | null> {
    const title = metadata?.rawTitle?.trim();
    const artist = metadata?.rawArtist?.trim();
    const combined = metadata?.combinedRaw?.trim();

    if (!title && !combined) {
      return null;
    }

    // 1) Public MusicBrainz search (free, no API key)
    const mbMatch = await this.musicbrainzSearch(title, artist, combined);
    if (mbMatch) {
      return mbMatch;
    }

    // 2) Public iTunes Search API fallback (free for lookup use-cases)
    const itunesMatch = await this.iTunesSearch(title, artist, combined);
    if (itunesMatch) {
      return itunesMatch;
    }

    return null;
  }

  private static async musicbrainzSearch(title?: string, artist?: string, combined?: string): Promise<MatchResult | null> {
    const userAgent = process.env.MUSICBRAINZ_USER_AGENT || "RadioPulseMonitor/1.0.0 ( contact@example.com )";
    const query = this.toSearchQuery(title, artist, combined);
    if (!query) return null;

    try {
      const response = await axios.get("https://musicbrainz.org/ws/2/recording", {
        params: {
          query,
          fmt: "json",
          limit: 5
        },
        headers: { "User-Agent": userAgent },
        timeout: 10000
      });

      const recordings = response.data?.recordings;
      if (!Array.isArray(recordings) || recordings.length === 0) return null;

      const top = recordings[0];
      const artistName = top["artist-credit"]?.[0]?.name;
      const releaseTitle = top.releases?.[0]?.title;
      const releaseDate = top["first-release-date"] || top.releases?.[0]?.date;
      const lengthRaw = top.length;
      const durationMs =
        typeof lengthRaw === "number" && lengthRaw > 0 ? lengthRaw : undefined;

      const normalizedScore = Math.min(Math.max((Number(top.score) || 0) / 100, 0), 1);
      const minScore = parseFloat(process.env.CATALOG_LOOKUP_MIN_SCORE || "0.65");
      if (normalizedScore < minScore) {
        return null;
      }

      return {
        score: normalizedScore,
        confidence: normalizedScore,
        recordingId: top.id,
        title: top.title,
        artist: artistName,
        releaseTitle,
        releaseDate,
        durationMs,
        sourceProvider: "musicbrainz_search",
        reasonCode: "catalog_lookup_musicbrainz"
      };
    } catch (error) {
      logger.warn({ error, query }, "MusicBrainz catalog lookup failed");
      return null;
    }
  }

  private static async iTunesSearch(title?: string, artist?: string, combined?: string): Promise<MatchResult | null> {
    const query = [artist, title].filter(Boolean).join(" ").trim() || combined;
    if (!query) return null;

    try {
      const response = await axios.get("https://itunes.apple.com/search", {
        params: {
          term: query,
          media: "music",
          entity: "song",
          limit: 5
        },
        timeout: 10000
      });

      const results = response.data?.results;
      if (!Array.isArray(results) || results.length === 0) return null;

      const best = results[0];
      const titleLower = (title || "").toLowerCase();
      const bestTrackLower = String(best.trackName || "").toLowerCase();
      const roughMatch = titleLower ? bestTrackLower.includes(titleLower) || titleLower.includes(bestTrackLower) : true;
      const score = roughMatch ? 0.72 : 0.55;
      const minScore = parseFloat(process.env.CATALOG_LOOKUP_MIN_SCORE || "0.65");
      if (score < minScore) return null;

      const trackTimeMs =
        typeof best.trackTimeMillis === "number" && best.trackTimeMillis > 0
          ? best.trackTimeMillis
          : undefined;

      return {
        score,
        confidence: score,
        title: best.trackName,
        artist: best.artistName,
        releaseTitle: best.collectionName,
        releaseDate: best.releaseDate ? String(best.releaseDate).slice(0, 10) : undefined,
        genre: best.primaryGenreName,
        durationMs: trackTimeMs,
        sourceProvider: "itunes_search",
        reasonCode: "catalog_lookup_itunes"
      };
    } catch (error) {
      logger.warn({ error, query }, "iTunes catalog lookup failed");
      return null;
    }
  }

  private static toSearchQuery(title?: string, artist?: string, combined?: string): string | null {
    if (title && artist) return `recording:"${title}" AND artist:"${artist}"`;
    if (title) return `recording:"${title}"`;
    if (combined) return combined;
    return null;
  }
}
