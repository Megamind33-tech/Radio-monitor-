import axios from "axios";
import { logger } from "../lib/logger.js";
import { MatchResult, NormalizedMetadata } from "../types.js";

/** Deezer public search is unauthenticated; stay polite (MB is already ~1 rps). */
let lastDeezerAt = 0;
const DEEZER_MIN_INTERVAL_MS = 400;

export class CatalogLookupService {
  static async lookupFromMetadata(metadata: NormalizedMetadata | null): Promise<MatchResult | null> {
    const title = metadata?.rawTitle?.trim();
    const artist = metadata?.rawArtist?.trim();
    const combined = metadata?.combinedRaw?.trim();

    if (!title && !combined) {
      return null;
    }

    // 1) Public MusicBrainz search (free, no API key)
    if (process.env.MUSICBRAINZ_SEARCH_ENABLED !== "false") {
      const mbMatch = await this.musicbrainzSearch(title, artist, combined);
      if (mbMatch) {
        return mbMatch;
      }
    }

    // 2) Public iTunes Search API fallback (free for lookup use-cases)
    if (process.env.ITUNES_LOOKUP_ENABLED !== "false") {
      const itunesMatch = await this.iTunesSearch(title, artist, combined);
      if (itunesMatch) {
        return itunesMatch;
      }
    }

    // 3) Deezer public search (no API key for basic search - rate-limit friendly)
    if (process.env.DEEZER_LOOKUP_ENABLED !== "false") {
      const dz = await this.deezerSearch(title, artist, combined);
      if (dz) return dz;
    }

    // 4) TheAudioDB public search (free, no API key for basic metadata search)
    if (process.env.THEAUDIODB_LOOKUP_ENABLED !== "false") {
      const adb = await this.theAudioDbSearch(title, artist, combined);
      if (adb) return adb;
    }

    return null;
  }

  private static async musicbrainzSearch(title?: string, artist?: string, combined?: string): Promise<MatchResult | null> {
    const userAgent =
      process.env.MUSICBRAINZ_USER_AGENT || "MOSTIFY/1.0.0 ( chansamax198@gmail.com )";
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

  private static async deezerSearch(
    title?: string,
    artist?: string,
    combined?: string
  ): Promise<MatchResult | null> {
    const q = [artist, title].filter(Boolean).join(" ").trim() || (combined ?? "").trim();
    if (!q || q.length < 2) return null;

    const now = Date.now();
    const wait = lastDeezerAt + DEEZER_MIN_INTERVAL_MS - now;
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    lastDeezerAt = Date.now();

    try {
      const response = await axios.get("https://api.deezer.com/search", {
        params: { q, limit: 5 },
        timeout: 10000,
        headers: {
          "User-Agent":
            process.env.MUSICBRAINZ_USER_AGENT || "MOSTIFY/1.0.0 ( chansamax198@gmail.com )",
        },
      });

      const items = response.data?.data;
      if (!Array.isArray(items) || items.length === 0) return null;

      const titleLower = (title || "").toLowerCase();
      const artistLower = (artist || "").toLowerCase();
      let best = items[0];
      let bestScore = 0;

      for (const it of items) {
        const tn = String(it.title || "").toLowerCase();
        const an = String(it.artist?.name || "").toLowerCase();
        let s = 0.55;
        if (titleLower && (tn.includes(titleLower) || titleLower.includes(tn))) s += 0.12;
        if (artistLower && (an.includes(artistLower) || artistLower.includes(an))) s += 0.12;
        if (titleLower && artistLower && tn.includes(titleLower) && an.includes(artistLower)) s += 0.1;
        if (s > bestScore) {
          bestScore = s;
          best = it;
        }
      }

      const minScore = parseFloat(process.env.CATALOG_LOOKUP_MIN_SCORE || "0.65");
      if (bestScore < minScore) return null;

      const durSec = typeof best.duration === "number" ? best.duration : undefined;

      return {
        score: bestScore,
        confidence: bestScore,
        title: best.title,
        artist: best.artist?.name,
        releaseTitle: best.album?.title,
        releaseDate: best.album?.release_date
          ? String(best.album.release_date).slice(0, 10)
          : undefined,
        durationMs: durSec && durSec > 0 ? durSec * 1000 : undefined,
        sourceProvider: "deezer_search",
        reasonCode: "catalog_lookup_deezer",
      };
    } catch (error) {
      logger.warn({ error, q }, "Deezer catalog lookup failed");
      return null;
    }
  }

  private static toSearchQuery(title?: string, artist?: string, combined?: string): string | null {
    if (title && artist) return `recording:"${title}" AND artist:"${artist}"`;
    if (title) return `recording:"${title}"`;
    if (combined) return combined;
    return null;
  }

  private static async theAudioDbSearch(
    title?: string,
    artist?: string,
    combined?: string
  ): Promise<MatchResult | null> {
    const queryTrack = (title || "").trim() || (combined || "").trim();
    if (!queryTrack || queryTrack.length < 2) return null;
    const queryArtist = (artist || "").trim();

    try {
      const response = await axios.get("https://www.theaudiodb.com/api/v1/json/2/searchtrack.php", {
        params: {
          s: queryArtist || undefined,
          t: queryTrack,
        },
        timeout: 10000,
      });
      const tracks = response.data?.track;
      if (!Array.isArray(tracks) || tracks.length === 0) return null;

      const targetTitle = queryTrack.toLowerCase();
      const targetArtist = queryArtist.toLowerCase();
      let best = tracks[0];
      let bestScore = 0.5;
      for (const t of tracks) {
        const tTitle = String(t?.strTrack || "").toLowerCase();
        const tArtist = String(t?.strArtist || "").toLowerCase();
        let score = 0.5;
        if (targetTitle && (tTitle.includes(targetTitle) || targetTitle.includes(tTitle))) score += 0.15;
        if (targetArtist && (tArtist.includes(targetArtist) || targetArtist.includes(tArtist))) score += 0.15;
        if (targetTitle && targetArtist && tTitle.includes(targetTitle) && tArtist.includes(targetArtist)) score += 0.1;
        if (score > bestScore) {
          bestScore = score;
          best = t;
        }
      }

      const minScore = parseFloat(process.env.CATALOG_LOOKUP_MIN_SCORE || "0.65");
      if (bestScore < minScore) return null;

      const durMsRaw = Number(best?.intDuration);
      const durationMs = Number.isFinite(durMsRaw) && durMsRaw > 0 ? Math.round(durMsRaw) : undefined;
      return {
        score: bestScore,
        confidence: bestScore,
        title: best?.strTrack || undefined,
        artist: best?.strArtist || undefined,
        releaseTitle: best?.strAlbum || undefined,
        releaseDate: best?.intYearReleased ? String(best.intYearReleased) : undefined,
        genre: best?.strGenre || undefined,
        durationMs,
        sourceProvider: "theaudiodb_search",
        reasonCode: "catalog_lookup_theaudiodb",
      };
    } catch (error) {
      logger.warn({ error, queryTrack, queryArtist }, "TheAudioDB catalog lookup failed");
      return null;
    }
  }
}
