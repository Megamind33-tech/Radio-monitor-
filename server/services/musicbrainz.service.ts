import axios from "axios";
import { logger } from "../lib/logger.js";
import { MatchResult } from "../types.js";
import { parseFeaturedFromArtist, titleWithoutFeaturing } from "../lib/track-credits.js";

function joinArtistCredit(credits: unknown): string | undefined {
  if (!Array.isArray(credits) || credits.length === 0) return undefined;
  let out = "";
  for (const c of credits) {
    if (!c || typeof c !== "object") continue;
    const o = c as { name?: string; joinphrase?: string };
    if (typeof o.name === "string") out += o.name;
    if (typeof o.joinphrase === "string") out += o.joinphrase;
  }
  const s = out.trim();
  return s || undefined;
}

function firstLabelName(release: Record<string, unknown>): string | undefined {
  const li = release["label-info"];
  if (!Array.isArray(li) || li.length === 0) return undefined;
  for (const entry of li) {
    if (!entry || typeof entry !== "object") continue;
    const label = (entry as { label?: { name?: string } }).label;
    if (label && typeof label.name === "string" && label.name.trim()) return label.name.trim();
  }
  return undefined;
}

function pickReleaseForEnrichment(
  releases: unknown
): { release: Record<string, unknown>; title?: string; date?: string; country?: string; label?: string } | null {
  if (!Array.isArray(releases) || releases.length === 0) return null;
  for (const r of releases) {
    if (!r || typeof r !== "object") continue;
    const rel = r as Record<string, unknown>;
    const status = typeof rel.status === "string" ? rel.status.toLowerCase() : "";
    if (status === "official" || !status) {
      const title = typeof rel.title === "string" ? rel.title : undefined;
      const date = typeof rel.date === "string" ? rel.date : undefined;
      const country = typeof rel.country === "string" ? rel.country : undefined;
      const label = firstLabelName(rel);
      return { release: rel, title, date, country, label };
    }
  }
  const rel = releases[0] as Record<string, unknown>;
  return {
    release: rel,
    title: typeof rel.title === "string" ? rel.title : undefined,
    date: typeof rel.date === "string" ? rel.date : undefined,
    country: typeof rel.country === "string" ? rel.country : undefined,
    label: firstLabelName(rel),
  };
}

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
          inc: "artists+releases+isrcs+genres+artist-credits",
          fmt: "json",
        },
        headers: {
          'User-Agent': userAgent
        },
        timeout: 10000
      });

      const data = response.data;

      const len = data.length;
      const durationMs = typeof len === "number" && len > 0 ? len : match.durationMs;

      const displayArtist =
        joinArtistCredit(data["artist-credit"]) ||
        data["artist-credit"]?.[0]?.name ||
        match.displayArtist ||
        match.artist;
      const titleRaw = data.title || match.title;
      const picked = pickReleaseForEnrichment(data.releases);
      const releaseTitle = picked?.title ?? match.releaseTitle;
      const releaseDate = picked?.date ?? match.releaseDate;
      const labelName = picked?.label ?? match.labelName;
      const countryCode = picked?.country ?? match.countryCode;

      const creditParsed = parseFeaturedFromArtist(displayArtist);
      const artistForMatch = creditParsed.primaryArtist || match.artist;
      const featuredFromCredit = creditParsed.featured;
      const titleFeat = titleWithoutFeaturing(titleRaw);
      const featuredArtists = [...new Set([...featuredFromCredit])];

      const enriched: MatchResult = {
        ...match,
        title: titleRaw,
        artist: artistForMatch,
        displayArtist: displayArtist || undefined,
        titleWithoutFeat: titleFeat || undefined,
        featuredArtists: featuredArtists.length ? featuredArtists : undefined,
        releaseTitle,
        releaseDate,
        isrcs: data.isrcs,
        genre: data.genres?.[0]?.name,
        labelName,
        countryCode,
        durationMs,
        sourceProvider: "musicbrainz",
      };

      return enriched;
    } catch (error) {
      logger.error({ error, mbid: match.recordingId }, "MusicBrainz enrichment failed");
      return match;
    }
  }
}
