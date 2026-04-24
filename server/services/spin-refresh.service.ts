/**
 * spin-refresh.service.ts
 * -----------------------
 * Detects StationSongSpin rows whose metadata is poorly structured — all fields
 * crammed into one box (e.g. title = "Wyclef Jean - Gone Till November feat. Lauryn Hill")
 * — and refreshes them using free catalog APIs (iTunes → Deezer → MusicBrainz search).
 *
 * "Poorly structured" is defined as:
 *   a) artistNorm is empty  AND  titleNorm contains a known separator or "feat."
 *   b) titleNorm is very long (>80 chars) — likely a combined ICY blob
 *   c) artistNorm equals titleNorm (swapped during ICY parse)
 *
 * When a catalog lookup returns a cleaner result (shorter title + non-empty artist),
 * the spin row is updated in-place.  The fix is also written back to LocalFingerprint
 * if a chromaprint fingerprint was ever saved for that song.
 *
 * Rate: ≤ 1 spin refreshed per second (avoids hammering iTunes / Deezer).
 */

import { prisma } from "../lib/prisma.js";
import { logger } from "../lib/logger.js";
import axios from "axios";
import { parseFeaturedFromArtist, titleWithoutFeaturing } from "../lib/track-credits.js";

interface CatalogHit {
  title: string;
  artist: string;
  album?: string;
  genre?: string;
  releaseDate?: string;
  durationMs?: number;
  sourceProvider: string;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function normalize(s: string): string {
  return (s ?? "")
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/** Returns true if the catalog hit is genuinely better-structured than the current entry. */
function isBetterStructure(
  current: { artistNorm: string; titleNorm: string },
  hit: CatalogHit
): boolean {
  const newArtist = normalize(hit.artist);
  const newTitle = normalize(hit.title);
  if (!newArtist || !newTitle) return false;
  // Must provide a non-empty artist where we had none, OR shorten the title.
  const artistImproved = !current.artistNorm && newArtist.length >= 2;
  const titleShorter = newTitle.length < current.titleNorm.length - 5;
  return artistImproved || titleShorter;
}

// ---------------------------------------------------------------------------
// Catalog lookups (mirrors CatalogLookupService but returns raw hit)
// ---------------------------------------------------------------------------

async function itunesLookup(query: string): Promise<CatalogHit | null> {
  try {
    const resp = await axios.get("https://itunes.apple.com/search", {
      params: { term: query, media: "music", entity: "song", limit: 5 },
      timeout: 10000,
    });
    const results: unknown[] = resp.data?.results ?? [];
    if (!results.length) return null;

    const best = results[0] as Record<string, unknown>;
    const trackName = String(best.trackName ?? "").trim();
    const artistName = String(best.artistName ?? "").trim();
    if (!trackName || !artistName) return null;

    return {
      title: trackName,
      artist: artistName,
      album: best.collectionName ? String(best.collectionName) : undefined,
      genre: best.primaryGenreName ? String(best.primaryGenreName) : undefined,
      releaseDate: best.releaseDate ? String(best.releaseDate).slice(0, 10) : undefined,
      durationMs: typeof best.trackTimeMillis === "number" ? best.trackTimeMillis : undefined,
      sourceProvider: "itunes_search",
    };
  } catch {
    return null;
  }
}

async function deezerLookup(query: string): Promise<CatalogHit | null> {
  try {
    const resp = await axios.get("https://api.deezer.com/search", {
      params: { q: query, limit: 5 },
      timeout: 10000,
    });
    const items: unknown[] = resp.data?.data ?? [];
    if (!items.length) return null;

    const best = items[0] as Record<string, unknown>;
    const title = String((best.title as string) ?? "").trim();
    const artist = String(((best.artist as Record<string,unknown>)?.name as string) ?? "").trim();
    if (!title || !artist) return null;

    const durSec = typeof best.duration === "number" ? best.duration : undefined;
    return {
      title,
      artist,
      album: ((best.album as Record<string,unknown>)?.title as string | undefined),
      durationMs: durSec ? durSec * 1000 : undefined,
      sourceProvider: "deezer_search",
    };
  } catch {
    return null;
  }
}

async function catalogLookupWithRateLimit(query: string): Promise<CatalogHit | null> {
  // Respect a 1-second minimum gap (enforced by the batch loop delay).
  const itunes = await itunesLookup(query);
  if (itunes) return itunes;
  const deezer = await deezerLookup(query);
  return deezer;
}

// ---------------------------------------------------------------------------
// Badly-structured spin detection
// ---------------------------------------------------------------------------

const SEPARATOR_RE = /\s[-–—\/]\s/;
const FEAT_RE = /\bfeat[.\s]|\bft[.\s]|\(feat|\(ft/i;

function isPoorlyStructured(spin: {
  artistNorm: string;
  titleNorm: string;
  titleLast: string;
}): { poor: boolean; reason: string } {
  const aN = (spin.artistNorm ?? "").trim();
  const tN = (spin.titleNorm ?? "").trim();
  const tL = (spin.titleLast ?? "").trim();

  if (!aN && (SEPARATOR_RE.test(tL) || FEAT_RE.test(tL))) {
    return { poor: true, reason: "artist_empty_separator_in_title" };
  }
  if (!aN && FEAT_RE.test(tN)) {
    return { poor: true, reason: "artist_empty_feat_in_title_norm" };
  }
  if (tL.length > 80 && !aN) {
    return { poor: true, reason: "title_too_long_no_artist" };
  }
  if (aN && tN && aN === tN) {
    return { poor: true, reason: "artist_equals_title" };
  }
  return { poor: false, reason: "" };
}

// ---------------------------------------------------------------------------
// Public service
// ---------------------------------------------------------------------------

export class SpinRefreshService {
  private static running = false;
  private static lastRunAt: Date | null = null;

  static status() {
    return { running: this.running, lastRunAt: this.lastRunAt };
  }

  /**
   * Scan for poorly-structured spins and attempt to refresh their metadata.
   * Processes at most `limit` rows, with a 1-second inter-request gap.
   */
  static async runBatch(opts?: { limit?: number; stationId?: string }): Promise<{
    scanned: number;
    refreshed: number;
    noImprovement: number;
    errored: number;
  }> {
    if (this.running) {
      return { scanned: 0, refreshed: 0, noImprovement: 0, errored: 0 };
    }
    this.running = true;
    this.lastRunAt = new Date();

    const limit = Math.min(200, Math.max(1, opts?.limit ?? 50));
    let scanned = 0;
    let refreshed = 0;
    let noImprovement = 0;
    let errored = 0;

    try {
      // Find candidate rows — prioritise oldest (haven't been refreshed yet).
      const candidates = await prisma.stationSongSpin.findMany({
        where: {
          ...(opts?.stationId ? { stationId: opts.stationId } : {}),
          // Only look at entries that have some raw combined text to search with.
          originalCombinedRaw: { not: null },
        },
        orderBy: { firstPlayedAt: "asc" },
        take: limit * 4, // Over-fetch; we filter in JS to find poorly structured.
        select: {
          id: true,
          stationId: true,
          artistNorm: true,
          titleNorm: true,
          artistLast: true,
          titleLast: true,
          albumLast: true,
          originalCombinedRaw: true,
        },
      });

      const poor = candidates.filter((s) => isPoorlyStructured(s).poor).slice(0, limit);
      scanned = poor.length;

      for (const spin of poor) {
        const diagnosis = isPoorlyStructured(spin);
        const query = (spin.originalCombinedRaw ?? spin.titleLast ?? "").trim();
        if (!query) {
          noImprovement++;
          continue;
        }

        try {
          const hit = await catalogLookupWithRateLimit(query);

          if (!hit || !isBetterStructure({ artistNorm: spin.artistNorm, titleNorm: spin.titleNorm }, hit)) {
            noImprovement++;
            logger.debug(
              { spinId: spin.id, query, diagnosis: diagnosis.reason },
              "SpinRefresh: no better structure from catalog"
            );
          } else {
            const newArtistNorm = normalize(hit.artist);
            const newTitleNorm = normalize(hit.title);
            const newAlbumNorm = normalize(hit.album ?? "");

            await prisma.stationSongSpin.update({
              where: { id: spin.id },
              data: {
                artistNorm: newArtistNorm,
                titleNorm: newTitleNorm,
                albumNorm: newAlbumNorm,
                artistLast: hit.artist,
                titleLast: hit.title,
                albumLast: hit.album ?? spin.albumLast,
              },
            });

            // Also patch LocalFingerprint entries that reference this artist/title combo.
            const feat = parseFeaturedFromArtist(hit.artist);
            const featuredJson =
              feat.featured.length > 0 ? JSON.stringify(feat.featured) : undefined;
            const titleWo = titleWithoutFeaturing(hit.title) || null;
            await prisma.localFingerprint.updateMany({
              where: {
                artist: { equals: spin.artistLast || null },
                title: { equals: spin.titleLast || null },
              },
              data: {
                artist: feat.primaryArtist || hit.artist,
                title: hit.title,
                displayArtist: hit.artist,
                titleWithoutFeat: titleWo,
                featuredArtistsJson: featuredJson,
                releaseTitle: hit.album ?? undefined,
                genre: hit.genre ?? undefined,
                durationMs: hit.durationMs ?? undefined,
              },
            });

            logger.info(
              {
                spinId: spin.id,
                before: { artist: spin.artistLast, title: spin.titleLast },
                after: { artist: hit.artist, title: hit.title, album: hit.album },
                source: hit.sourceProvider,
                diagnosis: diagnosis.reason,
              },
              "SpinRefresh: metadata corrected"
            );
            refreshed++;
          }
        } catch (error) {
          errored++;
          logger.warn({ error, spinId: spin.id }, "SpinRefresh: error during catalog lookup");
        }

        // 1-second inter-request gap to be polite to iTunes / Deezer.
        await new Promise<void>((r) => setTimeout(r, 1000));
      }
    } finally {
      this.running = false;
      logger.info({ scanned, refreshed, noImprovement, errored }, "SpinRefresh batch complete");
    }

    return { scanned, refreshed, noImprovement, errored };
  }

  /**
   * Returns a count + sample of spins currently flagged as poorly structured,
   * for the dashboard API endpoint.
   */
  static async listNeedsRefresh(opts?: {
    stationId?: string;
    take?: number;
  }): Promise<{
    total: number;
    items: Array<{
      id: string;
      stationId: string;
      artistLast: string;
      titleLast: string;
      albumLast: string;
      originalCombinedRaw: string | null;
      reason: string;
      playCount: number;
      lastPlayedAt: Date;
    }>;
  }> {
    const take = Math.min(500, Math.max(1, opts?.take ?? 100));

    const rows = await prisma.stationSongSpin.findMany({
      where: {
        ...(opts?.stationId ? { stationId: opts.stationId } : {}),
        originalCombinedRaw: { not: null },
      },
      orderBy: { lastPlayedAt: "desc" },
      take: take * 5,
      select: {
        id: true,
        stationId: true,
        artistNorm: true,
        titleNorm: true,
        artistLast: true,
        titleLast: true,
        albumLast: true,
        originalCombinedRaw: true,
        playCount: true,
        lastPlayedAt: true,
      },
    });

    const poor = rows
      .map((r) => ({ ...r, _diag: isPoorlyStructured(r) }))
      .filter((r) => r._diag.poor)
      .slice(0, take);

    return {
      total: poor.length,
      items: poor.map((r) => ({
        id: r.id,
        stationId: r.stationId,
        artistLast: r.artistLast,
        titleLast: r.titleLast,
        albumLast: r.albumLast,
        originalCombinedRaw: r.originalCombinedRaw,
        reason: r._diag.reason,
        playCount: r.playCount,
        lastPlayedAt: r.lastPlayedAt,
      })),
    };
  }
}
