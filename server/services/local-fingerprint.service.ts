import crypto from "node:crypto";
import { prisma } from "../lib/prisma.js";
import { logger } from "../lib/logger.js";
import { FingerprintResult, MatchResult, NormalizedMetadata } from "../types.js";
import { parseFeaturedFromArtist, titleWithoutFeaturing } from "../lib/track-credits.js";

/**
 * Self-learned Chromaprint fingerprint library.
 *
 * Two jobs:
 *  1. **Lookup**: given a fresh fingerprint, find the closest known recording
 *     without calling AcoustID or MusicBrainz. This is a local, free, unlimited
 *     identifier — repeat plays never hit external APIs.
 *  2. **Learn**: when an external resolver (AcoustID / trusted stream metadata)
 *     confirms a match, persist the fingerprint so next time we can answer
 *     locally. The library gets better the longer the monitor runs.
 *
 * Matching strategy (fast path first):
 *  - Exact match by SHA-1 of the fingerprint string (same capture → instant hit).
 *  - Prefix match (first 48 chars of the fingerprint string) scoped by duration
 *    within ±3 seconds. Chromaprint is deterministic for a given audio segment,
 *    so the prefix is a very tight candidate filter.
 *  - For each candidate we compute a character-level similarity ratio and
 *    require it clears `LOCAL_FP_MIN_SIMILARITY` (default 0.80). That tolerates
 *    the small differences you get between captures of the same song from the
 *    same stream.
 */

const EXACT_PREFIX_SIMILARITY_DEFAULT = 0.8;
const CHROMAPRINT_BER_THRESHOLD_DEFAULT = 0.35;
const PREFIX_LEN = 48;
const DURATION_TOLERANCE_SEC_DEFAULT = 30;
const MAX_CHROMAPRINT_CANDIDATES = 300;

function sha1(value: string): string {
  return crypto.createHash("sha1").update(value).digest("hex");
}

function prefix(fp: string): string {
  return fp.slice(0, PREFIX_LEN);
}

function similarityRatio(a: string, b: string): number {
  if (!a || !b) return 0;
  if (a === b) return 1;
  const len = Math.min(a.length, b.length);
  if (len === 0) return 0;
  let same = 0;
  for (let i = 0; i < len; i++) {
    if (a.charCodeAt(i) === b.charCodeAt(i)) same++;
  }
  const maxLen = Math.max(a.length, b.length);
  return same / maxLen;
}

/**
 * Decode a Chromaprint base64 fingerprint into an array of uint32 samples.
 * Chromaprint's base64 is URL-safe (`-`/`_` instead of `+`/`/`) and the body
 * starts with a 4-byte header (version + sample count) followed by compressed
 * subfingerprint bytes. We ignore the structured decoding and just work on the
 * raw bytes as a uint32 array for bitwise comparison — that is enough for
 * same-song recognition because identical audio produces byte-identical output.
 */
function decodeChromaprint(base64: string): Uint32Array | null {
  if (!base64) return null;
  try {
    const normalized = base64.replace(/-/g, "+").replace(/_/g, "/");
    const padded = normalized.padEnd(normalized.length + ((4 - (normalized.length % 4)) % 4), "=");
    const buf = Buffer.from(padded, "base64");
    if (buf.length < 8) return null;
    const trimmed = buf.subarray(4, Math.floor((buf.length - 4) / 4) * 4 + 4);
    if (trimmed.length < 4) return null;
    const arr = new Uint32Array(trimmed.length / 4);
    for (let i = 0; i < arr.length; i++) {
      arr[i] = trimmed.readUInt32LE(i * 4);
    }
    return arr;
  } catch {
    return null;
  }
}

function popcount32(v: number): number {
  let x = v >>> 0;
  x = x - ((x >>> 1) & 0x55555555);
  x = (x & 0x33333333) + ((x >>> 2) & 0x33333333);
  x = (x + (x >>> 4)) & 0x0f0f0f0f;
  return ((x * 0x01010101) >>> 24) & 0xff;
}

/**
 * Best-alignment BER (bit-error-rate) between two Chromaprint sample sequences.
 * Slides `query` over `candidate` with a stride, sums the hamming distance for
 * each aligned sample, and returns the lowest `errors / totalBits`. 0.0 means
 * identical, 0.5 means random, and AcoustID typically calls anything below
 * ~0.35 a confident match.
 */
function bestBitErrorRate(query: Uint32Array, candidate: Uint32Array): number {
  if (!query.length || !candidate.length) return 1;
  if (query.length > candidate.length) {
    return bestBitErrorRate(candidate, query);
  }
  const qn = query.length;
  const cn = candidate.length;
  const stride = Math.max(1, Math.floor(qn / 32));
  let best = 1;
  for (let offset = 0; offset <= cn - qn; offset += stride) {
    let errors = 0;
    for (let i = 0; i < qn; i++) {
      errors += popcount32((query[i] ^ candidate[offset + i]) >>> 0);
    }
    const ber = errors / (qn * 32);
    if (ber < best) best = ber;
    if (best === 0) break;
  }
  return best;
}

function parseFloatEnv(key: string, fallback: number): number {
  const v = process.env[key];
  if (!v) return fallback;
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : fallback;
}

function parseIntEnv(key: string, fallback: number): number {
  const v = process.env[key];
  if (!v) return fallback;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : fallback;
}

function isLearningEnabled(): boolean {
  const v = process.env.LOCAL_FP_LEARNING_ENABLED;
  if (!v) return true;
  const t = String(v).trim().toLowerCase();
  return !(t === "0" || t === "false" || t === "no" || t === "off");
}

function deriveCreditFields(match: MatchResult): {
  displayArtist: string | null;
  titleWithoutFeat: string | null;
  featuredArtistsJson: string | null;
} {
  const title = (match.title ?? "").trim();
  const primary = (match.artist ?? "").trim();
  const displayArtist =
    (match.displayArtist ?? "").trim() ||
    (match.featuredArtists?.length && primary
      ? `${primary} feat. ${match.featuredArtists.join(", ")}`
      : primary || null);
  const titleWo = (match.titleWithoutFeat ?? "").trim() || titleWithoutFeaturing(title) || null;
  const fromTitleFeat = parseFeaturedFromArtist(title).featured;
  const fromCredit = parseFeaturedFromArtist(displayArtist || primary).featured;
  const merged = [...new Set([...(match.featuredArtists ?? []), ...fromCredit, ...fromTitleFeat])].filter(
    (x) => x.length > 0
  );
  const featuredArtistsJson = merged.length ? JSON.stringify(merged) : null;
  return {
    displayArtist: displayArtist || null,
    titleWithoutFeat: titleWo,
    featuredArtistsJson,
  };
}

function durationMsFromMatch(match: MatchResult | null, fpDurationSec: number): number | null {
  if (match?.durationMs && match.durationMs > 0) return Math.round(match.durationMs);
  if (fpDurationSec > 0) return fpDurationSec * 1000;
  return null;
}

export class LocalFingerprintService {
  /**
   * Look up a fingerprint in the local library. Returns a MatchResult when a
   * high-confidence local hit is found — callers can short-circuit AcoustID/MB.
   */
  static async lookup(fp: FingerprintResult): Promise<MatchResult | null> {
    if (!fp?.fingerprint || !fp.duration) return null;

    const prefixSimMin = parseFloatEnv(
      "LOCAL_FP_MIN_SIMILARITY",
      EXACT_PREFIX_SIMILARITY_DEFAULT
    );
    const berThreshold = parseFloatEnv(
      "LOCAL_FP_MAX_BER",
      CHROMAPRINT_BER_THRESHOLD_DEFAULT
    );
    const durationTolerance = parseIntEnv(
      "LOCAL_FP_DURATION_TOLERANCE_SEC",
      DURATION_TOLERANCE_SEC_DEFAULT
    );
    const hash = sha1(fp.fingerprint);
    const pfx = prefix(fp.fingerprint);
    const durMin = Math.max(1, fp.duration - durationTolerance);
    const durMax = fp.duration + durationTolerance;

    try {
      const exact = await prisma.localFingerprint.findUnique({
        where: { fingerprintSha1: hash },
      });
      if (exact) {
        await this.bumpMatchStats(exact.id, 1);
        logger.info(
          {
            localFingerprintId: exact.id,
            title: exact.title,
            artist: exact.artist,
            match: "exact_sha1",
          },
          "Local fingerprint library hit"
        );
        return this.toMatchResult(exact, 1);
      }

      // Fast path: strict prefix + tight duration filter, exact-character similarity.
      const prefixCandidates = await prisma.localFingerprint.findMany({
        where: {
          fingerprintPrefix: pfx,
          durationSec: { gte: durMin, lte: durMax },
        },
        take: 25,
      });
      let best: { row: (typeof prefixCandidates)[number]; similarity: number } | null = null;
      for (const row of prefixCandidates) {
        const s = similarityRatio(row.fingerprint, fp.fingerprint);
        if (!best || s > best.similarity) {
          best = { row, similarity: s };
        }
      }
      if (best && best.similarity >= prefixSimMin) {
        await this.bumpMatchStats(best.row.id, best.similarity);
        logger.info(
          {
            localFingerprintId: best.row.id,
            title: best.row.title,
            artist: best.row.artist,
            similarity: Number(best.similarity.toFixed(3)),
            candidates: prefixCandidates.length,
            match: "prefix_similarity",
          },
          "Local fingerprint library hit"
        );
        return this.toMatchResult(best.row, best.similarity);
      }

      // Slow path: Chromaprint-style bitwise alignment against candidates with a
      // compatible duration. This catches repeat plays where the capture happens
      // to start at a different offset in the track.
      const query = decodeChromaprint(fp.fingerprint);
      if (!query) return null;

      const durationCandidates = await prisma.localFingerprint.findMany({
        where: {
          durationSec: { gte: durMin, lte: durMax },
        },
        take: MAX_CHROMAPRINT_CANDIDATES,
        orderBy: { lastMatchedAt: "desc" },
      });

      let bestAligned: {
        row: (typeof durationCandidates)[number];
        ber: number;
      } | null = null;
      for (const row of durationCandidates) {
        const cand = decodeChromaprint(row.fingerprint);
        if (!cand) continue;
        const ber = bestBitErrorRate(query, cand);
        if (!bestAligned || ber < bestAligned.ber) {
          bestAligned = { row, ber };
        }
        if (bestAligned.ber === 0) break;
      }

      if (bestAligned && bestAligned.ber <= berThreshold) {
        const similarity = 1 - bestAligned.ber;
        await this.bumpMatchStats(bestAligned.row.id, similarity);
        logger.info(
          {
            localFingerprintId: bestAligned.row.id,
            title: bestAligned.row.title,
            artist: bestAligned.row.artist,
            ber: Number(bestAligned.ber.toFixed(3)),
            similarity: Number(similarity.toFixed(3)),
            candidates: durationCandidates.length,
            match: "chromaprint_ber",
          },
          "Local fingerprint library hit"
        );
        return this.toMatchResult(bestAligned.row, similarity);
      }

      return null;
    } catch (error) {
      logger.warn({ error }, "Local fingerprint lookup failed");
      return null;
    }
  }

  /**
   * Learn a fingerprint from a confirmed resolver result (AcoustID / MB / trusted
   * ICY + audio). Safe to call for every successful match — dedupes by SHA-1.
   */
  static async learn(input: {
    fp: FingerprintResult;
    match: MatchResult | null;
    metadata?: NormalizedMetadata | null;
    source: "acoustid" | "stream_metadata" | "manual";
  }): Promise<void> {
    if (!isLearningEnabled()) return;
    const { fp, match, metadata, source } = input;
    if (!fp?.fingerprint || !fp.duration) return;

    const title = (match?.title || metadata?.rawTitle || "").trim() || null;
    const artist = (match?.artist || metadata?.rawArtist || "").trim() || null;
    if (!title && !artist) {
      return;
    }

    const hash = sha1(fp.fingerprint);
    const pfx = prefix(fp.fingerprint);
    const confidence = match?.confidence ?? match?.score ?? 0.7;
    const isrcsJson =
      match?.isrcs && match.isrcs.length ? JSON.stringify(match.isrcs) : null;
    const credits = match ? deriveCreditFields(match) : { displayArtist: null, titleWithoutFeat: null, featuredArtistsJson: null };
    const durationMs = durationMsFromMatch(match, fp.duration);

    const rich = {
      releaseTitle: match?.releaseTitle ?? null,
      releaseDate: match?.releaseDate ?? null,
      genre: match?.genre ?? null,
      displayArtist: credits.displayArtist,
      titleWithoutFeat: credits.titleWithoutFeat,
      featuredArtistsJson: credits.featuredArtistsJson,
      labelName: match?.labelName?.trim() || null,
      countryCode: match?.countryCode?.trim() || null,
      durationMs,
      acoustidTrackId: match?.acoustidTrackId ?? null,
      recordingMbid: match?.recordingId ?? null,
      isrcsJson,
    };

    try {
      const existing = await prisma.localFingerprint.findUnique({
        where: { fingerprintSha1: hash },
      });
      if (existing) {
        await prisma.localFingerprint.update({
          where: { id: existing.id },
          data: {
            title: title ?? existing.title,
            artist: artist ?? existing.artist,
            releaseTitle: rich.releaseTitle ?? existing.releaseTitle,
            releaseDate: rich.releaseDate ?? existing.releaseDate,
            genre: rich.genre ?? existing.genre,
            displayArtist: rich.displayArtist ?? existing.displayArtist,
            titleWithoutFeat: rich.titleWithoutFeat ?? existing.titleWithoutFeat,
            featuredArtistsJson: rich.featuredArtistsJson ?? existing.featuredArtistsJson,
            labelName: rich.labelName ?? existing.labelName,
            countryCode: rich.countryCode ?? existing.countryCode,
            durationMs: rich.durationMs ?? existing.durationMs,
            acoustidTrackId: rich.acoustidTrackId ?? existing.acoustidTrackId,
            recordingMbid: rich.recordingMbid ?? existing.recordingMbid,
            isrcsJson: rich.isrcsJson ?? existing.isrcsJson,
            confidence: Math.max(existing.confidence, confidence),
            timesMatched: existing.timesMatched + 1,
            lastMatchedAt: new Date(),
          },
        });
        return;
      }

      await prisma.localFingerprint.create({
        data: {
          fingerprint: fp.fingerprint,
          fingerprintSha1: hash,
          fingerprintPrefix: pfx,
          durationSec: fp.duration,
          title,
          artist,
          ...rich,
          source,
          confidence,
        },
      });
      logger.info(
        { title, artist, source, confidence },
        "Local fingerprint library learned a new recording"
      );
    } catch (error) {
      logger.warn({ error, source }, "Failed to persist learned fingerprint");
    }
  }

  /**
   * Increment aggregate play count on library rows that represent the same identified song
   * (MusicBrainz id preferred, else primary artist + title).
   */
  static async bumpPlayAggregates(params: {
    recordingMbid?: string | null;
    artist?: string | null;
    title?: string | null;
  }): Promise<void> {
    const mbid = params.recordingMbid?.trim();
    const artist = (params.artist ?? "").trim();
    const title = (params.title ?? "").trim();
    try {
      if (mbid) {
        await prisma.localFingerprint.updateMany({
          where: { recordingMbid: mbid },
          data: { playCountTotal: { increment: 1 }, lastMatchedAt: new Date() },
        });
        return;
      }
      if (artist.length >= 2 && title.length >= 2) {
        await prisma.localFingerprint.updateMany({
          where: { artist, title },
          data: { playCountTotal: { increment: 1 }, lastMatchedAt: new Date() },
        });
      }
    } catch (error) {
      logger.debug({ error }, "bumpPlayAggregates skipped");
    }
  }

  private static async bumpMatchStats(id: string, similarity: number): Promise<void> {
    try {
      await prisma.localFingerprint.update({
        where: { id },
        data: {
          timesMatched: { increment: 1 },
          lastMatchedAt: new Date(),
          confidence: similarity,
        },
      });
    } catch (error) {
      logger.debug({ error, id }, "Failed to bump local fingerprint stats (non-fatal)");
    }
  }

  private static toMatchResult(
    row: {
      id: string;
      title: string | null;
      artist: string | null;
      displayArtist: string | null;
      titleWithoutFeat: string | null;
      featuredArtistsJson: string | null;
      releaseTitle: string | null;
      releaseDate: string | null;
      genre: string | null;
      labelName: string | null;
      countryCode: string | null;
      durationMs: number | null;
      acoustidTrackId: string | null;
      recordingMbid: string | null;
      isrcsJson: string | null;
      durationSec: number;
    },
    similarity: number
  ): MatchResult {
    let isrcs: string[] | undefined;
    if (row.isrcsJson) {
      try {
        const parsed = JSON.parse(row.isrcsJson);
        if (Array.isArray(parsed)) {
          isrcs = parsed.filter((x): x is string => typeof x === "string");
        }
      } catch {
        isrcs = undefined;
      }
    }
    let featuredArtists: string[] | undefined;
    if (row.featuredArtistsJson) {
      try {
        const parsed = JSON.parse(row.featuredArtistsJson);
        if (Array.isArray(parsed)) {
          featuredArtists = parsed.filter((x): x is string => typeof x === "string");
        }
      } catch {
        featuredArtists = undefined;
      }
    }
    const durationMs =
      typeof row.durationMs === "number" && row.durationMs > 0
        ? row.durationMs
        : row.durationSec > 0
          ? row.durationSec * 1000
          : undefined;
    return {
      score: similarity,
      confidence: similarity,
      recordingId: row.recordingMbid ?? undefined,
      acoustidTrackId: row.acoustidTrackId ?? undefined,
      title: row.title ?? undefined,
      artist: row.artist ?? undefined,
      displayArtist: row.displayArtist ?? undefined,
      titleWithoutFeat: row.titleWithoutFeat ?? undefined,
      featuredArtists,
      releaseTitle: row.releaseTitle ?? undefined,
      releaseDate: row.releaseDate ?? undefined,
      genre: row.genre ?? undefined,
      labelName: row.labelName ?? undefined,
      countryCode: row.countryCode ?? undefined,
      isrcs,
      durationMs,
      sourceProvider: "local_fingerprint",
      reasonCode: "local_fingerprint_library",
    };
  }

  static async stats(): Promise<{
    totalRecordings: number;
    totalMatches: number;
    latestLearnedAt: Date | null;
    latestMatchedAt: Date | null;
  }> {
    try {
      const total = await prisma.localFingerprint.count();
      const agg = await prisma.localFingerprint.aggregate({
        _sum: { timesMatched: true },
        _max: { firstLearnedAt: true, lastMatchedAt: true },
      });
      return {
        totalRecordings: total,
        totalMatches: Number(agg._sum.timesMatched ?? 0),
        latestLearnedAt: agg._max.firstLearnedAt ?? null,
        latestMatchedAt: agg._max.lastMatchedAt ?? null,
      };
    } catch (error) {
      logger.warn({ error }, "Failed to read local fingerprint stats");
      return {
        totalRecordings: 0,
        totalMatches: 0,
        latestLearnedAt: null,
        latestMatchedAt: null,
      };
    }
  }
}
