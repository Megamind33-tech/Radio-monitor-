import axios from "axios";
import { logger } from "../lib/logger.js";
import { FingerprintResult, MatchResult } from "../types.js";

/** AcoustID recording object (subset of API JSON). */
interface AcoustidRecordingJson {
  id?: string;
  title?: string;
  duration?: number;
  artists?: Array<{ name?: string }>;
  /** Number of contributing fingerprints in AcoustID DB — Picard/Beets use this to prefer stable IDs. */
  sources?: number;
}

interface AcoustidResultJson {
  id?: string;
  score?: number;
  recordings?: AcoustidRecordingJson[];
}

function parseEnvFloat(key: string, fallback: number): number {
  const raw = process.env[key];
  if (raw == null || raw === "") return fallback;
  const n = Number.parseFloat(raw);
  return Number.isFinite(n) ? n : fallback;
}

function parseEnvInt(key: string, fallback: number): number {
  const raw = process.env[key];
  if (raw == null || raw === "") return fallback;
  const n = Number.parseInt(raw, 10);
  return Number.isFinite(n) ? n : fallback;
}

/**
 * Picard/Beets-style rules for Chromaprint → AcoustID → MusicBrainz:
 * - Require a minimum AcoustID **track** score (fingerprint vs cluster).
 * - Prefer recordings with more **sources** (community confidence).
 * - When the analyzed clip is long enough to approximate full-track length,
 *   prefer recordings whose MB duration agrees (reduces wrong-recording picks on ambiguous clusters).
 * - Scan **all** API results and **all** recordings, not only `results[0].recordings[0]`.
 */
function pickBestRecording(
  fp: FingerprintResult,
  results: AcoustidResultJson[]
): { recording: AcoustidRecordingJson; trackScore: number; acoustidTrackId?: string; rankWeight: number } | null {
  const minTrackScore = parseEnvFloat("ACOUSTID_MIN_SCORE", 0.52);
  /** Only compare `fp.duration` to `recording.duration` when the sample is this long (seconds). Short stream clips are not full-track length. */
  const durationApplyMinSampleSec = parseEnvInt("ACOUSTID_DURATION_APPLY_MIN_SAMPLE_SEC", 90);
  const durationToleranceSec = parseEnvFloat("ACOUSTID_DURATION_TOLERANCE_SEC", 4);
  const durationMatchBonus = parseEnvFloat("ACOUSTID_DURATION_MATCH_BONUS", 1.06);
  /** Max fractional boost from `sources` (log-scaled). */
  const sourcesBoostCap = parseEnvFloat("ACOUSTID_SOURCES_BOOST_CAP", 0.12);

  let best: {
    recording: AcoustidRecordingJson;
    trackScore: number;
    acoustidTrackId?: string;
    rankWeight: number;
  } | null = null;

  for (const result of results) {
    const trackScore = typeof result.score === "number" ? result.score : 0;
    if (trackScore < minTrackScore) continue;

    const trackId = typeof result.id === "string" ? result.id : undefined;
    const recordings = result.recordings ?? [];

    for (const recording of recordings) {
      if (!recording.id) continue;

      let rankWeight = trackScore;

      const sources = typeof recording.sources === "number" && recording.sources > 0 ? recording.sources : 0;
      if (sources > 0) {
        rankWeight *= 1 + Math.min(sourcesBoostCap, Math.log1p(sources) / 18);
      }

      const recDurSec =
        typeof recording.duration === "number" && recording.duration > 0 ? recording.duration : null;
      if (recDurSec != null && fp.duration >= durationApplyMinSampleSec) {
        if (Math.abs(fp.duration - recDurSec) <= durationToleranceSec) {
          rankWeight *= durationMatchBonus;
        }
      }

      if (!best || rankWeight > best.rankWeight) {
        best = { recording, trackScore, acoustidTrackId: trackId, rankWeight };
      }
    }
  }

  return best;
}

export class AcoustidService {
  private static lastRequestAt: number = 0;
  private static readonly RATE_LIMIT_MS = 500; // 2 req/sec to be safe (Limit is 3)

  private static async throttle() {
    const now = Date.now();
    const wait = this.lastRequestAt + this.RATE_LIMIT_MS - now;
    if (wait > 0) {
      await new Promise((resolve) => setTimeout(resolve, wait));
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

      const response = await axios.post(
        "https://api.acoustid.org/v2/lookup",
        new URLSearchParams({
          client: apiKey,
          duration: fp.duration.toString(),
          fingerprint: fp.fingerprint,
          // Picard-style: enough MB context for enrichment and for source/duration ranking.
          meta: "recordings releasegroups releases tracks compress sources",
        }),
        {
          headers: { "Content-Type": "application/x-www-form-urlencoded" },
          timeout: 10000,
        }
      );

      const data = response.data;
      if (data.status !== "ok" || !data.results || data.results.length === 0) {
        logger.debug({ status: data.status }, "AcoustID returned no results");
        return null;
      }

      const picked = pickBestRecording(fp, data.results as AcoustidResultJson[]);
      if (!picked) {
        const minTrackScore = parseEnvFloat("ACOUSTID_MIN_SCORE", 0.52);
        logger.debug({ minTrackScore }, "AcoustID: no recording passed score / empty recordings");
        return null;
      }

      const { recording, trackScore, acoustidTrackId, rankWeight } = picked;
      const durSec = typeof recording.duration === "number" && recording.duration > 0 ? recording.duration : undefined;

      const match: MatchResult = {
        score: trackScore,
        recordingId: recording.id,
        acoustidTrackId,
        title: recording.title,
        artist: recording.artists?.[0]?.name,
        durationMs: durSec ? Math.round(durSec * 1000) : undefined,
        sourceProvider: configuredClient ? "acoustid" : "acoustid_open",
        confidence: trackScore,
        reasonCode: configuredClient ? "fingerprint_acoustid" : "fingerprint_acoustid_open",
      };

      logger.debug(
        {
          trackScore,
          rankWeight,
          recordingId: recording.id,
          sources: recording.sources,
          candidates: (data.results as AcoustidResultJson[]).length,
        },
        "AcoustID best recording selected (sources/duration-weighted)"
      );

      return match;
    } catch (error) {
      logger.error({ error }, "AcoustID API request failed");
      return null;
    }
  }
}
