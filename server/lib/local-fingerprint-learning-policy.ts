import type { DetectionMethod, MatchResult } from "../types.js";

/** Stored on LocalFingerprint.source (free string; dashboard groups by value). */
export type LocalFingerprintLearnSource = "acoustid" | "stream_metadata" | "manual" | "catalog";

function parseFloatEnv(key: string, fallback: number): number {
  const v = process.env[key];
  if (!v) return fallback;
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : fallback;
}

/**
 * Map a resolved MatchResult back to a coarse library provenance label.
 */
export function learnSourceFromMatch(match: MatchResult | null): LocalFingerprintLearnSource {
  const p = (match?.sourceProvider ?? "").toLowerCase();
  if (p.includes("acoustid")) return "acoustid";
  if (p === "audd" || p === "acrcloud") return "manual";
  if (
    p.includes("itunes") ||
    p.includes("deezer") ||
    p.includes("musicbrainz") ||
    p.includes("catalog")
  ) {
    return "catalog";
  }
  return "stream_metadata";
}

/**
 * Whether this detection method + match is strong enough to teach the canonical
 * LocalFingerprint table from captured audio (avoids poisoning from weak catalog guesses).
 */
export function isTrustedIdentityForLocalLearning(method: DetectionMethod, match: MatchResult | null): boolean {
  if (!match) return false;
  const title = (match.title ?? "").trim();
  const artist = (match.artist ?? "").trim();
  if (title.length < 2 || artist.length < 2) return false;

  if (
    method === "fingerprint_local" ||
    method === "fingerprint_acoustid" ||
    method === "fingerprint_audd" ||
    method === "fingerprint_acrcloud"
  ) {
    return true;
  }

  if (method === "catalog_lookup") {
    const minCat = parseFloatEnv("LOCAL_FP_LEARN_MIN_CATALOG_CONFIDENCE", 0.82);
    return (match.confidence ?? 0) >= minCat;
  }

  if (method === "stream_metadata") {
    return !!(match.recordingId || match.acoustidTrackId);
  }

  return false;
}
