import type { DetectionMethod, MatchResult } from "../types.js";

function parseEnvFloat(key: string, fallback: number): number {
  const v = process.env[key];
  if (!v) return fallback;
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : fallback;
}

function norm(s: string | null | undefined): string {
  return (s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function fingerprintMethodForProvider(provider: MatchResult["sourceProvider"]): DetectionMethod {
  if (provider === "local_fingerprint") {
    return "fingerprint_local";
  }
  if (provider === "audd") {
    return "fingerprint_audd";
  }
  return "fingerprint_acoustid";
}

/**
 * When AcoustID and text catalog both return candidates, prefer AcoustID if score
 * clears threshold or if catalog disagrees (closer identification from audio).
 */
export function mergeAcoustidAndCatalog(
  audio: MatchResult | null,
  catalog: MatchResult | null,
  minAcoustidPrefer: number,
  /** When <1, scales catalog confidence so marginal AcoustID can win (suspicious ICY). */
  catalogTrustFactor: number = 1
): {
  match: MatchResult | null;
  method: DetectionMethod;
  reasonCode: string | null;
} {
  const trust = Math.max(0.2, Math.min(1, catalogTrustFactor));
  const scaledCatalog =
    catalog && trust < 1
      ? { ...catalog, confidence: (catalog.confidence ?? 0) * trust }
      : catalog;

  /** When audio disagrees with catalog, require at least this AcoustID score to prefer audio (accuracy). */
  const minAcoustidOverCatalog = parseEnvFloat("ACOUSTID_MIN_SCORE_OVER_CATALOG", 0.62);

  if (audio && scaledCatalog) {
    const audioMethod = fingerprintMethodForProvider(audio.sourceProvider);
    const sameTitle = norm(audio.title) === norm(scaledCatalog.title);
    const sameArtist = norm(audio.artist) === norm(scaledCatalog.artist);
    if (sameTitle && sameArtist) {
      return { match: audio, method: audioMethod, reasonCode: null };
    }
    /** Disagreement: do not let marginal AcoustID beat catalog (see ACOUSTID_MIN_SCORE_OVER_CATALOG). */
    if (audio.score >= minAcoustidOverCatalog) {
      return {
        match: audio,
        method: audioMethod,
        reasonCode:
          audio.score >= minAcoustidPrefer
            ? "acoustid_preferred_over_catalog"
            : "acoustid_preferred_marginal_over_catalog",
      };
    }
    if (scaledCatalog.confidence >= 0.88) {
      return {
        match: scaledCatalog,
        method: "catalog_lookup",
        reasonCode:
          trust < 1 ? "catalog_higher_confidence_than_acoustid_scaled" : "catalog_higher_confidence_than_acoustid",
      };
    }
    const catConf = scaledCatalog.confidence ?? 0;
    /** Catalog this weak cannot veto a fingerprint that cleared minAcoustidPrefer. */
    const catalogWeakCeiling = parseEnvFloat("CATALOG_WEAK_CONFIDENCE_FOR_FP", 0.65);
    if (audio.score >= minAcoustidPrefer && catConf < catalogWeakCeiling) {
      return {
        match: audio,
        method: audioMethod,
        reasonCode: "acoustid_preferred_weak_catalog",
      };
    }
    return {
      match: scaledCatalog,
      method: "catalog_lookup",
      reasonCode: "catalog_preferred_low_acoustid_score",
    };
  }
  if (audio) {
    return { match: audio, method: fingerprintMethodForProvider(audio.sourceProvider), reasonCode: null };
  }
  if (scaledCatalog) {
    return { match: scaledCatalog, method: "catalog_lookup", reasonCode: trust < 1 ? "catalog_only_low_trust" : null };
  }
  return { match: null, method: "stream_metadata", reasonCode: null };
}
