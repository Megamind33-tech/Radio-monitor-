import type { DetectionMethod, MatchResult } from "../types.js";

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

  if (audio && scaledCatalog) {
    const audioMethod = fingerprintMethodForProvider(audio.sourceProvider);
    const sameTitle = norm(audio.title) === norm(scaledCatalog.title);
    const sameArtist = norm(audio.artist) === norm(scaledCatalog.artist);
    if (sameTitle && sameArtist) {
      return { match: audio, method: audioMethod, reasonCode: null };
    }
    if (audio.score >= minAcoustidPrefer) {
      return {
        match: audio,
        method: audioMethod,
        reasonCode: "acoustid_preferred_over_catalog",
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
    return {
      match: audio,
      method: audioMethod,
      reasonCode: "acoustid_preferred_over_catalog",
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
