import type { DetectionMethod, MatchResult } from "../types.js";

function norm(s: string | null | undefined): string {
  return (s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

/**
 * When AcoustID and text catalog both return candidates, prefer AcoustID if score
 * clears threshold or if catalog disagrees (closer identification from audio).
 */
export function mergeAcoustidAndCatalog(
  audio: MatchResult | null,
  catalog: MatchResult | null,
  minAcoustidPrefer: number
): {
  match: MatchResult | null;
  method: DetectionMethod;
  reasonCode: string | null;
} {
  if (audio && catalog) {
    const sameTitle = norm(audio.title) === norm(catalog.title);
    const sameArtist = norm(audio.artist) === norm(catalog.artist);
    if (sameTitle && sameArtist) {
      return { match: audio, method: "fingerprint_acoustid", reasonCode: null };
    }
    if (audio.score >= minAcoustidPrefer) {
      return {
        match: audio,
        method: "fingerprint_acoustid",
        reasonCode: "acoustid_preferred_over_catalog",
      };
    }
    if (catalog.confidence >= 0.88) {
      return {
        match: catalog,
        method: "catalog_lookup",
        reasonCode: "catalog_higher_confidence_than_acoustid",
      };
    }
    return {
      match: audio,
      method: "fingerprint_acoustid",
      reasonCode: "acoustid_preferred_over_catalog",
    };
  }
  if (audio) {
    return { match: audio, method: "fingerprint_acoustid", reasonCode: null };
  }
  if (catalog) {
    return { match: catalog, method: "catalog_lookup", reasonCode: null };
  }
  return { match: null, method: "stream_metadata", reasonCode: null };
}
