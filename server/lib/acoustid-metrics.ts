/**
 * In-process AcoustID operational counters (since process start).
 * Surfaces near-real-time contribution vs "key set but never fires".
 */

import type { MatchResult, NormalizedMetadata } from "../types.js";

function norm(s: string | null | undefined): string {
  return (s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

export type AcoustidMetricsSnapshot = {
  /** Any client key (registered or open) present. */
  acoustidConfigured: boolean;
  acoustidSkipsNoClient: number;
  acoustidCalls: number;
  acoustidHits: number;
  acoustidMisses: number;
  acoustidLowScoreRejects: number;
  acoustidEmptyOrBadStatus: number;
  acoustidMissingRecording: number;
  acoustidRequestFailures: number;
  acoustidFinalWins: number;
  acoustidAgreementWithMetadata: number;
  acoustidContradictionsWithMetadata: number;
  acoustidMetadataNotComparable: number;
};

const m = {
  skipsNoClient: 0,
  calls: 0,
  hits: 0,
  misses: 0,
  lowScoreRejects: 0,
  emptyOrBadStatus: 0,
  missingRecording: 0,
  requestFailures: 0,
  finalWins: 0,
  agreementWithMetadata: 0,
  contradictionsWithMetadata: 0,
  metadataNotComparable: 0,
};

export function isAcoustidClientConfigured(): boolean {
  return !!(process.env.ACOUSTID_API_KEY || process.env.ACOUSTID_OPEN_CLIENT);
}

export function getAcoustidMetricsSnapshot(): AcoustidMetricsSnapshot {
  return {
    acoustidConfigured: isAcoustidClientConfigured(),
    acoustidSkipsNoClient: m.skipsNoClient,
    acoustidCalls: m.calls,
    acoustidHits: m.hits,
    acoustidMisses: m.misses,
    acoustidLowScoreRejects: m.lowScoreRejects,
    acoustidEmptyOrBadStatus: m.emptyOrBadStatus,
    acoustidMissingRecording: m.missingRecording,
    acoustidRequestFailures: m.requestFailures,
    acoustidFinalWins: m.finalWins,
    acoustidAgreementWithMetadata: m.agreementWithMetadata,
    acoustidContradictionsWithMetadata: m.contradictionsWithMetadata,
    acoustidMetadataNotComparable: m.metadataNotComparable,
  };
}

/** Test-only reset. */
export function resetAcoustidMetricsForTests(): void {
  m.skipsNoClient = 0;
  m.calls = 0;
  m.hits = 0;
  m.misses = 0;
  m.lowScoreRejects = 0;
  m.emptyOrBadStatus = 0;
  m.missingRecording = 0;
  m.requestFailures = 0;
  m.finalWins = 0;
  m.agreementWithMetadata = 0;
  m.contradictionsWithMetadata = 0;
  m.metadataNotComparable = 0;
}

export function recordAcoustidSkipNoClient(): void {
  m.skipsNoClient += 1;
}

export function recordAcoustidApiCall(): void {
  m.calls += 1;
}

export function recordAcoustidRequestFailure(): void {
  m.misses += 1;
  m.requestFailures += 1;
}

export function recordAcoustidEmptyOrBadStatus(): void {
  m.misses += 1;
  m.emptyOrBadStatus += 1;
}

export function recordAcoustidLowScoreReject(): void {
  m.misses += 1;
  m.lowScoreRejects += 1;
}

export function recordAcoustidMissingRecording(): void {
  m.misses += 1;
  m.missingRecording += 1;
}

export function recordAcoustidHit(): void {
  m.hits += 1;
}

/**
 * When AcoustID returned a candidate, compare to parsed stream metadata (if any).
 */
export function recordAcoustidMetadataComparison(
  acoustidMatch: MatchResult,
  metadata: NormalizedMetadata | null | undefined
): void {
  const icyArtist = norm(metadata?.rawArtist);
  const icyTitle = norm(metadata?.rawTitle);
  if (icyArtist.length < 2 || icyTitle.length < 2) {
    m.metadataNotComparable += 1;
    return;
  }
  const fpArtist = norm(acoustidMatch.artist);
  const fpTitle = norm(acoustidMatch.title);
  if (fpArtist.length < 2 || fpTitle.length < 2) {
    m.metadataNotComparable += 1;
    return;
  }
  if (fpArtist === icyArtist && fpTitle === icyTitle) {
    m.agreementWithMetadata += 1;
  } else {
    m.contradictionsWithMetadata += 1;
  }
}

/**
 * After merge + second-pass catalog: AcoustID audio identity was the persisted fingerprint win.
 */
export function recordAcoustidFinalWinIfApplicable(
  finalMatch: MatchResult | null,
  finalMethod: string
): void {
  if (!finalMatch) return;
  if (finalMethod !== "fingerprint_acoustid") return;
  const sp = finalMatch.sourceProvider ?? "";
  if (sp !== "acoustid" && sp !== "acoustid_open") return;
  m.finalWins += 1;
}
