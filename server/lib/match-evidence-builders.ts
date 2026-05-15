/**
 * Build MatchEvidence rows from normalized stream/provider metadata and resolver matches.
 */

import type { MatchResult, NormalizedMetadata } from "../types.js";
import type { EvidenceTrustTier, MatchEvidence, MatchEvidenceType } from "./match-engine-v2-types.js";

export function normEvidenceText(s: string | null | undefined): string {
  return (s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function trustFromMetaScore(metaTrust01: number, splitConfidence?: number): EvidenceTrustTier {
  if (metaTrust01 <= 0) return "untrusted";
  const sc = typeof splitConfidence === "number" ? splitConfidence : 0.75;
  if (metaTrust01 >= 1 && sc >= 0.85) return "high";
  if (metaTrust01 >= 0.5) return "medium";
  return "low";
}

export function evidenceFromNormalizedMetadata(input: {
  evidenceType: "icy_metadata" | "provider_metadata";
  meta: NormalizedMetadata;
  stationId: string;
  metaTrust01: number;
  staleFlag?: boolean;
  junkFlag?: boolean;
  qualityFlags?: string[];
  contradictionFlags?: string[];
  sourceLabel?: string;
}): MatchEvidence {
  const {
    evidenceType,
    meta,
    stationId,
    metaTrust01,
    staleFlag,
    junkFlag,
    qualityFlags,
    contradictionFlags,
    sourceLabel,
  } = input;
  const combined = (meta.combinedRaw ?? "").trim();
  const rawTitle = (meta.rawTitle ?? "").trim();
  const rawArtist = (meta.rawArtist ?? "").trim();
  const confidence =
    typeof meta.splitConfidence === "number" && meta.splitConfidence > 0 ? meta.splitConfidence : metaTrust01;

  return {
    evidenceType,
    sourceProvider: sourceLabel ?? meta.sourceType ?? "stream_metadata",
    rawText: combined.slice(0, 500),
    parsedArtist: rawArtist || undefined,
    parsedTitle: rawTitle || undefined,
    normalizedArtist: normEvidenceText(rawArtist) || undefined,
    normalizedTitle: normEvidenceText(rawTitle) || undefined,
    confidence: Math.max(0, Math.min(1, confidence)),
    score: typeof meta.splitConfidence === "number" ? meta.splitConfidence : undefined,
    timestamp: new Date().toISOString(),
    stationId,
    contradictionFlags: contradictionFlags?.length ? [...contradictionFlags] : undefined,
    qualityFlags: qualityFlags?.length ? [...qualityFlags] : undefined,
    staleFlag,
    junkFlag,
    evidenceTrustTier: trustFromMetaScore(metaTrust01, meta.splitConfidence),
  };
}

export function evidenceFromMatchResult(input: {
  evidenceType: MatchEvidenceType;
  match: MatchResult;
  stationId: string;
  audioSamplePath?: string;
  trustTier?: EvidenceTrustTier;
}): MatchEvidence {
  const { evidenceType, match, stationId, audioSamplePath, trustTier } = input;
  const title = (match.title ?? "").trim();
  const artist = (match.artist ?? "").trim();
  const tier: EvidenceTrustTier =
    trustTier ??
    (evidenceType === "local_fingerprint"
      ? "high"
      : evidenceType === "acoustid"
        ? "high"
        : evidenceType === "audd" || evidenceType === "acrcloud"
          ? "high"
          : "medium");

  return {
    evidenceType,
    sourceProvider: match.sourceProvider,
    rawText: [artist, title].filter(Boolean).join(" - ").slice(0, 500),
    parsedArtist: artist || undefined,
    parsedTitle: title || undefined,
    normalizedArtist: normEvidenceText(artist) || undefined,
    normalizedTitle: normEvidenceText(title) || undefined,
    confidence: Math.max(0, Math.min(1, match.confidence ?? match.score ?? 0)),
    score: match.score,
    recordingMbid: match.recordingId,
    acoustidTrackId: match.acoustidTrackId,
    isrcs: match.isrcs,
    durationMs: match.durationMs,
    timestamp: new Date().toISOString(),
    stationId,
    audioSamplePath,
    evidenceTrustTier: tier,
  };
}

/** True when both lanes have substantive text and they clearly disagree (not substring). */
export function icyProviderCombinedDisagree(
  icy: NormalizedMetadata | null,
  provider: NormalizedMetadata | null
): boolean {
  const a = normEvidenceText(icy?.combinedRaw ?? "");
  const b = normEvidenceText(provider?.combinedRaw ?? "");
  if (a.length < 6 || b.length < 6) return false;
  if (a === b) return false;
  if (a.includes(b) || b.includes(a)) return false;
  return true;
}
