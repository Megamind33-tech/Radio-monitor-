/**
 * MATCHING ENGINE V2 — formal evidence and fusion outputs.
 * Used as the shared contract for coordinated collection + arbitration (live + recovery).
 */

import type { DetectionMethod } from "../types.js";

/** Which subsystem produced a piece of evidence. */
export type MatchEvidenceType =
  | "icy_metadata"
  | "provider_metadata"
  | "local_fingerprint"
  | "acoustid"
  | "audd"
  | "acrcloud"
  | "trusted_catalog"
  | "historical_station_signal";

/** Coarse trust for policy / fusion (distinct from numeric confidence). */
export type EvidenceTrustTier = "high" | "medium" | "low" | "untrusted";

export interface MatchEvidence {
  evidenceType: MatchEvidenceType;
  /** e.g. stream_icy, fastcast, icecast_status, shoutcast, tunein */
  sourceProvider?: string;
  rawText?: string;
  parsedArtist?: string;
  parsedTitle?: string;
  normalizedArtist?: string;
  normalizedTitle?: string;
  confidence: number;
  /** Lane-specific strength (e.g. AcoustID score, local similarity). */
  score?: number;
  recordingMbid?: string;
  acoustidTrackId?: string;
  isrcs?: string[];
  durationMs?: number;
  timestamp: string;
  stationId: string;
  audioSamplePath?: string;
  contradictionFlags?: string[];
  qualityFlags?: string[];
  staleFlag?: boolean;
  junkFlag?: boolean;
  evidenceTrustTier: EvidenceTrustTier;
}

export type FusedMatchStatus =
  | "matched"
  | "unresolved"
  | "candidate_review"
  | "non_music"
  | "contradictory_evidence";

export interface FusedMatchDecision {
  status: FusedMatchStatus;
  finalArtist?: string;
  finalTitle?: string;
  finalRecordingMbid?: string;
  finalSourceProvider?: string;
  finalDetectionMethod: DetectionMethod;
  finalConfidence: number;
  reasonCode: string;
  winningEvidence?: MatchEvidence;
  supportingEvidence: MatchEvidence[];
  conflictingEvidence: MatchEvidence[];
  shouldLearnFingerprint: boolean;
  shouldArchiveUnresolved: boolean;
  shouldQueueRecovery: boolean;
  /** Serializable arbitration trace (scores, thresholds, lane suppressions). */
  diagnosticsJson?: string;
}
