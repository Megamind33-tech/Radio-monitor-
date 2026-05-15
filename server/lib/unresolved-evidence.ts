/**
 * Evidence tiers and title-cluster identity for UnresolvedSample recovery.
 * Keeps royalty logs honest: distinguishes fingerprint misses from title-only lanes,
 * weak stream-learned fingerprints, catalogue gaps, and programme/junk metadata.
 */

import type { LocalFingerprint, VerifiedTrack } from "@prisma/client";
import { prisma } from "./prisma.js";
import { classifyMusicContent } from "./music-content-filter.js";
import { normalizeSongPart } from "./song-spin.js";

export const RecoveryReason = {
  FINGERPRINT_UNAVAILABLE: "fingerprint_unavailable",
  FINGERPRINT_EXHAUSTED: "fingerprint_exhausted_high_trust_resolvers",
  FINGERPRINT_ONLY_NO_TITLE: "fingerprint_only_no_title_metadata",
  TITLE_AUTO_VERIFIED: "title_recovery_verified_track_exact_pair",
  TITLE_AUTO_TRUSTED_LOCAL: "title_recovery_trusted_localfp_exact_pair",
  WEAK_METADATA_REVIEW: "weak_metadata_candidate_needs_review",
  NO_EXACT_TITLE_SUPPORT: "no_exact_title_support_needs_catalogue_enrichment",
  PROGRAMME_OR_NON_MUSIC: "programme_or_non_music_metadata",
  DIRTY_WEB_TITLE: "dirty_web_title_metadata",
  PENDING_CLASSIFICATION: "pending_classification",
  RECOVERED_FROM_AUDIO: "recovered_from_unresolved_sample_audio",
} as const;

export type RecoveryReasonCode = (typeof RecoveryReason)[keyof typeof RecoveryReason];

const DIRTY_WEB_PATTERNS: readonly RegExp[] = [
  /\bofficial\s+video\b/i,
  /\bofficial\s+audio\b/i,
  /\b(full\s+)?(hd\s+)?lyrics?\b/i,
  /\bmp3\s+download\b/i,
  /\bfree\s+download\b/i,
  /\bwatch\s*:/i,
  /\bon\s+youtube\b/i,
  /\bvideo\s+premiere\b/i,
  /\b\d{3,}\s*kbps\b/i,
];

export function buildTitleNormKey(
  artist: string | null | undefined,
  title: string | null | undefined
): string | null {
  const t = normalizeSongPart(title);
  if (!t || t.length < 2) return null;
  const a = normalizeSongPart(artist);
  if (!a || a.length < 2) return null;
  return `${a}\t${t}`;
}

export function isDirtyWebTitleText(text: string | null | undefined): boolean {
  const s = (text ?? "").trim();
  if (!s) return false;
  const lower = s.toLowerCase();
  for (const re of DIRTY_WEB_PATTERNS) {
    if (re.test(lower)) return true;
  }
  return false;
}

export function isTrustedLocalFingerprintSource(source: string | null | undefined): boolean {
  const s = (source ?? "").toLowerCase();
  return s === "acoustid" || s === "manual";
}

export function isWeakLocalFingerprintSource(source: string | null | undefined): boolean {
  const s = (source ?? "").toLowerCase();
  if (s === "stream_metadata") return true;
  // Future-proof: treat unknown backfill-ish sources as weak until audited.
  if (s.includes("archive") || s.includes("backfill")) return true;
  return false;
}

function programmeSignals(text: string | null | undefined): boolean {
  const t = (text ?? "").trim();
  if (!t) return false;
  const c = classifyMusicContent(t);
  return !c.isMusic;
}

/**
 * Fast metadata-only screen (no DB). Used to skip expensive fingerprinting for obvious junk/programme rows.
 */
export function screenProgrammeOrDirtyWeb(input: {
  parsedArtist: string | null | undefined;
  parsedTitle: string | null | undefined;
  rawStreamText: string | null | undefined;
}): "dirty_web" | "programme" | null {
  const raw = (input.rawStreamText ?? "").trim();
  const pa = (input.parsedArtist ?? "").trim();
  const pt = (input.parsedTitle ?? "").trim();
  for (const d of [raw, pt, pa]) {
    if (d && isDirtyWebTitleText(d)) return "dirty_web";
  }
  if (programmeSignals(pt) || programmeSignals(raw)) return "programme";
  return null;
}

export type TitleEvidenceAnalysis =
  | { kind: "dirty_web"; reason: typeof RecoveryReason.DIRTY_WEB_TITLE; titleNormKey: string | null }
  | { kind: "programme"; reason: typeof RecoveryReason.PROGRAMME_OR_NON_MUSIC; titleNormKey: string | null }
  | { kind: "fingerprint_only"; reason: typeof RecoveryReason.FINGERPRINT_ONLY_NO_TITLE; titleNormKey: string | null }
  | {
      kind: "verified_exact";
      track: VerifiedTrack;
      titleNormKey: string;
    }
  | {
      kind: "trusted_local_exact";
      row: LocalFingerprint;
      titleNormKey: string;
    }
  | {
      kind: "weak_local_only";
      titleNormKey: string;
      weakRows: LocalFingerprint[];
    }
  | { kind: "no_exact_support"; titleNormKey: string };

export async function analyzeTitleEvidence(input: {
  parsedArtist: string | null | undefined;
  parsedTitle: string | null | undefined;
  rawStreamText: string | null | undefined;
}): Promise<TitleEvidenceAnalysis> {
  const raw = (input.rawStreamText ?? "").trim();
  const pa = (input.parsedArtist ?? "").trim();
  const pt = (input.parsedTitle ?? "").trim();

  const dirtyTargets = [raw, pt, pa].filter(Boolean);
  for (const d of dirtyTargets) {
    if (isDirtyWebTitleText(d)) {
      return { kind: "dirty_web", reason: RecoveryReason.DIRTY_WEB_TITLE, titleNormKey: buildTitleNormKey(pa, pt) };
    }
  }

  if (programmeSignals(pt) || programmeSignals(raw)) {
    return { kind: "programme", reason: RecoveryReason.PROGRAMME_OR_NON_MUSIC, titleNormKey: buildTitleNormKey(pa, pt) };
  }

  const titleNormKey = buildTitleNormKey(pa, pt);
  if (!titleNormKey) {
    return { kind: "fingerprint_only", reason: RecoveryReason.FINGERPRINT_ONLY_NO_TITLE, titleNormKey: null };
  }

  const normArtist = normalizeSongPart(pa);
  const normTitle = normalizeSongPart(pt);

  const verifiedRows = await prisma.$queryRaw<VerifiedTrack[]>`
    SELECT *
    FROM "VerifiedTrack"
    WHERE lower(trim("artist")) = ${normArtist}
      AND lower(trim("title")) = ${normTitle}
    LIMIT 5
  `;
  if (verifiedRows.length > 0) {
    return { kind: "verified_exact", track: verifiedRows[0]!, titleNormKey };
  }

  const lfRows = await prisma.$queryRaw<LocalFingerprint[]>`
    SELECT *
    FROM "LocalFingerprint"
    WHERE lower(trim(ifnull("artist", ''))) = ${normArtist}
      AND lower(trim(ifnull("title", ''))) = ${normTitle}
    LIMIT 80
  `;

  const trusted = lfRows.filter((r) => isTrustedLocalFingerprintSource(r.source));
  const weak = lfRows.filter((r) => isWeakLocalFingerprintSource(r.source));

  if (trusted.length > 0) {
    const best = [...trusted].sort((a, b) => (b.confidence ?? 0) - (a.confidence ?? 0) || (b.timesMatched ?? 0) - (a.timesMatched ?? 0))[0]!;
    return { kind: "trusted_local_exact", row: best, titleNormKey };
  }

  if (weak.length > 0) {
    return { kind: "weak_local_only", titleNormKey, weakRows: weak };
  }

  return { kind: "no_exact_support", titleNormKey };
}

export function recoveryPriorityForEvidence(ev: TitleEvidenceAnalysis): number {
  switch (ev.kind) {
    case "verified_exact":
      return 1000;
    case "trusted_local_exact":
      return 950;
    case "weak_local_only":
      return 520;
    case "no_exact_support":
      return 320;
    case "fingerprint_only":
      return 120;
    case "dirty_web":
    case "programme":
      return 10;
    default:
      return 0;
  }
}

export function matchResultFromVerifiedTrack(track: VerifiedTrack): import("../types.js").MatchResult {
  return {
    score: 1,
    title: track.title,
    artist: track.artist,
    releaseTitle: track.album ?? undefined,
    confidence: 0.99,
    sourceProvider: "musicbrainz_search",
    recordingId: undefined,
    reasonCode: RecoveryReason.TITLE_AUTO_VERIFIED,
  };
}

export function matchResultFromLocalFingerprint(row: LocalFingerprint): import("../types.js").MatchResult {
  return {
    score: Math.max(0.75, row.confidence || 0.85),
    title: row.title || undefined,
    artist: row.artist || undefined,
    releaseTitle: row.releaseTitle ?? undefined,
    releaseDate: row.releaseDate ?? undefined,
    genre: row.genre ?? undefined,
    confidence: Math.max(0.8, row.confidence || 0.85),
    sourceProvider: "local_fingerprint",
    recordingId: row.recordingMbid ?? undefined,
    acoustidTrackId: row.acoustidTrackId ?? undefined,
    durationMs: row.durationMs ?? (row.durationSec ? row.durationSec * 1000 : undefined),
    reasonCode: RecoveryReason.TITLE_AUTO_TRUSTED_LOCAL,
  };
}
