/**
 * MATCHING ENGINE V2 — central arbitration: audio vs catalog merge, second-pass catalog,
 * and FusedMatchDecision assembly. Implementations for numeric merge rules stay in
 * `audio-id-merge.ts`; this service is the only production entry callers should use.
 */

import type { DetectionMethod, MatchResult, NormalizedMetadata } from "../types.js";
import type { FusedMatchDecision, MatchEvidence } from "../lib/match-engine-v2-types.js";
import { mergeAcoustidAndCatalog } from "../lib/audio-id-merge.js";
import { CatalogLookupService } from "./catalog-lookup.service.js";
import {
  evidenceFromMatchResult,
  evidenceFromNormalizedMetadata,
  icyProviderCombinedDisagree,
  normEvidenceText,
} from "../lib/match-evidence-builders.js";

export type MergeAudioCatalogResult = ReturnType<typeof mergeAcoustidAndCatalog>;

function fingerprintEvidenceType(
  source: "local" | "acoustid" | "audd" | "acrcloud" | null
): "local_fingerprint" | "acoustid" | "audd" | "acrcloud" {
  if (source === "local") return "local_fingerprint";
  if (source === "audd") return "audd";
  if (source === "acrcloud") return "acrcloud";
  return "acoustid";
}

export type LivePollFusionInput = {
  stationId: string;
  /** Chosen metadata row for catalog / trust (ICY ?? provider ?? TuneIn). */
  metadata: NormalizedMetadata | null;
  icyMeta: NormalizedMetadata | null;
  providerMeta: NormalizedMetadata | null;
  /** 0–1 trust in chosen metadata row for catalog merge. */
  metaTrust01: number;
  audioMatch: MatchResult | null;
  audioMatchSource: "local" | "acoustid" | "audd" | "acrcloud" | null;
  /** Primary-pass catalog candidate (may differ from final if second-pass won). */
  primaryCatalogMatch: MatchResult | null;
  /** After merge + optional second-pass catalog. */
  finalMatch: MatchResult | null;
  finalMethod: DetectionMethod;
  mergeReasonCode: string | null;
  secondPassCatalogApplied: boolean;
  /** Mirrors monitor learn gate inputs. */
  shouldLearnFingerprint: boolean;
  shouldArchiveUnresolved: boolean;
  shouldQueueRecovery: boolean;
};

export class MatchFusionService {
  /**
   * Merge audio fingerprint candidate vs catalog text hit (AcoustID score thresholds,
   * catalog trust scaling). Delegates to `mergeAcoustidAndCatalog`.
   */
  static mergeAudioCatalog(
    audio: MatchResult | null,
    catalog: MatchResult | null,
    minAcoustidPrefer: number,
    catalogTrustFactor = 1
  ): MergeAudioCatalogResult {
    return mergeAcoustidAndCatalog(audio, catalog, minAcoustidPrefer, catalogTrustFactor);
  }

  /**
   * Second catalog pass: use combined stream line as title candidate (same policy as
   * live monitor). Caller applies gates (no existing match, fingerprint enabled, non-junk metadata).
   */
  static async secondPassCatalogLookup(metadata: NormalizedMetadata): Promise<MatchResult | null> {
    return CatalogLookupService.lookupFromMetadata({
      ...metadata,
      rawArtist: metadata.rawArtist || "",
      rawTitle: metadata.rawTitle || metadata.combinedRaw || "",
    });
  }

  /**
   * Reconstruct stream metadata context from a prior DetectionLog (recovery / reprocess).
   */
  static metadataSnapshotFromDetectionLog(
    linked: { rawStreamText: string | null; parsedArtist: string | null; parsedTitle: string | null } | null
  ): NormalizedMetadata | null {
    if (!linked) return null;
    const raw = (linked.rawStreamText ?? "").trim();
    const ra = (linked.parsedArtist ?? "").trim();
    const rt = (linked.parsedTitle ?? "").trim();
    const combined = raw || (ra && rt ? `${ra} - ${rt}` : ra || rt || "");
    if (!combined) return null;
    return {
      combinedRaw: combined,
      rawArtist: ra || undefined,
      rawTitle: rt || undefined,
      sourceType: "stream_metadata",
    };
  }

  /**
   * `matchDiagnosticsJson` for unresolved recovery — same `fusionV2` shape as live polls.
   */
  static recoveryMatchDiagnosticsJson(input: {
    stationId: string;
    unresolvedSampleId: string;
    linkedDetectionLogId: string | null;
    recoveryMeta: NormalizedMetadata | null;
    audioMatch: MatchResult;
    audioMatchSource: "local" | "acoustid" | "audd" | "acrcloud";
    merged: MergeAudioCatalogResult;
    finalMatch: MatchResult;
    finalMethod: DetectionMethod;
    recoveredViaAcoustid: boolean;
    recoveredViaAudd: boolean;
    recoveredViaAcrcloud: boolean;
  }): string {
    const metaTrust01 = input.recoveryMeta?.combinedRaw ? 0.45 : 0;
    const fusionDecision = MatchFusionService.buildLivePollDecision({
      stationId: input.stationId,
      metadata: input.recoveryMeta,
      icyMeta: input.recoveryMeta,
      providerMeta: null,
      metaTrust01,
      audioMatch: input.audioMatch,
      audioMatchSource: input.audioMatchSource,
      primaryCatalogMatch: null,
      finalMatch: input.finalMatch,
      finalMethod: input.finalMethod,
      mergeReasonCode: input.merged.reasonCode ?? null,
      secondPassCatalogApplied: false,
      shouldLearnFingerprint: !!(
        input.recoveredViaAcoustid ||
        input.recoveredViaAudd ||
        input.recoveredViaAcrcloud
      ),
      shouldArchiveUnresolved: false,
      shouldQueueRecovery: false,
    });
    return JSON.stringify({
      pollReason: "unresolved_recovery",
      recoveryMode: true,
      unresolvedSampleId: input.unresolvedSampleId,
      linkedDetectionLogId: input.linkedDetectionLogId,
      recoveredViaAcoustid: input.recoveredViaAcoustid,
      recoveredViaAudd: input.recoveredViaAudd,
      recoveredViaAcrcloud: input.recoveredViaAcrcloud,
      fingerprintAttempts: [
        {
          attempt: 1,
          delaySec: 0,
          sampleSec: 0,
          outcome: `recovery_match_${input.audioMatchSource}`,
        },
      ],
      fusionV2: MatchFusionService.summarizeForDiagnostics(fusionDecision),
    });
  }

  /**
   * Build a FusedMatchDecision for diagnostics and downstream recovery alignment.
   * Does not change match/method — caller passes the already-resolved outcome.
   */
  static buildLivePollDecision(input: LivePollFusionInput): FusedMatchDecision {
    const {
      stationId,
      metadata,
      icyMeta,
      providerMeta,
      metaTrust01,
      audioMatch,
      audioMatchSource,
      primaryCatalogMatch,
      finalMatch,
      finalMethod,
      mergeReasonCode,
      secondPassCatalogApplied,
      shouldLearnFingerprint,
      shouldArchiveUnresolved,
      shouldQueueRecovery,
    } = input;

    const supporting: MatchEvidence[] = [];
    const conflicting: MatchEvidence[] = [];

    let icyEv: MatchEvidence | null = null;
    if (icyMeta) {
      const disagree = icyProviderCombinedDisagree(icyMeta, providerMeta);
      icyEv = evidenceFromNormalizedMetadata({
        evidenceType: "icy_metadata",
        meta: icyMeta,
        stationId,
        metaTrust01,
        staleFlag: metaTrust01 <= 0,
        junkFlag: false,
        contradictionFlags: disagree ? ["provider_combined_text_differs"] : undefined,
      });
      supporting.push(icyEv);
    }

    if (providerMeta) {
      const provEv = evidenceFromNormalizedMetadata({
        evidenceType: "provider_metadata",
        meta: providerMeta,
        stationId,
        metaTrust01: providerMeta === metadata ? metaTrust01 : Math.max(0.3, metaTrust01 * 0.85),
        sourceLabel: "provider_chain",
      });
      supporting.push(provEv);
      if (icyProviderCombinedDisagree(icyMeta, providerMeta)) {
        conflicting.push({
          ...provEv,
          qualityFlags: [...(provEv.qualityFlags ?? []), "differs_from_icy_combined"],
        });
      }
    }

    let audioEv: MatchEvidence | null = null;
    if (audioMatch) {
      const et = fingerprintEvidenceType(audioMatchSource);
      audioEv = evidenceFromMatchResult({
        evidenceType: et,
        match: audioMatch,
        stationId,
      });
      supporting.push(audioEv);
    }

    let catalogEv: MatchEvidence | null = null;
    const catalogSource = finalMatch && finalMethod === "catalog_lookup" ? finalMatch : primaryCatalogMatch;
    if (catalogSource) {
      catalogEv = evidenceFromMatchResult({
        evidenceType: "trusted_catalog",
        match: catalogSource,
        stationId,
        trustTier: secondPassCatalogApplied ? "medium" : "high",
      });
      supporting.push(catalogEv);
    }

    if (audioMatch && primaryCatalogMatch) {
      const sameTitle = normEvidenceText(audioMatch.title) === normEvidenceText(primaryCatalogMatch.title);
      const sameArtist = normEvidenceText(audioMatch.artist) === normEvidenceText(primaryCatalogMatch.artist);
      if (!sameTitle || !sameArtist) {
        conflicting.push(
          evidenceFromMatchResult({
            evidenceType: "trusted_catalog",
            match: primaryCatalogMatch,
            stationId,
            trustTier: "medium",
          })
        );
        if (audioEv) {
          conflicting.push({
            ...audioEv,
            qualityFlags: [...(audioEv.qualityFlags ?? []), "differs_from_primary_catalog"],
          });
        }
      }
    }

    const status: FusedMatchDecision["status"] = finalMatch
      ? "matched"
      : finalMethod === "stream_metadata"
        ? "candidate_review"
        : "unresolved";

    let winning: MatchEvidence | undefined;
    if (finalMatch) {
      if (finalMethod === "catalog_lookup") {
        winning = evidenceFromMatchResult({
          evidenceType: "trusted_catalog",
          match: finalMatch,
          stationId,
          trustTier: secondPassCatalogApplied ? "medium" : "high",
        });
      } else if (
        finalMethod === "fingerprint_local" ||
        finalMethod === "fingerprint_acoustid" ||
        finalMethod === "fingerprint_audd" ||
        finalMethod === "fingerprint_acrcloud"
      ) {
        const et = fingerprintEvidenceType(audioMatchSource);
        winning = evidenceFromMatchResult({
          evidenceType: et,
          match: finalMatch,
          stationId,
        });
      } else if (finalMethod === "stream_metadata") {
        if (metadata) {
          winning = evidenceFromNormalizedMetadata({
            evidenceType: "icy_metadata",
            meta: metadata,
            stationId,
            metaTrust01,
            sourceLabel: "stream_metadata_match",
          });
        }
      }
    }

    const reasonCode =
      mergeReasonCode ||
      (status === "matched" ? "fusion_matched" : "fusion_unresolved");

    const decision: FusedMatchDecision = {
      status,
      finalArtist: finalMatch?.artist,
      finalTitle: finalMatch?.title,
      finalRecordingMbid: finalMatch?.recordingId,
      finalSourceProvider: finalMatch?.sourceProvider,
      finalDetectionMethod: finalMethod,
      finalConfidence: finalMatch?.confidence ?? finalMatch?.score ?? 0,
      reasonCode,
      winningEvidence: winning,
      supportingEvidence: supporting,
      conflictingEvidence: conflicting,
      shouldLearnFingerprint,
      shouldArchiveUnresolved,
      shouldQueueRecovery,
      diagnosticsJson: JSON.stringify({
        secondPassCatalogApplied,
        mergeReasonCode,
        audioMatchSource,
        primaryCatalogHadMatch: !!primaryCatalogMatch,
      }),
    };

    return decision;
  }

  /** Compact shape for DetectionLog.matchDiagnosticsJson (avoid huge payloads). */
  static summarizeForDiagnostics(d: FusedMatchDecision): Record<string, unknown> {
    let secondPassCatalogApplied = false;
    try {
      if (d.diagnosticsJson) {
        const parsed = JSON.parse(d.diagnosticsJson) as { secondPassCatalogApplied?: boolean };
        secondPassCatalogApplied = !!parsed.secondPassCatalogApplied;
      }
    } catch {
      // ignore
    }
    return {
      status: d.status,
      reasonCode: d.reasonCode,
      finalMethod: d.finalDetectionMethod,
      finalConfidence: d.finalConfidence,
      finalTitle: d.finalTitle,
      finalArtist: d.finalArtist,
      shouldLearnFingerprint: d.shouldLearnFingerprint,
      secondPassCatalogApplied,
      lanes: d.supportingEvidence.map((e) => ({
        type: e.evidenceType,
        tier: e.evidenceTrustTier,
        source: e.sourceProvider,
      })),
      conflicts: d.conflictingEvidence.length,
      winningType: d.winningEvidence?.evidenceType ?? null,
    };
  }
}
