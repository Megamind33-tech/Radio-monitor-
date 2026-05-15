import { icyProviderCombinedDisagree, normEvidenceText } from "../lib/match-evidence-builders.js";
import { MatchFusionService } from "../services/match-fusion.service.js";

function assert(condition: boolean, message: string) {
  if (!condition) throw new Error(message);
}

function run() {
  assert(normEvidenceText("  A  B ") === "a b", "norm");

  const icy = { combinedRaw: "Artist - Song A", rawArtist: "Artist", rawTitle: "Song A", sourceType: "stream_metadata" as const };
  const prov = { combinedRaw: "Other - Song B", rawArtist: "Other", rawTitle: "Song B", sourceType: "stream_metadata" as const };
  assert(icyProviderCombinedDisagree(icy, prov), "expect disagree");

  const d = MatchFusionService.buildLivePollDecision({
    stationId: "s1",
    metadata: icy,
    icyMeta: icy,
    providerMeta: prov,
    metaTrust01: 1,
    audioMatch: null,
    audioMatchSource: null,
    primaryCatalogMatch: null,
    finalMatch: null,
    finalMethod: "unresolved",
    mergeReasonCode: "no_match",
    secondPassCatalogApplied: false,
    shouldLearnFingerprint: false,
    shouldArchiveUnresolved: true,
    shouldQueueRecovery: false,
  });
  assert(d.status === "unresolved", "status");
  assert(d.conflictingEvidence.length >= 1, "expect icy/provider conflict evidence");

  const sum = MatchFusionService.summarizeForDiagnostics(d);
  assert(typeof sum.conflicts === "number", "summary");

  console.log("match-fusion.service.spec.ts: ok");
}

run();
