import { mergeAcoustidAndCatalog } from "../lib/audio-id-merge.js";
import { icyProviderCombinedDisagree, normEvidenceText } from "../lib/match-evidence-builders.js";
import { MatchFusionService } from "./match-fusion.service.js";

function assert(condition: boolean, message: string) {
  if (!condition) throw new Error(message);
}

function run() {
  assert(normEvidenceText("  A  B ") === "a b", "norm");

  const catalog = {
    score: 0.7,
    confidence: 0.7,
    title: "Catalog Title",
    artist: "Catalog Artist",
    sourceProvider: "itunes_search" as const,
  };
  const audio = {
    score: 0.9,
    confidence: 0.9,
    title: "Catalog Title",
    artist: "Catalog Artist",
    sourceProvider: "acoustid" as const,
  };
  const viaService = MatchFusionService.mergeAudioCatalog(audio, catalog, 0.55, 1);
  const viaLib = mergeAcoustidAndCatalog(audio, catalog, 0.55, 1);
  assert(viaService.match?.title === viaLib.match?.title, "MatchFusionService.mergeAudioCatalog matches library");
  assert(viaService.method === viaLib.method, "method");

  const icy = { combinedRaw: "Artist - Song A", rawArtist: "Artist", rawTitle: "Song A", sourceType: "stream_metadata" as const };
  const prov = { combinedRaw: "Other - Song B", rawArtist: "Other", rawTitle: "Song B", sourceType: "stream_metadata" as const };
  assert(icyProviderCombinedDisagree(icy, prov), "expect disagree");

  const d = MatchFusionService.buildLivePollDecision({
    stationId: "s1",
    metadata: icy,
    icyMeta: icy,
    providerMeta: prov,
    orbMeta: null,
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

  const mergedOnly = MatchFusionService.mergeAudioCatalog(audio, null, 0.55);
  const recoveryDiag = MatchFusionService.recoveryMatchDiagnosticsJson({
    stationId: "s2",
    unresolvedSampleId: "u1",
    linkedDetectionLogId: "d1",
    recoveryMeta: icy,
    audioMatch: audio,
    audioMatchSource: "acoustid",
    merged: mergedOnly,
    finalMatch: audio,
    finalMethod: "fingerprint_acoustid",
    recoveredViaAcoustid: true,
    recoveredViaAudd: false,
    recoveredViaAcrcloud: false,
  });
  const parsed = JSON.parse(recoveryDiag) as { recoveryMode?: boolean; fusionV2?: { conflicts?: number } };
  assert(parsed.recoveryMode === true, "recovery flag");
  assert(parsed.fusionV2 && typeof parsed.fusionV2.conflicts === "number", "recovery fusionV2");

  console.log("match-fusion.service.spec.ts: ok");
}

run();
