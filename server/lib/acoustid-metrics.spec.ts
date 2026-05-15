import {
  getAcoustidMetricsSnapshot,
  recordAcoustidMetadataComparison,
  recordAcoustidFinalWinIfApplicable,
  resetAcoustidMetricsForTests,
} from "./acoustid-metrics.js";

function assert(condition: boolean, message: string) {
  if (!condition) throw new Error(message);
}

function run() {
  resetAcoustidMetricsForTests();

  recordAcoustidMetadataComparison(
    { score: 0.9, confidence: 0.9, title: "Song", artist: "Artist A", sourceProvider: "acoustid" },
    { rawTitle: "Song", rawArtist: "Artist A", sourceType: "stream_metadata", combinedRaw: "Artist A - Song" }
  );
  let s = getAcoustidMetricsSnapshot();
  assert(s.acoustidAgreementWithMetadata === 1, "expect agreement");

  recordAcoustidMetadataComparison(
    { score: 0.9, confidence: 0.9, title: "Other", artist: "Artist A", sourceProvider: "acoustid" },
    { rawTitle: "Song", rawArtist: "Artist A", sourceType: "stream_metadata", combinedRaw: "Artist A - Song" }
  );
  s = getAcoustidMetricsSnapshot();
  assert(s.acoustidContradictionsWithMetadata === 1, "expect contradiction");

  recordAcoustidFinalWinIfApplicable(
    { score: 0.9, confidence: 0.9, title: "X", artist: "Y", sourceProvider: "acoustid" },
    "fingerprint_acoustid"
  );
  s = getAcoustidMetricsSnapshot();
  assert(s.acoustidFinalWins === 1, "expect final win");

  recordAcoustidFinalWinIfApplicable(
    { score: 0.9, confidence: 0.9, title: "X", artist: "Y", sourceProvider: "acoustid" },
    "catalog_lookup"
  );
  s = getAcoustidMetricsSnapshot();
  assert(s.acoustidFinalWins === 1, "catalog should not increment acoustid final wins");

  resetAcoustidMetricsForTests();
  console.log("acoustid-metrics.spec.ts: ok");
}

run();
