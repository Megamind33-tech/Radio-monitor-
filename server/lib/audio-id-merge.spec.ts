import { mergeAcoustidAndCatalog } from "./audio-id-merge.js";

function assert(condition: boolean, message: string) {
  if (!condition) throw new Error(message);
}

function run() {
  const catalog = {
    score: 0.7,
    confidence: 0.7,
    title: "Catalog Title",
    artist: "Catalog Artist",
    sourceProvider: "itunes_search" as const,
  };
  const weakAudio = {
    score: 0.55,
    confidence: 0.55,
    title: "Different",
    artist: "Artist",
    sourceProvider: "acoustid" as const,
  };
  const r1 = mergeAcoustidAndCatalog(weakAudio, catalog, 0.55, 1);
  assert(r1.match?.title === "Catalog Title", "marginal AcoustID should lose to catalog");
  assert(r1.reasonCode === "catalog_preferred_low_acoustid_score", r1.reasonCode ?? "");

  const strongAudio = {
    score: 0.65,
    confidence: 0.65,
    title: "Other",
    artist: "A",
    sourceProvider: "acoustid" as const,
  };
  const r2 = mergeAcoustidAndCatalog(strongAudio, catalog, 0.55, 1);
  assert(r2.match?.title === "Other", "AcoustID over 0.62 should win over marginal catalog");
  assert(r2.reasonCode === "acoustid_preferred_over_catalog", r2.reasonCode ?? "");

  const midAudio = {
    score: 0.58,
    confidence: 0.58,
    title: "X",
    artist: "Y",
    sourceProvider: "acoustid" as const,
  };
  const weakCat = { ...catalog, confidence: 0.62 };
  const r3 = mergeAcoustidAndCatalog(midAudio, weakCat, 0.55, 1);
  assert(r3.match?.title === "X", "AcoustID in (prefer,minOver) with weak catalog should prefer audio");
  assert(r3.reasonCode === "acoustid_preferred_weak_catalog", r3.reasonCode ?? "");

  const strongCat = { ...catalog, confidence: 0.95, title: "Wrong Title", artist: "Wrong Artist" };
  const localAudio = {
    score: 0.9,
    confidence: 0.9,
    title: "True Song",
    artist: "True Artist",
    sourceProvider: "local_fingerprint" as const,
  };
  const r4 = mergeAcoustidAndCatalog(localAudio, strongCat, 0.55, 1);
  assert(r4.match?.title === "True Song", "strong local_fingerprint must win over disagreeing high-confidence catalog");
  assert(
    r4.reasonCode === "local_fingerprint_preferred_over_disagreeing_catalog",
    r4.reasonCode ?? ""
  );

  console.log("audio-id-merge.spec: ok");
}

run();
