import assert from "node:assert";
import { isTrustedIdentityForLocalLearning, learnSourceFromMatch } from "./local-fingerprint-learning-policy.js";

function run() {
  const weakCat = {
    score: 0.7,
    confidence: 0.5,
    title: "Track",
    artist: "Artist",
    sourceProvider: "itunes_search" as const,
  };
  assert.strictEqual(
    isTrustedIdentityForLocalLearning("catalog_lookup", weakCat),
    false,
    "low-confidence catalog should not be trusted for library learning"
  );

  const strongCat = { ...weakCat, confidence: 0.9 };
  assert.strictEqual(isTrustedIdentityForLocalLearning("catalog_lookup", strongCat), true);

  const fpLocal = {
    score: 0.91,
    confidence: 0.91,
    title: "Song",
    artist: "Artist",
    sourceProvider: "local_fingerprint" as const,
  };
  assert.strictEqual(isTrustedIdentityForLocalLearning("fingerprint_local", fpLocal), true);

  const metaWeak = {
    score: 1,
    confidence: 1,
    title: "Song",
    artist: "Artist",
    sourceProvider: "stream_metadata" as const,
  };
  assert.strictEqual(isTrustedIdentityForLocalLearning("stream_metadata", metaWeak), false);

  const metaStrong = { ...metaWeak, recordingId: "mbid-rec" };
  assert.strictEqual(isTrustedIdentityForLocalLearning("stream_metadata", metaStrong), true);

  assert.strictEqual(learnSourceFromMatch({ score: 1, confidence: 1, sourceProvider: "audd" }), "manual");
  assert.strictEqual(learnSourceFromMatch({ score: 1, confidence: 1, sourceProvider: "itunes_search" }), "catalog");

  console.log("local-fingerprint-learning-policy.spec: ok");
}

run();
