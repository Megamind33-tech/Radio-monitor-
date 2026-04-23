import "dotenv/config";
import assert from "node:assert";
import crypto from "node:crypto";
import { prisma } from "../lib/prisma.js";
import { LocalFingerprintService } from "./local-fingerprint.service.js";
import { FingerprintResult } from "../types.js";

/**
 * Smoke test for the self-learned fingerprint library.
 *
 * Exercises three behaviours we rely on at runtime:
 *   1. Learning from a metadata-confirmed match persists a row.
 *   2. An identical fingerprint hits the exact SHA-1 path.
 *   3. A noisy near-duplicate fingerprint (identical bytes + 2 extra bytes)
 *      still matches via the Chromaprint BER alignment path.
 */

function encodeChromaprintFromInts(values: number[]): string {
  const payload = Buffer.alloc(4 + values.length * 4);
  payload.writeUInt8(1, 0);
  payload.writeUInt8(values.length & 0xff, 1);
  payload.writeUInt8((values.length >> 8) & 0xff, 2);
  payload.writeUInt8((values.length >> 16) & 0xff, 3);
  for (let i = 0; i < values.length; i++) {
    payload.writeUInt32LE(values[i] >>> 0, 4 + i * 4);
  }
  return payload
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

async function cleanup(tags: string[]) {
  for (const tag of tags) {
    await prisma.localFingerprint
      .deleteMany({ where: { title: tag } })
      .catch(() => undefined);
  }
}

async function run() {
  const tag = `local-fp-spec-${crypto.randomBytes(4).toString("hex")}`;
  const tag2 = `${tag}-nearby`;
  try {
    const base = Array.from({ length: 80 }, (_, i) => (i * 2654435761) >>> 0);
    const fingerprint = encodeChromaprintFromInts(base);
    const fp: FingerprintResult = {
      fingerprint,
      duration: 25,
      backendUsed: "test",
    };

    // 1. learn
    await LocalFingerprintService.learn({
      fp,
      match: {
        score: 0.9,
        confidence: 0.9,
        title: tag,
        artist: "Spec Artist",
        sourceProvider: "stream_metadata",
      },
      source: "stream_metadata",
    });
    const learned = await prisma.localFingerprint.findFirst({ where: { title: tag } });
    assert.ok(learned, "expected LocalFingerprintService.learn to persist a row");

    // 2. exact lookup
    const exact = await LocalFingerprintService.lookup(fp);
    assert.ok(exact, "expected exact lookup to return a match");
    assert.strictEqual(exact?.title, tag);
    assert.strictEqual(exact?.sourceProvider, "local_fingerprint");

    // 3. BER alignment lookup: same core fingerprint wrapped in a few different
    //    leading/trailing samples (as if the recapture happened to start at a
    //    slightly different offset in the track).
    const wrapped = [0x01020304, 0x05060708, ...base, 0x09101112];
    const nearFp: FingerprintResult = {
      fingerprint: encodeChromaprintFromInts(wrapped),
      duration: 26,
      backendUsed: "test",
    };
    await LocalFingerprintService.learn({
      fp: nearFp,
      match: {
        score: 0.85,
        confidence: 0.85,
        title: tag2,
        artist: "Spec Artist Nearby",
        sourceProvider: "stream_metadata",
      },
      source: "stream_metadata",
    });

    // Query is the original `base` samples (identical core) with a non-matching
    // prefix so the SHA-1 and prefix-similarity paths can't hit. Only the BER
    // aligner should find the song.
    const queryInts = [0xffffffff, 0xffffffff, ...base];
    const query: FingerprintResult = {
      fingerprint: encodeChromaprintFromInts(queryInts),
      duration: 25,
      backendUsed: "test",
    };

    // Clear the exact-match row first so BER has to do the work.
    await prisma.localFingerprint.deleteMany({ where: { title: tag } });

    const nearMatch = await LocalFingerprintService.lookup(query);
    assert.ok(nearMatch, "expected aligned BER lookup to match a near-duplicate fingerprint");
    assert.strictEqual(nearMatch?.sourceProvider, "local_fingerprint");

    console.log("local-fingerprint.spec: ok");
  } finally {
    await cleanup([tag, tag2]);
    await prisma.$disconnect();
  }
}

run().catch((err) => {
  console.error("local-fingerprint.spec: FAIL", err);
  process.exit(1);
});
