import assert from "node:assert";
import { SampleStorageService } from "./sample-storage.service.js";

const base = {
  filePath: `${process.cwd()}/package.json`,
  recoveryStatus: "human_verified",
  fingerprintStatus: "fingerprinted",
  fingerprintedAt: new Date(),
  verifiedTrackId: "trk_1",
  originalSha256: "abc123",
  purgeStatus: "not_started",
};

const linked = { manuallyTagged: true, verifiedTrackId: "trk_1" };

{
  const res = SampleStorageService.checkEligibility({ ...base, recoveryStatus: "pending" }, linked);
  assert.equal(res.eligible, false);
  assert.equal(res.reason, "not_human_verified");
}
{
  const res = SampleStorageService.checkEligibility({ ...base, fingerprintStatus: "not_started" }, linked);
  assert.equal(res.eligible, false);
  assert.equal(res.reason, "not_fingerprinted");
}
{
  const res = SampleStorageService.checkEligibility({ ...base, verifiedTrackId: null }, linked);
  assert.equal(res.eligible, false);
  assert.equal(res.reason, "missing_verified_track");
}
{
  const res = SampleStorageService.checkEligibility({ ...base }, linked);
  assert.equal(res.eligible, true);
  assert.equal(res.reason, "eligible");
}
{
  const res = SampleStorageService.checkEligibility({ ...base, filePath: "/tmp/does-not-exist.wav" }, linked);
  assert.equal(res.eligible, false);
  assert.equal(res.reason, "file_missing");
}
{
  const res = SampleStorageService.checkEligibility({ ...base, fingerprintStatus: "failed" }, linked);
  assert.equal(res.eligible, false);
  assert.equal(res.reason, "fingerprint_failed");
}

console.log("sample-storage.service.spec.ts passed");

{
  const out = SampleStorageService.evaluateBackfillCandidate({
    filePath: "",
    hasHash: false,
    actualSize: undefined,
  });
  assert.equal(out.wouldUpdate, false);
  assert.equal(out.reason, "no_file_path");
}
{
  const out = SampleStorageService.evaluateBackfillCandidate({
    filePath: "/tmp/missing.wav",
    hasHash: false,
    actualSize: undefined,
  });
  assert.equal(out.wouldUpdate, false);
  assert.equal(out.reason, "file_missing");
}
{
  const out = SampleStorageService.evaluateBackfillCandidate({
    filePath: "/tmp/file.wav",
    hasHash: true,
    storedAudioBytes: 100,
    actualSize: 100,
    force: false,
  });
  assert.equal(out.wouldUpdate, false);
  assert.equal(out.reason, "already_up_to_date");
}
{
  const out = SampleStorageService.evaluateBackfillCandidate({
    filePath: "/tmp/file.wav",
    hasHash: false,
    storedAudioBytes: null,
    actualSize: 100,
    force: false,
  });
  assert.equal(out.wouldUpdate, true);
  assert.equal(out.reason, "missing_hash");
}

console.log("sample-storage backfill candidate tests passed");
