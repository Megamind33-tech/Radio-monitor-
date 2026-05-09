import assert from "node:assert";
import { RematchService } from "./rematch.service.js";

const dirty = RematchService.classifyStreamMetadata("Now On Air", "Phoenix Radio");
assert.equal(dirty.dirty, true);

const clean = RematchService.classifyStreamMetadata("Yo Maps - Try Again", "Phoenix Radio");
assert.equal(clean.dirty, false);

const strong = RematchService.scoreRematchEvidence({ fingerprintConfidence: 0.95, hasVerifiedTrack: true, dirtyMetadata: false });
const weak = RematchService.scoreRematchEvidence({ fingerprintConfidence: 0.6, hasVerifiedTrack: false, dirtyMetadata: true });
assert.ok(strong > weak);
console.log("rematch.service.spec.ts passed");
