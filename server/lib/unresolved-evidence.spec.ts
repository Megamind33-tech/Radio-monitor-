import assert from "node:assert/strict";
import { buildTitleNormKey, isDirtyWebTitleText, screenProgrammeOrDirtyWeb } from "./unresolved-evidence.js";

assert.equal(buildTitleNormKey("  Foo Bar ", "BAZ song "), "foo bar\tbaz song");
assert.equal(buildTitleNormKey("A", "Title"), null);
assert.equal(buildTitleNormKey("Artist", "T"), null);

assert.equal(isDirtyWebTitleText("Song name (Official Video)"), true);
assert.equal(isDirtyWebTitleText("Artist - Real Song"), false);

assert.equal(screenProgrammeOrDirtyWeb({ parsedArtist: "X", parsedTitle: "Morning Show", rawStreamText: null }), "programme");
assert.equal(screenProgrammeOrDirtyWeb({ parsedArtist: "A", parsedTitle: "Track", rawStreamText: "Official Video" }), "dirty_web");
assert.equal(screenProgrammeOrDirtyWeb({ parsedArtist: "A", parsedTitle: "Track - Radio Edit", rawStreamText: "A - Track" }), null);

console.log("unresolved-evidence.spec.ts OK");
