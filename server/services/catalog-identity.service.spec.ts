import assert from "node:assert";
import { CatalogIdentityService } from "./catalog-identity.service.js";

const artist = CatalogIdentityService.normalizeArtistName("Yo Maps feat. XYZ");
assert.equal(artist, "yo maps");

const title = CatalogIdentityService.normalizeSongTitle("Try Again (Official Audio) Lyrics mp3 download");
assert.ok(title.includes("try again"));
assert.ok(!title.includes("official"));

const k = CatalogIdentityService.buildTrackIdentityKeys({ artist: "Yo Maps", title: "Try Again" });
assert.ok(k.medium.artistTitle?.includes("yo maps"));

assert.equal(CatalogIdentityService.shouldLinkAsAlternateSource(0.9), true);
assert.equal(CatalogIdentityService.shouldRequireManualReview(0.7), true);
console.log("catalog-identity.service.spec.ts passed");
