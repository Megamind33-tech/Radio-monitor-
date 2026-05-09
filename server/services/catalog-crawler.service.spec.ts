import assert from "node:assert";
import { CatalogCrawlerService } from "./catalog-crawler.service.js";
import { buildSafeDetectionLogUpdate } from "../lib/human-verified-guard.js";
import { CatalogIdentityService } from "./catalog-identity.service.js";

assert.equal(CatalogCrawlerService.classifyUrl("https://x.com/song.mp3"), "direct_audio");
assert.equal(CatalogCrawlerService.classifyUrl("https://x.com/news/today"), "news");

const n1 = CatalogCrawlerService.normalizeUrl("https://x.com/a/?b=2&a=1#frag");
const n2 = CatalogCrawlerService.normalizeUrl("https://x.com/a/?a=1&b=2");
assert.equal(n1, n2);

const hi = CatalogCrawlerService.scoreSource({ classification: "direct_audio", contentType: "audio/mpeg", durationSec: 180, hasMetadata: true, success: true });
const lo = CatalogCrawlerService.scoreSource({ classification: "news", contentType: "text/html", failureCount: 3 });
assert.ok(hi > lo);

const title = CatalogIdentityService.normalizeSongTitle("Yo Maps ft X - Try Again (Official Music Video) mp3 download");
assert.ok(title.includes("try again"));
assert.ok(!title.includes("official"));

const protectedUpdate = buildSafeDetectionLogUpdate(
  { manuallyTagged: true, sourceProvider: "human_review", verifiedTrackId: "t1", titleFinal: "A", artistFinal: "B" },
  { titleFinal: "X", artistFinal: "Y", sourceProvider: "catalog_lookup", verifiedTrackId: "t2" },
  false
);
assert.equal((protectedUpdate as any).titleFinal, undefined);
assert.equal((protectedUpdate as any).sourceProvider, "human_review");

console.log("catalog-crawler.service.spec.ts passed");
