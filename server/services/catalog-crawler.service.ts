import axios from "axios";
import { prisma } from "../lib/prisma.js";
import { FingerprintService } from "./fingerprint.service.js";
import { LocalFingerprintService } from "./local-fingerprint.service.js";
import { CatalogIdentityService } from "./catalog-identity.service.js";
import crypto from "node:crypto";
import fs from "fs";
import os from "os";
import path from "path";
import { spawnSync } from "child_process";

export type UrlClass =
  | "direct_audio" | "direct_video" | "playlist" | "wordpress_media" | "sitemap"
  | "artist_page" | "album_page" | "label_catalog" | "station_playlist"
  | "generic_html" | "news" | "sports" | "non_media" | "dead_link" | "duplicate" | "unsupported" | "blocked";

const AUDIO_EXT = [".mp3", ".wav", ".flac", ".aac", ".m4a", ".ogg"];
const VIDEO_EXT = [".mp4", ".mkv", ".webm", ".mov"];
const PLAYLIST_CT = ["application/vnd.apple.mpegurl", "application/x-mpegurl", "audio/x-mpegurl"];
const AUDIO_CT = ["audio/mpeg", "audio/mp4", "audio/aac", "audio/wav", "audio/flac"];
const VIDEO_CT = ["video/mp4"];
const TEMP_ROOT = path.join(process.cwd(), "data/tmp/catalog-crawler");
const ZM_KEYWORDS = ["zambia","zambian","zed music","zedmusic","lusaka","copperbelt","ndola","kitwe","livingstone","chipata","kabwe","mufulira","chingola","solwezi","mongu","kasama","choma","kalindula","zamrock","kopala","zambian gospel","zambian hip hop","zambian dancehall","zambian rnb","zambian traditional"];
export const DUPLICATE_REASONS=["duplicate_url","duplicate_final_url","duplicate_sha256","duplicate_isrc","duplicate_fingerprint","duplicate_artist_title_high_confidence","possible_duplicate_artist_title","possible_duplicate_duration_mismatch","alternate_source_linked","needs_manual_duplicate_review","already_has_local_fingerprint","skipped_by_canonical_limit","skipped_by_run_limit","duration_too_short","duration_too_long","no_audio_stream","media_probe_failed","fingerprint_timeout","file_too_large","unsupported_content_type","bad_metadata"] as const;

export class CatalogCrawlerService {
  static tempRoot(): string {
    return TEMP_ROOT;
  }

  static ensureTempRoot() {
    fs.mkdirSync(TEMP_ROOT, { recursive: true });
  }
  static normalizeUrl(raw: string): string {
    try {
      const u = new URL(raw.trim());
      u.hash = "";
      const keys = [...u.searchParams.keys()].sort();
      const next = new URL(`${u.origin}${u.pathname}`);
      for (const k of keys) next.searchParams.set(k, u.searchParams.get(k) || "");
      return next.toString().replace(/\/$/, "");
    } catch {
      return raw.trim();
    }
  }

  static classifyUrl(raw: string): UrlClass {
    const u = raw.toLowerCase();
    if (/\/wp-json\/wp\/v2\/media/.test(u)) return "wordpress_media";
    if (/sitemap/.test(u) && /\.xml($|\?)/.test(u)) return "sitemap";
    if (/news|sports|politics/.test(u)) return /sports/.test(u) ? "sports" : "news";
    if (/\.(m3u8?|pls)($|\?)/.test(u)) return "playlist";
    if (AUDIO_EXT.some((e) => u.includes(e))) return "direct_audio";
    if (VIDEO_EXT.some((e) => u.includes(e))) return "direct_video";
    if (/playlist|nowplaying/.test(u)) return "station_playlist";
    if (/artist/.test(u)) return "artist_page";
    if (/album/.test(u)) return "album_page";
    if (/label|catalog/.test(u)) return "label_catalog";
    if (/^https?:\/\//.test(u)) return "generic_html";
    return "unsupported";
  }

  static scoreSource(params: { classification: UrlClass; contentType?: string | null; durationSec?: number | null; hasMetadata?: boolean; duplicate?: boolean; success?: boolean; failureCount?: number }): number {
    let score = 0;
    if (params.classification === "direct_audio") score += 45;
    if (params.classification === "direct_video") score += 30;
    if (params.classification === "wordpress_media") score += 30;
    if (params.classification === "playlist") score += 25;
    if (params.classification === "news" || params.classification === "sports" || params.classification === "generic_html") score -= 30;
    if (params.contentType?.startsWith("audio/")) score += 20;
    if (params.durationSec && params.durationSec >= 30) score += 15;
    if (params.hasMetadata) score += 10;
    if (params.duplicate) score -= 35;
    if (params.success) score += 10;
    if ((params.failureCount || 0) > 0) score -= Math.min(20, (params.failureCount || 0) * 4);
    return Math.max(0, Math.min(100, score));
  }

  static async discoverAndUpsert(input: { urls: string[]; discoveredFrom?: string; sourceType?: string }) {
    const out = [];
    for (const raw of input.urls) {
      const normalized = this.normalizeUrl(raw);
      const classification = this.classifyUrl(normalized);
      const existing = await prisma.catalogCrawlSource.findUnique({ where: { normalizedUrl: normalized } });
      if (existing) {
        out.push({ normalizedUrl: normalized, classification: "duplicate" as UrlClass, id: existing.id });
        continue;
      }
      const score = this.scoreSource({ classification });
      const row = await prisma.catalogCrawlSource.create({
        data: {
          canonicalUrl: raw,
          normalizedUrl: normalized,
          classification,
          sourceType: input.sourceType ?? "manual_seed",
          discoveredFrom: input.discoveredFrom ?? null,
          qualityScore: score,
          lastStatus: "discovered",
          lastCheckedAt: new Date(),
        },
      });
      out.push({ normalizedUrl: normalized, classification, id: row.id });
    }
    return out;
  }

  static async preflightUrl(rawUrl: string) {
    const normalized = this.normalizeUrl(rawUrl);
    const classification = this.classifyUrl(normalized);
    if (classification === "generic_html") return { ok: false, reason: "generic_html", normalized, classification };
    if (classification === "news") return { ok: false, reason: "likely_news", normalized, classification };
    if (classification === "sports") return { ok: false, reason: "likely_sports", normalized, classification };
    let head;
    try {
      head = await axios.head(normalized, {
        timeout: 12_000,
        maxRedirects: 5,
        validateStatus: (s) => s >= 200 && s < 400,
      });
    } catch {
      return { ok: false, reason: "head_failed", normalized, classification };
    }
    const finalUrl = this.normalizeUrl(String((head.request as any)?.res?.responseUrl || normalized));
    const contentType = String(head.headers["content-type"] || "").split(";")[0].trim().toLowerCase();
    const contentLength = Number(head.headers["content-length"] || 0);
    if (!contentType) return { ok: false, reason: "unsupported_content_type", normalized: finalUrl, classification };
    const supported = [...AUDIO_CT, ...VIDEO_CT, ...PLAYLIST_CT];
    if (!supported.includes(contentType)) return { ok: false, reason: "unsupported_content_type", normalized: finalUrl, classification, contentType, contentLength };
    if (contentLength > 0 && contentLength < 20_000) return { ok: false, reason: "content_length_too_small", normalized: finalUrl, classification, contentType, contentLength };
    if (contentLength > 400_000_000) return { ok: false, reason: "content_length_too_large", normalized: finalUrl, classification, contentType, contentLength };
    return { ok: true, reason: "ok", normalized: finalUrl, finalUrl, classification, contentType, contentLength };
  }

  static extractMetadataHints(params: { url: string; finalUrl?: string; wordpressTitle?: string; ogTitle?: string; ogDescription?: string; playlistEntry?: string }) {
    const sourceUrl = params.finalUrl || params.url;
    const slug = sourceUrl.split("/").pop() || "";
    const base = decodeURIComponent(slug).replace(/\.[a-z0-9]{2,5}$/i, "").replace(/[_-]+/g, " ").trim();
    const candidate = params.wordpressTitle || params.ogTitle || params.playlistEntry || base;
    const pieces = candidate.split(" - ");
    const artist = pieces.length > 1 ? pieces[0].trim() : "";
    const title = pieces.length > 1 ? pieces.slice(1).join(" - ").trim() : candidate.trim();
    const featured = /feat\.|ft\./i.test(title) ? "featured" : "";
    return {
      artist: artist || null,
      title: title || null,
      featuredHint: featured || null,
      album: null,
      label: null,
      isrc: null,
      country: null,
      sourceSociety: null,
      sourceUrl,
      metadataConfidence: candidate ? 0.55 : 0.2,
      raw: { ogTitle: params.ogTitle ?? null, ogDescription: params.ogDescription ?? null, wordpressTitle: params.wordpressTitle ?? null, playlistEntry: params.playlistEntry ?? null },
    };
  }

  static probeMediaFile(filePath: string) {
    const ffprobe = spawnSync("ffprobe", ["-v", "error", "-print_format", "json", "-show_streams", "-show_format", filePath], { encoding: "utf8", timeout: 10000 });
    if (ffprobe.status !== 0 || !ffprobe.stdout) return { ok: false, reason: "media_probe_failed", durationSec: null, codec: null, hasAudio: false };
    try {
      const data = JSON.parse(ffprobe.stdout);
      const streams = Array.isArray(data.streams) ? data.streams : [];
      const audio = streams.find((s: any) => s.codec_type === "audio");
      const durationSec = Number(data?.format?.duration || 0);
      if (!audio) return { ok: false, reason: "media_probe_failed", durationSec, codec: null, hasAudio: false };
      if (durationSec > 0 && durationSec < 20) return { ok: false, reason: "content_length_too_small", durationSec, codec: audio.codec_name || null, hasAudio: true };
      return { ok: true, reason: "ok", durationSec, codec: audio.codec_name || null, hasAudio: true };
    } catch {
      return { ok: false, reason: "media_probe_failed", durationSec: null, codec: null, hasAudio: false };
    }
  }

  static cleanupPreviewTempFiles() {
    this.ensureTempRoot();
    const files = fs.readdirSync(TEMP_ROOT).map((name) => path.join(TEMP_ROOT, name));
    const rows = files.map((p) => {
      const inside = path.resolve(p).startsWith(path.resolve(TEMP_ROOT) + path.sep);
      const size = inside && fs.existsSync(p) ? fs.statSync(p).size : 0;
      return { path: p, insideTempRoot: inside, size, cleanupEligible: inside };
    });
    const cleanupEligible = rows.filter((r) => r.cleanupEligible);
    return {
      dryRunOnly: true,
      tempRoot: TEMP_ROOT,
      tempFilesCount: rows.length,
      tempBytes: rows.reduce((a, b) => a + b.size, 0),
      cleanupEligibleCount: cleanupEligible.length,
      cleanupEligibleBytes: cleanupEligible.reduce((a, b) => a + b.size, 0),
      rows,
    };
  }


  static zambianSignalScore(text: string) {
    const t = text.toLowerCase();
    return ZM_KEYWORDS.reduce((acc, k) => acc + (t.includes(k) ? 8 : 0), 0);
  }

  static async findExistingTrackCandidates(meta: { artist?: string | null; title?: string | null; isrc?: string | null; sha256?: string | null; finalUrl?: string | null; canonicalUrl?: string | null; }) {
    const keys = CatalogIdentityService.buildTrackIdentityKeys(meta);
    const candidates: any[] = [];
    if (keys.strong.isrc) {
      const tracks = await prisma.verifiedTrack.findMany({ where: { isrc: keys.strong.isrc }, take: 5 });
      candidates.push(...tracks.map((t) => ({ type: "isrc", track: t, score: 0.98 })));
    }
    if (keys.medium.artistTitle) {
      const tracks = await prisma.verifiedTrack.findMany({ where: { artist: { contains: meta.artist || "",  }, title: { contains: meta.title || "",  } as any }, take: 8 });
      candidates.push(...tracks.map((t) => ({ type: "artist_title", track: t, score: 0.78 })));
    }
    return candidates;
  }

  static async runPriorityBatch(options?: { maxUrlsPerRun?: number; maxFingerprintsPerRun?: number; minDurationSeconds?: number; maxDurationSeconds?: number; skipIfTrackAlreadyHasFingerprint?: boolean; maxSourcesPerCanonicalTrack?: number; maxDuplicateCandidatesPerRun?: number; }) {
    const maxUrls = options?.maxUrlsPerRun ?? 25;
    const maxFps = options?.maxFingerprintsPerRun ?? 10;
    const minDur = options?.minDurationSeconds ?? 30;
    const maxDur = options?.maxDurationSeconds ?? 540;
    const rows = await prisma.catalogCrawlSource.findMany({ where: { fingerprintStatus: { in: ["not_started","failed"] } }, take: maxUrls, orderBy: { updatedAt: "desc" } });
    let fingerprintsAdded = 0; let duplicateQueueCount = 0; const out: any[] = [];
    const maxSourcesPerCanonicalTrack = options?.maxSourcesPerCanonicalTrack ?? 5;
    const maxDuplicateCandidatesPerRun = options?.maxDuplicateCandidatesPerRun ?? 100;
    for (const row of rows) {
      if (fingerprintsAdded >= maxFps) { out.push({ id: row.id, skipped: true, reason: "skipped_by_run_limit" }); continue; }
      const meta = this.extractMetadataHints({ url: row.canonicalUrl, finalUrl: row.finalUrl || row.normalizedUrl });
      const zmBoost = this.zambianSignalScore([row.canonicalUrl,row.normalizedUrl,meta.artist||"",meta.title||""].join(" "));
      const canonical = await CatalogIdentityService.findCanonicalCandidates({ sourceUrl: row.canonicalUrl, finalUrl: row.finalUrl || row.normalizedUrl, artist: meta.artist, title: meta.title, album: meta.album || undefined, isrc: meta.isrc || undefined, sha256: row.mediaSha256 || undefined });
      if (canonical.matchStrength === "strong" && canonical.bestCandidate?.verifiedTrackId) {
        const sourceCount = await prisma.catalogSource.count({ where: { verifiedTrackId: canonical.bestCandidate.verifiedTrackId } });
        if (sourceCount >= maxSourcesPerCanonicalTrack) {
          out.push({ id: row.id, skipped: true, reason: "skipped_by_canonical_limit" });
          continue;
        }
        await prisma.catalogSource.create({ data: { verifiedTrackId: canonical.bestCandidate.verifiedTrackId, sourceUrl: row.canonicalUrl, canonicalUrl: row.normalizedUrl, finalUrl: row.finalUrl, sourceType: row.sourceType, countryHint: row.country || null, countryConfidence: zmBoost > 10 ? 0.85 : 0.5, metadataJson: JSON.stringify(meta), duplicateReason: canonical.duplicateReason || "alternate_source_linked", confidence: canonical.confidence, fingerprintStatus: "skipped" } });
        await prisma.catalogCrawlSource.update({ where: { id: row.id }, data: { lastStatus: "duplicate", failureReason: canonical.duplicateReason || "alternate_source_linked" } });
        out.push({ id: row.id, linked: true, reason: canonical.duplicateReason || "alternate_source_linked", processing_saved: true });
        continue;
      }
      if (canonical.matchStrength === "medium" && canonical.bestCandidate?.verifiedTrackId && duplicateQueueCount < maxDuplicateCandidatesPerRun) {
        await prisma.catalogDuplicateReview.create({ data: { candidateSourceId: row.id, existingVerifiedTrackId: canonical.bestCandidate.verifiedTrackId, candidateArtist: meta.artist, candidateTitle: meta.title, existingArtist: canonical.bestCandidate.track?.artist || null, existingTitle: canonical.bestCandidate.track?.title || null, candidateDurationSeconds: null, existingDurationSeconds: null, confidence: canonical.confidence, reason: canonical.duplicateReason || "needs_manual_duplicate_review", evidenceJson: JSON.stringify({ canonical, meta }) } });
        duplicateQueueCount += 1;
        await prisma.catalogCrawlSource.update({ where: { id: row.id }, data: { lastStatus: "review", failureReason: "needs_manual_duplicate_review" } });
        out.push({ id: row.id, reviewQueued: true, reason: "needs_manual_duplicate_review" });
        continue;
      }
      const res = await this.fingerprintMediaSource(row.id);
      if (res.ok) fingerprintsAdded += 1;
      out.push({ id: row.id, ...res, zambianPriority: zmBoost });
    }
    return { scanned: rows.length, fingerprintsAdded, rows: out };
  }

  static async statusSummary() {
    const total = await prisma.catalogCrawlSource.count();
    const byClass = await prisma.catalogCrawlSource.groupBy({ by: ["classification"], _count: { _all: true } });
    const byFp = await prisma.catalogCrawlSource.groupBy({ by: ["fingerprintStatus"], _count: { _all: true } });
    const failures = await prisma.catalogCrawlSource.groupBy({ by: ["failureReason"], _count: { _all: true }, where: { failureReason: { not: null } } });
    const altSources = await prisma.catalogSource.count({ where: { duplicateReason: "alternate_source_linked" } });
    const possibleDupes = await prisma.catalogDuplicateReview.count({ where: { status: "pending" } });
    return { total, byClass, byFp, failures, altSources, possibleDupes };
  }

  static async fingerprintMediaSource(id: string) {
    const src = await prisma.catalogCrawlSource.findUnique({ where: { id } });
    if (!src) return { ok: false, reason: "not_found" };
    if (!["direct_audio", "direct_video"].includes(src.classification)) return { ok: false, reason: "not_media_class" };
    this.ensureTempRoot();
    const tmp = path.join(TEMP_ROOT, `crawler_${id}.bin`);
    try {
      const pre = await this.preflightUrl(src.normalizedUrl);
      if (!pre.ok) {
        await prisma.catalogCrawlSource.update({ where: { id }, data: { lastStatus: "blocked", failureReason: pre.reason, failureCount: { increment: 1 }, finalUrl: (pre as any).finalUrl ?? null, contentType: (pre as any).contentType ?? null } });
        return { ok: false, reason: pre.reason };
      }
      const res = await axios.get(pre.finalUrl || src.normalizedUrl, { responseType: "arraybuffer", timeout: 30_000, maxRedirects: 5 });
      fs.writeFileSync(tmp, Buffer.from(res.data));
      const probe = this.probeMediaFile(tmp);
      if (!probe.ok) {
        await prisma.catalogCrawlSource.update({ where: { id }, data: { lastStatus: "blocked", failureReason: probe.reason, failureCount: { increment: 1 }, contentType: pre.contentType || null, finalUrl: pre.finalUrl || null } });
        return { ok: false, reason: probe.reason };
      }
      const meta = this.extractMetadataHints({ url: src.canonicalUrl, finalUrl: pre.finalUrl || src.normalizedUrl });
      const fp = await FingerprintService.generateFingerprint(tmp);
      if (!fp) {
        await prisma.catalogCrawlSource.update({ where: { id }, data: { fingerprintStatus: "failed", failureReason: "fingerprint_failed", failureCount: { increment: 1 } } });
        return { ok: false, reason: "fingerprint_failed" };
      }
      await LocalFingerprintService.learn({
        fp,
        source: "manual",
        metadata: null,
        match: { title: meta.title ?? undefined, artist: meta.artist ?? undefined, confidence: meta.metadataConfidence, score: meta.metadataConfidence, sourceProvider: "local_fingerprint", reasonCode: "crawler_media_ingest" },
      });
      await prisma.catalogCrawlSource.update({ where: { id }, data: { fingerprintStatus: "fingerprinted", metadataStatus: "extracted", titleRaw: meta.title, artistRaw: meta.artist, finalUrl: pre.finalUrl || null, contentType: pre.contentType || null, lastStatus: "fingerprinted", lastCheckedAt: new Date() } });
      return { ok: true };
    } catch (e) {
      await prisma.catalogCrawlSource.update({ where: { id }, data: { lastStatus: "failed", failureReason: "download_failed", failureCount: { increment: 1 } } });
      return { ok: false, reason: "download_failed" };
    } finally {
      // Intentionally keep temp file cleanup out of this phase to avoid introducing delete flows.
    }
  }
}
