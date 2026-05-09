import crypto from "node:crypto";
import { prisma } from "../lib/prisma.js";

export type IdentityInput = {
  sourceUrl?: string | null;
  finalUrl?: string | null;
  artist?: string | null;
  title?: string | null;
  album?: string | null;
  isrc?: string | null;
  sha256?: string | null;
  verifiedTrackId?: string | null;
  fingerprintSha1?: string | null;
  durationSeconds?: number | null;
  sourcePageTitle?: string | null;
};

export class CatalogIdentityService {
  static normalizeArtistName(v?: string | null) { return this.cleanBase(v).replace(/\b(feat|ft|featuring)\b.*$/i, "").trim(); }
  static normalizeSongTitle(v?: string | null) {
    return this.cleanBase(v)
      .replace(/\b(official\s+music\s+video|official\s+video|official\s+audio|lyrics?|mp3\s*download|prod\.?\s*by|zambian\s+music|zed\s+music|remix|version)\b/gi, " ")
      .replace(/\s+/g, " ").trim();
  }
  static normalizeTrackIdentity(input: IdentityInput) {
    return { artistNorm: this.normalizeArtistName(input.artist), titleNorm: this.normalizeSongTitle(input.title), albumNorm: this.cleanBase(input.album), isrcNorm: (input.isrc || "").trim().toUpperCase() || null, sourceUrlNorm: this.normalizeUrl(input.sourceUrl || ""), finalUrlNorm: this.normalizeUrl(input.finalUrl || ""), durationSeconds: input.durationSeconds ?? null };
  }
  static buildTrackIdentityKeys(input: IdentityInput) {
    const n = this.normalizeTrackIdentity(input); const base = `${n.artistNorm}::${n.titleNorm}`.trim();
    return { strong: { isrc: n.isrcNorm, verifiedTrackId: input.verifiedTrackId || null, sha256: input.sha256 || null, fingerprintSha1: input.fingerprintSha1 || null, sourceUrl: n.sourceUrlNorm || null, finalUrl: n.finalUrlNorm || null }, medium: { artistTitle: base || null, artistTitleAlbum: base && n.albumNorm ? `${base}::${n.albumNorm}` : null, artistTitleDuration: base && n.durationSeconds ? `${base}::${Math.round(n.durationSeconds)}` : null }, weak: { urlSlug: this.slugFromUrl(n.finalUrlNorm || n.sourceUrlNorm || "") || null, rawTitle: this.cleanBase(input.title), sourcePageTitle: this.cleanBase(input.sourcePageTitle) } };
  }
  static async findCanonicalCandidates(input: IdentityInput) {
    const keys = this.buildTrackIdentityKeys(input); const candidates: any[] = [];
    if (keys.strong.sourceUrl || keys.strong.finalUrl) {
      const src = await prisma.catalogSource.findFirst({ where: { OR: [{ sourceUrl: input.sourceUrl || "" }, { finalUrl: input.finalUrl || "" }] } });
      if (src?.verifiedTrackId) candidates.push({ verifiedTrackId: src.verifiedTrackId, confidence: 0.99, matchStrength: "strong", duplicateReason: src.sourceUrl === input.sourceUrl ? "duplicate_url" : "duplicate_final_url", recommendedAction: "skip_existing_source" });
    }
    if (keys.strong.sha256) {
      const src = await prisma.catalogSource.findFirst({ where: { sha256: keys.strong.sha256 } });
      if (src?.verifiedTrackId) candidates.push({ verifiedTrackId: src.verifiedTrackId, confidence: 0.98, matchStrength: "strong", duplicateReason: "duplicate_sha256", recommendedAction: "skip_fingerprint_existing_track" });
    }
    if (keys.strong.isrc) {
      const vt = await prisma.verifiedTrack.findFirst({ where: { isrc: keys.strong.isrc } });
      if (vt) candidates.push({ verifiedTrackId: vt.id, confidence: 0.98, matchStrength: "strong", duplicateReason: "duplicate_isrc", recommendedAction: "link_alternate_source", track: vt });
    }
    if (keys.strong.verifiedTrackId) candidates.push({ verifiedTrackId: keys.strong.verifiedTrackId, confidence: 0.99, matchStrength: "strong", duplicateReason: "duplicate_fingerprint", recommendedAction: "link_alternate_source" });
    if (keys.strong.fingerprintSha1) {
      const fp = await prisma.localFingerprint.findFirst({ where: { fingerprintSha1: keys.strong.fingerprintSha1 } });
      if (fp) {
        const vt = await prisma.verifiedTrack.findFirst({ where: { artist: fp.artist || "", title: fp.title || "" } });
        if (vt) candidates.push({ verifiedTrackId: vt.id, confidence: 0.95, matchStrength: "strong", duplicateReason: "duplicate_fingerprint", recommendedAction: "skip_fingerprint_existing_track", track: vt });
      }
    }
    if (!candidates.length && keys.medium.artistTitle) {
      const vt = await prisma.verifiedTrack.findFirst({ where: { artist: { contains: input.artist || "" }, title: { contains: input.title || "" } as any } });
      if (vt) candidates.push({ verifiedTrackId: vt.id, confidence: 0.74, matchStrength: "medium", duplicateReason: "possible_duplicate_artist_title", recommendedAction: "needs_duplicate_review", track: vt });
    }
    const best = candidates.sort((a,b)=>b.confidence-a.confidence)[0];
    return { candidates, bestCandidate: best || null, confidence: best?.confidence || 0, matchStrength: best?.matchStrength || "none", duplicateReason: best?.duplicateReason || null, recommendedAction: best?.recommendedAction || "process_as_new" };
  }

  static scoreTrackCandidate(params: { keyMatch: "strong" | "medium" | "weak"; durationDeltaSec?: number | null; hasFingerprint?: boolean; }) { let score = params.keyMatch === "strong" ? 0.95 : params.keyMatch === "medium" ? 0.72 : 0.45; if ((params.durationDeltaSec ?? 0) > 8) score -= 0.2; if (params.hasFingerprint) score += 0.05; return Math.max(0, Math.min(1, score)); }
  static shouldCreateNewTrack(score: number) { return score < 0.75; }
  static shouldLinkAsAlternateSource(score: number) { return score >= 0.88; }
  static shouldRequireManualReview(score: number) { return score >= 0.6 && score < 0.88; }
  static fingerprintSha1(fp?: string | null) { if (!fp) return null; return crypto.createHash("sha1").update(fp).digest("hex"); }
  private static cleanBase(v?: string | null) { return (v || "").toLowerCase().replace(/\[[^\]]*\]|\([^)]*\)/g, " ").replace(/["'`.,!?:;|_/\-]+/g, " ").replace(/\s+/g, " ").trim(); }
  private static normalizeUrl(v: string) { try { const u = new URL(v.trim()); u.hash = ""; return `${u.origin}${u.pathname}`.replace(/\/$/, "").toLowerCase(); } catch { return ""; } }
  private static slugFromUrl(v: string) { const parts = v.split("/").filter(Boolean); return parts.length ? parts[parts.length - 1] : ""; }
}
