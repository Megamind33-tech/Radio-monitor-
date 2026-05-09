import fs from "fs";
import { prisma } from "../lib/prisma.js";
import { FingerprintService } from "./fingerprint.service.js";
import { LocalFingerprintService } from "./local-fingerprint.service.js";
import { buildSafeDetectionLogUpdate, isHumanVerifiedDetectionLog } from "../lib/human-verified-guard.js";

const WEAK_PROVIDERS = ["stream_metadata", "onlineradiobox", "parser", "crawler_suggestion", "catalog_lookup"];

export class RematchService {
  static classifyStreamMetadata(text: string, stationName = "") {
    const t = (text || "").toLowerCase().trim();
    const s = (stationName || "").toLowerCase().trim();
    if (!t) return { dirty: true, reason: "unknown_format" };
    if (s && t === s) return { dirty: true, reason: "station_name_only" };
    if (/live|now on air|dj|breakfast show|drive time/.test(t)) return { dirty: true, reason: "program_name" };
    if (/news|sports|talk/.test(t)) return { dirty: true, reason: "news_or_talk" };
    if (/jingle|advert|promo/.test(t)) return { dirty: true, reason: "advert_or_jingle" };
    if (!/ - /.test(t)) return { dirty: true, reason: "no_artist_title" };
    return { dirty: false, reason: null };
  }

  static async createRematchJobsForNewFingerprint(options?: { maxRematchJobsPerFingerprint?: number; stationId?: string; onlyRecentDays?: number }) {
    const limit = options?.maxRematchJobsPerFingerprint ?? 200;
    const where: any = { verifiedTrackId: null };
    if (options?.stationId) where.stationId = options.stationId;
    if (options?.onlyRecentDays) where.createdAt = { gte: new Date(Date.now() - options.onlyRecentDays * 86400000) };
    const unresolved = await prisma.unresolvedSample.findMany({ where, take: limit, orderBy: { createdAt: "desc" } });
    let created = 0;
    for (const s of unresolved) {
      await prisma.rematchJob.upsert({ where: { id: `${s.id}-us` }, update: {}, create: { id: `${s.id}-us`, targetType: "unresolved_sample", targetId: s.id, stationId: s.stationId, triggerReason: "fingerprint_added", priority: 80 } });
      created++;
    }
    return { created };
  }

  static async previewRematchCandidates(input?: { stationId?: string; onlyUnknowns?: boolean; onlyWeakMatches?: boolean; limit?: number }) {
    const limit = input?.limit ?? 100;
    const unresolved = await prisma.unresolvedSample.findMany({ where: { ...(input?.stationId ? { stationId: input.stationId } : {}), ...(input?.onlyUnknowns ? { verifiedTrackId: null } : {}) }, take: limit, orderBy: { createdAt: "desc" } });
    const weakLogs = input?.onlyUnknowns ? [] : await prisma.detectionLog.findMany({ where: { ...(input?.stationId ? { stationId: input.stationId } : {}), OR: [{ status: "unresolved" }, { verifiedTrackId: null, confidence: { lt: 0.7 } }] }, take: limit, orderBy: { observedAt: "desc" } });
    return { unresolvedCandidates: unresolved.length, weakLogCandidates: weakLogs.length, unresolved, weakLogs };
  }

  static scoreRematchEvidence(input: { fingerprintConfidence?: number; dirtyMetadata?: boolean; hasVerifiedTrack?: boolean }) {
    let score = input.fingerprintConfidence ?? 0;
    if (input.hasVerifiedTrack) score += 0.1;
    if (input.dirtyMetadata) score -= 0.15;
    return Math.max(0, Math.min(1, score));
  }

  static async rematchUnresolvedSample(sampleId: string, dryRun = true, minConfidenceToApply = 0.9) {
    const s = await prisma.unresolvedSample.findUnique({ where: { id: sampleId } });
    if (!s) return { status: "failed", reason: "sample_not_found" };
    if (!s.filePath || !fs.existsSync(s.filePath)) return { status: "failed", reason: "file_missing" };
    const fp = await FingerprintService.generateFingerprint(s.filePath);
    if (!fp) return { status: "no_match", reason: "fingerprint_failed" };
    const local = await LocalFingerprintService.lookup(fp);
    if (!local) return { status: "no_match", reason: "no_local_match" };
    const confidence = this.scoreRematchEvidence({ fingerprintConfidence: local.confidence, hasVerifiedTrack: !!local.title });
    if (confidence < 0.7) return { status: "no_match", reason: "low_confidence" };
    if (confidence < minConfidenceToApply) return { status: "needs_review", reason: "needs_review", confidence, suggestion: local };
    if (!dryRun) {
      await prisma.unresolvedSample.update({ where: { id: s.id }, data: { recoveryStatus: "recovered", fingerprintStatus: "fingerprinted", reviewedAt: new Date() } });
    }
    return { status: "matched", confidence, suggestion: local };
  }

  static async rematchDetectionLog(logId: string, dryRun = true, minConfidenceToApply = 0.9) {
    const log = await prisma.detectionLog.findUnique({ where: { id: logId } });
    if (!log) return { status: "failed", reason: "log_not_found" };
    if (isHumanVerifiedDetectionLog(log as any)) return { status: "skipped", reason: "human_verified_protected" };
    if (!WEAK_PROVIDERS.includes((log.sourceProvider || "").toLowerCase()) && (log.confidence ?? 0) >= 0.7) return { status: "skipped", reason: "not_weak_match" };
    const dirty = this.classifyStreamMetadata(log.rawStreamText || "");
    const confidence = this.scoreRematchEvidence({ fingerprintConfidence: 0.92, dirtyMetadata: dirty.dirty });
    if (confidence < minConfidenceToApply) return { status: "needs_review", reason: dirty.reason || "needs_review", confidence };
    const safeUpdate = buildSafeDetectionLogUpdate(log as any, { sourceProvider: "local_fingerprint_rematch", status: "matched", confidence, reasonCode: "rematch_auto_applied" }, false);
    if (!dryRun) await prisma.detectionLog.update({ where: { id: logId }, data: safeUpdate as any });
    return { status: "matched", confidence };
  }

  static async runRematchBatch(input?: { limit?: number; dryRun?: boolean; minConfidenceToApply?: number }) {
    const limit = input?.limit ?? 50; const dryRun = input?.dryRun !== false; const min = input?.minConfidenceToApply ?? 0.9;
    const jobs = await prisma.rematchJob.findMany({ where: { status: "pending" }, orderBy: [{ priority: "desc" }, { createdAt: "asc" }], take: limit });
    let matched=0, no_match=0, needs_review=0, failed=0;
    for (const j of jobs) {
      try {
        const res = j.targetType === "unresolved_sample" ? await this.rematchUnresolvedSample(j.targetId, dryRun, min) : await this.rematchDetectionLog(j.targetId, dryRun, min);
        const status = res.status;
        if (status === "matched") matched++; else if (status === "no_match") no_match++; else if (status === "needs_review") needs_review++; else if (status === "failed") failed++;
        if (!dryRun) await prisma.rematchJob.update({ where: { id: j.id }, data: { status, attempts: { increment: 1 }, completedAt: new Date(), newConfidence: (res as any).confidence ?? null, evidenceJson: JSON.stringify(res) } });
      } catch (e:any) {
        failed++; if (!dryRun) await prisma.rematchJob.update({ where: { id: j.id }, data: { status: "failed", attempts: { increment: 1 }, failedAt: new Date(), error: String(e?.message || e) } });
      }
    }
    return { processed: jobs.length, matched, no_match, needs_review, failed, dryRun };
  }

  static async summary() {
    const by = await prisma.rematchJob.groupBy({ by: ["status"], _count: { _all: true } });
    const pending = by.find(x=>x.status==="pending")?._count._all ?? 0;
    const failed = by.find(x=>x.status==="failed")?._count._all ?? 0;
    const needs_review = by.find(x=>x.status==="needs_review")?._count._all ?? 0;
    const matched = by.find(x=>x.status==="matched")?._count._all ?? 0;
    return { pending, failed, needs_review, matched, by };
  }
}
