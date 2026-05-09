import fs from "fs";
import crypto from "node:crypto";
import { prisma } from "../lib/prisma.js";

export type PurgeCheck = {
  eligible: boolean;
  reason: string;
  fileAvailable: boolean;
  fileSize: number;
};

export class SampleStorageService {
  /**
   * NOTE: These backfill methods should be treated as admin-only operations.
   * They never delete or move files; they only compute/store sha256 + file size.
   */
  static async previewHashBackfill(options?: {
    limit?: number;
    stationId?: string;
    onlyVerified?: boolean;
    force?: boolean;
  }) {
    return this.runHashBackfill({ ...options, dryRun: true });
  }

  static evaluateBackfillCandidate(input: {
    filePath?: string | null;
    hasHash: boolean;
    storedAudioBytes?: number | null;
    actualSize?: number;
    force?: boolean;
  }) {
    if (!input.filePath || !input.filePath.trim()) return { wouldUpdate: false, reason: "no_file_path" };
    if (input.actualSize == null) return { wouldUpdate: false, reason: "file_missing" };
    const needsHash = !!input.force || !input.hasHash;
    const needsBytes = input.storedAudioBytes == null || input.storedAudioBytes !== input.actualSize;
    if (!needsHash && !needsBytes) return { wouldUpdate: false, reason: "already_up_to_date" };
    return { wouldUpdate: true, reason: needsHash ? "missing_hash" : "audio_bytes_mismatch" };
  }

  static async runHashBackfill(options?: {
    limit?: number;
    stationId?: string;
    onlyVerified?: boolean;
    force?: boolean;
    dryRun?: boolean;
  }) {
    const limit = Math.min(1000, Math.max(1, Number(options?.limit ?? 100)));
    const dryRun = options?.dryRun !== false;
    const force = options?.force === true;
    const where: any = {
      ...(options?.stationId ? { stationId: options.stationId } : {}),
      ...(options?.onlyVerified ? { verifiedTrackId: { not: null }, recoveryStatus: { in: ["human_verified", "recovered"] } } : {}),
    };
    const rows = await prisma.unresolvedSample.findMany({
      where,
      orderBy: { createdAt: "desc" },
      take: limit,
      select: {
        id: true,
        stationId: true,
        filePath: true,
        originalSha256: true,
        audioBytes: true,
      },
    });
    const stationIds = [...new Set(rows.map((r) => r.stationId))];
    const stations = stationIds.length
      ? await prisma.station.findMany({ where: { id: { in: stationIds } }, select: { id: true, name: true } })
      : [];
    const stationById = new Map(stations.map((s) => [s.id, s]));

    let alreadyHasHash = 0;
    let missingHash = 0;
    let missingFile = 0;
    let noFilePath = 0;
    let wouldUpdateCount = 0;
    let estimatedBytesCovered = 0;
    let updatedCount = 0;

    const previewRows: Array<Record<string, unknown>> = [];
    for (const row of rows) {
      const hasHash = !!row.originalSha256;
      if (hasHash) alreadyHasHash += 1; else missingHash += 1;
      if (!row.filePath || !row.filePath.trim()) {
        noFilePath += 1;
        previewRows.push({
          sampleId: row.id,
          stationId: row.stationId,
          stationName: stationById.get(row.stationId)?.name ?? null,
          fileAvailable: false,
          fileSize: 0,
          hasHash,
          wouldUpdate: false,
          reason: "no_file_path",
        });
        continue;
      }
      if (!fs.existsSync(row.filePath)) {
        missingFile += 1;
        previewRows.push({
          sampleId: row.id,
          stationId: row.stationId,
          stationName: stationById.get(row.stationId)?.name ?? null,
          fileAvailable: false,
          fileSize: 0,
          hasHash,
          wouldUpdate: false,
          reason: "file_missing",
        });
        continue;
      }
      const actualSize = fs.statSync(row.filePath).size;
      const evaluated = this.evaluateBackfillCandidate({
        filePath: row.filePath,
        hasHash,
        storedAudioBytes: row.audioBytes,
        actualSize,
        force,
      });
      const needsHash = force || !row.originalSha256;
      const wouldUpdate = evaluated.wouldUpdate;
      if (!wouldUpdate) {
        previewRows.push({
          sampleId: row.id,
          stationId: row.stationId,
          stationName: stationById.get(row.stationId)?.name ?? null,
          fileAvailable: true,
          fileSize: actualSize,
          hasHash,
          wouldUpdate: false,
          reason: evaluated.reason,
        });
        continue;
      }
      wouldUpdateCount += 1;
      estimatedBytesCovered += actualSize;
      let nextHash = row.originalSha256;
      if (needsHash) {
        nextHash = this.computeSha256(row.filePath);
      }
      if (!dryRun) {
        await prisma.unresolvedSample.update({
          where: { id: row.id },
          data: {
            originalSha256: nextHash,
            audioBytes: actualSize,
            purgeDryRunAt: new Date(),
          },
        });
        updatedCount += 1;
      }
      previewRows.push({
        sampleId: row.id,
        stationId: row.stationId,
        stationName: stationById.get(row.stationId)?.name ?? null,
        fileAvailable: true,
        fileSize: actualSize,
        hasHash,
        wouldUpdate: true,
          reason: evaluated.reason,
        });
      }

    return {
      dryRunOnly: dryRun,
      totalScanned: rows.length,
      alreadyHasHash,
      missingHash,
      missingFile,
      noFilePath,
      wouldUpdateCount,
      updatedCount,
      estimatedBytesCovered,
      rows: previewRows,
    };
  }

  static checkEligibility(sample: {
    filePath: string;
    recoveryStatus: string;
    fingerprintStatus: string;
    fingerprintedAt: Date | null;
    verifiedTrackId: string | null;
    purgeStatus?: string | null;
    originalSha256?: string | null;
  }, linkedLog?: { manuallyTagged: boolean; verifiedTrackId: string | null } | null): PurgeCheck {
    if (!sample.filePath || !fs.existsSync(sample.filePath)) {
      return { eligible: false, reason: "file_missing", fileAvailable: false, fileSize: 0 };
    }
    const fileSize = fs.statSync(sample.filePath).size;
    if (!sample.verifiedTrackId) return { eligible: false, reason: "missing_verified_track", fileAvailable: true, fileSize };
    if (sample.recoveryStatus !== "human_verified" && sample.recoveryStatus !== "recovered") {
      return { eligible: false, reason: "not_human_verified", fileAvailable: true, fileSize };
    }
    if (sample.fingerprintStatus === "failed") return { eligible: false, reason: "fingerprint_failed", fileAvailable: true, fileSize };
    if (sample.fingerprintStatus !== "fingerprinted" || !sample.fingerprintedAt) {
      return { eligible: false, reason: "not_fingerprinted", fileAvailable: true, fileSize };
    }
    if (!sample.originalSha256) return { eligible: false, reason: "missing_file_hash", fileAvailable: true, fileSize };
    if (!linkedLog) return { eligible: false, reason: "no_linked_detection_log", fileAvailable: true, fileSize };
    if (!(linkedLog.manuallyTagged || (linkedLog.verifiedTrackId && linkedLog.verifiedTrackId.trim()))) {
      return { eligible: false, reason: "linked_log_not_verified", fileAvailable: true, fileSize };
    }
    if (sample.purgeStatus === "purged") return { eligible: false, reason: "already_purged", fileAvailable: true, fileSize };
    return { eligible: true, reason: "eligible", fileAvailable: true, fileSize };
  }

  static computeSha256(filePath: string): string {
    const hash = crypto.createHash("sha256");
    hash.update(fs.readFileSync(filePath));
    return hash.digest("hex");
  }

  static async storageSummary() {
    const rows = await prisma.unresolvedSample.findMany({ orderBy: { createdAt: "desc" } });
    const stationIds = [...new Set(rows.map((r) => r.stationId))];
    const stations = await prisma.station.findMany({
      where: { id: { in: stationIds } },
      select: { id: true, name: true },
    });
    const stationById = new Map(stations.map((s) => [s.id, s]));
    const logIds = rows.map((r) => r.detectionLogId).filter((id): id is string => !!id);
    const logs = logIds.length
      ? await prisma.detectionLog.findMany({
          where: { id: { in: logIds } },
          select: { id: true, manuallyTagged: true, verifiedTrackId: true },
        })
      : [];
    const logById = new Map(logs.map((l) => [l.id, l]));
    let totalBytes = 0;
    let withAudio = 0;
    let missingAudio = 0;
    let reviewed = 0;
    let humanVerified = 0;
    let fingerprinted = 0;
    let fingerprintFailed = 0;
    let eligible = 0;
    let notEligible = 0;
    let reclaimable = 0;
    const byStation = new Map<string, { stationId: string; stationName: string; sampleCount: number; audioBytes: number; purgeEligibleCount: number; reclaimableBytes: number }>();
    for (const row of rows) {
      const linkedLog = row.detectionLogId ? logById.get(row.detectionLogId) ?? null : null;
      const check = this.checkEligibility(row as any, linkedLog as any);
      if (check.fileAvailable) withAudio++; else missingAudio++;
      totalBytes += check.fileSize;
      if (row.recoveryStatus !== "pending") reviewed++;
      if (row.recoveryStatus === "human_verified" || row.recoveryStatus === "recovered") humanVerified++;
      if (row.fingerprintStatus === "fingerprinted") fingerprinted++;
      if (row.fingerprintStatus === "failed") fingerprintFailed++;
      if (check.eligible) {
        eligible++;
        reclaimable += check.fileSize;
      } else {
        notEligible++;
      }
      const stationName = stationById.get(row.stationId)?.name ?? "Unknown Station";
      const current = byStation.get(row.stationId) ?? {
        stationId: row.stationId,
        stationName,
        sampleCount: 0,
        audioBytes: 0,
        purgeEligibleCount: 0,
        reclaimableBytes: 0,
      };
      current.sampleCount += 1;
      current.audioBytes += check.fileSize;
      if (check.eligible) {
        current.purgeEligibleCount += 1;
        current.reclaimableBytes += check.fileSize;
      }
      byStation.set(row.stationId, current);
    }
    return {
      totalUnknownSampleCount: rows.length,
      countWithAudioFile: withAudio,
      countMissingAudioFile: missingAudio,
      totalAudioBytes: totalBytes,
      reviewedCount: reviewed,
      humanVerifiedCount: humanVerified,
      fingerprintedCount: fingerprinted,
      fingerprintFailedCount: fingerprintFailed,
      eligibleForPurgeCount: eligible,
      notEligibleForPurgeCount: notEligible,
      estimatedBytesReclaimable: reclaimable,
      byStation: Array.from(byStation.values()).sort((a, b) => b.audioBytes - a.audioBytes),
    };
  }
}
