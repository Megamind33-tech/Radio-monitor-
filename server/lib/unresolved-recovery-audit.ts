import { Prisma } from "@prisma/client";
import type { UnresolvedSample } from "@prisma/client";
import { prisma } from "./prisma.js";
import { RecoveryReason } from "./unresolved-evidence.js";
import type { MatchResult } from "../types.js";

/** DetectionLog.reasonCode values produced by automatic unresolved recovery (revert allowed when not manually tagged). */
export const REVERTABLE_RECOVERY_REASON_CODES = new Set<string>([
  RecoveryReason.TITLE_AUTO_VERIFIED,
  RecoveryReason.TITLE_AUTO_TRUSTED_LOCAL,
  "recovered_from_unresolved_sample",
]);

export type RecoveryAuditKind = "title_verified" | "title_trusted_localfp" | "audio_resolver";

export async function createUnresolvedRecoveryAudit(input: {
  row: UnresolvedSample;
  createdDetectionLogId: string;
  kind: RecoveryAuditKind;
  match: MatchResult;
  titleNormKey?: string | null;
}): Promise<void> {
  await prisma.unresolvedRecoveryAudit.create({
    data: {
      unresolvedSampleId: input.row.id,
      stationId: input.row.stationId,
      kind: input.kind,
      createdDetectionLogId: input.createdDetectionLogId,
      previousRecoveryStatus: input.row.recoveryStatus,
      previousRecoveryReason: input.row.recoveryReason ?? null,
      previousRecoveredAt: input.row.recoveredAt ?? null,
      previousVerifiedTrackId: input.row.verifiedTrackId ?? null,
      previousLastRecoveryError: input.row.lastRecoveryError ?? null,
      titleFinal: (input.match.title || "").trim() || null,
      artistFinal: (input.match.artist || "").trim() || null,
      titleNormKey: input.titleNormKey ?? input.row.titleNormKey ?? null,
      matchSnapshotJson: JSON.stringify({
        sourceProvider: input.match.sourceProvider,
        reasonCode: input.match.reasonCode,
        recordingId: input.match.recordingId,
        confidence: input.match.confidence,
      }),
    },
  });
}

export type UnresolvedClusterRow = {
  titleNormKey: string;
  recoveryReason: string | null;
  sampleCount: number;
  stationSpread: number;
  firstSeen: Date;
  lastSeen: Date;
  stationIdsSample: string[];
};

const DEFAULT_CLUSTER_REASONS = [
  RecoveryReason.WEAK_METADATA_REVIEW,
  RecoveryReason.NO_EXACT_TITLE_SUPPORT,
  RecoveryReason.PENDING_CLASSIFICATION,
  RecoveryReason.FINGERPRINT_ONLY_NO_TITLE,
  RecoveryReason.FINGERPRINT_EXHAUSTED,
];

export async function listUnresolvedClusters(input: {
  minSamples: number;
  limit: number;
  stationId?: string;
  recoveryReasons?: string[];
}): Promise<UnresolvedClusterRow[]> {
  const minSamples = Math.max(2, Math.min(500, input.minSamples));
  const limit = Math.min(200, Math.max(1, input.limit));
  const reasons = input.recoveryReasons?.length ? input.recoveryReasons : DEFAULT_CLUSTER_REASONS;
  const reasonParts = reasons.map((r) => Prisma.sql`${r}`);
  const reasonClause = input.recoveryReasons?.length
    ? Prisma.sql`"recoveryReason" IN (${Prisma.join(reasonParts)})`
    : Prisma.sql`("recoveryReason" IN (${Prisma.join(reasonParts)}) OR "recoveryReason" IS NULL)`;
  const stationFilter = input.stationId
    ? Prisma.sql`AND "stationId" = ${input.stationId}`
    : Prisma.empty;

  const raw = await prisma.$queryRaw<
    Array<{
      titleNormKey: string;
      recoveryReason: string | null;
      sampleCount: bigint;
      stationSpread: bigint;
      firstSeen: Date;
      lastSeen: Date;
      stationIdsSample: string | null;
    }>
  >(Prisma.sql`
    SELECT
      "titleNormKey" as titleNormKey,
      "recoveryReason" as recoveryReason,
      COUNT(*) as sampleCount,
      COUNT(DISTINCT "stationId") as stationSpread,
      MIN("createdAt") as firstSeen,
      MAX("createdAt") as lastSeen,
      GROUP_CONCAT(DISTINCT "stationId") as stationIdsSample
    FROM "UnresolvedSample"
    WHERE "titleNormKey" IS NOT NULL
      AND "recoveryStatus" IN ('pending', 'no_match', 'error')
      AND ${reasonClause}
      ${stationFilter}
    GROUP BY "titleNormKey", "recoveryReason"
    HAVING COUNT(*) >= ${minSamples}
    ORDER BY COUNT(*) DESC
    LIMIT ${limit}
  `);

  return raw.map((r) => ({
    titleNormKey: r.titleNormKey,
    recoveryReason: r.recoveryReason,
    sampleCount: Number(r.sampleCount),
    stationSpread: Number(r.stationSpread),
    firstSeen: r.firstSeen,
    lastSeen: r.lastSeen,
    stationIdsSample: (r.stationIdsSample ?? "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
      .slice(0, 24),
  }));
}

export async function revertUnresolvedRecoveryAudit(auditId: string, revertNote?: string): Promise<{
  ok: boolean;
  reason?: string;
}> {
  const audit = await prisma.unresolvedRecoveryAudit.findUnique({ where: { id: auditId } });
  if (!audit) return { ok: false, reason: "audit_not_found" };
  if (audit.revertedAt) return { ok: false, reason: "already_reverted" };

  const log = await prisma.detectionLog.findUnique({ where: { id: audit.createdDetectionLogId } });
  if (!log) return { ok: false, reason: "detection_log_missing" };
  if (log.manuallyTagged) return { ok: false, reason: "human_manual_tag_blocked" };
  const rc = (log.reasonCode ?? "").trim();
  if (!REVERTABLE_RECOVERY_REASON_CODES.has(rc)) return { ok: false, reason: "reason_code_not_revertable" };

  await prisma.$transaction(async (tx) => {
    await tx.detectionLog.delete({ where: { id: log.id } });

    const spin = await tx.stationSongSpin.findFirst({
      where: {
        stationId: audit.stationId,
        lastDetectionLogId: audit.createdDetectionLogId,
      },
    });
    if (spin) {
      const next = spin.playCount - 1;
      if (next <= 0) {
        await tx.stationSongSpin.delete({ where: { id: spin.id } });
      } else {
        await tx.stationSongSpin.update({
          where: { id: spin.id },
          data: {
            playCount: next,
            lastDetectionLogId: null,
          },
        });
      }
    }

    await tx.unresolvedSample.update({
      where: { id: audit.unresolvedSampleId },
      data: {
        recoveryStatus: audit.previousRecoveryStatus,
        recoveryReason: audit.previousRecoveryReason,
        recoveredAt: audit.previousRecoveredAt,
        verifiedTrackId: audit.previousVerifiedTrackId,
        lastRecoveryError: audit.previousLastRecoveryError,
      },
    });

    await tx.unresolvedRecoveryAudit.update({
      where: { id: auditId },
      data: {
        revertedAt: new Date(),
        revertNote: revertNote?.slice(0, 2000) ?? null,
      },
    });
  });

  return { ok: true };
}

export async function listRecoveryAudits(input: { take: number; onlyActive: boolean }) {
  const take = Math.min(200, Math.max(1, input.take));
  return prisma.unresolvedRecoveryAudit.findMany({
    where: input.onlyActive ? { revertedAt: null } : {},
    orderBy: { createdAt: "desc" },
    take,
  });
}
