#!/usr/bin/env npx tsx
/**
 * Dry-run unresolved backlog analyzer: counts by recoveryStatus, recoveryReason,
 * and top repeated titleNormKey clusters (SQLite).
 *
 * Usage: npx tsx scripts/unresolved_recovery_dry_run.ts
 */
import { prisma } from "../server/lib/prisma.js";
import { RecoveryReason } from "../server/lib/unresolved-evidence.js";

async function main() {
  const byStatus = await prisma.unresolvedSample.groupBy({
    by: ["recoveryStatus"],
    _count: { _all: true },
  });
  const byReason = await prisma.unresolvedSample.groupBy({
    by: ["recoveryReason"],
    _count: { _all: true },
  });

  console.log("=== UnresolvedSample by recoveryStatus ===");
  for (const r of byStatus.sort((a, b) => Number(b._count._all) - Number(a._count._all))) {
    console.log(`  ${r.recoveryStatus}: ${r._count._all}`);
  }

  console.log("\n=== By recoveryReason (semantic lane) ===");
  for (const r of byReason.sort((a, b) => Number(b._count._all) - Number(a._count._all))) {
    const key = r.recoveryReason ?? "(null)";
    console.log(`  ${key}: ${r._count._all}`);
  }

  const targets = [
    { label: "auto_recoverable_verified_title", reason: RecoveryReason.TITLE_AUTO_VERIFIED },
    { label: "auto_recoverable_trusted_localfp_title", reason: RecoveryReason.TITLE_AUTO_TRUSTED_LOCAL },
    { label: "weak_metadata_candidate_review", reason: RecoveryReason.WEAK_METADATA_REVIEW },
    { label: "no_exact_title_support_enrichment", reason: RecoveryReason.NO_EXACT_TITLE_SUPPORT },
    { label: "programme_or_non_music", reason: RecoveryReason.PROGRAMME_OR_NON_MUSIC },
    { label: "dirty_web_title", reason: RecoveryReason.DIRTY_WEB_TITLE },
    { label: "fingerprint_only_no_title", reason: RecoveryReason.FINGERPRINT_ONLY_NO_TITLE },
  ];

  console.log("\n=== Lane summary (fixed reason codes) ===");
  for (const t of targets) {
    const n = await prisma.unresolvedSample.count({ where: { recoveryReason: t.reason } });
    console.log(`  ${t.label}: ${n}`);
  }

  const topClusters = await prisma.$queryRaw<Array<{ titleNormKey: string; cnt: bigint }>>`
    SELECT "titleNormKey", COUNT(*) as cnt
    FROM "UnresolvedSample"
    WHERE "titleNormKey" IS NOT NULL
    GROUP BY "titleNormKey"
    ORDER BY cnt DESC
    LIMIT 25
  `;

  console.log("\n=== Top 25 titleNormKey clusters (all reasons) ===");
  for (const row of topClusters) {
    console.log(`  ${Number(row.cnt)}×  ${row.titleNormKey}`);
  }

  const dayAgo = new Date(Date.now() - 86400000);
  const [c24, r24] = await Promise.all([
    prisma.unresolvedSample.count({ where: { createdAt: { gte: dayAgo } } }),
    prisma.unresolvedSample.count({
      where: { recoveredAt: { gte: dayAgo }, recoveryStatus: "recovered" },
    }),
  ]);
  console.log("\n=== 24h flow ===");
  console.log(`  created: ${c24}`);
  console.log(`  recovered: ${r24}`);
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
