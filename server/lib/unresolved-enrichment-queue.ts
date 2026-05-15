/**
 * Build catalogue enrichment RematchJob rows for high-repeat "no exact title support" clusters.
 */
import { prisma } from "./prisma.js";
import { RecoveryReason } from "./unresolved-evidence.js";

export async function enqueueCatalogueEnrichmentJobs(input: {
  take: number;
  minRepeat: number;
  dryRun: boolean;
}): Promise<{ clusters: number; jobsCreated: number }> {
  const take = Math.min(500, Math.max(1, input.take));
  const minRepeat = Math.min(500, Math.max(2, input.minRepeat));

  const clusters = await prisma.$queryRaw<Array<{ titleNormKey: string; cnt: bigint }>>`
    SELECT "titleNormKey", COUNT(*) as cnt
    FROM "UnresolvedSample"
    WHERE "recoveryReason" = ${RecoveryReason.NO_EXACT_TITLE_SUPPORT}
      AND "titleNormKey" IS NOT NULL
    GROUP BY "titleNormKey"
    HAVING COUNT(*) >= ${minRepeat}
    ORDER BY cnt DESC
    LIMIT ${take}
  `;

  let jobsCreated = 0;
  for (const c of clusters) {
    const rep = await prisma.unresolvedSample.findFirst({
      where: {
        titleNormKey: c.titleNormKey,
        recoveryReason: RecoveryReason.NO_EXACT_TITLE_SUPPORT,
      },
      orderBy: { createdAt: "desc" },
      select: { id: true, stationId: true },
    });
    if (!rep) continue;
    const priority = 60 + Math.min(35, Number(c.cnt));
    const jobId = `enrich-${rep.id}`;
    if (input.dryRun) {
      jobsCreated += 1;
      continue;
    }
    await prisma.rematchJob.upsert({
      where: { id: jobId },
      update: { priority, triggerReason: "catalogue_enrichment_cluster" },
      create: {
        id: jobId,
        targetType: "unresolved_sample",
        targetId: rep.id,
        stationId: rep.stationId,
        triggerReason: "catalogue_enrichment_cluster",
        priority,
      },
    });
    jobsCreated += 1;
  }

  return { clusters: clusters.length, jobsCreated };
}
