#!/usr/bin/env npx tsx
/**
 * Enqueue RematchJob rows for high-repeat catalogue enrichment targets.
 *
 *   npx tsx scripts/unresolved_enqueue_enrichment.ts --dry-run
 *   npx tsx scripts/unresolved_enqueue_enrichment.ts --apply --take 40 --min-repeat 5
 */
import { enqueueCatalogueEnrichmentJobs } from "../server/lib/unresolved-enrichment-queue.js";
import { prisma } from "../server/lib/prisma.js";

function numArg(name: string, fallback: number): number {
  const raw = process.argv.find((a) => a.startsWith(`${name}=`));
  if (!raw) return fallback;
  const n = Number(raw.split("=")[1]);
  return Number.isFinite(n) ? n : fallback;
}

async function main() {
  const dryRun = !process.argv.includes("--apply");
  const take = numArg("--take", 40);
  const minRepeat = numArg("--min-repeat", 5);
  const out = await enqueueCatalogueEnrichmentJobs({ take, minRepeat, dryRun });
  console.log(JSON.stringify({ ...out, dryRun }, null, 2));
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
