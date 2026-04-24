#!/usr/bin/env node
/**
 * scripts/drain_unresolved_aggressive.mjs
 * ─────────────────────────────────────────────────────────────────────────────
 * Drain the UnresolvedSample backlog using every identifier available:
 *   1. Local fingerprint library (free, fast)
 *   2. AcoustID (free, rate-limited)
 *   3. AudD (paid)
 *   4. ACRCloud (paid)
 *   5. Text catalog lookup from the associated DetectionLog's rawStreamText
 *
 * Each recovered match upgrades the DetectionLog (status → matched, source
 * provider populated) and teaches the local fingerprint library so repeat plays
 * are free next time.
 *
 * Usage:
 *   node scripts/drain_unresolved_aggressive.mjs --max-passes 10 --batch 30
 *   node scripts/drain_unresolved_aggressive.mjs --station-id <id>
 *
 * This script imports the compiled TypeScript services from dist/server/. Make
 * sure `npm run build` has been run (setup_digitalocean.sh does it for you).
 * ─────────────────────────────────────────────────────────────────────────────
 */
import "dotenv/config";
import { existsSync } from "node:fs";
import { PrismaClient } from "@prisma/client";

const DIST = "../dist/server";

function parseArgs() {
  const args = process.argv.slice(2);
  const out = { maxPasses: 5, batch: 25, stationId: null, cooldownSec: 30 };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--max-passes" && args[i + 1]) out.maxPasses = Math.max(1, parseInt(args[++i], 10) || 5);
    else if (a === "--batch" && args[i + 1]) out.batch = Math.max(1, Math.min(200, parseInt(args[++i], 10) || 25));
    else if (a === "--station-id" && args[i + 1]) out.stationId = args[++i];
    else if (a === "--cooldown" && args[i + 1]) out.cooldownSec = Math.max(0, parseInt(args[++i], 10) || 30);
  }
  return out;
}

async function loadServices() {
  // Fail fast with a clear message when the build hasn't been run.
  const buildMarker = new URL(`${DIST}/services/unresolved-recovery.service.js`, import.meta.url).pathname;
  if (!existsSync(buildMarker)) {
    throw new Error(`Compiled services not found at ${buildMarker}. Run "npm run build" first.`);
  }
  const { UnresolvedRecoveryService } = await import(`${DIST}/services/unresolved-recovery.service.js`);
  return { UnresolvedRecoveryService };
}

async function main() {
  const opts = parseArgs();
  const prisma = new PrismaClient();
  const { UnresolvedRecoveryService } = await loadServices();

  const baseWhere = { recoveryStatus: "pending", ...(opts.stationId ? { stationId: opts.stationId } : {}) };

  const initialCount = await prisma.unresolvedSample.count({ where: baseWhere });
  console.log(JSON.stringify({ step: "drain_start", pending: initialCount, maxPasses: opts.maxPasses, batch: opts.batch }));

  for (let pass = 1; pass <= opts.maxPasses; pass++) {
    const remaining = await prisma.unresolvedSample.count({ where: baseWhere });
    if (remaining === 0) {
      console.log(JSON.stringify({ step: "drain_empty", pass }));
      break;
    }
    const before = remaining;
    try {
      const summary = await UnresolvedRecoveryService.runBatch({
        limit: opts.batch,
        stationId: opts.stationId ?? undefined,
        continueWithoutAcoustid: true,
      });
      console.log(JSON.stringify({ step: "drain_pass", pass, before, summary }));
      if (summary.processed === 0) {
        console.log(JSON.stringify({ step: "drain_quiescent", pass }));
        break;
      }
    } catch (e) {
      console.error(JSON.stringify({ step: "drain_pass_error", pass, error: String(e?.message || e) }));
    }
    if (pass < opts.maxPasses) {
      await new Promise((r) => setTimeout(r, opts.cooldownSec * 1000));
    }
  }

  const finalCount = await prisma.unresolvedSample.count({ where: baseWhere });
  const recoveredCount = await prisma.unresolvedSample.count({
    where: { ...(opts.stationId ? { stationId: opts.stationId } : {}), recoveryStatus: "recovered" },
  });
  console.log(JSON.stringify({ step: "drain_done", initial: initialCount, finalPending: finalCount, totalRecovered: recoveredCount }));
  await prisma.$disconnect();
}

main().catch((e) => {
  console.error(JSON.stringify({ step: "fatal", error: String(e?.stack || e) }));
  process.exit(1);
});
