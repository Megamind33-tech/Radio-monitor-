#!/usr/bin/env npx tsx
/**
 * Safe title auto-recovery (verified + trusted LocalFingerprint pairs only).
 *
 * Usage:
 *   npx tsx scripts/unresolved_safe_title_recover.ts --dry-run
 *   npx tsx scripts/unresolved_safe_title_recover.ts --apply --limit 150
 */
import { UnresolvedRecoveryService } from "../server/services/unresolved-recovery.service.js";

function argFlag(name: string): boolean {
  return process.argv.includes(name);
}

async function main() {
  const dryRun = !argFlag("--apply");
  const limitArg = process.argv.find((a) => a.startsWith("--limit="));
  const limit = limitArg ? Number(limitArg.split("=")[1]) : 200;
  const out = await UnresolvedRecoveryService.runSafeTitleAutoRecoveryBatch({
    limit: Number.isFinite(limit) ? limit : 200,
    dryRun,
  });
  console.log(JSON.stringify(out, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
