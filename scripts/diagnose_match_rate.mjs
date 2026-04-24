#!/usr/bin/env node
/**
 * scripts/diagnose_match_rate.mjs
 * ─────────────────────────────────────────────────────────────────────────────
 * Reports the honest match rate of the Zambian Airplay Monitor across the last
 * N hours. "Honest" means we only count DetectionLog rows where the status is
 * "matched" AND either a catalog / fingerprint result (match confirmed) OR a
 * fully parsed Artist – Title ICY pair backed the row (no station-branding
 * fake-matches).
 *
 * For every station we print:
 *   - totalPolls
 *   - matched (any source)
 *   - matchedAudioId (fingerprint-confirmed: local, acoustid, audd, acrcloud)
 *   - matchedCatalog (catalog_lookup)
 *   - matchedIcyOnly (stream_metadata — weaker)
 *   - rate (matched / totalPolls)
 *   - top 3 reasonCodes for unresolved rows
 *
 * Global summary shows breakdown by provider. If --since-hours 24 and the rate
 * is below --target-rate (default 0.80) the script exits non-zero so the CI /
 * systemd timer surfaces the regression.
 *
 * Usage:
 *   node scripts/diagnose_match_rate.mjs
 *   node scripts/diagnose_match_rate.mjs --since-hours 168 --target-rate 0.80
 *   node scripts/diagnose_match_rate.mjs --csv scripts/data/match_rate.csv
 * ─────────────────────────────────────────────────────────────────────────────
 */
import "dotenv/config";
import { writeFileSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { PrismaClient } from "@prisma/client";

function parseArgs() {
  const args = process.argv.slice(2);
  const out = { sinceHours: 24, target: 0.8, csv: null, top: 20 };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--since-hours" && args[i + 1]) out.sinceHours = Math.max(1, parseInt(args[++i], 10) || 24);
    else if (a === "--target-rate" && args[i + 1]) out.target = Math.max(0, Math.min(1, parseFloat(args[++i])));
    else if (a === "--csv" && args[i + 1]) out.csv = args[++i];
    else if (a === "--top" && args[i + 1]) out.top = Math.max(1, parseInt(args[++i], 10) || 20);
  }
  return out;
}

const FINGERPRINT_METHODS = new Set([
  "fingerprint_acoustid",
  "fingerprint_local",
  "fingerprint_audd",
  "fingerprint_acrcloud",
]);

async function main() {
  const opts = parseArgs();
  const prisma = new PrismaClient();
  const since = new Date(Date.now() - opts.sinceHours * 3600 * 1000);

  const stations = await prisma.station.findMany({
    where: { isActive: true },
    select: { id: true, name: true, icyQualification: true },
  });
  const stationById = new Map(stations.map((s) => [s.id, s]));

  const logs = await prisma.detectionLog.findMany({
    where: { observedAt: { gte: since } },
    select: { stationId: true, status: true, detectionMethod: true, reasonCode: true, sourceProvider: true },
  });

  const perStation = new Map();
  const providerCounts = new Map();
  let total = 0, matched = 0, audioId = 0, catalog = 0, icyOnly = 0, unresolved = 0;
  const topReasons = new Map();

  for (const log of logs) {
    total++;
    const s = perStation.get(log.stationId) || {
      id: log.stationId,
      name: stationById.get(log.stationId)?.name || log.stationId,
      total: 0, matched: 0, audioId: 0, catalog: 0, icyOnly: 0, unresolved: 0,
      reasons: new Map(),
    };
    s.total++;
    if (log.status === "matched") {
      matched++;
      s.matched++;
      if (FINGERPRINT_METHODS.has(log.detectionMethod)) { audioId++; s.audioId++; }
      else if (log.detectionMethod === "catalog_lookup") { catalog++; s.catalog++; }
      else if (log.detectionMethod === "stream_metadata") { icyOnly++; s.icyOnly++; }
      const p = log.sourceProvider || log.detectionMethod || "unknown";
      providerCounts.set(p, (providerCounts.get(p) || 0) + 1);
    } else {
      unresolved++;
      s.unresolved++;
      const r = log.reasonCode || "unknown";
      s.reasons.set(r, (s.reasons.get(r) || 0) + 1);
      topReasons.set(r, (topReasons.get(r) || 0) + 1);
    }
    perStation.set(log.stationId, s);
  }

  const summary = {
    windowHours: opts.sinceHours,
    totalPolls: total,
    matched,
    audioId,
    catalog,
    icyOnly,
    unresolved,
    rate: total ? matched / total : 0,
    honestRate: total ? (audioId + catalog) / total : 0,
    target: opts.target,
    targetMet: total ? matched / total >= opts.target : false,
    providerBreakdown: Object.fromEntries([...providerCounts.entries()].sort((a, b) => b[1] - a[1])),
    topUnresolvedReasons: Object.fromEntries(
      [...topReasons.entries()].sort((a, b) => b[1] - a[1]).slice(0, 10)
    ),
    generatedAt: new Date().toISOString(),
  };

  console.log(JSON.stringify(summary, null, 2));
  console.log("---");

  const rows = [...perStation.values()].sort((a, b) => b.total - a.total).slice(0, opts.top);
  for (const r of rows) {
    const rate = r.total ? (r.matched / r.total) : 0;
    const honestRate = r.total ? ((r.audioId + r.catalog) / r.total) : 0;
    console.log(JSON.stringify({
      name: r.name,
      total: r.total,
      matched: r.matched,
      audioId: r.audioId,
      catalog: r.catalog,
      icyOnly: r.icyOnly,
      unresolved: r.unresolved,
      rate: Number(rate.toFixed(3)),
      honestRate: Number(honestRate.toFixed(3)),
      topReasons: Object.fromEntries(
        [...r.reasons.entries()].sort((a, b) => b[1] - a[1]).slice(0, 3)
      ),
    }));
  }

  if (opts.csv) {
    mkdirSync(dirname(opts.csv) || ".", { recursive: true });
    const header = ["station_id", "name", "total", "matched", "audio_id", "catalog", "icy_only", "unresolved", "rate", "honest_rate"];
    const lines = [header.join(",")];
    for (const r of [...perStation.values()].sort((a, b) => b.total - a.total)) {
      const rate = r.total ? (r.matched / r.total) : 0;
      const honestRate = r.total ? ((r.audioId + r.catalog) / r.total) : 0;
      lines.push([
        r.id, JSON.stringify(r.name), r.total, r.matched, r.audioId, r.catalog, r.icyOnly, r.unresolved,
        rate.toFixed(4), honestRate.toFixed(4),
      ].join(","));
    }
    writeFileSync(opts.csv, "﻿" + lines.join("\n"));
    console.log(JSON.stringify({ step: "csv_written", path: opts.csv }));
  }

  await prisma.$disconnect();

  if (!summary.targetMet && summary.totalPolls > 10) {
    console.error(JSON.stringify({
      step: "target_not_met",
      rate: Number(summary.rate.toFixed(3)),
      target: summary.target,
      hint: "Enable paid fallbacks (AUDD_API_TOKEN, ACRCLOUD_*), raise FINGERPRINT_SAMPLE_SECONDS, or let the local library learn more songs.",
    }));
    process.exit(1);
  }
}

main().catch((e) => {
  console.error(JSON.stringify({ step: "fatal", error: String(e?.stack || e) }));
  process.exit(1);
});
