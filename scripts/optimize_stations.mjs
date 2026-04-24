#!/usr/bin/env node
/**
 * scripts/optimize_stations.mjs
 * ─────────────────────────────────────────────────────────────────────────────
 * Auto-tune per-station monitor settings based on the last 24h of DetectionLog
 * history.  The goal is to move every station closer to the 80% match target
 * by copying the strategy that already-accurate stations are using:
 *
 *   - Low match rate + ICY present but junk  → FINGERPRINT_EVERY_POLL (per row),
 *     raise audioFingerprintIntervalSeconds down to 60s, raise sampleSeconds
 *     to 60s, raise fingerprintRetries to 3.
 *   - Low match rate + no ICY at all         → legacy fingerprint path always on,
 *     metadataPriorityEnabled stays true but audioFingerprint every 45-60s.
 *   - Very high match rate                   → keep current config; lock poll
 *     cadence at 90s to save resources.
 *   - Stuck on station-branding ICY only     → tighten metadataTrustTightness +1
 *     so future branding strings don't even try the ICY-only lane.
 *
 * Usage:
 *   node scripts/optimize_stations.mjs                 # dry-run (default)
 *   node scripts/optimize_stations.mjs --apply         # write settings to DB
 *   node scripts/optimize_stations.mjs --since-hours 48 --apply
 *   node scripts/optimize_stations.mjs --fix-all --apply
 * ─────────────────────────────────────────────────────────────────────────────
 */
import "dotenv/config";
import { PrismaClient } from "@prisma/client";

function parseArgs() {
  const args = process.argv.slice(2);
  const out = { apply: false, sinceHours: 24, fixAll: false, highRate: 0.85, lowRate: 0.5 };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--apply") out.apply = true;
    else if (a === "--since-hours" && args[i + 1]) out.sinceHours = Math.max(1, parseInt(args[++i], 10) || 24);
    else if (a === "--fix-all") out.fixAll = true;
    else if (a === "--high-rate" && args[i + 1]) out.highRate = Math.max(0, Math.min(1, parseFloat(args[++i])));
    else if (a === "--low-rate" && args[i + 1]) out.lowRate = Math.max(0, Math.min(1, parseFloat(args[++i])));
  }
  return out;
}

const FP_METHODS = new Set(["fingerprint_acoustid", "fingerprint_local", "fingerprint_audd", "fingerprint_acrcloud"]);

async function main() {
  const opts = parseArgs();
  const prisma = new PrismaClient();
  const since = new Date(Date.now() - opts.sinceHours * 3600 * 1000);

  const stations = await prisma.station.findMany({ where: { isActive: true } });
  const logs = await prisma.detectionLog.findMany({
    where: { observedAt: { gte: since } },
    select: { stationId: true, status: true, detectionMethod: true, reasonCode: true, parsedTitle: true, parsedArtist: true },
  });

  const statsByStation = new Map();
  for (const s of stations) statsByStation.set(s.id, { total: 0, matched: 0, fp: 0, catalog: 0, icyOnly: 0, icyPresent: 0, brandingOnly: 0 });
  for (const l of logs) {
    const s = statsByStation.get(l.stationId);
    if (!s) continue;
    s.total++;
    if (l.parsedTitle || l.parsedArtist) s.icyPresent++;
    if (l.status === "matched") {
      s.matched++;
      if (FP_METHODS.has(l.detectionMethod)) s.fp++;
      else if (l.detectionMethod === "catalog_lookup") s.catalog++;
      else if (l.detectionMethod === "stream_metadata") s.icyOnly++;
    } else if (l.reasonCode === "station_branding_only_not_a_song" || l.reasonCode === "icy_no_artist_title_pair") {
      s.brandingOnly++;
    }
  }

  const plan = [];
  for (const s of stations) {
    const st = statsByStation.get(s.id);
    if (!st) continue;
    // If there is not enough history and --fix-all wasn't passed, skip.
    if (st.total < 5 && !opts.fixAll) continue;
    const rate = st.total ? st.matched / st.total : 0;
    const icyPresentRate = st.total ? st.icyPresent / st.total : 0;
    const brandingRate = st.total ? st.brandingOnly / st.total : 0;

    const patch = {};
    const reasons = [];

    if (rate >= opts.highRate) {
      // High performer — relax polling to save resources.
      if (s.pollIntervalSeconds < 90) { patch.pollIntervalSeconds = 90; reasons.push("high_rate_relax_poll_90s"); }
    } else if (rate < opts.lowRate || opts.fixAll) {
      // Low performer — adopt the aggressive strategy.
      if (s.sampleSeconds < 60) { patch.sampleSeconds = 60; reasons.push("raise_sample_to_60s"); }
      if (s.audioFingerprintIntervalSeconds > 60) { patch.audioFingerprintIntervalSeconds = 60; reasons.push("fingerprint_every_60s"); }
      if ((s.fingerprintRetries ?? 0) < 3) { patch.fingerprintRetries = 3; reasons.push("retry_3_attempts"); }
      if ((s.fingerprintRetryDelayMs ?? 0) < 5000) { patch.fingerprintRetryDelayMs = 5000; reasons.push("retry_delay_5s"); }
      if (icyPresentRate < 0.25) {
        if ((s.metadataStaleSeconds ?? 0) > 120) { patch.metadataStaleSeconds = 120; reasons.push("metadata_stale_120s"); }
      }
    }

    if (brandingRate > 0.3 && (s.metadataTrustTightness ?? 0) < 2) {
      patch.metadataTrustTightness = Math.min(2, (s.metadataTrustTightness ?? 0) + 1);
      reasons.push("tighten_branding_filter");
    }

    if (Object.keys(patch).length === 0) continue;
    plan.push({ id: s.id, name: s.name, rate, icyPresentRate, brandingRate, patch, reasons });
  }

  console.log(JSON.stringify({
    step: "optimizer_report",
    totalStations: stations.length,
    totalLogs: logs.length,
    toChange: plan.length,
    apply: opts.apply,
    highRate: opts.highRate,
    lowRate: opts.lowRate,
  }, null, 2));

  for (const p of plan) {
    console.log(JSON.stringify({
      name: p.name, rate: Number(p.rate.toFixed(3)),
      icyPresentRate: Number(p.icyPresentRate.toFixed(3)),
      brandingRate: Number(p.brandingRate.toFixed(3)),
      patch: p.patch,
      reasons: p.reasons,
    }));
  }

  if (opts.apply && plan.length) {
    for (const p of plan) {
      try {
        await prisma.station.update({ where: { id: p.id }, data: p.patch });
      } catch (e) {
        console.error(JSON.stringify({ step: "apply_failed", id: p.id, error: String(e?.message || e) }));
      }
    }
    console.log(JSON.stringify({ step: "optimizer_applied", changed: plan.length }));
  } else if (!opts.apply && plan.length) {
    console.log(JSON.stringify({ step: "optimizer_dry_run", hint: "Re-run with --apply to persist." }));
  }

  await prisma.$disconnect();
}

main().catch((e) => {
  console.error(JSON.stringify({ step: "fatal", error: String(e?.stack || e) }));
  process.exit(1);
});
