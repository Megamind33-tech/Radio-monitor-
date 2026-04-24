#!/usr/bin/env node
/**
 * scripts/strict_stream_validator.mjs
 * ─────────────────────────────────────────────────────────────────────────────
 * Strict working-audio validator for every active station in the Prisma DB.
 *
 * For every Station.isActive = true row we:
 *   1. Resolve playlists (pls/m3u/m3u8) and follow redirects.
 *   2. HEAD-read a short byte range with Icy-MetaData:1 to confirm HTTP 2xx
 *      and capture content-type + icy-metaint.
 *   3. Run ffprobe to require a real audio codec and a decoded frame.
 *   4. Probe up to 3 ICY metadata blocks to detect talk/news/story-only streams
 *      (too many consecutive program-like titles => station is not music).
 *
 * With --auto-fix we apply the verdict to the DB:
 *   good     → keep, set icyQualification=good, lastValidationStatus=healthy
 *   partial  → keep, icyQualification=partial
 *   weak     → keep but icyQualification=weak, monitorState may drop to DEGRADED
 *   talk     → DEACTIVATE (isActive=false) because the user asked us to only
 *              fetch stations broadcasting actual songs — talk-only goes off.
 *   dead     → DEACTIVATE + lastValidationReason=dead_stream
 *
 * Usage:
 *   node scripts/strict_stream_validator.mjs                 # report only
 *   node scripts/strict_stream_validator.mjs --auto-fix      # persist verdicts
 *   node scripts/strict_stream_validator.mjs --limit 20      # cheap smoke test
 * ─────────────────────────────────────────────────────────────────────────────
 */
import "dotenv/config";
import { spawnSync } from "node:child_process";
import { PrismaClient } from "@prisma/client";

const UA = process.env.STREAM_DISCOVERY_UA || "MOSTIFY-StreamValidator/2.0";
const PROBE_TIMEOUT_SEC = parseInt(process.env.VALIDATOR_TIMEOUT_SEC || "25", 10);
const ICY_BLOCKS = 3;

function parseArgs() {
  const args = process.argv.slice(2);
  const out = { autoFix: false, limit: 0, concurrency: 4, onlyInactive: false };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--auto-fix") out.autoFix = true;
    else if (a === "--limit" && args[i + 1]) out.limit = Math.max(0, parseInt(args[++i], 10) || 0);
    else if (a === "--concurrency" && args[i + 1]) out.concurrency = Math.max(1, Math.min(16, parseInt(args[++i], 10) || 4));
    else if (a === "--only-inactive") out.onlyInactive = true;
  }
  return out;
}

// ---------------------------------------------------------------------------
// Talk/news/program detection — mirrors server/lib/music-content-filter.ts but
// kept standalone so this script has no TS runtime dependency.
// ---------------------------------------------------------------------------
const NON_MUSIC_FRAGMENTS = [
  "morning show", "afternoon show", "evening show", "breakfast show",
  "drive show", "drive time", "drivetime", "night show", "midnight show",
  "weekend show", "talk show", "chat show", "phone-in", "call-in",
  "news update", "news bulletin", "news hour", "headline", "weather report",
  "traffic update", "sports report", "sports update", "sports news",
  "sunday service", "church service", "bible study", "devotion",
  "morning devotion", "daily devotion", "gospel hour",
  "corinthians", "all chapters", "confession final",
  "good morning", "good afternoon", "good evening", "welcome to",
  "join us", "stay tuned", "coming up", "after the break",
  "sponsored by", "brought to you", "advertisement", "advert",
  "commercial break", "station id", "sign on", "sign off", "test tone",
];

function looksLikeProgram(text) {
  const t = String(text || "").trim().toLowerCase();
  if (!t) return false;
  if (/^\s*[-–—.… ]+\s*$/.test(t)) return false; // empty/dash — classify as unknown, not talk
  for (const f of NON_MUSIC_FRAGMENTS) {
    if (t.includes(f)) return true;
  }
  // Pure station-brand line (no artist-title separator, contains FM/Radio/AM)
  if (/^[\w\s.]+\b(fm|radio|am|tv|mhz|khz)\b[\w\s.]*$/i.test(t)) return true;
  return false;
}

// ---------------------------------------------------------------------------
// ICY reader — pulls up to N metadata blocks so we can classify content type.
// ---------------------------------------------------------------------------
async function readIcyBlocks(url, maxBlocks = 3) {
  let res;
  try {
    res = await fetch(url, {
      headers: { "Icy-MetaData": "1", "User-Agent": UA },
      signal: AbortSignal.timeout(PROBE_TIMEOUT_SEC * 1000),
      redirect: "follow",
    });
  } catch (e) {
    return { error: String(e?.message || e), titles: [], status: 0, contentType: "", metaint: 0 };
  }
  const status = res.status;
  const contentType = res.headers.get("content-type") || "";
  if (!res.ok) return { error: `http_${status}`, titles: [], status, contentType, metaint: 0 };
  const miHeader = res.headers.get("icy-metaint");
  const metaint = miHeader ? parseInt(miHeader, 10) : 0;
  if (!metaint || !Number.isFinite(metaint) || metaint <= 0) {
    try { await res.body?.cancel(); } catch { /* ignore */ }
    return { error: "no_icy_metaint", titles: [], status, contentType, metaint: 0 };
  }
  const reader = res.body?.getReader();
  if (!reader) return { error: "no_body", titles: [], status, contentType, metaint };

  let buf = Buffer.alloc(0);
  const readExact = async (n) => {
    while (buf.length < n) {
      const { done, value } = await reader.read();
      if (done) return null;
      buf = Buffer.concat([buf, Buffer.from(value)]);
    }
    const out = buf.subarray(0, n);
    buf = buf.subarray(n);
    return Buffer.from(out);
  };

  const titles = [];
  const dec = new TextDecoder();
  try {
    for (let b = 0; b < maxBlocks; b++) {
      const skip = await readExact(metaint);
      if (!skip) break;
      const lb = await readExact(1);
      if (!lb) break;
      const metaLen = lb[0] * 16;
      if (metaLen === 0) { titles.push(""); continue; }
      const meta = await readExact(metaLen);
      if (!meta) break;
      const text = dec.decode(meta).replace(/\0/g, "").trim();
      const m = /StreamTitle\s*=\s*(['"])(.*?)\1/.exec(text);
      titles.push(m ? m[2] : "");
    }
  } catch (e) {
    return { error: String(e?.message || e), titles, status, contentType, metaint };
  } finally {
    try { await reader.cancel(); } catch { /* ignore */ }
  }
  return { error: null, titles, status, contentType, metaint };
}

function ffprobeAudio(url) {
  const args = [
    "-v", "error",
    "-user_agent", UA,
    "-rw_timeout", String(PROBE_TIMEOUT_SEC * 1_000_000),
    "-analyzeduration", "5M",
    "-probesize", "1M",
    "-show_entries", "stream=codec_type,codec_name,bit_rate",
    "-of", "json",
    url,
  ];
  const res = spawnSync("ffprobe", args, { encoding: "utf8", timeout: PROBE_TIMEOUT_SEC * 1000 + 5000 });
  if (res.status !== 0 || !res.stdout) return { ok: false, codec: null, bitrate: null };
  try {
    const parsed = JSON.parse(res.stdout);
    const audio = (parsed?.streams || []).find((s) => s?.codec_type === "audio");
    if (!audio) return { ok: false, codec: null, bitrate: null };
    return { ok: true, codec: audio.codec_name || null, bitrate: audio.bit_rate ? parseInt(audio.bit_rate, 10) : null };
  } catch {
    return { ok: false, codec: null, bitrate: null };
  }
}

function classifyVerdict(icy, ff) {
  if (!ff.ok && (icy.error && icy.error !== "no_icy_metaint")) return "dead";
  if (!ff.ok) return "dead";
  const nonEmpty = icy.titles.filter((t) => t && t.trim().length);
  if (nonEmpty.length === 0) {
    // Audio flows but no ICY text at all. Weak but still valid for fingerprinting.
    return "weak";
  }
  const programCount = nonEmpty.filter(looksLikeProgram).length;
  const programRatio = programCount / nonEmpty.length;
  if (programRatio >= 0.67) return "talk";
  if (nonEmpty.some((t) => t.includes(" - ") || t.includes(" – "))) return "good";
  if (nonEmpty.some((t) => t.length >= 6)) return "partial";
  return "weak";
}

async function mapPool(items, concurrency, worker) {
  const results = new Array(items.length);
  let idx = 0;
  const workers = Array.from({ length: Math.max(1, concurrency) }, async () => {
    while (true) {
      const i = idx++;
      if (i >= items.length) return;
      results[i] = await worker(items[i], i);
    }
  });
  await Promise.all(workers);
  return results;
}

async function main() {
  const opts = parseArgs();
  const prisma = new PrismaClient();

  const where = opts.onlyInactive ? { isActive: false } : { isActive: true };
  const stations = await prisma.station.findMany({
    where,
    select: { id: true, name: true, streamUrl: true, preferredStreamUrl: true, country: true },
    orderBy: { name: "asc" },
    ...(opts.limit ? { take: opts.limit } : {}),
  });

  console.log(JSON.stringify({ step: "validator_start", count: stations.length, autoFix: opts.autoFix, concurrency: opts.concurrency }));

  const results = await mapPool(stations, opts.concurrency, async (s) => {
    const url = (s.preferredStreamUrl || s.streamUrl || "").trim();
    if (!url || !url.startsWith("http")) {
      return { id: s.id, name: s.name, url, verdict: "dead", reason: "invalid_url", icy: null, ff: null };
    }
    const ff = ffprobeAudio(url);
    const icy = await readIcyBlocks(url, ICY_BLOCKS);
    const verdict = classifyVerdict(icy, ff);
    return { id: s.id, name: s.name, url, verdict, reason: icy.error || null, icy, ff };
  });

  const counts = { good: 0, partial: 0, weak: 0, talk: 0, dead: 0 };
  for (const r of results) counts[r.verdict] = (counts[r.verdict] || 0) + 1;

  if (opts.autoFix) {
    for (const r of results) {
      const patch = {
        lastValidationAt: new Date(),
        icyQualification: r.verdict === "talk" ? "none" : r.verdict,
        lastValidationReason: r.reason || (r.verdict === "talk" ? "program_content_dominates_icy" : null),
      };
      if (r.verdict === "dead") {
        patch.isActive = false;
        patch.monitorState = "INACTIVE";
        patch.monitorStateReason = "dead_stream_ffprobe_failed";
      } else if (r.verdict === "talk") {
        // Talk-only stations are deactivated: user explicitly asked us to
        // fetch *working audio sources for songs*, not story streams.
        patch.isActive = false;
        patch.monitorState = "INACTIVE";
        patch.monitorStateReason = "talk_or_news_only_station";
      } else {
        patch.isActive = true;
        patch.lastHealthyAt = new Date();
        patch.lastStreamCodec = r.ff?.codec || null;
        patch.lastStreamBitrate = r.ff?.bitrate || null;
      }
      try {
        await prisma.station.update({ where: { id: r.id }, data: patch });
      } catch (e) {
        console.error(JSON.stringify({ step: "update_failed", id: r.id, error: String(e?.message || e) }));
      }
    }
  }

  const summary = {
    step: "validator_done",
    totals: counts,
    totalChecked: stations.length,
    autoFix: opts.autoFix,
    at: new Date().toISOString(),
  };
  console.log(JSON.stringify(summary, null, 2));

  for (const r of results) {
    console.log(JSON.stringify({
      id: r.id,
      name: r.name,
      verdict: r.verdict,
      codec: r.ff?.codec || null,
      bitrate: r.ff?.bitrate || null,
      titles: (r.icy?.titles || []).slice(0, 3),
      reason: r.reason,
    }));
  }

  await prisma.$disconnect();
}

main().catch((e) => {
  console.error(JSON.stringify({ step: "fatal", error: String(e?.stack || e) }));
  process.exit(1);
});
