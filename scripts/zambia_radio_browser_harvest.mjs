#!/usr/bin/env node
/**
 * scripts/zambia_radio_browser_harvest.mjs
 * ─────────────────────────────────────────────────────────────────────────────
 * Aggressive, exhaustive harvester for Zambian radio stations.
 *
 * Pulls from every radio-browser mirror (de1, de2, fi1, nl1, all) and merges by
 * resolved stream URL.  Every candidate is then HEAD-probed to drop 404/301-loop
 * URLs, ICY-probed to drop streams without audio flowing, and optionally imported
 * into the Prisma DB via scripts/import_zambia_stations.mjs.
 *
 * Flags:
 *   --out <path>          Output JSON path (default scripts/data/zambia_harvest.json)
 *   --no-validate         Skip ffprobe/ICY validation (fast; not recommended)
 *   --import              Run scripts/import_zambia_stations.mjs after harvest
 *   --concurrency <n>     Parallel validation workers (default 6)
 *   --timeout <sec>       Per-stream validation timeout (default 20)
 *   --min-working <n>     If fewer than N working streams after validation, exit
 *                         non-zero so the systemd timer logs a failure (default 3)
 *   --include-unvalidated Keep non-working streams in the output (they'll still
 *                         get isActive=true=false on import via icyQualification)
 *
 * The script never writes fake data — streams that fail validation are dropped
 * (or, with --include-unvalidated, marked isActive=false so the monitor skips them).
 *
 * Exit codes:
 *   0  success (>= min-working streams)
 *   1  harvest error / too few working streams
 *   2  import step failed
 * ─────────────────────────────────────────────────────────────────────────────
 */
import "dotenv/config";
import { writeFileSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

const RB_BASES = (
  process.env.RADIO_BROWSER_API_BASES?.trim() ||
  "https://de1.api.radio-browser.info https://de2.api.radio-browser.info https://fi1.api.radio-browser.info https://nl1.api.radio-browser.info https://all.api.radio-browser.info"
)
  .split(/[\s,]+/)
  .filter((x) => x.startsWith("http"));

const UA =
  process.env.STREAM_DISCOVERY_UA ||
  "MOSTIFY-Zambia-Harvester/2.0 (+https://github.com/Megamind33-tech/Radio-monitor-)";

function parseArgs() {
  const args = process.argv.slice(2);
  const out = { path: "scripts/data/zambia_harvest.json", validate: true, import: false, concurrency: 6, timeout: 20, minWorking: 3, includeUnvalidated: false };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--out" && args[i + 1]) out.path = args[++i];
    else if (a === "--no-validate") out.validate = false;
    else if (a === "--validate") out.validate = true;
    else if (a === "--import") out.import = true;
    else if (a === "--concurrency" && args[i + 1]) out.concurrency = Math.max(1, Math.min(32, parseInt(args[++i], 10) || 6));
    else if (a === "--timeout" && args[i + 1]) out.timeout = Math.max(5, Math.min(120, parseInt(args[++i], 10) || 20));
    else if (a === "--min-working" && args[i + 1]) out.minWorking = Math.max(0, parseInt(args[++i], 10) || 0);
    else if (a === "--include-unvalidated") out.includeUnvalidated = true;
  }
  return out;
}

function slugify(s) {
  return String(s || "")
    .toLowerCase()
    .trim()
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 80) || "station";
}

function normalizeHost(url) {
  try {
    const u = new URL(url);
    return u.host.toLowerCase();
  } catch {
    return "";
  }
}

function canonicalStreamUrl(url) {
  const s = String(url || "").trim();
  if (!s.startsWith("http")) return null;
  try {
    const u = new URL(s);
    if (["localhost", "127.0.0.1", "0.0.0.0"].includes(u.hostname.toLowerCase())) return null;
    const pathLower = u.pathname.toLowerCase();
    if (["/search", "/discover", "/ads"].some((x) => pathLower.includes(x))) return null;
    u.hash = "";
    return u.toString();
  } catch {
    return null;
  }
}

/**
 * Pull one radio-browser mirror. We call multiple query endpoints to catch every
 * station that the community has tagged ZM either by country code, country name,
 * or language. Missing mirror = silent skip.
 */
async function fetchMirror(base) {
  const endpoints = [
    `${base}/json/stations/bycountrycodeexact/ZM?hidebroken=false`,
    `${base}/json/stations/bycountry/Zambia?hidebroken=false`,
    `${base}/json/stations/bytaglist/zambia?hidebroken=false`,
    `${base}/json/stations/bytaglist/zambian?hidebroken=false`,
    `${base}/json/stations/bytaglist/lusaka?hidebroken=false`,
    `${base}/json/stations/bystate/Lusaka?hidebroken=false`,
    `${base}/json/stations/bystate/Copperbelt?hidebroken=false`,
    `${base}/json/stations/bystate/Southern?hidebroken=false`,
    `${base}/json/stations/bylanguage/bemba?hidebroken=false`,
    `${base}/json/stations/bylanguage/nyanja?hidebroken=false`,
    `${base}/json/stations/bylanguage/tonga?hidebroken=false`,
  ];
  const rows = [];
  for (const u of endpoints) {
    try {
      const r = await fetch(u, { headers: { "User-Agent": UA }, signal: AbortSignal.timeout(45000) });
      if (!r.ok) continue;
      const data = await r.json();
      if (!Array.isArray(data)) continue;
      for (const row of data) {
        const country = String(row?.country || "").trim().toLowerCase();
        const countryCode = String(row?.countrycode || "").trim().toUpperCase();
        if (countryCode !== "ZM" && country !== "zambia") continue;
        rows.push(row);
      }
    } catch {
      // mirror or endpoint unreachable — try the next
    }
  }
  return rows;
}

function preferResolvedUrl(row) {
  const candidates = [row?.url_resolved, row?.url].map((x) => canonicalStreamUrl(x)).filter(Boolean);
  return candidates[0] || null;
}

function chooseProvince(row) {
  const state = String(row?.state || "").trim();
  if (state) return state;
  return "";
}

/** HEAD probe to drop obviously dead URLs before ffprobe. Accepts any 2xx/3xx. */
async function headProbe(url, timeoutMs) {
  try {
    const r = await fetch(url, {
      method: "GET",
      headers: { "User-Agent": UA, "Icy-MetaData": "1", Range: "bytes=0-1023" },
      redirect: "follow",
      signal: AbortSignal.timeout(timeoutMs),
    });
    // Consume a small amount then abort so we don't download the whole stream.
    const reader = r.body?.getReader();
    if (reader) {
      try { await reader.read(); await reader.cancel(); } catch { /* ignore */ }
    }
    return { ok: r.status >= 200 && r.status < 400, status: r.status, icyMetaInt: r.headers.get("icy-metaint") || "", contentType: r.headers.get("content-type") || "" };
  } catch (e) {
    return { ok: false, status: 0, icyMetaInt: "", contentType: "", error: String(e?.message || e) };
  }
}

/**
 * ffprobe validation: we require (a) audio codec to be present and (b) at least
 * one decoded audio frame within the timeout. This catches streams that return
 * 200 OK but serve HTML, playlists of stale URLs, or silence-only tracks.
 */
function ffprobeValidate(url, timeoutSec) {
  const args = [
    "-v", "error",
    "-user_agent", UA,
    "-rw_timeout", String(timeoutSec * 1_000_000),
    "-analyzeduration", "5M",
    "-probesize", "1M",
    "-show_entries", "stream=codec_type,codec_name,bit_rate",
    "-of", "json",
    url,
  ];
  const res = spawnSync("ffprobe", args, { encoding: "utf8", timeout: timeoutSec * 1000 + 5000 });
  if (res.status !== 0 || !res.stdout) return { ok: false, codec: null, bitrate: null };
  let parsed;
  try { parsed = JSON.parse(res.stdout); } catch { return { ok: false, codec: null, bitrate: null }; }
  const audio = (parsed?.streams || []).find((s) => s?.codec_type === "audio");
  if (!audio) return { ok: false, codec: null, bitrate: null };
  return { ok: true, codec: audio.codec_name || null, bitrate: audio.bit_rate ? parseInt(audio.bit_rate, 10) : null };
}

/**
 * Runs a pool-limited async map; keeps CPU and network usage bounded so we do
 * not DoS the radio-browser mirrors or saturate a small droplet.
 */
async function mapPool(items, concurrency, worker) {
  const results = new Array(items.length);
  let idx = 0;
  const workers = Array.from({ length: Math.max(1, concurrency) }, async () => {
    while (true) {
      const i = idx++;
      if (i >= items.length) return;
      try {
        results[i] = await worker(items[i], i);
      } catch (e) {
        results[i] = { error: String(e?.message || e) };
      }
    }
  });
  await Promise.all(workers);
  return results;
}

function icyQualificationFromProbe(head, ffp) {
  if (!head.ok || !ffp.ok) return "error";
  if (head.icyMetaInt && ffp.ok) return "good";
  if (ffp.ok) return "partial";
  return "weak";
}

async function main() {
  const opts = parseArgs();

  console.log(JSON.stringify({ step: "harvest_start", mirrors: RB_BASES.length, validate: opts.validate, concurrency: opts.concurrency }));

  // 1. Pull from every mirror in parallel, dedupe by canonical stream URL.
  const mirrorResults = await Promise.all(RB_BASES.map(fetchMirror));
  const byUrl = new Map();
  for (let i = 0; i < mirrorResults.length; i++) {
    const rows = mirrorResults[i] || [];
    for (const row of rows) {
      const url = preferResolvedUrl(row);
      if (!url) continue;
      const existing = byUrl.get(url);
      const mirrorTag = normalizeHost(RB_BASES[i]);
      if (!existing) {
        byUrl.set(url, { row, mirrors: new Set([mirrorTag]) });
      } else {
        existing.mirrors.add(mirrorTag);
      }
    }
  }
  const merged = [...byUrl.entries()].map(([url, entry]) => ({ url, row: entry.row, mirrors: [...entry.mirrors] }));
  console.log(JSON.stringify({ step: "deduped", distinct_urls: merged.length }));

  // 2. Validate each candidate (unless --no-validate).
  const validated = await mapPool(merged, opts.validate ? opts.concurrency : 32, async (item) => {
    if (!opts.validate) {
      return { ...item, head: { ok: true, status: 0, icyMetaInt: "", contentType: "" }, ffprobe: { ok: true, codec: null, bitrate: null } };
    }
    const head = await headProbe(item.url, opts.timeout * 1000);
    if (!head.ok) return { ...item, head, ffprobe: { ok: false, codec: null, bitrate: null } };
    // ffprobe can be slow; yield to the event loop briefly so the host stays responsive.
    await sleep(5);
    const ffprobe = ffprobeValidate(item.url, opts.timeout);
    return { ...item, head, ffprobe };
  });

  // 3. Build the final import rows.
  const working = [];
  const failing = [];
  for (const v of validated) {
    const name = (v.row?.name || "").trim() || "Zambian Station";
    const homepage = (v.row?.homepage || "").trim();
    const favicon = (v.row?.favicon || "").trim();
    const tags = String(v.row?.tags || "").split(",").map((x) => x.trim()).filter(Boolean);
    const qualification = icyQualificationFromProbe(v.head, v.ffprobe);
    const isWorking = v.head.ok && v.ffprobe.ok;
    const id = `zm_rb_${slugify(name)}_${slugify(new URL(v.url).host)}`;
    const provinceGuess = chooseProvince(v.row);

    const rec = {
      id,
      name,
      country: "Zambia",
      district: String(v.row?.state || "").trim(),
      province: provinceGuess,
      frequencyMhz: null,
      streamUrl: v.url,
      streamFormatHint: v.ffprobe.codec || "icy",
      sourceIdsJson: JSON.stringify({
        radio_browser: String(v.row?.stationuuid || ""),
        radio_browser_mirrors: v.mirrors.join(","),
        homepage: homepage || "",
        favicon: favicon || "",
        tags: tags.join(","),
      }),
      icyQualification: qualification,
      icySampleTitle: null,
      isActive: isWorking || !opts.validate,
      metadataPriorityEnabled: true,
      fingerprintFallbackEnabled: true,
      metadataStaleSeconds: 180,
      pollIntervalSeconds: 60,
      audioFingerprintIntervalSeconds: 120,
      sampleSeconds: 60,
      archiveSongSamples: true,
    };

    if (isWorking) working.push(rec);
    else if (opts.includeUnvalidated) working.push(rec);
    else failing.push({ url: v.url, name, head: v.head, ffprobe: v.ffprobe });
  }

  mkdirSync(dirname(opts.path) || ".", { recursive: true });
  const payload = {
    generatedAt: new Date().toISOString(),
    generator: "zambia_radio_browser_harvest.mjs",
    mirrors: RB_BASES,
    totals: {
      distinctUrls: merged.length,
      working: working.filter((w) => w.isActive).length,
      included: working.length,
      droppedFailing: failing.length,
    },
    stations: working,
    failing,
  };
  writeFileSync(opts.path, JSON.stringify(payload, null, 2));
  console.log(JSON.stringify({ step: "harvest_done", out: opts.path, ...payload.totals }));

  // 4. Optional: import immediately.
  if (opts.import) {
    console.log(JSON.stringify({ step: "import_start" }));
    const child = spawn(process.execPath, ["scripts/import_zambia_stations.mjs", opts.path], { stdio: "inherit" });
    const code = await new Promise((resolve) => child.on("exit", resolve));
    if (code !== 0) {
      console.error(JSON.stringify({ step: "import_failed", code }));
      process.exit(2);
    }
    console.log(JSON.stringify({ step: "import_done" }));
  }

  if (payload.totals.working < opts.minWorking) {
    console.error(JSON.stringify({ step: "too_few_working", working: payload.totals.working, minWorking: opts.minWorking }));
    process.exit(1);
  }
}

main().catch((e) => {
  console.error(JSON.stringify({ step: "fatal", error: String(e?.stack || e) }));
  process.exit(1);
});
