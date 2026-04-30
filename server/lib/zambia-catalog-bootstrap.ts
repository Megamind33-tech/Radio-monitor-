import axios from "axios";
import { createHash } from "crypto";
import fs from "fs";
import path from "path";
import { prisma } from "./prisma.js";
import { logger } from "./logger.js";
import { validateCandidateStreamUrl } from "./stream-url-guard.js";

const UA =
  process.env.STREAM_DISCOVERY_UA ||
  process.env.STREAM_REFRESH_UA ||
  "Mozilla/5.0 (X11; Linux x86_64) RadioMonitor/1.0 (zambia-bootstrap)";

const RADIO_BROWSER_BASES = [
  "https://de1.api.radio-browser.info",
  "https://de2.api.radio-browser.info",
  "https://fi1.api.radio-browser.info",
  "https://nl1.api.radio-browser.info",
  "https://all.api.radio-browser.info",
];

/** Same as harvest/import stable id from URL (24 hex chars). */
export function stableRbIdFromUrl(url: string): string {
  const h = createHash("sha256").update(String(url).trim()).digest("hex").slice(0, 24);
  return `zm_rb_${h}`;
}

type RbRow = {
  name?: string;
  url?: string;
  url_resolved?: string;
  countrycode?: string;
  state?: string;
  tags?: string;
  stationuuid?: string;
  votes?: number;
  clickcount?: number;
  lastcheckok?: number;
};

type SeedStation = {
  id: string;
  name: string;
  country: string;
  district?: string;
  province?: string;
  frequencyMhz?: string | null;
  streamUrl: string;
  streamFormatHint?: string | null;
  sourceIdsJson?: string | null;
  icyQualification?: string | null;
  icySampleTitle?: string | null;
  isActive?: boolean;
  metadataPriorityEnabled?: boolean;
  fingerprintFallbackEnabled?: boolean;
  metadataStaleSeconds?: number;
  pollIntervalSeconds?: number;
  audioFingerprintIntervalSeconds?: number;
  sampleSeconds?: number;
  archiveSongSamples?: boolean;
};

function bootstrapEnabled(): boolean {
  const v = String(process.env.AUTO_ZAMBIA_CATALOG_BOOTSTRAP ?? "").trim().toLowerCase();
  if (v === "false" || v === "0" || v === "no" || v === "off") return false;
  if (v === "true" || v === "1" || v === "yes" || v === "on") return true;
  return process.env.NODE_ENV !== "production";
}

function defaultSeedPath(): string {
  const fromEnv = String(process.env.ZAMBIA_BOOTSTRAP_SEED_JSON || "").trim();
  if (fromEnv) {
    return path.isAbsolute(fromEnv) ? fromEnv : path.join(process.cwd(), fromEnv);
  }
  return path.join(process.cwd(), "scripts", "seed", "zambia_catalog_min.json");
}

/**
 * Load committed harvest-shaped JSON (same shape as `import_zambia_stations.mjs`) from disk.
 * Works in fresh clones where `scripts/data/` is gitignored and never populated.
 */
function loadSeedStationsFromDisk(): SeedStation[] {
  const p = defaultSeedPath();
  try {
    if (!fs.existsSync(p)) return [];
    const raw = fs.readFileSync(p, "utf-8");
    const data = JSON.parse(raw) as { stations?: SeedStation[] };
    if (!data || !Array.isArray(data.stations)) return [];
    return data.stations.filter((s) => s && typeof s.id === "string" && typeof s.streamUrl === "string");
  } catch (e) {
    logger.warn({ err: String(e), path: p }, "Zambia seed catalog read failed");
    return [];
  }
}

async function upsertSeedStation(row: SeedStation): Promise<boolean> {
  const urlCheck = validateCandidateStreamUrl(row.streamUrl);
  if (!urlCheck.accepted) return false;

  await prisma.station.upsert({
    where: { id: row.id },
    create: {
      id: row.id,
      name: row.name,
      country: row.country || "Zambia",
      district: row.district ?? "",
      province: row.province ?? "",
      frequencyMhz: row.frequencyMhz ?? null,
      streamUrl: urlCheck.canonicalUrl,
      streamFormatHint: row.streamFormatHint ?? "icy",
      sourceIdsJson: row.sourceIdsJson ?? null,
      icyQualification: row.icyQualification ?? "partial",
      icySampleTitle: row.icySampleTitle || null,
      isActive: row.isActive ?? true,
      metadataPriorityEnabled: row.metadataPriorityEnabled ?? true,
      fingerprintFallbackEnabled: row.fingerprintFallbackEnabled ?? true,
      metadataStaleSeconds: row.metadataStaleSeconds ?? 300,
      pollIntervalSeconds: row.pollIntervalSeconds ?? 120,
      audioFingerprintIntervalSeconds: row.audioFingerprintIntervalSeconds ?? 120,
      sampleSeconds: row.sampleSeconds ?? 20,
      archiveSongSamples: row.archiveSongSamples ?? true,
      monitorState: "UNKNOWN",
      contentClassification: "unknown",
      visibilityEnabled: true,
    },
    update: {
      name: row.name,
      streamUrl: urlCheck.canonicalUrl,
      sourceIdsJson: row.sourceIdsJson ?? undefined,
      icyQualification: row.icyQualification ?? undefined,
      isActive: row.isActive ?? true,
      visibilityEnabled: true,
    },
  });
  return true;
}

async function upsertZnbcBaseline(): Promise<number> {
  const rows = [
    {
      id: "zm_req_znbc_radio_1",
      name: "ZNBC RADIO 1",
      frequencyMhz: "102.9",
      streamUrl: "https://eu6.fastcast4u.com/proxy/radio1?mp=/1",
      sourceIdsJson: JSON.stringify({ direct: "fastcast4u-radio1", requested_seed: "znbc" }),
    },
    {
      id: "zm_req_znbc_radio_2",
      name: "ZNBC RADIO 2",
      frequencyMhz: "95.7",
      streamUrl: "https://eu6.fastcast4u.com/proxy/radio2?mp=/1",
      sourceIdsJson: JSON.stringify({ direct: "fastcast4u-radio2", requested_seed: "znbc" }),
    },
    {
      id: "zm_req_znbc_radio_4",
      name: "ZNBC RADIO 4",
      streamUrl: "https://stream.zeno.fm/hh6p7m5v8f8uv",
      sourceIdsJson: JSON.stringify({ zeno: "znbc-radio-4", requested_seed: "znbc" }),
    },
  ];
  let n = 0;
  for (const row of rows) {
    const urlCheck = validateCandidateStreamUrl(row.streamUrl);
    if (!urlCheck.accepted) continue;
    await prisma.station.upsert({
      where: { id: row.id },
      create: {
        id: row.id,
        name: row.name,
        country: "Zambia",
        district: "Lusaka",
        province: "Lusaka",
        frequencyMhz: row.frequencyMhz ?? null,
        streamUrl: urlCheck.canonicalUrl,
        streamFormatHint: "icy",
        sourceIdsJson: row.sourceIdsJson,
        icyQualification: "weak",
        isActive: true,
        metadataPriorityEnabled: true,
        fingerprintFallbackEnabled: true,
        metadataStaleSeconds: 300,
        pollIntervalSeconds: 120,
        audioFingerprintIntervalSeconds: 120,
        sampleSeconds: 20,
        archiveSongSamples: true,
        monitorState: "UNKNOWN",
        contentClassification: "unknown",
        visibilityEnabled: true,
      },
      update: {
        name: row.name,
        country: "Zambia",
        streamUrl: urlCheck.canonicalUrl,
        sourceIdsJson: row.sourceIdsJson,
        isActive: true,
        visibilityEnabled: true,
      },
    });
    n += 1;
  }
  return n;
}

const RB_PATHS = [
  "/json/stations/bycountrycodeexact/ZM",
  "/json/stations/bycountry/Zambia",
  "/json/stations/search?countrycode=ZM&limit=120&hidebroken=false",
];

async function fetchZambiaStationsFromRadioBrowser(maxStations: number): Promise<RbRow[]> {
  const bases = (process.env.RADIO_BROWSER_API_BASES || "")
    .split(/[\s,]+/)
    .map((s) => s.replace(/\/$/, ""))
    .filter(Boolean);
  const servers = bases.length > 0 ? bases : RADIO_BROWSER_BASES;

  const seen = new Set<string>();
  const out: RbRow[] = [];

  for (const base of servers) {
    for (const rbPath of RB_PATHS) {
      try {
        const url = `${base}${rbPath}`;
        const res = await axios.get<RbRow[]>(url, {
          timeout: 25_000,
          headers: { "User-Agent": UA, Accept: "application/json" },
          validateStatus: (s) => s === 200,
        });
        if (!Array.isArray(res.data)) continue;
        for (const row of res.data) {
          const u = String(row.url_resolved || row.url || "").trim();
          if (!u.startsWith("http")) continue;
          const cc = String(row.countrycode || "").toUpperCase();
          if (cc && cc !== "ZM") continue;
          if (seen.has(u)) continue;
          seen.add(u);
          out.push(row);
          if (out.length >= maxStations) return out;
        }
        if (out.length > 0) {
          return out;
        }
      } catch (e) {
        logger.warn({ err: String(e), base, rbPath }, "Radio Browser Zambia fetch failed; trying next");
      }
    }
  }
  return out;
}

/**
 * When the Station table is empty (fresh DB / mis-pointed SQLite file), load Zambia streams:
 * 1) ZNBC baseline, 2) committed seed JSON (`scripts/seed/zambia_catalog_min.json`), 3) Radio Browser mirrors.
 */
export async function ensureZambiaCatalogWhenEmpty(): Promise<{
  seeded: number;
  skipped: boolean;
  source: string;
}> {
  if (!bootstrapEnabled()) {
    return { seeded: 0, skipped: true, source: "disabled" };
  }

  const total = await prisma.station.count();
  if (total > 0) {
    return { seeded: 0, skipped: true, source: "already_has_stations" };
  }

  const maxRb = Math.min(
    120,
    Math.max(10, parseInt(process.env.ZAMBIA_BOOTSTRAP_MAX_STATIONS || "48", 10) || 48)
  );

  const znbc = await upsertZnbcBaseline();

  let seedCount = 0;
  const seedRows = loadSeedStationsFromDisk();
  for (const row of seedRows) {
    if (String(row.country || "").toLowerCase() !== "zambia") continue;
    const ok = await upsertSeedStation(row);
    if (ok) seedCount += 1;
  }

  const usedUrls = new Set(
    (
      await prisma.station.findMany({
        select: { streamUrl: true, preferredStreamUrl: true },
      })
    ).flatMap((s) => [s.streamUrl, s.preferredStreamUrl || ""])
  );

  const rbRows = await fetchZambiaStationsFromRadioBrowser(maxRb);
  const scored = rbRows
    .map((r) => {
      const u = String(r.url_resolved || r.url || "").trim();
      const check = validateCandidateStreamUrl(u);
      if (!check.accepted) return null;
      const votes = Number(r.votes) || 0;
      const clicks = Number(r.clickcount) || 0;
      const okBonus = r.lastcheckok === 1 ? 50 : 0;
      const score = votes * 3 + clicks + okBonus;
      return { row: r, url: check.canonicalUrl, score };
    })
    .filter((x): x is NonNullable<typeof x> => !!x)
    .sort((a, b) => b.score - a.score);

  let rbSeeded = 0;
  for (const { row, url } of scored) {
    if (rbSeeded >= maxRb) break;
    if (usedUrls.has(url)) continue;

    const id = stableRbIdFromUrl(url);
    const name = String(row.name || "Zambia station").trim() || "Zambia station";
    const district = String(row.state || "").trim();
    const tags = String(row.tags || "").trim();
    const uuid = String(row.stationuuid || "").trim();

    await prisma.station.upsert({
      where: { id },
      create: {
        id,
        name,
        country: "Zambia",
        district: district || "",
        province: "",
        streamUrl: url,
        streamFormatHint: "icy",
        sourceIdsJson: JSON.stringify({
          radio_browser: uuid || undefined,
          radio_browser_country: "ZM",
          radio_browser_tags: tags || undefined,
        }),
        icyQualification: "partial",
        isActive: true,
        metadataPriorityEnabled: true,
        fingerprintFallbackEnabled: true,
        metadataStaleSeconds: 300,
        pollIntervalSeconds: 120,
        audioFingerprintIntervalSeconds: 120,
        sampleSeconds: 20,
        archiveSongSamples: true,
        monitorState: "UNKNOWN",
        contentClassification: "unknown",
        visibilityEnabled: true,
      },
      update: {
        name,
        streamUrl: url,
        isActive: true,
        visibilityEnabled: true,
      },
    });
    usedUrls.add(url);
    rbSeeded += 1;
  }

  const seeded = znbc + seedCount + rbSeeded;
  const sourceParts = [`znbc=${znbc}`];
  if (seedCount) sourceParts.push(`seed_file=${seedCount}`);
  sourceParts.push(`radio_browser=${rbSeeded}`);

  logger.warn(
    { znbcStations: znbc, seedFileStations: seedCount, radioBrowserStations: rbSeeded, total: seeded },
    "Station catalog was empty — bootstrapped Zambia catalog. For more stations: npm run harvest:zambia && npm run import:zambia (writes scripts/data/, gitignored)."
  );

  return {
    seeded,
    skipped: false,
    source: sourceParts.join(", "),
  };
}
