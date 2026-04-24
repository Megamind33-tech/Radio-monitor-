import axios from "axios";
import { createHash } from "crypto";
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

type RbRow = {
  name?: string;
  url?: string;
  url_resolved?: string;
  countrycode?: string;
  state?: string;
  tags?: string;
  votes?: number;
  clickcount?: number;
  lastcheckok?: number;
};

function bootstrapEnabled(): boolean {
  const v = String(process.env.AUTO_ZAMBIA_CATALOG_BOOTSTRAP ?? "").trim().toLowerCase();
  if (v === "false" || v === "0" || v === "no" || v === "off") return false;
  if (v === "true" || v === "1" || v === "yes" || v === "on") return true;
  // Default: dev/staging on; production off unless explicitly enabled
  return process.env.NODE_ENV !== "production";
}

function stableRbId(url: string): string {
  const h = createHash("sha256").update(url).digest("hex").slice(0, 20);
  return `zm_rb_${h}`;
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
        district: row.name.includes("ZNBC") ? "Lusaka" : "",
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

async function fetchZambiaStationsFromRadioBrowser(maxStations: number): Promise<RbRow[]> {
  const bases = (process.env.RADIO_BROWSER_API_BASES || "")
    .split(/[\s,]+/)
    .map((s) => s.replace(/\/$/, ""))
    .filter(Boolean);
  const servers = bases.length > 0 ? bases : RADIO_BROWSER_BASES;

  for (const base of servers) {
    try {
      const url = `${base}/json/stations/bycountrycodeexact/ZM`;
      const res = await axios.get<RbRow[]>(url, {
        timeout: 25_000,
        headers: { "User-Agent": UA, Accept: "application/json" },
        validateStatus: (s) => s === 200,
      });
      if (Array.isArray(res.data) && res.data.length > 0) {
        return res.data;
      }
    } catch (e) {
      logger.warn({ err: String(e), base }, "Radio Browser Zambia fetch failed; trying next mirror");
    }
  }
  return [];
}

/**
 * When the Station table is empty (fresh DB / mis-pointed SQLite file), load real
 * Zambian streams: ZNBC baseline + Radio Browser countrycode=ZM. No non-Zambia demo URLs.
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

    const id = stableRbId(url);
    const name = String(row.name || "Zambia station").trim() || "Zambia station";
    const district = String(row.state || "").trim();
    const tags = String(row.tags || "").trim();

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

  const seeded = znbc + rbSeeded;
  logger.warn(
    { znbcStations: znbc, radioBrowserStations: rbSeeded, total: seeded },
    "Station catalog was empty — bootstrapped Zambia catalog (ZNBC + Radio Browser ZM). Run npm run harvest:zambia && npm run import:zambia for a fuller curated list."
  );

  return {
    seeded,
    skipped: false,
    source: rbRows.length ? "znbc_plus_radio_browser" : znbc ? "znbc_only" : "none",
  };
}
