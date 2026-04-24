/**
 * Usage: node scripts/import_zambia_stations.mjs [path/to/zambia_harvest.json]
 * Upserts stations from harvest JSON into Prisma (SQLite/Postgres).
 * Merges sourceIdsJson on update so the same stream URL keeps hints from all sites (e.g. radio_garden + onlineradiobox).
 */
import { readFileSync } from "fs";
import { PrismaClient } from "@prisma/client";

const args = process.argv.slice(2);
const replaceAll = args.includes("--replace");
const replaceStationCatalogOnly = args.includes("--replace-stations-only");
const pathArg = args.find((a) => !a.startsWith("--") && a.endsWith(".json"));
const path = pathArg || "scripts/data/zambia_harvest.json";
const raw = readFileSync(path, "utf-8");
const data = JSON.parse(raw);
const stations = data.stations;
if (!Array.isArray(stations)) {
  console.error("Invalid JSON: expected { stations: [...] }");
  process.exit(1);
}

/** @param {string | null | undefined} json */
function parseSourceIds(json) {
  if (!json || typeof json !== "string") return {};
  try {
    const o = JSON.parse(json);
    return o && typeof o === "object" && !Array.isArray(o) ? { ...o } : {};
  } catch {
    return {};
  }
}

/** @param {string | null | undefined} a @param {string | null | undefined} b */
function mergeSourceIdsJson(a, b) {
  const merged = { ...parseSourceIds(a), ...parseSourceIds(b) };
  const keys = Object.keys(merged).sort();
  if (keys.length === 0) return null;
  const out = {};
  for (const k of keys) {
    const v = merged[k];
    if (typeof v === "string" && v.trim()) out[k] = v.trim();
  }
  return Object.keys(out).length ? JSON.stringify(out) : null;
}

function validateCandidateStreamUrl(url) {
  const canonicalUrl = String(url || "").trim();
  if (!canonicalUrl.startsWith("http")) {
    return { accepted: false, reason: "non_http_url", canonicalUrl };
  }
  try {
    const u = new URL(canonicalUrl);
    const host = u.hostname.toLowerCase();
    const pathName = u.pathname.toLowerCase();
    if (["localhost", "127.0.0.1", "0.0.0.0"].includes(host)) {
      return { accepted: false, reason: "invalid_host", canonicalUrl };
    }
    if (["/search", "/discover", "/ads"].some((x) => pathName.includes(x))) {
      return { accepted: false, reason: "non_stream_path", canonicalUrl };
    }
    return { accepted: true, reason: "ok", canonicalUrl };
  } catch {
    return { accepted: false, reason: "invalid_url", canonicalUrl };
  }
}

const prisma = new PrismaClient();

if (replaceAll && replaceStationCatalogOnly) {
  console.error("Use either --replace or --replace-stations-only, not both.");
  process.exit(1);
}

if (replaceAll) {
  await prisma.stationSongSpin.deleteMany();
  await prisma.songSampleArchive.deleteMany();
  await prisma.detectionLog.deleteMany();
  await prisma.jobRun.deleteMany();
  await prisma.currentNowPlaying.deleteMany();
  await prisma.unresolvedSample.deleteMany();
  await prisma.station.deleteMany();
  console.log("Replaced entire station catalog and detections (--replace).");
} else if (replaceStationCatalogOnly) {
  // Keep historical detections/logs for trend continuity.
  // We do NOT delete stations here because historical DetectionLog rows are FK-linked.
  // Instead, upsert incoming rows and deactivate Zambia rows that are no longer in the feed.
  console.log("Replacing station catalog shape via upsert/deactivate (detections preserved).");
} else {
  // Remove non-Zambia test/seed stations only
  const outsiders = await prisma.station.findMany({
    where: { NOT: { country: "Zambia" } },
    select: { id: true },
  });
  if (outsiders.length > 0) {
    const ids = outsiders.map((s) => s.id);
    await prisma.detectionLog.deleteMany({ where: { stationId: { in: ids } } });
    await prisma.jobRun.deleteMany({ where: { stationId: { in: ids } } });
    await prisma.currentNowPlaying.deleteMany({ where: { stationId: { in: ids } } });
    const removed = await prisma.station.deleteMany({ where: { id: { in: ids } } });
    console.log(`Removed ${removed.count} non-Zambia station(s).`);
  }
}

async function upsertStreamEndpointRow(stationId, streamUrl, sourceIdsJson, quality, isCurrent) {
  const ids = parseSourceIds(sourceIdsJson);
  const entry = Object.entries(ids)[0] || null;
  const source = entry?.[0] || "import";
  const sourceDetail = entry?.[1] || null;
  const qualityText = String(quality || "").toLowerCase();
  const isLowQuality = qualityText && !["good", "partial"].includes(qualityText);
  const suppressed = source === "zeno" && isLowQuality;
  const status =
    qualityText === "error" || qualityText === "none"
      ? "inactive"
      : isLowQuality
        ? "degraded"
        : "healthy";
  await prisma.stationStreamEndpoint.upsert({
    where: { stationId_streamUrl: { stationId, streamUrl } },
    create: {
      stationId,
      source,
      sourceDetail,
      streamUrl,
      resolvedUrl: streamUrl,
      isCurrent,
      isSuppressed: suppressed,
      lastValidationStatus: status,
      lastFailureReason: suppressed ? "low_quality_or_unverified_zeno_candidate" : null,
    },
    update: {
      source,
      sourceDetail,
      isCurrent,
      isSuppressed: suppressed,
      lastValidationStatus: status,
      lastFailureReason: suppressed ? "low_quality_or_unverified_zeno_candidate" : null,
    },
  });
}

async function ensureZnbcBaselineStations() {
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
  for (const row of rows) {
    const urlCheck = validateCandidateStreamUrl(row.streamUrl);
    if (!urlCheck.accepted) continue;
    await prisma.station.upsert({
      where: { id: row.id },
      create: {
        id: row.id,
        name: row.name,
        country: "Zambia",
        district: "Zambia",
        province: "",
        frequencyMhz: row.frequencyMhz ?? null,
        streamUrl: urlCheck.canonicalUrl,
        streamFormatHint: "icy",
        sourceIdsJson: row.sourceIdsJson,
        icyQualification: "weak",
        icySampleTitle: null,
        isActive: true,
        metadataPriorityEnabled: true,
        fingerprintFallbackEnabled: true,
        metadataStaleSeconds: 300,
        pollIntervalSeconds: 120,
        audioFingerprintIntervalSeconds: 300,
        sampleSeconds: 20,
        archiveSongSamples: true,
        monitorState: "UNKNOWN",
        contentClassification: "unknown",
        visibilityEnabled: true,
        deepValidationIntervalSeconds: 600,
        failureThreshold: 3,
        recoveryThreshold: 2,
      },
      update: {
        name: row.name,
        country: "Zambia",
        frequencyMhz: row.frequencyMhz ?? null,
        streamUrl: urlCheck.canonicalUrl,
        sourceIdsJson: row.sourceIdsJson,
        isActive: true,
        visibilityEnabled: true,
      },
    });
    await upsertStreamEndpointRow(row.id, urlCheck.canonicalUrl, row.sourceIdsJson, "partial", true);
  }
}

let upserted = 0;
const incomingIds = new Set(stations.map((s) => s?.id).filter((id) => typeof id === "string"));
for (const row of stations) {
  const {
    id,
    name,
    country,
    district,
    province,
    frequencyMhz,
    streamUrl,
    streamFormatHint,
    sourceIdsJson,
    icyQualification,
    icySampleTitle,
    isActive,
    metadataPriorityEnabled,
    fingerprintFallbackEnabled,
    metadataStaleSeconds,
    pollIntervalSeconds,
    audioFingerprintIntervalSeconds,
    sampleSeconds,
    archiveSongSamples,
  } = row;
  const urlCheck = validateCandidateStreamUrl(streamUrl);
  if (!urlCheck.accepted) {
    console.warn(`Skipping station ${id || name}: invalid candidate stream (${urlCheck.reason})`);
    continue;
  }

  const existing = await prisma.station.findUnique({
    where: { id },
    select: { sourceIdsJson: true, isActive: true, streamUrl: true, lastGoodAudioAt: true },
  });
  const mergedSources = mergeSourceIdsJson(existing?.sourceIdsJson, sourceIdsJson);
  const incomingSources = parseSourceIds(sourceIdsJson);
  // In monitoring mode, keep stations enabled after catalog refreshes so they
  // continue to be polled unless they are explicitly removed as stale below.
  const nextIsActive = replaceStationCatalogOnly ? true : isActive ?? true;

  const qualityText = String(icyQualification || "").toLowerCase();
  const weakOrUnverified = !qualityText || ["weak", "none", "error", "pending"].includes(qualityText);
  const shouldKeepExistingWorking =
    replaceStationCatalogOnly &&
    !!existing?.streamUrl &&
    !!existing?.lastGoodAudioAt &&
    !!incomingSources.zeno &&
    weakOrUnverified;

  const finalStreamUrl = shouldKeepExistingWorking
    ? existing.streamUrl
    : urlCheck.canonicalUrl;

  await prisma.station.upsert({
    where: { id },
    create: {
      id,
      name,
      country,
      district: district ?? "",
      province: province ?? "",
      frequencyMhz: frequencyMhz ?? null,
      streamUrl: finalStreamUrl,
      streamFormatHint: streamFormatHint ?? null,
      sourceIdsJson: mergedSources ?? sourceIdsJson ?? null,
      icyQualification: icyQualification ?? null,
      icySampleTitle: icySampleTitle || null,
      isActive: nextIsActive,
      metadataPriorityEnabled: metadataPriorityEnabled ?? true,
      fingerprintFallbackEnabled: fingerprintFallbackEnabled ?? true,
      metadataStaleSeconds: metadataStaleSeconds ?? 300,
      pollIntervalSeconds: pollIntervalSeconds ?? 120,
      audioFingerprintIntervalSeconds: audioFingerprintIntervalSeconds ?? 300,
      sampleSeconds: sampleSeconds ?? 20,
      archiveSongSamples: archiveSongSamples ?? true,
      monitorState: "UNKNOWN",
      contentClassification: "unknown",
      visibilityEnabled: true,
      deepValidationIntervalSeconds: 600,
      failureThreshold: 3,
      recoveryThreshold: 2,
    },
    update: {
      name,
      country,
      district: district ?? "",
      province: province ?? "",
      frequencyMhz: frequencyMhz ?? null,
      streamUrl: finalStreamUrl,
      streamFormatHint: streamFormatHint ?? null,
      sourceIdsJson: mergedSources ?? sourceIdsJson ?? null,
      icyQualification: icyQualification ?? null,
      icySampleTitle: icySampleTitle || null,
      isActive: nextIsActive,
      metadataPriorityEnabled: metadataPriorityEnabled ?? true,
      fingerprintFallbackEnabled: fingerprintFallbackEnabled ?? true,
      metadataStaleSeconds: metadataStaleSeconds ?? 300,
      pollIntervalSeconds: pollIntervalSeconds ?? 120,
      audioFingerprintIntervalSeconds: audioFingerprintIntervalSeconds ?? 300,
      sampleSeconds: sampleSeconds ?? 20,
      archiveSongSamples: archiveSongSamples ?? true,
      monitorState: replaceStationCatalogOnly ? undefined : "UNKNOWN",
      contentClassification: replaceStationCatalogOnly ? undefined : "unknown",
      visibilityEnabled: true,
      deepValidationIntervalSeconds: 600,
      failureThreshold: 3,
      recoveryThreshold: 2,
    },
  });
  await upsertStreamEndpointRow(id, finalStreamUrl, mergedSources ?? sourceIdsJson ?? null, icyQualification, true);
  upserted++;
}

if (replaceStationCatalogOnly) {
  const stale = await prisma.station.updateMany({
    where: {
      country: "Zambia",
      id: { notIn: Array.from(incomingIds) },
      isActive: true,
    },
    data: {
      isActive: false,
      visibilityEnabled: false,
      monitorState: "INACTIVE",
      monitorStateReason: "not present in latest catalog sync",
    },
  });
  console.log(`Deactivated ${stale.count} stale Zambia station(s) not present in import.`);
}

await ensureZnbcBaselineStations();

// Keep endpoint table aligned: non-current for rows absent from latest import.
if (incomingIds.size > 0) {
  await prisma.stationStreamEndpoint.updateMany({
    where: {
      stationId: { notIn: Array.from(incomingIds) },
      isCurrent: true,
    },
    data: { isCurrent: false },
  });
}

const active = await prisma.station.count({ where: { isActive: true } });
const total = await prisma.station.count();
const rgLike = await prisma.station.count({
  where: {
    country: "Zambia",
    sourceIdsJson: { contains: "radio_garden" },
  },
});
console.log(`Upserted ${upserted} stations. DB total=${total}, active=${active}, with_radio_garden_hint=${rgLike}.`);

await prisma.$disconnect();
