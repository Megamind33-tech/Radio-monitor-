/**
 * Usage: node scripts/import_zambia_stations.mjs [path/to/zambia_harvest.json]
 * Upserts stations from harvest JSON into Prisma (SQLite/Postgres).
 * Merges sourceIdsJson on update so the same stream URL keeps hints from all sites (e.g. radio_garden + onlineradiobox).
 */
import { readFileSync } from "fs";
import { PrismaClient } from "@prisma/client";

const args = process.argv.slice(2);
const replaceAll = args.includes("--replace");
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

const prisma = new PrismaClient();

if (replaceAll) {
  await prisma.stationSongSpin.deleteMany();
  await prisma.songSampleArchive.deleteMany();
  await prisma.detectionLog.deleteMany();
  await prisma.jobRun.deleteMany();
  await prisma.currentNowPlaying.deleteMany();
  await prisma.unresolvedSample.deleteMany();
  await prisma.station.deleteMany();
  console.log("Replaced entire station catalog (--replace).");
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

let upserted = 0;
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

  const existing = await prisma.station.findUnique({
    where: { id },
    select: { sourceIdsJson: true },
  });
  const mergedSources = mergeSourceIdsJson(existing?.sourceIdsJson, sourceIdsJson);

  await prisma.station.upsert({
    where: { id },
    create: {
      id,
      name,
      country,
      district: district ?? "",
      province: province ?? "",
      frequencyMhz: frequencyMhz ?? null,
      streamUrl,
      streamFormatHint: streamFormatHint ?? null,
      sourceIdsJson: mergedSources ?? sourceIdsJson ?? null,
      icyQualification: icyQualification ?? null,
      icySampleTitle: icySampleTitle || null,
      isActive: isActive ?? true,
      metadataPriorityEnabled: metadataPriorityEnabled ?? true,
      fingerprintFallbackEnabled: fingerprintFallbackEnabled ?? true,
      metadataStaleSeconds: metadataStaleSeconds ?? 300,
      pollIntervalSeconds: pollIntervalSeconds ?? 120,
      audioFingerprintIntervalSeconds: audioFingerprintIntervalSeconds ?? 300,
      sampleSeconds: sampleSeconds ?? 20,
      archiveSongSamples: archiveSongSamples ?? true,
    },
    update: {
      name,
      country,
      district: district ?? "",
      province: province ?? "",
      frequencyMhz: frequencyMhz ?? null,
      streamUrl,
      streamFormatHint: streamFormatHint ?? null,
      sourceIdsJson: mergedSources ?? sourceIdsJson ?? null,
      icyQualification: icyQualification ?? null,
      icySampleTitle: icySampleTitle || null,
      isActive: isActive ?? true,
      metadataPriorityEnabled: metadataPriorityEnabled ?? true,
      fingerprintFallbackEnabled: fingerprintFallbackEnabled ?? true,
      metadataStaleSeconds: metadataStaleSeconds ?? 300,
      pollIntervalSeconds: pollIntervalSeconds ?? 120,
      audioFingerprintIntervalSeconds: audioFingerprintIntervalSeconds ?? 300,
      sampleSeconds: sampleSeconds ?? 20,
      archiveSongSamples: archiveSongSamples ?? true,
    },
  });
  upserted++;
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
