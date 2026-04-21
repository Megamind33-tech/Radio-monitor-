/**
 * Usage: node scripts/import_zambia_stations.mjs [path/to/zambia_harvest.json]
 * Upserts stations from harvest JSON into Prisma (SQLite/Postgres).
 */
import { readFileSync } from "fs";
import { PrismaClient } from "@prisma/client";

const path = process.argv[2] || "scripts/data/zambia_harvest.json";
const raw = readFileSync(path, "utf-8");
const data = JSON.parse(raw);
const stations = data.stations;
if (!Array.isArray(stations)) {
  console.error("Invalid JSON: expected { stations: [...] }");
  process.exit(1);
}

const prisma = new PrismaClient();

let upserted = 0;
for (const row of stations) {
  const {
    id,
    name,
    country,
    district,
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
    sampleSeconds,
  } = row;

  await prisma.station.upsert({
    where: { id },
    create: {
      id,
      name,
      country,
      district: district ?? "",
      streamUrl,
      streamFormatHint: streamFormatHint ?? null,
      sourceIdsJson: sourceIdsJson ?? null,
      icyQualification: icyQualification ?? null,
      icySampleTitle: icySampleTitle || null,
      isActive: isActive ?? true,
      metadataPriorityEnabled: metadataPriorityEnabled ?? true,
      fingerprintFallbackEnabled: fingerprintFallbackEnabled ?? false,
      metadataStaleSeconds: metadataStaleSeconds ?? 300,
      pollIntervalSeconds: pollIntervalSeconds ?? 120,
      sampleSeconds: sampleSeconds ?? 20,
    },
    update: {
      name,
      country,
      district: district ?? "",
      streamUrl,
      streamFormatHint: streamFormatHint ?? null,
      sourceIdsJson: sourceIdsJson ?? null,
      icyQualification: icyQualification ?? null,
      icySampleTitle: icySampleTitle || null,
      isActive: isActive ?? true,
      metadataPriorityEnabled: metadataPriorityEnabled ?? true,
      fingerprintFallbackEnabled: fingerprintFallbackEnabled ?? false,
      metadataStaleSeconds: metadataStaleSeconds ?? 300,
      pollIntervalSeconds: pollIntervalSeconds ?? 120,
      sampleSeconds: sampleSeconds ?? 20,
    },
  });
  upserted++;
}

const active = await prisma.station.count({ where: { isActive: true } });
const total = await prisma.station.count();
console.log(`Upserted ${upserted} stations. DB total=${total}, active=${active}.`);

await prisma.$disconnect();
