#!/usr/bin/env node
/**
 * Plug-and-play: harvest Zambia stations (all sources + merged Radio Garden hints) then import into Prisma.
 *
 *   npm run sync:zambia
 *   npm run sync:zambia -- --max-probe 1000
 *   npm run sync:zambia -- --replace   (full catalog wipe + reimport — destructive)
 */
import { spawnSync } from "child_process";
import { dirname, join } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const argv = process.argv.slice(2);
if (!argv.includes("--replace") && !argv.includes("--replace-stations-only")) {
  // Safe default: preserve historical detections while refreshing the station catalog.
  // Full wipe remains available via explicit --replace.
  argv.push("--replace-stations-only");
}
const replace = argv.includes("--replace");
const replaceStationsOnly = argv.includes("--replace-stations-only");
const maxIdx = argv.findIndex((a) => a === "--max-probe");
const maxProbe =
  maxIdx >= 0 && argv[maxIdx + 1] ? String(argv[maxIdx + 1]) : "800";

const tuneinQueryArgs = [];
for (let i = 0; i < argv.length; i++) {
  if (argv[i] === "--tunein-query" && argv[i + 1]) {
    tuneinQueryArgs.push("--tunein-query", argv[++i]);
  }
}

const harvestArgs = [
  join(root, "scripts/zambia_station_harvest.py"),
  "--max-probe",
  maxProbe,
  "--out",
  join(root, "scripts/data/zambia_harvest.json"),
  ...tuneinQueryArgs,
];

console.log("[sync:zambia] Harvest:", `python3 zambia_station_harvest.py --max-probe ${maxProbe}`);
const h = spawnSync("python3", harvestArgs, { stdio: "inherit", cwd: root, env: process.env });
if (h.status !== 0) {
  console.error("[sync:zambia] Harvest failed.");
  process.exit(h.status ?? 1);
}

const importArgs = [join(root, "scripts/import_zambia_stations.mjs")];
if (replace) importArgs.push("--replace");
if (replaceStationsOnly) importArgs.push("--replace-stations-only");
importArgs.push(join(root, "scripts/data/zambia_harvest.json"));

console.log("[sync:zambia] Running import:", "node", ...importArgs.map((p) => p.replace(root + "/", "")));
const i = spawnSync(process.execPath, importArgs, { stdio: "inherit", cwd: root, env: process.env });
if (i.status !== 0) {
  console.error("[sync:zambia] Import failed.");
  process.exit(i.status ?? 1);
}

console.log("[sync:zambia] Done. Start monitor: npm run build && npm run start");
