/**
 * Poll OnlineRadioBox track scraper (scraper2.onlineradiobox.com/{radioId}?l=...)
 * for stations whose sourceIdsJson includes onlineradiobox (e.g. "zm.phoenix").
 *
 * When the API returns updated > 0 and a title, appends a real DetectionLog row.
 * No fabricated history — only what ORB reports while this script runs.
 *
 * Usage: node scripts/orb_track_poller.mjs
 * Run on a short interval via cron or systemd timer so ORB track updates are captured.
 *
 * Requires: DATABASE_URL, stations imported with onlineradiobox in sourceIdsJson
 */
import "dotenv/config";
import { readFileSync, writeFileSync, mkdirSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { PrismaClient } from "@prisma/client";
import { upsertSongSpinOnNewPlay } from "./song_spin_upsert.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const STATE_PATH = join(__dirname, "data", "orb_track_state.json");
const MAX_TRACK_AGE_SECONDS = Math.max(
  60,
  Number.parseInt(process.env.ORB_MAX_TRACK_AGE_SECONDS || "900", 10) || 900
);
const FETCH_TIMEOUT_MS = Math.max(
  1500,
  Number.parseInt(process.env.ORB_FETCH_TIMEOUT_MS || "2500", 10) || 2500
);
const MAX_STATIONS_PER_RUN = Math.max(
  3,
  Number.parseInt(process.env.ORB_MAX_STATIONS_PER_RUN || "8", 10) || 8
);

function loadState() {
  try {
    return JSON.parse(readFileSync(STATE_PATH, "utf8"));
  } catch {
    return {};
  }
}

function saveState(state) {
  mkdirSync(dirname(STATE_PATH), { recursive: true });
  writeFileSync(STATE_PATH, JSON.stringify(state, null, 2));
}

/** @param {string | null | undefined} json */
function parseOrbRadioId(json) {
  if (!json) return null;
  try {
    const o = JSON.parse(json);
    const v = o.onlineradiobox;
    if (typeof v === "string" && v.includes(".")) return v;
    const url = typeof o.onlineRadioBoxUrl === "string" ? o.onlineRadioBoxUrl : "";
    const m = url.match(/onlineradiobox\.com\/([a-z]{2})\/([^/?#]+)/i);
    if (m?.[1] && m?.[2]) return `${m[1].toLowerCase()}.${m[2].toLowerCase()}`;
    return null;
  } catch {
    return null;
  }
}

/** @param {Record<string, unknown>} data */
function artistTitleFromOrb(data) {
  const title = String(data.title ?? "").trim();
  if (title && title.includes(" - ")) {
    const i = title.indexOf(" - ");
    return {
      artist: title.slice(0, i).trim(),
      song: title.slice(i + 3).trim(),
      combined: title,
    };
  }
  const ia = String(data.iArtist ?? "").trim();
  const iname = String(data.iName ?? "").trim();
  if (ia || iname) {
    return {
      artist: ia,
      song: iname,
      combined: [ia, iname].filter(Boolean).join(" - ") || title,
    };
  }
  return { artist: "", song: title, combined: title };
}

async function main() {
  const prisma = new PrismaClient();
  const state = loadState();
  const stations = await prisma.station.findMany({
    where: { country: "Zambia", isActive: true },
    select: { id: true, name: true, sourceIdsJson: true },
  });
  const orbStations = stations.filter((st) => parseOrbRadioId(st.sourceIdsJson));
  const cursor = Number.isInteger(state.__cursor) ? state.__cursor : 0;
  const ordered = orbStations.length
    ? [...orbStations.slice(cursor), ...orbStations.slice(0, cursor)]
    : [];
  const runStations = ordered.slice(0, Math.min(MAX_STATIONS_PER_RUN, ordered.length));
  state.__cursor = orbStations.length ? (cursor + runStations.length) % orbStations.length : 0;

  let newRows = 0;
  for (const st of runStations) {
    const radioId = parseOrbRadioId(st.sourceIdsJson);
    if (!radioId) continue;

    const lastL = state[radioId]?.l ?? 0;
    const url = `https://scraper2.onlineradiobox.com/${encodeURIComponent(radioId)}?l=${lastL}`;
    let data;
    try {
      const res = await fetch(url, {
        headers: { "User-Agent": "ZambiaMonitor/1.0 ORB-track" },
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
      });
      data = await res.json();
    } catch (e) {
      console.error(st.name, String(e));
      continue;
    }

    if (!data || data.updated === 0 || data.updated === undefined) continue;
    if (data.alias && data.alias !== radioId) continue;
    const updatedSec = Number(data.updated);
    if (!Number.isFinite(updatedSec) || updatedSec <= 0) continue;
    const ageSec = Math.floor(Date.now() / 1000) - updatedSec;
    if (ageSec > MAX_TRACK_AGE_SECONDS) {
      state[radioId] = { l: data.updated, stale: true, ageSec };
      continue;
    }

    const { artist, song, combined } = artistTitleFromOrb(data);
    if (!song && !combined) continue;

    const lastLog = await prisma.detectionLog.findFirst({
      where: { stationId: st.id },
      orderBy: { observedAt: "desc" },
    });
    if (
      lastLog &&
      lastLog.titleFinal === (song || combined) &&
      lastLog.artistFinal === artist
    ) {
      state[radioId] = { l: data.updated };
      continue;
    }

    const created = await prisma.detectionLog.create({
      data: {
        stationId: st.id,
        detectionMethod: "onlineradiobox_track",
        rawStreamText: combined || null,
        parsedArtist: artist || null,
        parsedTitle: song || null,
        titleFinal: song || combined || null,
        artistFinal: artist || null,
        status: "matched",
        sourceProvider: "onlineradiobox",
      },
      select: { id: true },
    });
    await upsertSongSpinOnNewPlay(prisma, {
      stationId: st.id,
      artist,
      title: song || combined,
      album: null,
      detectionLogId: created.id,
    });
    state[radioId] = { l: data.updated };
    newRows++;
  }

  saveState(state);
  console.log(`ORB track poll: appended ${newRows} detection row(s).`);
  await prisma.$disconnect();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
