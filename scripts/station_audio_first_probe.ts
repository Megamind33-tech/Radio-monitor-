import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const DB = "prisma/dev_runtime.db";
const SAMPLE_SECONDS = Number(process.env.SAMPLE_SECONDS || "35");
const LIMIT_STATIONS = Number(process.env.LIMIT_STATIONS || "12");
const TOKENS = String(process.env.FORCE_STATION_TOKENS || "hot,hone,horn,power,phoenix,qfm")
  .split(",")
  .map((x) => x.trim().toLowerCase())
  .filter(Boolean);

function sh(cmd: string, args: string[], timeoutMs = 90000) {
  return execFileSync(cmd, args, {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    timeout: timeoutMs,
  });
}

function sqliteJson(sql: string): any[] {
  const out = sh("sqlite3", ["-json", DB, sql], 30000);
  if (!out.trim()) return [];
  return JSON.parse(out);
}

function esc(s: string) {
  return s.replace(/'/g, "''");
}

function findStations() {
  const where = TOKENS.map((t) => `lower(s.name) LIKE '%${esc(t)}%'`).join(" OR ");

  const rows = sqliteJson(`
    SELECT
      s.id AS stationId,
      s.name AS stationName,
      COALESCE(e.streamUrl, s.streamUrl) AS streamUrl
    FROM Station s
    LEFT JOIN StationStreamEndpoint e
      ON e.stationId = s.id
     AND COALESCE(e.isSuppressed, 0) = 0
    WHERE ${where}
      AND COALESCE(e.streamUrl, s.streamUrl) IS NOT NULL
      AND COALESCE(e.streamUrl, s.streamUrl) != ''
    GROUP BY s.id
    ORDER BY s.name
    LIMIT ${LIMIT_STATIONS};
  `);

  return rows;
}

function captureSample(streamUrl: string, wavPath: string) {
  sh("ffmpeg", [
    "-nostdin",
    "-hide_banner",
    "-loglevel", "error",
    "-rw_timeout", "30000000",
    "-reconnect", "1",
    "-reconnect_streamed", "1",
    "-reconnect_at_eof", "1",
    "-reconnect_delay_max", "5",
    "-user_agent", "Mozilla/5.0",
    "-t", String(SAMPLE_SECONDS),
    "-i", streamUrl,
    "-vn",
    "-ac", "1",
    "-ar", "16000",
    "-y",
    wavPath,
  ], (SAMPLE_SECONDS + 55) * 1000);

  const size = fs.existsSync(wavPath) ? fs.statSync(wavPath).size : 0;
  if (size < 10000) throw new Error(`sample_too_small:${size}`);
}

function fpcalc(wavPath: string) {
  const out = sh("fpcalc", ["-json", wavPath], 60000);
  const parsed = JSON.parse(out);
  if (!parsed.fingerprint) throw new Error("fpcalc_no_fingerprint");
  return {
    fingerprint: String(parsed.fingerprint),
    duration: Number(parsed.duration || 0),
  };
}

async function loadLocalFingerprint() {
  const serviceFile = path.resolve("server/services/local-fingerprint.service.ts");
  const mod: any = await import(pathToFileURL(serviceFile).href);

  if (!mod.LocalFingerprintService?.lookup) {
    throw new Error("LocalFingerprintService.lookup not found");
  }

  return mod.LocalFingerprintService;
}

async function lookup(LocalFingerprintService: any, fp: any) {
  try {
    return await LocalFingerprintService.lookup(fp);
  } catch (e1) {
    try {
      return await LocalFingerprintService.lookup(fp.fingerprint);
    } catch (e2: any) {
      throw new Error(String(e2?.message || e2));
    }
  }
}

async function main() {
  console.log("=== STATION AUDIO-FIRST PROBE ===");
  console.log("tokens:", TOKENS.join(","));
  console.log("sample_seconds:", SAMPLE_SECONDS);

  const stations = findStations();
  console.log("stations_found:", stations.length);

  if (!stations.length) {
    console.log("NO_STATIONS_FOUND_FOR_TOKENS");
    return;
  }

  const LocalFingerprintService = await loadLocalFingerprint();

  let tested = 0;
  let fingerprinted = 0;
  let matched = 0;
  let failed = 0;

  for (const st of stations) {
    tested += 1;
    const tmpdir = fs.mkdtempSync(path.join(os.tmpdir(), "station-audio-first-"));
    const wav = path.join(tmpdir, "sample.wav");

    console.log("");
    console.log(`=== STATION ${tested}/${stations.length}: ${st.stationName} ===`);
    console.log("stationId:", st.stationId);
    console.log("streamUrl:", st.streamUrl);

    try {
      captureSample(st.streamUrl, wav);
      const fp = fpcalc(wav);
      fingerprinted += 1;

      const result = await lookup(LocalFingerprintService, fp);

      if (result) {
        matched += 1;
        console.log("AUDIO_FIRST_MATCH", JSON.stringify(result).slice(0, 1200));
      } else {
        console.log("AUDIO_FIRST_NO_MATCH");
      }
    } catch (e: any) {
      failed += 1;
      console.log("AUDIO_FIRST_FAILED", String(e?.message || e).slice(0, 500));
    } finally {
      fs.rmSync(tmpdir, { recursive: true, force: true });
    }
  }

  console.log("");
  console.log("=== SUMMARY ===");
  console.log(JSON.stringify({
    tested,
    fingerprinted,
    matched,
    failed,
    match_rate_percent: fingerprinted ? Number(((matched / fingerprinted) * 100).toFixed(2)) : 0,
    temp_audio_kept: false,
  }, null, 2));
}

main().catch((e) => {
  console.error("FATAL", e);
  process.exit(1);
});
