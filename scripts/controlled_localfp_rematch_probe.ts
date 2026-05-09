import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

const DB = "prisma/dev_runtime.db";
const LIMIT = Number(process.env.LIMIT || "50");
const SERVICE_FILE = process.env.LOCALFP_SERVICE_FILE || "";

function sh(cmd: string, args: string[]) {
  return execFileSync(cmd, args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
}

function sqliteJson(sql: string): any[] {
  const out = sh("sqlite3", ["-json", DB, sql]);
  if (!out.trim()) return [];
  return JSON.parse(out);
}

function fpcalc(filePath: string): { duration: number; fingerprint: string } | null {
  try {
    const out = sh("fpcalc", ["-json", filePath]);
    const parsed = JSON.parse(out);
    if (!parsed.fingerprint) return null;
    return {
      duration: Number(parsed.duration || 0),
      fingerprint: String(parsed.fingerprint),
    };
  } catch {
    return null;
  }
}

async function loadLookupFunction() {
  const abs = path.resolve(SERVICE_FILE);
  const mod = await import(pathToFileURL(abs).href);

  const methodNames = ["lookup", "lookupFingerprint", "match", "matchFingerprint", "findMatch"];

  for (const [name, exported] of Object.entries(mod)) {
    const obj: any = exported;

    for (const method of methodNames) {
      if (obj && typeof obj[method] === "function") {
        console.log("USING_EXPORT_OBJECT_METHOD", name, method);
        return async (fp: string) => obj[method](fp);
      }
    }

    if (typeof obj === "function") {
      try {
        const inst = new obj();
        for (const method of methodNames) {
          if (inst && typeof inst[method] === "function") {
            console.log("USING_CLASS_METHOD", name, method);
            return async (fp: string) => inst[method](fp);
          }
        }
      } catch {
        // constructor may need args; ignore
      }
    }
  }

  throw new Error("Could not find callable lookup/match method in " + SERVICE_FILE);
}

function looksLikeAudioPath(v: any) {
  if (!v || typeof v !== "string") return false;
  const s = v.toLowerCase();
  return (
    (s.includes("unresolved_samples") || s.startsWith("/opt/radio-monitor/")) &&
    /\.(wav|mp3|m4a|aac|ogg)$/i.test(s)
  );
}

function resolveFile(v: string) {
  if (fs.existsSync(v)) return v;
  const p = path.resolve("/opt/radio-monitor", v);
  if (fs.existsSync(p)) return p;
  return "";
}

async function main() {
  console.log("=== CONTROLLED LOCALFP REMATCH PROBE ===");
  console.log("limit:", LIMIT);
  console.log("service:", SERVICE_FILE);

  const rows = sqliteJson(`
    SELECT rowid, *
    FROM UnresolvedSample
    WHERE recoveryStatus IN ('no_match','pending')
    ORDER BY rowid DESC
    LIMIT ${LIMIT * 4};
  `);

  const samples: { rowid: number; file: string; row: any }[] = [];
  for (const row of rows) {
    for (const [k, v] of Object.entries(row)) {
      if (!looksLikeAudioPath(v)) continue;
      const file = resolveFile(String(v));
      if (file) {
        samples.push({ rowid: Number(row.rowid), file, row });
        break;
      }
    }
    if (samples.length >= LIMIT) break;
  }

  console.log("usable_samples:", samples.length);

  const lookup = await loadLookupFunction();

  let tested = 0;
  let fingerprinted = 0;
  let matched = 0;
  let failed = 0;

  const hits: any[] = [];

  for (const s of samples) {
    tested += 1;

    const fp = fpcalc(s.file);
    if (!fp) {
      failed += 1;
      console.log("FP_FAIL", s.rowid, s.file);
      continue;
    }

    fingerprinted += 1;

    let result: any = null;
    try {
      result = await lookup(fp.fingerprint);
    } catch (e: any) {
      failed += 1;
      console.log("LOOKUP_FAIL", s.rowid, String(e?.message || e).slice(0, 200));
      continue;
    }

    const text = JSON.stringify(result || {});
    const isHit =
      !!result &&
      result !== false &&
      text !== "{}" &&
      !/no[_ -]?match|null/i.test(text);

    if (isHit) {
      matched += 1;
      hits.push({ rowid: s.rowid, file: s.file, result });
      console.log("MATCH", s.rowid, text.slice(0, 500));
    } else {
      console.log("NO_MATCH", s.rowid);
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
  }, null, 2));

  console.log("");
  console.log("=== FIRST HITS ===");
  for (const h of hits.slice(0, 10)) {
    console.log(JSON.stringify(h).slice(0, 1000));
  }
}

main().catch((e) => {
  console.error("FATAL", e);
  process.exit(1);
});
