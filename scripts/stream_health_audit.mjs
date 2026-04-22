/**
 * Automated stream health check for all active stations in the Prisma DB.
 * Connects with ICY, reads multiple metadata blocks, classifies: good | partial | weak | none | dead | error
 *
 * No fake titles — only what the stream sends. Optional AcoustID spot-check when ACOUSTID_API_KEY is set.
 *
 * Usage:
 *   node scripts/stream_health_audit.mjs
 *   node scripts/stream_health_audit.mjs --out scripts/data/stream_health.csv
 *   node scripts/stream_health_audit.mjs --no-acoustid
 *   node scripts/stream_health_audit.mjs --limit 5
 */
import "dotenv/config";
import { writeFileSync, mkdirSync } from "fs";
import { dirname } from "path";
import { spawnSync } from "child_process";
import { PrismaClient } from "@prisma/client";

function parseArgs() {
  const args = process.argv.slice(2);
  let out = "scripts/data/stream_health.csv";
  let acoustid = true;
  let limit = 0;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--out" && args[i + 1]) out = args[++i];
    if (args[i] === "--no-acoustid") acoustid = false;
    if (args[i] === "--limit" && args[i + 1]) limit = Math.max(0, parseInt(args[++i], 10) || 0);
  }
  return { out, acoustid, limit };
}

function isJunkTitle(s) {
  if (!s || String(s).trim().length < 2) return true;
  const t = String(s).trim();
  if (t === "-" || t === " - " || t === "...") return true;
  const low = t.toLowerCase();
  if (
    ["offline", "not broadcasting", "stream offline", "no stream", "temporarily unavailable"].some((x) =>
      low.includes(x)
    )
  )
    return true;
  return false;
}

function titleQuality(titles) {
  const cleaned = titles.map((x) => (x ?? "").trim()).filter((x) => x.length);
  const nonJunk = cleaned.filter((x) => !isJunkTitle(x));
  if (!nonJunk.length) {
    if (cleaned.length) return { q: "weak", sample: cleaned[0].slice(0, 200) };
    return { q: "none", sample: null };
  }
  const best = nonJunk.reduce((a, b) => {
    const sa = (a.includes(" - ") ? 50 : 0) + a.length;
    const sb = (b.includes(" - ") ? 50 : 0) + b.length;
    return sb >= sa ? b : a;
  });
  if (best.includes(" - ") || best.length > 12) return { q: "good", sample: best.slice(0, 500) };
  if (best.length >= 4) return { q: "partial", sample: best.slice(0, 500) };
  return { q: "weak", sample: best.slice(0, 500) };
}

/**
 * Consume exactly n bytes from a Web ReadableStream reader, buffering partial chunks
 * so oversized reads do not desynchronize ICY parsing.
 */
function createByteReader(reader) {
  let buf = Buffer.alloc(0);
  return {
    async readExact(n) {
      while (buf.length < n) {
        const { done, value } = await reader.read();
        if (done) return { ok: false, data: null };
        buf = Buffer.concat([buf, Buffer.from(value)]);
      }
      const data = buf.subarray(0, n);
      buf = buf.subarray(n);
      return { ok: true, data: Buffer.from(data) };
    },
  };
}

function parseStreamTitle(metaStr) {
  let title = null;
  for (const field of metaStr.split(";")) {
    const f = field.trim();
    if (f.startsWith("StreamTitle=")) {
      let v = f.slice("StreamTitle=".length).trim();
      if ((v.startsWith("'") && v.endsWith("'")) || (v.startsWith('"') && v.endsWith('"'))) v = v.slice(1, -1);
      title = v.trim();
      break;
    }
  }
  return title;
}

async function readIcyBlocks(streamUrl, maxBlocks = 3) {
  let res;
  try {
    res = await fetch(streamUrl, {
      headers: { "Icy-MetaData": "1", "User-Agent": "MOSTIFY/1.0 stream-health-audit" },
      signal: AbortSignal.timeout(45000),
    });
  } catch (e) {
    return { error: String(e?.message || e), titles: [] };
  }
  if (!res.ok) return { error: `http_${res.status}`, titles: [] };
  const mi = res.headers.get("icy-metaint");
  if (!mi) return { error: "no_icy_metaint", titles: [] };
  const metaint = parseInt(mi, 10);
  const reader = res.body?.getReader();
  if (!reader) return { error: "no_body", titles: [] };
  const br = createByteReader(reader);
  const titles = [];
  const dec = new TextDecoder();
  for (let b = 0; b < maxBlocks; b++) {
    const skip = await br.readExact(metaint);
    if (!skip.ok) return { error: "eof_audio", titles };

    const lb = await br.readExact(1);
    if (!lb.ok) return { error: "eof_before_meta_len", titles };
    const metaLen = lb.data[0] * 16;
    if (metaLen === 0) {
      titles.push("");
      continue;
    }
    const meta = await br.readExact(metaLen);
    if (!meta.ok) return { error: "eof_meta", titles };
    const text = dec.decode(meta.data).replace(/\0/g, "").trim();
    titles.push(parseStreamTitle(text));
  }
  try {
    await reader.cancel();
  } catch {
    /* ignore */
  }
  return { error: null, titles };
}

async function acoustidSpotCheck(streamUrl, seconds = 18) {
  const key = process.env.ACOUSTID_API_KEY;
  if (!key) return { ok: false, note: "no_acoustid_key", title: "", artist: "" };
  const ff = spawnSync(
    "ffmpeg",
    ["-y", "-i", streamUrl, "-t", String(seconds), "-vn", "-ac", "1", "-ar", "11025", "-f", "wav", "pipe:1"],
    { encoding: "buffer", maxBuffer: 20 * 1024 * 1024, timeout: 60000 }
  );
  if (ff.status !== 0 || !ff.stdout?.length) return { ok: false, note: "ffmpeg_failed", title: "", artist: "" };
  const fp = spawnSync("fpcalc", ["-json", "-"], { input: ff.stdout, encoding: "utf8", maxBuffer: 2 * 1024 * 1024 });
  if (fp.status !== 0) return { ok: false, note: "fpcalc_failed", title: "", artist: "" };
  let data;
  try {
    data = JSON.parse(fp.stdout);
  } catch {
    return { ok: false, note: "fpcalc_parse", title: "", artist: "" };
  }
  const body = new URLSearchParams({
    client: key,
    duration: String(Math.round(data.duration)),
    fingerprint: data.fingerprint,
    meta: "recordings",
  });
  try {
    const resp = await fetch("https://api.acoustid.org/v2/lookup", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body,
      signal: AbortSignal.timeout(15000),
    });
    const j = await resp.json();
    const rec = j?.results?.[0]?.recordings?.[0];
    if (j?.status === "ok" && rec?.title)
      return {
        ok: true,
        note: "matched",
        title: rec.title,
        artist: rec.artists?.[0]?.name ?? "",
      };
    return {
      ok: false,
      note: j?.status === "ok" ? "no_match" : String(j?.error?.message || "api_error"),
      title: "",
      artist: "",
    };
  } catch (e) {
    return { ok: false, note: String(e?.message || e), title: "", artist: "" };
  }
}

async function main() {
  const { out, acoustid, limit } = parseArgs();
  const prisma = new PrismaClient();
  const stations = await prisma.station.findMany({
    where: { isActive: true },
    select: { id: true, name: true, streamUrl: true },
    orderBy: { name: "asc" },
    ...(limit > 0 ? { take: limit } : {}),
  });

  const rows = [];
  const header = [
    "station_id",
    "name",
    "icy_verdict",
    "icy_sample",
    "icy_error",
    "block1_title",
    "block2_title",
    "block3_title",
    "acoustid_ok",
    "acoustid_note",
    "acoustid_title",
    "acoustid_artist",
  ];

  for (const s of stations) {
    const icy = await readIcyBlocks(s.streamUrl, 3);
    let verdict = "dead";
    let sample = "";
    if (icy.error === "no_icy_metaint") verdict = "none";
    else if (icy.error) verdict = "error";
    else {
      const tq = titleQuality(icy.titles);
      verdict = tq.q;
      sample = tq.sample || "";
    }

    let ac = { ok: false, note: "skipped", title: "", artist: "" };
    if (acoustid && verdict !== "dead" && verdict !== "error" && !icy.error) {
      ac = await acoustidSpotCheck(s.streamUrl);
    } else if (!acoustid) ac.note = "disabled";

    rows.push([
      s.id,
      s.name,
      verdict,
      sample || "",
      icy.error || "",
      icy.titles[0] ?? "",
      icy.titles[1] ?? "",
      icy.titles[2] ?? "",
      ac.ok ? "yes" : "no",
      ac.note,
      ac.title || "",
      ac.artist || "",
    ]);

    await new Promise((r) => setTimeout(r, 400));
  }

  const esc = (c) => `"${String(c).replace(/"/g, '""')}"`;
  const lines = [header.join(",")];
  for (const r of rows) lines.push(r.map(esc).join(","));
  mkdirSync(dirname(out) || ".", { recursive: true });
  writeFileSync(out, "\uFEFF" + lines.join("\n"), "utf8");

  const counts = (v) => rows.filter((r) => r[2] === v).length;
  console.log(
    JSON.stringify(
      {
        stations: stations.length,
        csv: out,
        good: counts("good"),
        partial: counts("partial"),
        weak: counts("weak"),
        none: counts("none"),
        dead: counts("dead"),
        error: counts("error"),
      },
      null,
      2
    )
  );
  await prisma.$disconnect();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
