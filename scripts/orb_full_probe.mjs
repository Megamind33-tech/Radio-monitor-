/**
 * Aggressively probe the OnlineRadioBox API for ALL Zambian stations.
 * 
 * Strategy:
 * 1. Discover slugs from ORB HTML pages (country + all city pages)
 * 2. Generate candidate slugs from all DB station names
 * 3. Probe scraper2.onlineradiobox.com for each candidate
 * 4. Update sourceIdsJson for ALL matches and create missing stations
 *
 * Usage: node scripts/orb_full_probe.mjs [--dry-run]
 */
import "dotenv/config";
import { PrismaClient } from "@prisma/client";

const DRY_RUN = process.argv.includes("--dry-run");
const prisma = new PrismaClient();
const UA = "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0 Safari/537.36";
const ORB_BASE = "https://onlineradiobox.com";

const ZM_CITY_SLUGS = new Set([
  "chikuni", "chingola", "chipata", "choma", "isoka", "itezhitezhi",
  "kabwe", "kalomo", "kalulushi", "kapiri_mposhi", "kasama", "katete",
  "kayambi", "keembe", "kitwe", "luanshya", "lundazi", "lunga", "lusaka",
  "mansa", "mazabuka", "mbala", "mkushi", "mongu", "mpongwe", "mpulungu",
  "muchinga", "mufulira", "mungwi", "ndola", "senga_hill", "sesheke",
  "umezi", "white_mwandi",
]);

async function fetchHtml(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": UA, "Accept-Language": "en-US,en;q=0.9" },
    signal: AbortSignal.timeout(20000),
  });
  if (!res.ok) return "";
  return res.text();
}

async function probeOrbApi(radioId) {
  try {
    const res = await fetch(
      `https://scraper2.onlineradiobox.com/${encodeURIComponent(radioId)}?l=0`,
      { headers: { "User-Agent": "ZambiaMonitor/1.0" }, signal: AbortSignal.timeout(5000) }
    );
    if (!res.ok) return null;
    const data = await res.json();
    if (data && data.updated && data.updated > 0) {
      return data;
    }
    return null;
  } catch {
    return null;
  }
}

function generateSlugsFromName(name) {
  const base = name
    .toLowerCase()
    .replace(/\blive\b/gi, "")
    .replace(/\bzambia\b/gi, "")
    .replace(/\bradio\b/gi, "")
    .replace(/\s*[0-9]+(\.[0-9]+)?\s*(fm|mhz)?\s*/gi, "")
    .trim();

  const fullNorm = name.toLowerCase()
    .replace(/\blive\b/gi, "")
    .replace(/\bzambia\b/gi, "")
    .trim();

  const slugs = new Set();
  
  // Various slug generation strategies
  const alphaOnly = base.replace(/[^a-z]/g, "");
  if (alphaOnly.length >= 3) slugs.add(`zm.${alphaOnly}`);
  
  const dashSep = base.replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
  if (dashSep.length >= 3) slugs.add(`zm.${dashSep}`);
  
  const firstWord = base.split(/\s+/)[0].replace(/[^a-z0-9]/g, "");
  if (firstWord.length >= 3) slugs.add(`zm.${firstWord}`);

  // With numbers (frequency-based)
  const withNums = fullNorm.replace(/[^a-z0-9]/g, "");
  if (withNums.length >= 3) slugs.add(`zm.${withNums}`);

  // Common patterns: "XXX FM" -> "xxx", "XXX Radio" -> "xxx"
  const fmMatch = name.match(/^([a-z]+)\s*(?:fm|radio)/i);
  if (fmMatch && fmMatch[1].length >= 3) slugs.add(`zm.${fmMatch[1].toLowerCase()}`);

  // Full name with spaces removed
  const fullNoSpace = fullNorm.replace(/\s+/g, "").replace(/[^a-z0-9]/g, "");
  if (fullNoSpace.length >= 3 && fullNoSpace.length <= 30) slugs.add(`zm.${fullNoSpace}`);

  // Numbered stations: "Hope FM 94.1" -> "zm.hope", "zm.hope941"  
  const numMatch = name.match(/([0-9]+\.?[0-9]*)/);
  if (numMatch && alphaOnly.length >= 3) {
    const numSlug = alphaOnly + numMatch[1].replace(".", "");
    slugs.add(`zm.${numSlug}`);
  }

  return [...slugs].filter(s => s.length > 3 && !ZM_CITY_SLUGS.has(s.replace("zm.", "")));
}

async function discoverHtmlSlugs() {
  const slugs = new Set();
  
  // Country page
  console.log("Fetching ORB Zambia country page...");
  const countryHtml = await fetchHtml(`${ORB_BASE}/zm/`);
  for (const m of countryHtml.matchAll(/href="\/zm\/([^"#?/]+)\/?"/g)) {
    const s = m[1].toLowerCase();
    if (!ZM_CITY_SLUGS.has(s)) slugs.add(`zm.${s}`);
  }

  // City pages
  console.log(`Fetching ${ZM_CITY_SLUGS.size} city pages...`);
  for (const city of ZM_CITY_SLUGS) {
    try {
      await new Promise(r => setTimeout(r, 200));
      const html = await fetchHtml(`${ORB_BASE}/zm/${city}/`);
      for (const m of html.matchAll(/href="\/zm\/([^"#?/]+)\/?"/g)) {
        const s = m[1].toLowerCase();
        if (!ZM_CITY_SLUGS.has(s)) slugs.add(`zm.${s}`);
      }
    } catch {}
  }

  // Also try the "all stations" page if it exists
  try {
    const allHtml = await fetchHtml(`${ORB_BASE}/zm/all/`);
    for (const m of allHtml.matchAll(/href="\/zm\/([^"#?/]+)\/?"/g)) {
      const s = m[1].toLowerCase();
      if (!ZM_CITY_SLUGS.has(s)) slugs.add(`zm.${s}`);
    }
  } catch {}

  // Try genre pages for more coverage
  const genres = ["pop", "rock", "gospel", "news", "talk", "african", "dance", "hiphop", "rnb", "jazz", "classical", "country", "reggae"];
  for (const g of genres) {
    try {
      await new Promise(r => setTimeout(r, 150));
      const html = await fetchHtml(`${ORB_BASE}/zm/genre/${g}/`);
      for (const m of html.matchAll(/href="\/zm\/([^"#?/]+)\/?"/g)) {
        const s = m[1].toLowerCase();
        if (!ZM_CITY_SLUGS.has(s) && !s.startsWith("genre")) slugs.add(`zm.${s}`);
      }
    } catch {}
  }

  return slugs;
}

async function main() {
  console.log(DRY_RUN ? "[DRY RUN MODE]" : "[LIVE MODE]");
  console.log("=".repeat(60));

  // 1. Discover slugs from HTML
  const htmlSlugs = await discoverHtmlSlugs();
  console.log(`\nDiscovered ${htmlSlugs.size} station slugs from HTML pages.`);

  // 2. Get all DB stations and generate candidate slugs
  const dbStations = await prisma.station.findMany({
    where: { country: "Zambia", isActive: true },
    select: { id: true, name: true, sourceIdsJson: true, streamUrl: true },
  });

  const dbCandidateSlugs = new Set();
  const slugToDbStation = new Map();
  
  for (const st of dbStations) {
    // Skip stations that already have ORB
    try {
      const j = JSON.parse(st.sourceIdsJson || "{}");
      if (j.onlineradiobox) continue;
    } catch {}

    const slugs = generateSlugsFromName(st.name);
    for (const slug of slugs) {
      dbCandidateSlugs.add(slug);
      if (!slugToDbStation.has(slug)) slugToDbStation.set(slug, st);
    }
  }

  console.log(`Generated ${dbCandidateSlugs.size} candidate slugs from ${dbStations.length} DB stations.`);

  // 3. Merge all candidates
  const allCandidates = new Set([...htmlSlugs, ...dbCandidateSlugs]);
  
  // Remove already-connected ones
  const alreadyConnected = new Set();
  for (const st of dbStations) {
    try {
      const j = JSON.parse(st.sourceIdsJson || "{}");
      if (j.onlineradiobox) alreadyConnected.add(j.onlineradiobox);
    } catch {}
  }
  
  const toProbe = [...allCandidates].filter(s => !alreadyConnected.has(s));
  console.log(`\nTotal candidates to probe: ${toProbe.length} (excluding ${alreadyConnected.size} already connected)`);

  // 4. Probe ORB API for all candidates
  console.log("Probing ORB API...");
  const activeOrb = new Map(); // radioId -> { title, updated }
  let probed = 0;

  for (const radioId of toProbe.sort()) {
    await new Promise(r => setTimeout(r, 150));
    const data = await probeOrbApi(radioId);
    if (data) {
      activeOrb.set(radioId, data);
      const title = data.title || "N/A";
      console.log(`  ✓ ${radioId} → "${title}"`);
    }
    probed++;
    if (probed % 20 === 0) console.log(`  ...probed ${probed}/${toProbe.length}`);
  }

  console.log(`\n=== Found ${activeOrb.size} active ORB stations ===`);

  // 5. Match and update
  let updated = 0;
  let created = 0;
  const matched = [];
  const unmatched = [];

  for (const [radioId, data] of activeOrb) {
    // Try to find matching DB station
    const dbStation = slugToDbStation.get(radioId);
    
    if (dbStation) {
      matched.push({ radioId, data, dbStation });
      if (!DRY_RUN) {
        const existing = JSON.parse(dbStation.sourceIdsJson || "{}");
        existing.onlineradiobox = radioId;
        await prisma.station.update({
          where: { id: dbStation.id },
          data: { sourceIdsJson: JSON.stringify(existing) },
        });
        // Also upsert CurrentNowPlaying
        const title = String(data.title || "").trim();
        let artist = "", song = title;
        if (title.includes(" - ")) {
          const i = title.indexOf(" - ");
          artist = title.slice(0, i).trim();
          song = title.slice(i + 3).trim();
        }
        await prisma.currentNowPlaying.upsert({
          where: { stationId: dbStation.id },
          update: { title: song || title, artist: artist || null, sourceProvider: "onlineradiobox", streamText: title, updatedAt: new Date() },
          create: { stationId: dbStation.id, title: song || title || undefined, artist: artist || undefined, sourceProvider: "onlineradiobox", streamText: title || undefined },
        });
        updated++;
      }
      console.log(`  Match: ${radioId} → "${dbStation.name}" (${dbStation.id})`);
    } else {
      unmatched.push({ radioId, data });
    }
  }

  // For unmatched, try to get stream URL from station page and create new entries
  console.log(`\nFetching stream URLs for ${unmatched.length} unmatched ORB stations...`);
  for (const { radioId, data } of unmatched) {
    const slug = radioId.replace("zm.", "");
    let streamUrl = "";
    
    try {
      await new Promise(r => setTimeout(r, 200));
      const html = await fetchHtml(`${ORB_BASE}/zm/${slug}/`);
      const btnMatch = /stream="(https?:\/\/[^"]+)"/i.exec(html);
      if (btnMatch) streamUrl = btnMatch[1].trim();
    } catch {}

    const title = String(data.title || "").trim();
    let artist = "", song = title;
    if (title.includes(" - ")) {
      const i = title.indexOf(" - ");
      artist = title.slice(0, i).trim();
      song = title.slice(i + 3).trim();
    }
    const stationName = data.alias ? data.alias.replace("zm.", "").replace(/-/g, " ") : slug.replace(/-/g, " ");
    const displayName = stationName.split(" ").map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");

    console.log(`  New: ${radioId} "${displayName}" stream=${streamUrl ? "yes" : "none"} now="${title}"`);

    if (!DRY_RUN && streamUrl) {
      const stationId = `zm_orb_${slug}`;
      try {
        const existing = await prisma.station.findUnique({ where: { id: stationId } });
        if (!existing) {
          await prisma.station.create({
            data: {
              id: stationId,
              name: displayName,
              country: "Zambia",
              district: "Zambia",
              streamUrl,
              streamFormatHint: "icy",
              sourceIdsJson: JSON.stringify({ onlineradiobox: radioId }),
              isActive: true,
              metadataPriorityEnabled: true,
              fingerprintFallbackEnabled: true,
            },
          });
          // Set CurrentNowPlaying immediately
          await prisma.currentNowPlaying.upsert({
            where: { stationId },
            update: { title: song || title, artist: artist || null, sourceProvider: "onlineradiobox", streamText: title, updatedAt: new Date() },
            create: { stationId, title: song || title || undefined, artist: artist || undefined, sourceProvider: "onlineradiobox", streamText: title || undefined },
          });
          created++;
        } else {
          // Station exists, just update ORB ID and now-playing
          const srcJson = JSON.parse(existing.sourceIdsJson || "{}");
          srcJson.onlineradiobox = radioId;
          await prisma.station.update({ where: { id: stationId }, data: { sourceIdsJson: JSON.stringify(srcJson) } });
          await prisma.currentNowPlaying.upsert({
            where: { stationId },
            update: { title: song || title, artist: artist || null, sourceProvider: "onlineradiobox", streamText: title, updatedAt: new Date() },
            create: { stationId, title: song || title || undefined, artist: artist || undefined, sourceProvider: "onlineradiobox", streamText: title || undefined },
          });
          updated++;
        }
      } catch (e) {
        console.error(`    Error: ${e.message}`);
      }
    }
  }

  console.log(`\n${"=".repeat(60)}`);
  console.log(`Results:`);
  console.log(`  Already connected: ${alreadyConnected.size}`);
  console.log(`  Newly matched to existing DB stations: ${updated}`);
  console.log(`  New stations created: ${created}`);
  console.log(`  Total ORB-connected after this run: ${alreadyConnected.size + updated + created}`);
  
  if (DRY_RUN) console.log("\n[DRY RUN] No changes applied.");
  
  await prisma.$disconnect();
}

main().catch(e => { console.error(e); process.exit(1); });
