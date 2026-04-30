/**
 * Mass-connect ALL OnlineRadioBox Zambian stations to the database.
 * 
 * Discovers 100+ station slugs via paginated country page,
 * probes the ORB API for each, filters junk/non-music content,
 * and creates/updates stations with clean now-playing data.
 *
 * Usage: node scripts/orb_mass_connect.mjs [--dry-run]
 */
import "dotenv/config";
import { PrismaClient } from "@prisma/client";

const DRY_RUN = process.argv.includes("--dry-run");
const prisma = new PrismaClient();
const UA = "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0 Safari/537.36";
const ORB_BASE = "https://onlineradiobox.com";

const CITY_SLUGS = new Set([
  "chikuni","chingola","chipata","choma","isoka","itezhitezhi","kabwe","kalomo",
  "kalulushi","kapiri_mposhi","kasama","katete","kayambi","keembe","kitwe",
  "luanshya","lundazi","lunga","lusaka","mansa","mazabuka","mbala","mkushi",
  "mongu","mpongwe","mpulungu","muchinga","mufulira","mungwi","ndola",
  "senga_hill","sesheke","umezi","white_mwandi","all","stations","list","genre",
]);

function isJunkMetadata(artist, song, combined) {
  const text = (combined || `${artist} - ${song}`).toLowerCase();
  const titleLow = (song || "").toLowerCase();
  const artistLow = (artist || "").toLowerCase();

  if (/you are listening/i.test(text)) return true;
  if (/now playing on/i.test(text)) return true;
  if (/this is .* (fm|radio)/i.test(text)) return true;
  if (/keeping you entertained/i.test(text)) return true;
  if (/your (next )?play on us/i.test(text)) return true;
  if (/station (beta|id|promo)/i.test(text)) return true;
  if (/^\s*jingle/i.test(titleLow)) return true;
  if (/direct chat/i.test(text)) return true;
  if (/\b(programme|program)\b/i.test(text)) return true;
  if (/\b(interview|discussion|talk ?show)\b/i.test(text)) return true;
  if (/\b(sermon|preaching|evangelism|homily)\b/i.test(text)) return true;
  if (/\b(bible|scripture)\b/i.test(artistLow)) return true;
  if (/^\(?\w*\s*bible\)/i.test(artistLow)) return true;
  if (/chapter/i.test(titleLow) && /bible|luke|matthew|john|genesis|psalm/i.test(text)) return true;
  if (/kufungua anga/i.test(text)) return true;
  if (/day \d+.*lango/i.test(text)) return true;
  if (/\bmix(tape|down)?\s*(vol|episode|\d)/i.test(text)) return true;
  if (/\bdj .* mix\b/i.test(text)) return true;
  if (/\bdrive mix\b/i.test(text)) return true;
  if (/\bgroove top\s*\d/i.test(text)) return true;
  if (/\blistener.*choice\b/i.test(text)) return true;
  if (/\bafrobeat \d{4}\b.*\bmix/i.test(text)) return true;
  if (/y2mate/i.test(artistLow)) return true;
  if (/y2mate\.com/i.test(text) && !artist) return true;
  if (/\(official\s*(hd|video|visualizer|audio)\)/i.test(titleLow)) return true;
  if (/official visualizer/i.test(text)) return true;
  if (/\(\d+k\)\s*$/i.test(titleLow)) return true;
  if (/\b(128|256|320)k\)?\s*$/i.test(titleLow)) return true;
  if (!song && !artist) return true;
  if ((song || "").trim().length < 2 && (artist || "").trim().length < 2) return true;
  if (/^(yes|no|test|live|on air)$/i.test((song || "").trim())) return true;
  if (/confession final/i.test(text)) return true;

  // Motivational / sermon videos
  if (/\b(motivational|inspirational)\b.*\bvideo\b/i.test(text)) return true;
  if (/discovering.*destiny/i.test(text)) return true;
  if (/\bapostle\b/i.test(artistLow) && /\b(glory|power|faith)\b/i.test(titleLow)) return true;
  if (/\b(fulfilling|destiny|powerful)\b/i.test(titleLow) && titleLow.length > 40) return true;

  // Station promos / jingles
  if (/\bradio playing\b/i.test(text)) return true;
  if (/only .* artists/i.test(artistLow)) return true;
  if (/^music\s*\d*\.\w{3,4}$/i.test(titleLow)) return true;

  // BBC/news programmes
  if (/bbc.*service/i.test(text)) return true;
  if (/world questions/i.test(text)) return true;

  // File extensions in title
  if (/\.(mp3|m4a|wav|mpeg|ogg)\s*$/i.test(titleLow)) return true;
  if (/\(\d+k\)\.(m4a|mp3)/i.test(titleLow)) return true;

  return false;
}

function parseTitle(rawTitle) {
  const title = String(rawTitle || "").trim();
  if (title.includes(" - ")) {
    const i = title.indexOf(" - ");
    return { artist: title.slice(0, i).trim(), song: title.slice(i + 3).trim(), combined: title };
  }
  return { artist: "", song: title, combined: title };
}

async function fetchHtml(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": UA, "Accept-Language": "en-US,en;q=0.9" },
    signal: AbortSignal.timeout(20000),
  });
  if (!res.ok) return "";
  return res.text();
}

async function probeOrb(radioId) {
  try {
    const res = await fetch(
      `https://scraper2.onlineradiobox.com/${encodeURIComponent(radioId)}?l=0`,
      { headers: { "User-Agent": "ZambiaMonitor/1.0" }, signal: AbortSignal.timeout(5000) }
    );
    if (!res.ok) return null;
    const data = await res.json();
    if (data && data.updated && data.updated > 0) return data;
    return null;
  } catch { return null; }
}

async function getStreamUrl(slug) {
  try {
    const html = await fetchHtml(`${ORB_BASE}/zm/${slug}/`);
    const m = /stream="(https?:\/\/[^"]+)"/i.exec(html);
    return m ? m[1].trim() : "";
  } catch { return ""; }
}

async function discoverAllSlugs() {
  const slugs = new Set();
  
  // Paginate country page (up to 10 pages)
  for (let page = 1; page <= 10; page++) {
    const url = page === 1 ? `${ORB_BASE}/zm/` : `${ORB_BASE}/zm/?p=${page}`;
    try {
      const html = await fetchHtml(url);
      const before = slugs.size;
      for (const m of html.matchAll(/href="\/zm\/([a-z0-9][a-z0-9_-]*[a-z0-9])\/?"/gi)) {
        const s = m[1].toLowerCase();
        if (!CITY_SLUGS.has(s)) slugs.add(s);
      }
      if (slugs.size === before && page > 1) break;
    } catch { break; }
  }

  // Also try city pages for extra coverage
  for (const city of CITY_SLUGS) {
    if (["all","stations","list","genre"].includes(city)) continue;
    try {
      await new Promise(r => setTimeout(r, 100));
      const html = await fetchHtml(`${ORB_BASE}/zm/${city}/`);
      for (const m of html.matchAll(/href="\/zm\/([a-z0-9][a-z0-9_-]*[a-z0-9])\/?"/gi)) {
        const s = m[1].toLowerCase();
        if (!CITY_SLUGS.has(s)) slugs.add(s);
      }
    } catch {}
  }

  // Additional common Zambian station slug patterns to try
  const extraSlugs = [
    "znbc1","znbc2","znbc4","znbcradio1","znbcradio2","znbcradio4",
    "hotfm","hotfmzambia","qfm","qfmzambia","kfm","kfmzambia",
    "phoenixfm","phoenixradio","breezefm","breezeradio",
    "sunfm","sunradio","safm","powerfmkabwe","power879",
    "joy","joyfm","joyradio","royalfm","royal","diamondfm","diamond",
    "yangeni","unza","unzaradio","zmfm","zambezi","zambezifm",
    "flyfm","fly","radiozambia","bangweulu","mpika","solwezi",
    "mayfm","may","kasempa","kafue","kafuefm","livingstone",
    "livingstonefm","mazabukafm","mazabuka","chongwe","luapula",
    "northernfm","northern","southernfm","southern","easternfm",
    "westprovinceradio","copperbelt","cbfm","itezhi","komboni",
    "kombonifm","komboniradio","yatsani","yatrfm","brt","brtfm",
    "radiocbfm","cbradio","radiomulungushi","mulungushi","mkondo",
    "radiolumwana","lumwana","cosmopolitan","cosmo","cosmofm",
    "galaxyfm","galaxy","vibes","vibesfm","vibeszambia",
    "africafm","africaradio","tropics","tropicsfm","chete","chetefm",
    "flash","flashfm","icengelo","unzafm","goodmorningzambia",
    "mpangwe","roanfm","blazefm","blaze","yfm","yarfm",
    "zambianmusic","zmradio","kwacha","kwachafm","kwacharadio",
    "faithradio","faith","gvtv","ichengelo","passionradio","passion",
    "zedgospel","zgfm","goodshepherd","shepherd",
  ];
  for (const s of extraSlugs) {
    slugs.add(s);
  }

  return slugs;
}

async function main() {
  console.log(DRY_RUN ? "[DRY RUN]" : "[LIVE MODE]");
  console.log("=".repeat(60));

  // 1. Discover all slugs
  console.log("Discovering ORB Zambia station slugs (paginated)...");
  const slugs = await discoverAllSlugs();
  console.log(`Found ${slugs.size} station slugs.`);

  // 2. Get existing DB state
  const dbStations = await prisma.station.findMany({
    where: { country: "Zambia" },
    select: { id: true, name: true, sourceIdsJson: true, streamUrl: true, isActive: true },
  });
  const existingOrbIds = new Set();
  const stationByOrbId = new Map();
  for (const st of dbStations) {
    try {
      const j = JSON.parse(st.sourceIdsJson || "{}");
      if (j.onlineradiobox) {
        existingOrbIds.add(j.onlineradiobox);
        stationByOrbId.set(j.onlineradiobox, st);
      }
    } catch {}
  }

  // 3. Probe all slugs
  const allRadioIds = [...slugs].map(s => `zm.${s}`);
  console.log(`\nProbing ${allRadioIds.length} radio IDs (${existingOrbIds.size} already in DB)...`);
  
  let probed = 0;
  let activeClean = 0;
  let activeJunk = 0;
  let connected = 0;
  let created = 0;

  for (const radioId of allRadioIds.sort()) {
    await new Promise(r => setTimeout(r, 120));
    const data = await probeOrb(radioId);
    probed++;
    if (probed % 20 === 0) console.log(`  ...probed ${probed}/${allRadioIds.length}`);
    
    if (!data) continue;

    const { artist, song, combined } = parseTitle(data.title);
    const junk = isJunkMetadata(artist, song, combined);
    
    if (junk) {
      activeJunk++;
      continue;
    }
    
    activeClean++;
    const slug = radioId.replace("zm.", "");

    // Already in DB?
    if (existingOrbIds.has(radioId)) {
      // Update CurrentNowPlaying for existing station
      const st = stationByOrbId.get(radioId);
      if (st && !DRY_RUN) {
        await prisma.currentNowPlaying.upsert({
          where: { stationId: st.id },
          update: { title: song || combined, artist: artist || null, sourceProvider: "onlineradiobox", streamText: combined, updatedAt: new Date() },
          create: { stationId: st.id, title: song || combined || undefined, artist: artist || undefined, sourceProvider: "onlineradiobox", streamText: combined || undefined },
        });
      }
      connected++;
      console.log(`  ✓ ${radioId} (existing) → "${combined}"`);
      continue;
    }

    // New station — get stream URL and create
    const streamUrl = await getStreamUrl(slug);
    const displayName = slug.replace(/([0-9]+)/g, " $1").replace(/([a-z])([A-Z])/g, "$1 $2").split(/[-_]/).map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(" ").trim();

    console.log(`  + ${radioId} "${displayName}" stream=${streamUrl ? "yes" : "none"} → "${combined}"`);

    if (!DRY_RUN) {
      const stationId = `zm_orb_${slug}`;
      try {
        const existing = await prisma.station.findUnique({ where: { id: stationId } });
        if (!existing) {
          if (!streamUrl) continue;
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
          created++;
        } else {
          // Update ORB ID if missing
          const srcJson = JSON.parse(existing.sourceIdsJson || "{}");
          if (!srcJson.onlineradiobox) {
            srcJson.onlineradiobox = radioId;
            await prisma.station.update({ where: { id: stationId }, data: { sourceIdsJson: JSON.stringify(srcJson), isActive: true } });
          }
          connected++;
        }
        // Set CurrentNowPlaying
        await prisma.currentNowPlaying.upsert({
          where: { stationId },
          update: { title: song || combined, artist: artist || null, sourceProvider: "onlineradiobox", streamText: combined, updatedAt: new Date() },
          create: { stationId, title: song || combined || undefined, artist: artist || undefined, sourceProvider: "onlineradiobox", streamText: combined || undefined },
        });
      } catch (e) {
        console.error(`    Error: ${e.message}`);
      }
    }
  }

  // Also probe DB stations that don't have ORB yet (by generating slugs from names)
  console.log("\nProbing unconnected DB stations by name...");
  const noOrbStations = dbStations.filter(st => {
    try { return !JSON.parse(st.sourceIdsJson || "{}").onlineradiobox && st.isActive; } catch { return st.isActive; }
  });

  for (const st of noOrbStations) {
    const baseName = st.name.toLowerCase()
      .replace(/\blive\b/g, "").replace(/\s*fm\s*/g, "").replace(/\s*radio\s*/g, "")
      .replace(/[0-9]+(\.[0-9]+)?/g, "").replace(/\s*mhz\s*/g, "")
      .replace(/\bzambia\b/g, "").trim();
    
    const candidates = new Set();
    const alpha = baseName.replace(/[^a-z]/g, "");
    if (alpha.length >= 3) candidates.add(`zm.${alpha}`);
    const first = baseName.split(/\s+/)[0].replace(/[^a-z0-9]/g, "");
    if (first.length >= 3) candidates.add(`zm.${first}`);

    for (const radioId of candidates) {
      if (existingOrbIds.has(radioId)) continue;
      await new Promise(r => setTimeout(r, 120));
      const data = await probeOrb(radioId);
      if (!data) continue;

      const { artist, song, combined } = parseTitle(data.title);
      if (isJunkMetadata(artist, song, combined)) continue;

      console.log(`  ✓ "${st.name}" → ${radioId} → "${combined}"`);
      activeClean++;
      connected++;
      existingOrbIds.add(radioId);

      if (!DRY_RUN) {
        const srcJson = JSON.parse(st.sourceIdsJson || "{}");
        srcJson.onlineradiobox = radioId;
        await prisma.station.update({ where: { id: st.id }, data: { sourceIdsJson: JSON.stringify(srcJson) } });
        await prisma.currentNowPlaying.upsert({
          where: { stationId: st.id },
          update: { title: song || combined, artist: artist || null, sourceProvider: "onlineradiobox", streamText: combined, updatedAt: new Date() },
          create: { stationId: st.id, title: song || combined || undefined, artist: artist || undefined, sourceProvider: "onlineradiobox", streamText: combined || undefined },
        });
      }
      break;
    }
  }

  console.log(`\n${"=".repeat(60)}`);
  console.log(`Results:`);
  console.log(`  Total slugs discovered: ${slugs.size}`);
  console.log(`  Active with CLEAN music: ${activeClean}`);
  console.log(`  Active but JUNK (filtered): ${activeJunk}`);
  console.log(`  Existing stations updated: ${connected}`);
  console.log(`  New stations created: ${created}`);
  console.log(`  Total ORB with now-playing: ${activeClean}`);
  if (DRY_RUN) console.log("\n[DRY RUN] No changes applied.");

  await prisma.$disconnect();
}

main().catch(e => { console.error(e); process.exit(1); });
