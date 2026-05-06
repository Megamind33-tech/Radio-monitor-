/**
 * Scrape all Zambian radio stations from OnlineRadioBox and connect them to
 * existing stations in the database by updating sourceIdsJson with the ORB radio ID.
 *
 * Matching strategy:
 * 1. Exact stream URL match (most reliable)
 * 2. Fuzzy name matching (normalized, stripped of "FM", "live", frequencies, etc.)
 *
 * Usage: node scripts/orb_connect_zambia.mjs [--dry-run]
 */
import "dotenv/config";
import { PrismaClient } from "@prisma/client";

const DRY_RUN = process.argv.includes("--dry-run");
const prisma = new PrismaClient();

const UA = "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0 Safari/537.36";
const ORB_BASE = "https://onlineradiobox.com";

const ZM_CITY_SLUGS = [
  "Chikuni", "Chingola", "Chipata", "Choma", "Isoka", "Itezhitezhi",
  "Kabwe", "Kalomo", "Kalulushi", "Kapiri_Mposhi", "Kasama", "Katete",
  "Keembe", "Kitwe", "Luanshya", "Lundazi", "Lunga", "Lusaka", "Mansa",
  "Mazabuka", "Mbala", "Mkushi", "Mongu", "Mpongwe", "Mpulungu",
  "Muchinga", "Mufulira", "Mungwi", "Ndola", "Senga_Hill", "Sesheke",
  "Umezi", "White_Mwandi",
];

async function fetchHtml(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": UA, "Accept-Language": "en-US,en;q=0.9" },
    signal: AbortSignal.timeout(30000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.text();
}

function extractStationPaths(html) {
  const paths = new Set();
  const re = /href="(\/zm\/[^"#?]+)"/g;
  let m;
  while ((m = re.exec(html)) !== null) {
    let p = m[1].replace(/\/+$/, "");
    if (!p || p === "/zm") continue;
    if (p.includes("/genre/")) continue;
    paths.add(p + "/");
  }
  return paths;
}

function extractStationPlayButtons(html) {
  const results = [];
  const re = /class="[^"]*station_play[^"]*"([^>]*)>/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const attrs = m[1];
    const streamMatch = /stream="(https?:\/\/[^"]+)"/i.exec(attrs);
    if (!streamMatch) continue;
    const stream = streamMatch[1].trim();
    const nameMatch = /radioName="([^"]*)"/i.exec(attrs);
    const idMatch = /radioId="([^"]*)"/i.exec(attrs);
    const name = nameMatch ? nameMatch[1].trim() : "";
    const rid = idMatch ? idMatch[1].trim() : "";
    if (stream.startsWith("http")) {
      results.push({ stream, name, radioId: rid });
    }
  }
  return results;
}

function extractStationSlugFromPath(path) {
  const parts = path.replace(/\/+$/, "").split("/");
  return parts[parts.length - 1] || null;
}

function isCitySlug(slug) {
  return ZM_CITY_SLUGS.some((c) => c.toLowerCase() === slug.toLowerCase());
}

function normalizeForMatch(name) {
  return name
    .toLowerCase()
    .replace(/\blive\b/g, "")
    .replace(/\bradio\b/g, "")
    .replace(/\bfm\b/g, "")
    .replace(/\bzambia\b/g, "")
    .replace(/[0-9]+(\.[0-9]+)?(\s*(mhz|fm))?/g, "")
    .replace(/[^a-z]/g, "")
    .trim();
}

function normalizeUrl(url) {
  try {
    const u = new URL(url);
    return u.origin + u.pathname.replace(/\/+$/, "");
  } catch {
    return url.toLowerCase().replace(/\/+$/, "");
  }
}

function similarity(a, b) {
  if (!a || !b) return 0;
  if (a === b) return 1;
  const longer = a.length > b.length ? a : b;
  const shorter = a.length > b.length ? b : a;
  if (longer.length === 0) return 1;
  const editDistance = levenshtein(longer, shorter);
  return (longer.length - editDistance) / longer.length;
}

function levenshtein(a, b) {
  const matrix = [];
  for (let i = 0; i <= b.length; i++) matrix[i] = [i];
  for (let j = 0; j <= a.length; j++) matrix[0][j] = j;
  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      if (b[i - 1] === a[j - 1]) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1,
          matrix[i][j - 1] + 1,
          matrix[i - 1][j] + 1
        );
      }
    }
  }
  return matrix[b.length][a.length];
}

async function discoverAllOrbStations() {
  console.log("Fetching ORB Zambia country page...");
  const countryHtml = await fetchHtml(`${ORB_BASE}/zm/`);
  const allPaths = extractStationPaths(countryHtml);

  // Add city listing paths
  for (const slug of ZM_CITY_SLUGS) {
    allPaths.add(`/zm/${slug}/`);
  }

  // Separate city listing pages from station pages
  const cityPaths = [];
  const stationPaths = new Set();
  for (const p of allPaths) {
    const slug = extractStationSlugFromPath(p);
    if (slug && isCitySlug(slug)) {
      cityPaths.push(p);
    } else {
      stationPaths.add(p);
    }
  }

  // Fetch city pages to discover more station paths
  console.log(`Fetching ${cityPaths.length} city listing pages...`);
  for (const cp of cityPaths) {
    try {
      await new Promise((r) => setTimeout(r, 300));
      const html = await fetchHtml(`${ORB_BASE}${cp}`);
      const morePaths = extractStationPaths(html);
      for (const mp of morePaths) {
        const slug = extractStationSlugFromPath(mp);
        if (slug && !isCitySlug(slug)) {
          stationPaths.add(mp);
        }
      }
    } catch (e) {
      console.warn(`  skip city ${cp}: ${e.message}`);
    }
  }

  console.log(`Discovered ${stationPaths.size} station pages. Fetching details...`);

  // Fetch each station page to get stream URL and radioId
  const orbStations = new Map(); // radioId -> { name, stream, slug, radioId }
  let fetched = 0;

  for (const sp of [...stationPaths].sort()) {
    const slug = extractStationSlugFromPath(sp);
    if (!slug) continue;

    try {
      await new Promise((r) => setTimeout(r, 200));
      const html = await fetchHtml(`${ORB_BASE}${sp}`);
      const buttons = extractStationPlayButtons(html);
      for (const btn of buttons) {
        const rid = btn.radioId || `zm.${slug}`;
        const orbId = rid.includes(".") ? rid : `zm.${rid}`;
        if (!orbStations.has(orbId)) {
          orbStations.set(orbId, {
            name: btn.name || slug,
            stream: btn.stream,
            slug,
            radioId: orbId,
          });
        }
      }
      // If no buttons found but this is a station page, try the slug
      if (buttons.length === 0) {
        // Try scraper2 API directly
        const testId = `zm.${slug}`;
        try {
          const apiRes = await fetch(
            `https://scraper2.onlineradiobox.com/${encodeURIComponent(testId)}?l=0`,
            { headers: { "User-Agent": "ZambiaMonitor/1.0" }, signal: AbortSignal.timeout(5000) }
          );
          if (apiRes.ok) {
            const data = await apiRes.json();
            if (data && (data.title || data.updated)) {
              orbStations.set(testId, {
                name: slug.replace(/-/g, " "),
                stream: "",
                slug,
                radioId: testId,
              });
            }
          }
        } catch {}
      }
      fetched++;
      if (fetched % 10 === 0) console.log(`  ...fetched ${fetched}/${stationPaths.size}`);
    } catch (e) {
      console.warn(`  skip station ${sp}: ${e.message}`);
    }
  }

  console.log(`Found ${orbStations.size} ORB stations with valid IDs.`);
  return [...orbStations.values()];
}

async function matchAndUpdate(orbStations) {
  const dbStations = await prisma.station.findMany({
    where: { country: "Zambia", isActive: true },
    select: { id: true, name: true, sourceIdsJson: true, streamUrl: true, preferredStreamUrl: true },
  });

  // Build lookup maps
  const urlToDbStation = new Map();
  for (const st of dbStations) {
    if (st.streamUrl) urlToDbStation.set(normalizeUrl(st.streamUrl), st);
    if (st.preferredStreamUrl) urlToDbStation.set(normalizeUrl(st.preferredStreamUrl), st);
  }

  const matched = [];
  const unmatched = [];
  const alreadyConnected = [];

  for (const orb of orbStations) {
    // Check if already connected
    const existingWithOrb = dbStations.find((st) => {
      try {
        const j = JSON.parse(st.sourceIdsJson || "{}");
        return j.onlineradiobox === orb.radioId;
      } catch {
        return false;
      }
    });
    if (existingWithOrb) {
      alreadyConnected.push({ orb, dbStation: existingWithOrb });
      continue;
    }

    // Strategy 1: Match by stream URL
    let matchedStation = null;
    if (orb.stream) {
      matchedStation = urlToDbStation.get(normalizeUrl(orb.stream));
    }

    // Strategy 2: Fuzzy name match (strict — require high similarity)
    if (!matchedStation) {
      const orbNorm = normalizeForMatch(orb.name);
      let bestScore = 0;
      let bestStation = null;
      for (const st of dbStations) {
        // Skip stations already having an ORB id
        try {
          const j = JSON.parse(st.sourceIdsJson || "{}");
          if (j.onlineradiobox) continue;
        } catch {}

        const dbNorm = normalizeForMatch(st.name);
        if (!orbNorm || !dbNorm || orbNorm.length < 3 || dbNorm.length < 3) continue;

        // Try exact normalized match first
        if (orbNorm === dbNorm) {
          bestStation = st;
          bestScore = 1;
          break;
        }
        // Substring containment — only if the shorter is at least 70% of the longer
        const shorter = orbNorm.length < dbNorm.length ? orbNorm : dbNorm;
        const longer = orbNorm.length < dbNorm.length ? dbNorm : orbNorm;
        if (shorter.length >= longer.length * 0.7) {
          if (longer.includes(shorter) || shorter.includes(longer)) {
            const score = 0.92;
            if (score > bestScore) {
              bestScore = score;
              bestStation = st;
            }
            continue;
          }
        }
        // Levenshtein similarity — require very high match
        const score = similarity(orbNorm, dbNorm);
        if (score > bestScore && score >= 0.85) {
          bestScore = score;
          bestStation = st;
        }
      }
      if (bestStation && bestScore >= 0.85) {
        matchedStation = bestStation;
      }
    }

    if (matchedStation) {
      matched.push({ orb, dbStation: matchedStation, });
    } else {
      unmatched.push(orb);
    }
  }

  console.log(`\n=== Results ===`);
  console.log(`Already connected: ${alreadyConnected.length}`);
  console.log(`New matches: ${matched.length}`);
  console.log(`Unmatched ORB stations: ${unmatched.length}`);

  if (matched.length > 0) {
    console.log(`\n--- New matches to apply ---`);
    for (const { orb, dbStation } of matched) {
      console.log(`  ORB "${orb.name}" (${orb.radioId}) → DB "${dbStation.name}" (${dbStation.id})`);
    }
  }

  if (unmatched.length > 0) {
    console.log(`\n--- Unmatched ORB stations (no DB counterpart found) ---`);
    for (const orb of unmatched) {
      console.log(`  "${orb.name}" (${orb.radioId}) stream: ${orb.stream || "N/A"}`);
    }
  }

  if (DRY_RUN) {
    console.log("\n[DRY RUN] No changes applied.");
    return { matched: matched.length, updated: 0, created: 0 };
  }

  // Apply updates to matched stations
  let updated = 0;
  for (const { orb, dbStation } of matched) {
    try {
      const existing = JSON.parse(dbStation.sourceIdsJson || "{}");
      existing.onlineradiobox = orb.radioId;
      await prisma.station.update({
        where: { id: dbStation.id },
        data: { sourceIdsJson: JSON.stringify(existing) },
      });
      updated++;
    } catch (e) {
      console.error(`  Failed to update ${dbStation.name}: ${e.message}`);
    }
  }

  // Create new stations for unmatched ORB entries that have a stream URL
  let created = 0;
  for (const orb of unmatched) {
    if (!orb.stream) continue;
    try {
      const stationId = `zm_orb_${orb.slug}`;
      const existing = await prisma.station.findUnique({ where: { id: stationId } });
      if (existing) continue;

      await prisma.station.create({
        data: {
          id: stationId,
          name: orb.name,
          country: "Zambia",
          district: "Zambia",
          streamUrl: orb.stream,
          streamFormatHint: "icy",
          sourceIdsJson: JSON.stringify({ onlineradiobox: orb.radioId }),
          isActive: true,
          metadataPriorityEnabled: true,
          fingerprintFallbackEnabled: true,
        },
      });
      created++;
      console.log(`  Created station: "${orb.name}" (${stationId})`);
    } catch (e) {
      console.error(`  Failed to create ${orb.name}: ${e.message}`);
    }
  }

  console.log(`\nUpdated ${updated} existing stations with ORB IDs.`);
  console.log(`Created ${created} new stations from ORB.`);
  return { matched: matched.length, updated, created };
}

async function main() {
  console.log(DRY_RUN ? "[DRY RUN MODE]" : "[LIVE MODE - will update DB]");
  console.log("=".repeat(60));

  const orbStations = await discoverAllOrbStations();
  const result = await matchAndUpdate(orbStations);

  // Also try direct API probing for unmatched DB stations
  console.log("\n--- Attempting direct ORB API probe for remaining unmatched DB stations ---");
  const dbStations = await prisma.station.findMany({
    where: { country: "Zambia", isActive: true },
    select: { id: true, name: true, sourceIdsJson: true },
  });
  const stillNoOrb = dbStations.filter((st) => {
    try {
      const j = JSON.parse(st.sourceIdsJson || "{}");
      return !j.onlineradiobox;
    } catch {
      return true;
    }
  });

  let probeMatches = 0;
  for (const st of stillNoOrb) {
    // Generate candidate slugs from station name
    const baseName = st.name
      .toLowerCase()
      .replace(/\blive\b/g, "")
      .replace(/\s+fm\s*$/i, "")
      .replace(/\s*[0-9]+(\.[0-9]+)?\s*(fm|mhz)?\s*/g, "")
      .replace(/\s+zambia$/i, "")
      .trim();

    const slugCandidates = [
      baseName.replace(/[^a-z0-9]+/g, ""),
      baseName.replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, ""),
      baseName.split(/\s+/)[0],
    ].filter((s) => s.length >= 3);

    for (const slug of [...new Set(slugCandidates)]) {
      const testId = `zm.${slug}`;
      try {
        await new Promise((r) => setTimeout(r, 250));
        const res = await fetch(
          `https://scraper2.onlineradiobox.com/${encodeURIComponent(testId)}?l=0`,
          { headers: { "User-Agent": "ZambiaMonitor/1.0" }, signal: AbortSignal.timeout(4000) }
        );
        if (!res.ok) continue;
        const data = await res.json();
        if (data && data.updated && data.updated > 0) {
          console.log(`  Probe hit: "${st.name}" → ${testId} (title: ${data.title || "N/A"})`);
          if (!DRY_RUN) {
            const existing = JSON.parse(st.sourceIdsJson || "{}");
            existing.onlineradiobox = testId;
            await prisma.station.update({
              where: { id: st.id },
              data: { sourceIdsJson: JSON.stringify(existing) },
            });
          }
          probeMatches++;
          break;
        }
      } catch {}
    }
  }

  console.log(`\nDirect probe found ${probeMatches} additional matches.`);
  console.log(`\nTotal: ${result.updated + probeMatches} stations connected to ORB.`);

  await prisma.$disconnect();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
