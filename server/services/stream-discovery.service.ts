import axios from "axios";
import { logger } from "../lib/logger.js";
import { classifyStreamUrl } from "../lib/stream-source.js";
import { validateCandidateStreamUrl } from "../lib/stream-url-guard.js";
import { StreamRefreshService } from "./stream-refresh.service.js";

const UA =
  process.env.STREAM_DISCOVERY_UA ||
  process.env.STREAM_REFRESH_UA ||
  "Mozilla/5.0 (X11; Linux x86_64) RadioMonitor/1.0 (stream-discovery)";

const TUNEIN_SEARCH = "https://opml.radiotime.com/Search.ashx";
const TUNEIN_TUNE = "https://opml.radiotime.com/Tune.ashx";

/** Public API mirrors — entire search tries each until one responds (stability). */
const DEFAULT_RADIO_BROWSER_BASES = [
  "https://de1.api.radio-browser.info",
  "https://de2.api.radio-browser.info",
  "https://fi1.api.radio-browser.info",
  "https://nl1.api.radio-browser.info",
  "https://all.api.radio-browser.info",
];

function radioBrowserBases(): string[] {
  const raw = process.env.RADIO_BROWSER_API_BASES?.trim();
  if (raw) {
    return raw
      .split(/[\s,]+/)
      .map((s) => s.replace(/\/$/, ""))
      .filter(Boolean);
  }
  return [...DEFAULT_RADIO_BROWSER_BASES];
}

function countryCodeHint(country: string): string | null {
  const c = String(country || "")
    .trim()
    .toLowerCase();
  if (!c) return null;
  if (c === "zambia" || c === "zm") return "ZM";
  if (c.length === 2 && /^[a-z]{2}$/i.test(c)) return c.toUpperCase();
  return null;
}

function normName(s: string): string {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function nameSimilarity(stationName: string, candidateName: string): number {
  const a = new Set(normName(stationName).split(/\s+/).filter((x) => x.length > 1));
  const b = new Set(normName(candidateName).split(/\s+/).filter((x) => x.length > 1));
  if (a.size === 0 || b.size === 0) return 0;
  let inter = 0;
  for (const x of a) if (b.has(x)) inter++;
  return inter / Math.max(a.size, b.size);
}

export type StreamDiscoveryCandidate = {
  streamUrl: string;
  name?: string;
  source: string;
  server?: string;
  detail?: string;
  tier: ReturnType<typeof classifyStreamUrl>["tier"];
  qualityScore: number;
  nameMatch: number;
};

export type StreamDiscoveryResult = {
  stationId: string;
  queryUsed: string[];
  serversTried: string[];
  errors: string[];
  candidates: StreamDiscoveryCandidate[];
};

type RbStation = {
  name?: string;
  url?: string;
  url_resolved?: string;
  country?: string;
  countrycode?: string;
  votes?: number;
  clickcount?: number;
};

function pushRbRows(
  rows: RbStation[] | null | undefined,
  source: string,
  server: string,
  stationName: string,
  out: Map<string, StreamDiscoveryCandidate>
): void {
  if (!Array.isArray(rows)) return;
  for (const row of rows) {
    const u = String(row.url_resolved || row.url || "").trim();
    if (!u.startsWith("http")) continue;
    const v = validateCandidateStreamUrl(u);
    if (!v.accepted) continue;
    const cls = classifyStreamUrl(v.canonicalUrl);
    const nm = String(row.name || "").trim();
    const nameMatch = nm ? nameSimilarity(stationName, nm) : 0;
    const bonus = Math.min(15, (Number(row.votes) || 0) / 50 + (Number(row.clickcount) || 0) / 200);
    const qualityScore = Math.min(100, Math.round(cls.qualityScore + bonus + nameMatch * 12));
    const key = v.canonicalUrl;
    const prev = out.get(key);
    if (!prev || qualityScore > prev.qualityScore) {
      out.set(key, {
        streamUrl: v.canonicalUrl,
        name: nm || undefined,
        source,
        server,
        detail: row.country || row.countrycode,
        tier: cls.tier,
        qualityScore,
        nameMatch,
      });
    }
  }
}

/** OPML Search.ashx — extract TuneIn station ids from first page of results. */
async function tuneinStationIdsForQuery(query: string): Promise<{ ids: string[]; titles: string[] }> {
  const url = `${TUNEIN_SEARCH}?${new URLSearchParams({ query, types: "station" }).toString()}`;
  const res = await axios.get<string>(url, {
    timeout: 25_000,
    headers: { "User-Agent": UA, Accept: "application/xml,text/xml,*/*" },
    validateStatus: (s) => s === 200,
  });
  const xml = typeof res.data === "string" ? res.data : "";
  const ids: string[] = [];
  const titles: string[] = [];
  const re =
    /<outline[^>]*type="audio"[^>]*>/gi;
  let m: RegExpExecArray | null;
  while ((m = re.exec(xml)) !== null) {
    const block = m[0];
    const urlM = /URL="([^"]+)"/i.exec(block);
    const textM = /text="([^"]*)"/i.exec(block);
    const href = urlM?.[1] || "";
    const idM = /[?&]id=([sp][0-9]+)/i.exec(href);
    if (!idM) continue;
    const sid = idM[1];
    if (!ids.includes(sid)) {
      ids.push(sid);
      titles.push((textM?.[1] || sid).replace(/&amp;/g, "&"));
    }
    if (ids.length >= 12) break;
  }
  return { ids, titles };
}

async function resolveTuneinStreamUrl(stationId: string): Promise<string | null> {
  const tuneUrl = `${TUNEIN_TUNE}?id=${encodeURIComponent(stationId)}`;
  try {
    const res = await axios.get<string>(tuneUrl, {
      timeout: 25_000,
      maxRedirects: 8,
      headers: { "User-Agent": UA },
      validateStatus: (s) => s >= 200 && s < 400,
      responseType: "text",
    });
    const body = typeof res.data === "string" ? res.data.trim() : "";
    const line = body.split(/\r?\n/).find((l) => l.trim().startsWith("http"))?.trim() || "";
    if (line.startsWith("http")) {
      const v = validateCandidateStreamUrl(line);
      return v.accepted ? v.canonicalUrl : null;
    }
  } catch {
    return null;
  }
  return null;
}

/**
 * Multi-server discovery: Radio-Browser mirrors (by country + by name), TuneIn OPML search + resolve,
 * and harvest hints (MyTuner / ORB / Streema). No station website required.
 */
export class StreamDiscoveryService {
  static async discoverForStation(input: {
    id: string;
    name: string;
    country: string;
    streamUrl: string;
    sourceIdsJson: string | null;
  }): Promise<StreamDiscoveryResult> {
    const serversTried: string[] = [];
    const errors: string[] = [];
    const queryUsed: string[] = [];
    const byUrl = new Map<string, StreamDiscoveryCandidate>();

    const bases = radioBrowserBases();
    const cc = countryCodeHint(input.country);
    const nameQ = input.name.trim();
    queryUsed.push(nameQ);
    if (cc) queryUsed.push(`country:${cc}`);

    // --- Radio-Browser: by country on every mirror (merge union) ---
    if (cc) {
      const countryPath = `/json/stations/bycountrycodeexact/${encodeURIComponent(cc)}`;
      await Promise.all(
        bases.map(async (base) => {
          const url = `${base}${countryPath}`;
          serversTried.push(url);
          try {
            const res = await axios.get<RbStation[]>(url, {
              timeout: 22_000,
              headers: { "User-Agent": UA, Accept: "application/json" },
              validateStatus: (s) => s === 200,
            });
            pushRbRows(Array.isArray(res.data) ? res.data : [], "radio_browser_by_country", base, nameQ, byUrl);
          } catch (e) {
            errors.push(`${base} country: ${String(e).slice(0, 100)}`);
          }
        })
      );
    }

    // --- Radio-Browser: by name on **every** mirror (merge union — stability / completeness) ---
    const searchPath = `/json/stations/byname/${encodeURIComponent(nameQ)}?limit=80&hidebroken=true`;
    await Promise.all(
      bases.map(async (base) => {
        const url = `${base}${searchPath}`;
        serversTried.push(url);
        try {
          const res = await axios.get<RbStation[]>(url, {
            timeout: 22_000,
            headers: { "User-Agent": UA, Accept: "application/json" },
            validateStatus: (s) => s === 200,
          });
          pushRbRows(Array.isArray(res.data) ? res.data : [], "radio_browser_by_name", base, nameQ, byUrl);
        } catch (e) {
          errors.push(`${base} name search: ${String(e).slice(0, 100)}`);
        }
      })
    );

    // --- TuneIn: search + resolve first matches ---
    try {
      const queries = Array.from(new Set([nameQ, `${nameQ} ${input.country}`.trim()].filter(Boolean)));
      for (const q of queries) {
        if (!q) continue;
        const { ids, titles } = await tuneinStationIdsForQuery(q);
        serversTried.push(`${TUNEIN_SEARCH}?query=${encodeURIComponent(q)}`);
        for (let i = 0; i < ids.length; i++) {
          const sid = ids[i];
          const title = titles[i] || sid;
          const resolved = await resolveTuneinStreamUrl(sid);
          serversTried.push(`tunein:resolve:${sid}`);
          if (!resolved) continue;
          const v = validateCandidateStreamUrl(resolved);
          if (!v.accepted) continue;
          const cls = classifyStreamUrl(v.canonicalUrl);
          const nameMatch = nameSimilarity(nameQ, title);
          const qualityScore = Math.min(100, Math.round(cls.qualityScore + nameMatch * 15));
          const key = v.canonicalUrl;
          const prev = byUrl.get(key);
          const row: StreamDiscoveryCandidate = {
            streamUrl: v.canonicalUrl,
            name: title,
            source: "tunein_opml",
            detail: sid,
            tier: cls.tier,
            qualityScore,
            nameMatch,
          };
          if (!prev || row.qualityScore > prev.qualityScore) byUrl.set(key, row);
        }
      }
    } catch (e) {
      errors.push(`tunein: ${String(e).slice(0, 160)}`);
    }

    // --- Harvest hints (same servers as stream refresh) ---
    try {
      const hints = await StreamRefreshService.collectDirectUrlsFromHints(input.sourceIdsJson);
      for (const h of hints) {
        const v = validateCandidateStreamUrl(h.streamUrl);
        if (!v.accepted) continue;
        const cls = classifyStreamUrl(v.canonicalUrl);
        const nameMatch = 0.35;
        const qualityScore = Math.min(100, Math.round(cls.qualityScore + 6));
        const key = v.canonicalUrl;
        const prev = byUrl.get(key);
        const row: StreamDiscoveryCandidate = {
          streamUrl: v.canonicalUrl,
          source: `harvest_${h.source}`,
          detail: h.detail,
          tier: cls.tier,
          qualityScore,
          nameMatch,
        };
        if (!prev || row.qualityScore > prev.qualityScore) byUrl.set(key, row);
      }
    } catch (e) {
      errors.push(`harvest_hints: ${String(e).slice(0, 120)}`);
    }

    // Current URL as baseline (so operator can compare)
    const cur = validateCandidateStreamUrl(input.streamUrl);
    if (cur.accepted) {
      const cls = classifyStreamUrl(cur.canonicalUrl);
      if (!byUrl.has(cur.canonicalUrl)) {
        byUrl.set(cur.canonicalUrl, {
          streamUrl: cur.canonicalUrl,
          source: "current_station_row",
          tier: cls.tier,
          qualityScore: cls.qualityScore,
          nameMatch: 1,
        });
      }
    }

    const candidates = Array.from(byUrl.values()).sort((a, b) => {
      if (b.qualityScore !== a.qualityScore) return b.qualityScore - a.qualityScore;
      if (b.nameMatch !== a.nameMatch) return b.nameMatch - a.nameMatch;
      const tierRank = (t: string) =>
        ({ direct: 4, official_cdn: 3, relay: 2, aggregator: 1, unknown: 0 }[t] ?? 0);
      return tierRank(b.tier) - tierRank(a.tier);
    });

    return {
      stationId: input.id,
      queryUsed,
      serversTried: [...new Set(serversTried)].slice(0, 200),
      errors,
      candidates,
    };
  }
}
