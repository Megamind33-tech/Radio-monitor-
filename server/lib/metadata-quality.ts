/**
 * Heuristics for ICY/stream metadata: reject program/slogan/DJ noise and flag text
 * that should not drive catalog alone (prefer fingerprint).
 */

import { classifyMusicContent } from "./music-content-filter.js";

function norm(s: string | null | undefined): string {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function normalizeStationKey(name: string): string {
  return String(name || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

const PROGRAM_PATTERNS =
  /\b(drive time|drive\s*@|morning show|afternoon show|evening show|breakfast show|lunchtime|news\s*@|sports\s*hour|top\s*\d+\s*countdown|chart\s*show|request\s*show|talk\s*show|phone[-\s]?in|listener\s*line|advert(?:isement|ising)?\s|sponsor(?:ed)?\b|dj\s+\w+|with\s+your\s+host|hosted\s+by|live\s+from\s+the\s+studio)\b/i;

const SLOGAN_HINTS =
  /\b(number\s*one|your\s*favorite|feel\s*the|tune\s*in|listen\s*live|streaming\s*24|non[-\s]?stop|best\s*hits|more\s*music|less\s*talk|we\s*play|we\s*are|official\s*station)\b/i;

/** Short all-caps / title-case lines are often slogans, not tracks. */
function looksLikeSloganLine(combined: string): boolean {
  const t = combined.trim();
  if (t.length < 6 || t.length > 72) return false;
  const letters = t.replace(/[^A-Za-z]/g, "");
  if (letters.length < 5) return false;
  const upperRatio = letters.replace(/[^A-Z]/g, "").length / letters.length;
  return upperRatio > 0.65 && !t.includes(" - ");
}

export type MetadataQuality = {
  okForCatalog: boolean;
  forceFingerprint: boolean;
  catalogConfidenceScale: number;
  reasons: string[];
};

export function assessMetadataQuality(
  combinedRaw: string | null | undefined,
  rawTitle: string | null | undefined,
  rawArtist: string | null | undefined,
  stationDisplayName: string | null | undefined,
  tightness: number
): MetadataQuality {
  const reasons: string[] = [];
  const combined = String(combinedRaw ?? "").trim();
  const title = String(rawTitle ?? "").trim();
  const artist = String(rawArtist ?? "").trim();
  const line = combined || [artist, title].filter(Boolean).join(" - ") || title || artist;

  if (!line || line.length < 2 || line === "-" || line === " - " || line === "...") {
    reasons.push("empty_or_junk");
    return { okForCatalog: false, forceFingerprint: true, catalogConfidenceScale: 0, reasons };
  }

  const content = classifyMusicContent(line);
  if (!content.isMusic) {
    reasons.push(`content_filter:${content.reason}`);
    return {
      okForCatalog: false,
      forceFingerprint: true,
      catalogConfidenceScale: Math.min(0.45, 0.35),
      reasons,
    };
  }

  let okForCatalog = true;
  let forceFingerprint = false;
  let catalogConfidenceScale = 1;

  const low = line.toLowerCase();
  if (PROGRAM_PATTERNS.test(line)) {
    reasons.push("program_or_show_text");
    okForCatalog = false;
    forceFingerprint = true;
  }

  if (SLOGAN_HINTS.test(low)) {
    reasons.push("slogan_or_promo");
    okForCatalog = false;
    forceFingerprint = true;
  }

  if (looksLikeSloganLine(line)) {
    reasons.push("slogan_line_shape");
    okForCatalog = false;
    forceFingerprint = true;
  }

  const stKey = normalizeStationKey(stationDisplayName || "");
  const lineKey = normalizeStationKey(line);
  if (stKey.length >= 6 && lineKey.length >= 6 && (lineKey.includes(stKey) || stKey.includes(lineKey))) {
    reasons.push("metadata_echoes_station_name");
    okForCatalog = false;
    forceFingerprint = true;
  }

  if (tightness >= 1) {
    if (/\b(radio|fm|live|on\s*air|streaming)\b/i.test(line) && line.length < 36) {
      reasons.push("tight_branding_short");
      okForCatalog = false;
      forceFingerprint = true;
    }
  }
  if (tightness >= 2) {
    if (/\b(fm|am)\b/i.test(line) && !line.includes(" - ") && line.split(/\s+/).length <= 5) {
      reasons.push("tight_callsign_like");
      catalogConfidenceScale = Math.min(catalogConfidenceScale, 0.85);
      forceFingerprint = true;
    }
  }

  if (!okForCatalog || forceFingerprint) {
    catalogConfidenceScale = Math.min(catalogConfidenceScale, 0.75);
  }

  return {
    okForCatalog,
    forceFingerprint,
    catalogConfidenceScale: Math.max(0.35, Math.min(1, catalogConfidenceScale)),
    reasons,
  };
}
