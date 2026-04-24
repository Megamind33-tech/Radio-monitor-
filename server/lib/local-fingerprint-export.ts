import type { LocalFingerprint } from "@prisma/client";

export type LibraryExportRow = {
  title: string;
  titleWithoutFeat: string | null;
  artist: string;
  displayArtist: string | null;
  featuredArtists: string[];
  album: string | null;
  durationMs: number | null;
  durationSec: number;
  plays: number;
  label: string | null;
  genre: string | null;
  country: string | null;
  isrcs: string[];
  recordingMbid: string | null;
  acoustidTrackId: string | null;
  source: string;
  confidence: number;
  lastMatchedAt: string;
};

function parseFeaturedJson(s: string | null | undefined): string[] {
  if (!s) return [];
  try {
    const j = JSON.parse(s);
    return Array.isArray(j) ? j.filter((x): x is string => typeof x === "string") : [];
  } catch {
    return [];
  }
}

function parseIsrcs(s: string | null | undefined): string[] {
  if (!s) return [];
  try {
    const j = JSON.parse(s);
    return Array.isArray(j) ? j.filter((x): x is string => typeof x === "string") : [];
  } catch {
    return [];
  }
}

/** Rows suitable for catalog / rights reporting — not raw ICY noise. */
export function isQualityLibraryRow(row: LocalFingerprint): boolean {
  const title = (row.title ?? "").trim();
  const artist = (row.artist ?? "").trim();
  if (title.length < 2 || artist.length < 2) return false;
  const hasId = !!(row.recordingMbid?.trim() || row.acoustidTrackId?.trim());
  const hasAlbumOrLabel = !!(row.releaseTitle?.trim() || row.labelName?.trim());
  const hasDur = (row.durationMs && row.durationMs > 0) || row.durationSec > 0;
  if (hasId && hasDur) return true;
  if (row.confidence >= 0.72 && hasDur) return true;
  if (row.timesMatched >= 2 && hasDur) return true;
  if (hasId && hasAlbumOrLabel) return true;
  return false;
}

export function localFingerprintToExportRow(row: LocalFingerprint): LibraryExportRow {
  const durationMs =
    typeof row.durationMs === "number" && row.durationMs > 0
      ? row.durationMs
      : row.durationSec > 0
        ? row.durationSec * 1000
        : null;
  return {
    title: (row.title ?? "").trim(),
    titleWithoutFeat: row.titleWithoutFeat?.trim() || null,
    artist: (row.artist ?? "").trim(),
    displayArtist: row.displayArtist?.trim() || null,
    featuredArtists: parseFeaturedJson(row.featuredArtistsJson),
    album: row.releaseTitle?.trim() || null,
    durationMs,
    durationSec: row.durationSec,
    plays: row.playCountTotal,
    label: row.labelName?.trim() || null,
    genre: row.genre?.trim() || null,
    country: row.countryCode?.trim() || null,
    isrcs: parseIsrcs(row.isrcsJson),
    recordingMbid: row.recordingMbid?.trim() || null,
    acoustidTrackId: row.acoustidTrackId?.trim() || null,
    source: row.source,
    confidence: row.confidence,
    lastMatchedAt: row.lastMatchedAt.toISOString(),
  };
}

export function exportRowsToCsv(rows: LibraryExportRow[]): string {
  const headers = [
    "title",
    "title_without_feat",
    "artist",
    "display_artist",
    "featured_artists",
    "album",
    "duration_ms",
    "duration_sec",
    "plays",
    "label",
    "genre",
    "country",
    "isrcs",
    "recording_mbid",
    "acoustid_track_id",
    "source",
    "confidence",
    "last_matched_at",
  ];
  const esc = (v: string | number | null) => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  const lines = [headers.join(",")];
  for (const r of rows) {
    lines.push(
      [
        esc(r.title),
        esc(r.titleWithoutFeat),
        esc(r.artist),
        esc(r.displayArtist),
        esc(r.featuredArtists.join("; ")),
        esc(r.album),
        esc(r.durationMs),
        esc(r.durationSec),
        esc(r.plays),
        esc(r.label),
        esc(r.genre),
        esc(r.country),
        esc(r.isrcs.join("; ")),
        esc(r.recordingMbid),
        esc(r.acoustidTrackId),
        esc(r.source),
        esc(Number(r.confidence.toFixed(4))),
        esc(r.lastMatchedAt),
      ].join(",")
    );
  }
  return lines.join("\n") + "\n";
}
